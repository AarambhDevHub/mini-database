use crate::utils::error::{DatabaseError, Result};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};
use tracing::{debug, warn};

/// Types of locks that can be acquired
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockType {
    Shared,             // Read lock
    Exclusive,          // Write lock
    IntentionShared,    // Intention to acquire shared locks on children
    IntentionExclusive, // Intention to acquire exclusive locks on children
}

/// Lock information
#[derive(Debug, Clone)]
pub struct Lock {
    pub lock_type: LockType,
    pub transaction_id: String,
    pub resource_id: String,
    pub acquired_at: chrono::DateTime<chrono::Utc>,
}

/// Manages locks for transaction isolation
pub struct LockManager {
    /// Resource locks: resource_id -> locks
    locks: DashMap<String, Vec<Lock>>,

    /// Transaction locks: transaction_id -> resource_ids
    transaction_locks: DashMap<String, HashSet<String>>,

    /// Wait-for graph for deadlock detection
    wait_for_graph: Arc<RwLock<HashMap<String, HashSet<String>>>>,

    /// Lock timeout
    lock_timeout: Duration,
}

impl LockManager {
    /// Create a new lock manager
    pub fn new() -> Self {
        Self {
            locks: DashMap::new(),
            transaction_locks: DashMap::new(),
            wait_for_graph: Arc::new(RwLock::new(HashMap::new())),
            lock_timeout: Duration::from_secs(10), // 10 second timeout
        }
    }

    /// Create lock manager with custom timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        let mut manager = Self::new();
        manager.lock_timeout = timeout;
        manager
    }

    /// Acquire a lock on a resource
    pub async fn acquire_lock(
        &self,
        transaction_id: String,
        resource_id: String,
        lock_type: LockType,
    ) -> Result<()> {
        debug!(
            "Transaction {} requesting {:?} lock on resource {}",
            transaction_id, lock_type, resource_id
        );

        // Try to acquire lock with timeout
        match timeout(
            self.lock_timeout,
            self.try_acquire_lock(transaction_id.clone(), resource_id.clone(), lock_type),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                warn!(
                    "Lock timeout for transaction {} on resource {}",
                    transaction_id, resource_id
                );
                Err(DatabaseError::Transaction(format!(
                    "Lock timeout on resource {}",
                    resource_id
                )))
            }
        }
    }

    /// Try to acquire lock (internal implementation)
    async fn try_acquire_lock(
        &self,
        transaction_id: String,
        resource_id: String,
        lock_type: LockType,
    ) -> Result<()> {
        loop {
            // Check if lock can be granted
            if self
                .can_grant_lock(&transaction_id, &resource_id, lock_type)
                .await?
            {
                // Grant the lock
                let lock = Lock {
                    lock_type,
                    transaction_id: transaction_id.clone(),
                    resource_id: resource_id.clone(),
                    acquired_at: chrono::Utc::now(),
                };

                // Add to resource locks
                self.locks
                    .entry(resource_id.clone())
                    .or_insert_with(Vec::new)
                    .push(lock);

                // Add to transaction locks
                self.transaction_locks
                    .entry(transaction_id.clone())
                    .or_insert_with(HashSet::new)
                    .insert(resource_id.clone());

                debug!(
                    "Granted {:?} lock on resource {} to transaction {}",
                    lock_type, resource_id, transaction_id
                );

                return Ok(());
            }

            // Check for deadlock
            if self
                .would_cause_deadlock(&transaction_id, &resource_id)
                .await?
            {
                return Err(DatabaseError::Transaction(format!(
                    "Deadlock detected for transaction {}",
                    transaction_id
                )));
            }

            // Add to wait-for graph
            self.add_to_wait_for_graph(&transaction_id, &resource_id)
                .await;

            // Wait a bit and retry
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Check if lock can be granted
    async fn can_grant_lock(
        &self,
        transaction_id: &str,
        resource_id: &str,
        requested_type: LockType,
    ) -> Result<bool> {
        if let Some(existing_locks) = self.locks.get(resource_id) {
            for lock in existing_locks.iter() {
                // Skip locks held by the same transaction
                if lock.transaction_id == transaction_id {
                    continue;
                }

                // Check compatibility
                if !self.are_locks_compatible(requested_type, lock.lock_type) {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Check if two lock types are compatible
    fn are_locks_compatible(&self, lock1: LockType, lock2: LockType) -> bool {
        match (lock1, lock2) {
            // Shared locks are compatible with shared and intention shared
            (LockType::Shared, LockType::Shared) => true,
            (LockType::Shared, LockType::IntentionShared) => true,

            // Shared locks are NOT compatible with exclusive or intention exclusive
            (LockType::Shared, LockType::Exclusive) => false,
            (LockType::Shared, LockType::IntentionExclusive) => false,

            // Exclusive locks are NOT compatible with anything
            (LockType::Exclusive, LockType::Shared) => false,
            (LockType::Exclusive, LockType::Exclusive) => false,
            (LockType::Exclusive, LockType::IntentionShared) => false,
            (LockType::Exclusive, LockType::IntentionExclusive) => false,

            // Intention Shared locks are compatible with shared and other intention locks
            (LockType::IntentionShared, LockType::Shared) => true,
            (LockType::IntentionShared, LockType::IntentionShared) => true,
            (LockType::IntentionShared, LockType::IntentionExclusive) => true,

            // Intention Shared is NOT compatible with exclusive
            (LockType::IntentionShared, LockType::Exclusive) => false,

            // Intention Exclusive locks are compatible with intention locks but not shared/exclusive
            (LockType::IntentionExclusive, LockType::IntentionShared) => true,
            (LockType::IntentionExclusive, LockType::IntentionExclusive) => true,

            // Intention Exclusive is NOT compatible with shared or exclusive
            (LockType::IntentionExclusive, LockType::Shared) => false,
            (LockType::IntentionExclusive, LockType::Exclusive) => false,
        }
    }

    /// Check if granting lock would cause deadlock
    async fn would_cause_deadlock(&self, transaction_id: &str, resource_id: &str) -> Result<bool> {
        // Get transactions currently holding locks on this resource
        if let Some(locks) = self.locks.get(resource_id) {
            let wait_graph = self.wait_for_graph.read().await;

            for lock in locks.iter() {
                if lock.transaction_id != transaction_id {
                    // Check if there's a cycle in wait-for graph
                    if self.has_cycle_to(&wait_graph, &lock.transaction_id, transaction_id) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Check for cycle in wait-for graph
    fn has_cycle_to(&self, graph: &HashMap<String, HashSet<String>>, from: &str, to: &str) -> bool {
        let mut visited = HashSet::new();
        let mut stack = vec![from.to_string()];

        while let Some(current) = stack.pop() {
            if current == to {
                return true;
            }

            if visited.contains(&current) {
                continue;
            }

            visited.insert(current.clone());

            if let Some(neighbors) = graph.get(&current) {
                for neighbor in neighbors {
                    if !visited.contains(neighbor) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }

        false
    }

    /// Add edge to wait-for graph
    async fn add_to_wait_for_graph(&self, waiter: &str, resource_id: &str) {
        let mut graph = self.wait_for_graph.write().await;

        if let Some(locks) = self.locks.get(resource_id) {
            for lock in locks.iter() {
                if lock.transaction_id != waiter {
                    graph
                        .entry(waiter.to_string())
                        .or_insert_with(HashSet::new)
                        .insert(lock.transaction_id.clone());
                }
            }
        }
    }

    /// Release all locks held by a transaction
    pub async fn release_transaction_locks(&self, transaction_id: &str) -> Result<()> {
        debug!("Releasing all locks for transaction {}", transaction_id);

        if let Some((_, resource_ids)) = self.transaction_locks.remove(transaction_id) {
            for resource_id in resource_ids {
                if let Some(mut locks) = self.locks.get_mut(&resource_id) {
                    locks.retain(|lock| lock.transaction_id != transaction_id);

                    // Remove resource entry if no locks remain
                    if locks.is_empty() {
                        drop(locks); // Release the mutable reference
                        self.locks.remove(&resource_id);
                    }
                }
            }
        }

        // Remove from wait-for graph
        let mut graph = self.wait_for_graph.write().await;
        graph.remove(transaction_id);

        // Remove edges pointing to this transaction
        for (_, waiters) in graph.iter_mut() {
            waiters.remove(transaction_id);
        }

        debug!("Released all locks for transaction {}", transaction_id);
        Ok(())
    }

    /// Get locks held by a transaction
    pub fn get_transaction_locks(&self, transaction_id: &str) -> Vec<Lock> {
        let mut transaction_locks = Vec::new();

        for entry in self.locks.iter() {
            let _resource_id = entry.key();
            let locks = entry.value();

            for lock in locks.iter() {
                if lock.transaction_id == transaction_id {
                    transaction_locks.push(lock.clone());
                }
            }
        }

        transaction_locks
    }

    /// Get all locks on a resource
    pub fn get_resource_locks(&self, resource_id: &str) -> Vec<Lock> {
        self.locks
            .get(resource_id)
            .map(|locks| locks.clone())
            .unwrap_or_default()
    }

    /// Get lock manager statistics
    pub async fn get_statistics(&self) -> LockManagerStats {
        let total_locks: usize = self.locks.iter().map(|entry| entry.value().len()).sum();

        let active_transactions = self.transaction_locks.len();

        let wait_graph = self.wait_for_graph.read().await;
        let waiting_transactions = wait_graph.len();

        LockManagerStats {
            total_locks,
            active_transactions,
            waiting_transactions,
            resources_locked: self.locks.len(),
        }
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Lock manager statistics
#[derive(Debug, Clone)]
pub struct LockManagerStats {
    pub total_locks: usize,
    pub active_transactions: usize,
    pub waiting_transactions: usize,
    pub resources_locked: usize,
}
