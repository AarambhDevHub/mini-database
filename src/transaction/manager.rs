use crate::client::DatabaseClient;
use crate::transaction::{
    LockManager, LockType, LogRecordType, OperationType, ResourceType, Transaction,
    TransactionIsolationLevel, WriteAheadLog, WriteOperation,
};
use crate::types::{Edge, Node};
use crate::utils::error::{DatabaseError, Result};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

use super::TransactionManagerConfig;

/// Transaction Manager implementing ACID properties
pub struct TransactionManager {
    /// Database client for operations
    client: DatabaseClient,

    /// Active transactions
    active_transactions: DashMap<String, Transaction>,

    /// Lock manager for isolation
    lock_manager: Arc<LockManager>,

    /// Write-ahead log for durability
    wal: Arc<WriteAheadLog>,

    /// Transaction cleanup task handle
    cleanup_task: Arc<tokio::task::JoinHandle<()>>,

    /// Optional checkpoint task handle (wrapped in Arc)
    checkpoint_task: Option<Arc<tokio::task::JoinHandle<()>>>,

    /// Transaction manager configuration
    config: TransactionManagerConfig,

    /// Transaction statistics
    statistics: Arc<RwLock<TransactionManagerStatistics>>,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub async fn new(client: DatabaseClient, wal_path: PathBuf, force_sync: bool) -> Result<Self> {
        let config = TransactionManagerConfig {
            wal_path,
            force_sync,
            ..Default::default()
        };

        Self::with_config(client, config).await
    }

    /// Create transaction manager with custom configuration
    pub async fn with_config(
        client: DatabaseClient,
        config: TransactionManagerConfig,
    ) -> Result<Self> {
        info!("Creating TransactionManager with custom config");

        let lock_manager = Arc::new(LockManager::with_timeout(config.lock_timeout));
        let wal = Arc::new(WriteAheadLog::new(config.wal_path.clone(), config.force_sync).await?);

        // Initialize statistics
        let statistics = Arc::new(RwLock::new(TransactionManagerStatistics::new()));

        // Start cleanup task with custom interval
        let cleanup_task = Arc::new({
            let active_transactions = DashMap::new();
            let lock_manager_clone = lock_manager.clone();
            let wal_clone = wal.clone();
            let cleanup_interval = config.cleanup_interval;

            tokio::spawn(async move {
                let mut cleanup_timer = interval(cleanup_interval);
                loop {
                    cleanup_timer.tick().await;
                    Self::cleanup_expired_transactions(
                        &active_transactions,
                        &lock_manager_clone,
                        &wal_clone,
                    )
                    .await;
                }
            })
        });

        // Start checkpoint task if configured
        let checkpoint_task = if config.checkpoint_interval > Duration::from_secs(0) {
            let wal_clone = wal.clone();
            let statistics_clone = statistics.clone();
            let checkpoint_interval = config.checkpoint_interval;

            Some(Arc::new(tokio::spawn(async move {
                let mut checkpoint_timer = interval(checkpoint_interval);
                loop {
                    checkpoint_timer.tick().await;
                    if let Err(e) = Self::perform_checkpoint(&wal_clone).await {
                        error!("Checkpoint failed: {}", e);
                    } else {
                        // Update statistics
                        let mut stats = statistics_clone.write().await;
                        stats.checkpoints_performed += 1;
                    }
                }
            })))
        } else {
            None
        };

        let manager = Self {
            client,
            active_transactions: DashMap::new(),
            lock_manager,
            wal,
            cleanup_task,
            checkpoint_task,
            config,
            statistics,
        };

        // Recover from log on startup
        manager.recover().await?;

        info!("TransactionManager created with custom configuration");
        Ok(manager)
    }

    /// Begin a new transaction
    pub async fn begin_transaction(
        &self,
        isolation_level: TransactionIsolationLevel,
    ) -> Result<String> {
        let transaction = Transaction::new(isolation_level);
        let transaction_id = transaction.id.clone();

        // Log transaction begin
        self.wal.log_begin(transaction_id.clone()).await?;

        // Add to active transactions
        self.active_transactions
            .insert(transaction_id.clone(), transaction);

        info!(
            "Started transaction {} with isolation {:?}",
            transaction_id, isolation_level
        );
        Ok(transaction_id)
    }

    /// Begin transaction with timeout
    pub async fn begin_transaction_with_timeout(
        &self,
        isolation_level: TransactionIsolationLevel,
        timeout_seconds: u64,
    ) -> Result<String> {
        let transaction = Transaction::with_timeout(isolation_level, timeout_seconds);
        let transaction_id = transaction.id.clone();

        // Log transaction begin
        self.wal.log_begin(transaction_id.clone()).await?;

        // Add to active transactions
        self.active_transactions
            .insert(transaction_id.clone(), transaction);

        info!(
            "Started transaction {} with isolation {:?} and timeout {}s",
            transaction_id, isolation_level, timeout_seconds
        );
        Ok(transaction_id)
    }

    /// Commit a transaction
    pub async fn commit_transaction(&self, transaction_id: &str) -> Result<()> {
        debug!("Committing transaction {}", transaction_id);

        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return self
                .abort_transaction_internal(transaction_id, "Transaction expired")
                .await;
        }

        // Prepare phase
        transaction.prepare()?;

        // Log commit
        self.wal.log_commit(transaction_id.to_string()).await?;

        // Force sync WAL for durability
        self.wal.sync().await?;

        // Apply all writes
        for write_op in transaction.write_set.values() {
            self.apply_write_operation(write_op).await?;
        }

        // Commit transaction
        transaction.commit()?;

        // Update transaction state
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        // Release locks
        self.lock_manager
            .release_transaction_locks(transaction_id)
            .await?;

        // Remove from active transactions
        self.active_transactions.remove(transaction_id);

        info!("Committed transaction {}", transaction_id);
        Ok(())
    }

    /// Abort a transaction
    pub async fn abort_transaction(&self, transaction_id: &str) -> Result<()> {
        self.abort_transaction_internal(transaction_id, "User requested abort")
            .await
    }

    /// Internal abort implementation
    async fn abort_transaction_internal(&self, transaction_id: &str, reason: &str) -> Result<()> {
        debug!("Aborting transaction {}: {}", transaction_id, reason);

        if let Some((_, mut transaction)) = self.active_transactions.remove(transaction_id) {
            // Log abort
            self.wal.log_abort(transaction_id.to_string()).await?;

            // Rollback all changes (they weren't applied yet since we use WAL)
            transaction.abort()?;

            // Release locks
            self.lock_manager
                .release_transaction_locks(transaction_id)
                .await?;

            warn!("Aborted transaction {}: {}", transaction_id, reason);
            Ok(())
        } else {
            Err(DatabaseError::Transaction(format!(
                "Transaction not found: {}",
                transaction_id
            )))
        }
    }

    /// Create a node within a transaction
    pub async fn create_node_tx(&self, transaction_id: &str, node: Node) -> Result<String> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        // Acquire exclusive lock on the node
        let resource_id = format!("node:{}", node.id);
        self.lock_manager
            .acquire_lock(
                transaction_id.to_string(),
                resource_id.clone(),
                LockType::Exclusive,
            )
            .await?;

        // Serialize node for logging
        let node_data = bincode::serialize(&node).map_err(|e| {
            DatabaseError::Serialization(format!("Failed to serialize node: {}", e))
        })?;

        // Create write operation
        let write_op = WriteOperation {
            operation_type: OperationType::Create,
            resource_type: ResourceType::Node,
            resource_id: resource_id.clone(),
            old_value: None,
            new_value: Some(node_data),
            timestamp: chrono::Utc::now(),
        };

        // Log the write operation
        let prev_lsn = transaction
            .write_set
            .values()
            .map(|op| op.timestamp.timestamp_micros() as u64)
            .max();

        self.wal
            .log_write(transaction_id.to_string(), write_op.clone(), prev_lsn)
            .await?;

        // Add to transaction write set
        transaction.add_write(
            OperationType::Create,
            ResourceType::Node,
            resource_id,
            None,
            Some(bincode::serialize(&node).unwrap()),
        );

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!("Added node creation to transaction {}", transaction_id);
        Ok(node.id.clone())
    }

    /// Get a node within a transaction
    pub async fn get_node_tx(&self, transaction_id: &str, node_id: &str) -> Result<Option<Node>> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("node:{}", node_id);

        // Check isolation level and acquire appropriate lock
        let lock_type = match transaction.isolation_level {
            TransactionIsolationLevel::ReadUncommitted => {
                // No locking needed for read uncommitted
                LockType::IntentionShared
            }
            _ => LockType::Shared,
        };

        if transaction.isolation_level != TransactionIsolationLevel::ReadUncommitted {
            self.lock_manager
                .acquire_lock(transaction_id.to_string(), resource_id.clone(), lock_type)
                .await?;
        }

        // Check if node is in transaction's write set first
        if let Some(write_op) = transaction.write_set.get(&resource_id) {
            match write_op.operation_type {
                OperationType::Delete => return Ok(None),
                OperationType::Create | OperationType::Update => {
                    if let Some(data) = &write_op.new_value {
                        let node = bincode::deserialize(data).map_err(|e| {
                            DatabaseError::Serialization(format!(
                                "Failed to deserialize node: {}",
                                e
                            ))
                        })?;
                        return Ok(Some(node));
                    }
                }
            }
        }

        // Add to read set
        transaction.add_read(resource_id.clone());

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        // Get from database
        let node = self.client.get_node(node_id).await?;

        debug!("Read node {} in transaction {}", node_id, transaction_id);
        Ok(node)
    }

    /// Update a node within a transaction
    pub async fn update_node_tx(&self, transaction_id: &str, node: Node) -> Result<bool> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("node:{}", node.id);

        // Acquire exclusive lock
        self.lock_manager
            .acquire_lock(
                transaction_id.to_string(),
                resource_id.clone(),
                LockType::Exclusive,
            )
            .await?;

        // Get current node for rollback
        let old_node = self.client.get_node(&node.id).await?;
        let old_data = old_node.as_ref().map(|n| bincode::serialize(n).unwrap());

        // Serialize new node
        let new_data = bincode::serialize(&node).map_err(|e| {
            DatabaseError::Serialization(format!("Failed to serialize node: {}", e))
        })?;

        // Create write operation
        let write_op = WriteOperation {
            operation_type: OperationType::Update,
            resource_type: ResourceType::Node,
            resource_id: resource_id.clone(),
            old_value: old_data,
            new_value: Some(new_data.clone()),
            timestamp: chrono::Utc::now(),
        };

        // Log the write operation
        let prev_lsn = transaction
            .write_set
            .values()
            .map(|op| op.timestamp.timestamp_micros() as u64)
            .max();

        self.wal
            .log_write(transaction_id.to_string(), write_op.clone(), prev_lsn)
            .await?;

        // Add to transaction write set
        transaction.add_write(
            OperationType::Update,
            ResourceType::Node,
            resource_id,
            write_op.old_value,
            Some(new_data),
        );

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!("Added node update to transaction {}", transaction_id);
        Ok(old_node.is_some())
    }

    /// Delete a node within a transaction
    pub async fn delete_node_tx(&self, transaction_id: &str, node_id: &str) -> Result<bool> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("node:{}", node_id);

        // Acquire exclusive lock
        self.lock_manager
            .acquire_lock(
                transaction_id.to_string(),
                resource_id.clone(),
                LockType::Exclusive,
            )
            .await?;

        // Get current node for rollback
        let old_node = self.client.get_node(node_id).await?;
        let old_data = old_node.as_ref().map(|n| bincode::serialize(n).unwrap());

        if old_node.is_none() {
            return Ok(false);
        }

        // Create write operation
        let write_op = WriteOperation {
            operation_type: OperationType::Delete,
            resource_type: ResourceType::Node,
            resource_id: resource_id.clone(),
            old_value: old_data.clone(),
            new_value: None,
            timestamp: chrono::Utc::now(),
        };

        // Log the write operation
        let prev_lsn = transaction
            .write_set
            .values()
            .map(|op| op.timestamp.timestamp_micros() as u64)
            .max();

        self.wal
            .log_write(transaction_id.to_string(), write_op.clone(), prev_lsn)
            .await?;

        // Add to transaction write set
        transaction.add_write(
            OperationType::Delete,
            ResourceType::Node,
            resource_id,
            old_data,
            None,
        );

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!("Added node deletion to transaction {}", transaction_id);
        Ok(true)
    }

    /// Create an edge within a transaction
    pub async fn create_edge_tx(&self, transaction_id: &str, edge: Edge) -> Result<String> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("edge:{}", edge.id);

        // Acquire exclusive lock on the edge
        self.lock_manager
            .acquire_lock(
                transaction_id.to_string(),
                resource_id.clone(),
                LockType::Exclusive,
            )
            .await?;

        // Serialize edge for logging
        let edge_data = bincode::serialize(&edge).map_err(|e| {
            DatabaseError::Serialization(format!("Failed to serialize edge: {}", e))
        })?;

        // Create write operation
        let write_op = WriteOperation {
            operation_type: OperationType::Create,
            resource_type: ResourceType::Edge,
            resource_id: resource_id.clone(),
            old_value: None,
            new_value: Some(edge_data.clone()),
            timestamp: chrono::Utc::now(),
        };

        // Log the write operation
        let prev_lsn = transaction
            .write_set
            .values()
            .map(|op| op.timestamp.timestamp_micros() as u64)
            .max();

        self.wal
            .log_write(transaction_id.to_string(), write_op.clone(), prev_lsn)
            .await?;

        // Add to transaction write set
        transaction.add_write(
            OperationType::Create,
            ResourceType::Edge,
            resource_id,
            None,
            Some(edge_data),
        );

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!("Added edge creation to transaction {}", transaction_id);
        Ok(edge.id.clone())
    }

    /// Update an edge within a transaction
    pub async fn update_edge_tx(&self, transaction_id: &str, edge: Edge) -> Result<bool> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("edge:{}", edge.id);

        // Acquire exclusive lock
        self.lock_manager
            .acquire_lock(
                transaction_id.to_string(),
                resource_id.clone(),
                LockType::Exclusive,
            )
            .await?;

        // Get current edge for rollback
        let old_edge = self.client.get_edge(&edge.id).await?;
        let old_data = old_edge.as_ref().map(|e| bincode::serialize(e).unwrap());

        if old_edge.is_none() {
            return Ok(false);
        }

        // Serialize new edge
        let new_data = bincode::serialize(&edge).map_err(|e| {
            DatabaseError::Serialization(format!("Failed to serialize edge: {}", e))
        })?;

        // Create write operation
        let write_op = WriteOperation {
            operation_type: OperationType::Update,
            resource_type: ResourceType::Edge,
            resource_id: resource_id.clone(),
            old_value: old_data,
            new_value: Some(new_data.clone()),
            timestamp: chrono::Utc::now(),
        };

        // Log the write operation
        let prev_lsn = transaction
            .write_set
            .values()
            .map(|op| op.timestamp.timestamp_micros() as u64)
            .max();

        self.wal
            .log_write(transaction_id.to_string(), write_op.clone(), prev_lsn)
            .await?;

        // Add to transaction write set
        transaction.add_write(
            OperationType::Update,
            ResourceType::Edge,
            resource_id,
            write_op.old_value,
            Some(new_data),
        );

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!("Added edge update to transaction {}", transaction_id);
        Ok(true)
    }

    /// Delete an edge within a transaction
    pub async fn delete_edge_tx(&self, transaction_id: &str, edge_id: &str) -> Result<bool> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("edge:{}", edge_id);

        // Acquire exclusive lock
        self.lock_manager
            .acquire_lock(
                transaction_id.to_string(),
                resource_id.clone(),
                LockType::Exclusive,
            )
            .await?;

        // Get current edge for rollback
        let old_edge = self.client.get_edge(edge_id).await?;
        let old_data = old_edge.as_ref().map(|e| bincode::serialize(e).unwrap());

        if old_edge.is_none() {
            return Ok(false);
        }

        // Create write operation
        let write_op = WriteOperation {
            operation_type: OperationType::Delete,
            resource_type: ResourceType::Edge,
            resource_id: resource_id.clone(),
            old_value: old_data.clone(),
            new_value: None,
            timestamp: chrono::Utc::now(),
        };

        // Log the write operation
        let prev_lsn = transaction
            .write_set
            .values()
            .map(|op| op.timestamp.timestamp_micros() as u64)
            .max();

        self.wal
            .log_write(transaction_id.to_string(), write_op.clone(), prev_lsn)
            .await?;

        // Add to transaction write set
        transaction.add_write(
            OperationType::Delete,
            ResourceType::Edge,
            resource_id,
            old_data,
            None,
        );

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!("Added edge deletion to transaction {}", transaction_id);
        Ok(true)
    }

    /// Get an edge within a transaction
    pub async fn get_edge_tx(&self, transaction_id: &str, edge_id: &str) -> Result<Option<Edge>> {
        let mut transaction = self.get_active_transaction(transaction_id)?;

        // Check if transaction is expired
        if transaction.is_expired() {
            return Err(DatabaseError::Transaction(
                "Transaction expired".to_string(),
            ));
        }

        let resource_id = format!("edge:{}", edge_id);

        // Check isolation level and acquire appropriate lock
        let lock_type = match transaction.isolation_level {
            TransactionIsolationLevel::ReadUncommitted => {
                // No locking needed for read uncommitted
                LockType::IntentionShared
            }
            _ => LockType::Shared,
        };

        if transaction.isolation_level != TransactionIsolationLevel::ReadUncommitted {
            self.lock_manager
                .acquire_lock(transaction_id.to_string(), resource_id.clone(), lock_type)
                .await?;
        }

        // Check if edge is in transaction's write set first
        if let Some(write_op) = transaction.write_set.get(&resource_id) {
            match write_op.operation_type {
                OperationType::Delete => return Ok(None),
                OperationType::Create | OperationType::Update => {
                    if let Some(data) = &write_op.new_value {
                        let edge = bincode::deserialize(data).map_err(|e| {
                            DatabaseError::Serialization(format!(
                                "Failed to deserialize edge: {}",
                                e
                            ))
                        })?;
                        return Ok(Some(edge));
                    }
                }
            }
        }

        // Add to read set
        transaction.add_read(resource_id.clone());

        // Update transaction
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        // Get from database
        let edge = self.client.get_edge(edge_id).await?;

        debug!("Read edge {} in transaction {}", edge_id, transaction_id);
        Ok(edge)
    }

    /// Create a savepoint
    pub async fn create_savepoint(
        &self,
        transaction_id: &str,
        savepoint_name: String,
    ) -> Result<()> {
        let mut transaction = self.get_active_transaction(transaction_id)?;
        transaction.create_savepoint(savepoint_name.clone())?;
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!(
            "Created savepoint '{}' in transaction {}",
            savepoint_name, transaction_id
        );
        Ok(())
    }

    /// Rollback to a savepoint
    pub async fn rollback_to_savepoint(
        &self,
        transaction_id: &str,
        savepoint_name: &str,
    ) -> Result<()> {
        let mut transaction = self.get_active_transaction(transaction_id)?;
        transaction.rollback_to_savepoint(savepoint_name)?;
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        info!(
            "Rolled back to savepoint '{}' in transaction {}",
            savepoint_name, transaction_id
        );
        Ok(())
    }

    /// Release a savepoint
    pub async fn release_savepoint(
        &self,
        transaction_id: &str,
        savepoint_name: &str,
    ) -> Result<()> {
        let mut transaction = self.get_active_transaction(transaction_id)?;
        transaction.release_savepoint(savepoint_name)?;
        self.active_transactions
            .insert(transaction_id.to_string(), transaction);

        debug!(
            "Released savepoint '{}' in transaction {}",
            savepoint_name, transaction_id
        );
        Ok(())
    }

    /// Get transaction statistics
    pub async fn get_transaction_stats(
        &self,
        transaction_id: &str,
    ) -> Result<crate::transaction::TransactionStats> {
        let transaction = self.get_active_transaction(transaction_id)?;
        Ok(transaction.statistics())
    }

    /// Get all active transaction IDs
    pub fn get_active_transaction_ids(&self) -> Vec<String> {
        self.active_transactions
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get active transaction count
    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.len()
    }

    // Helper methods

    /// Get active transaction
    fn get_active_transaction(&self, transaction_id: &str) -> Result<Transaction> {
        self.active_transactions
            .get(transaction_id)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                DatabaseError::Transaction(format!("Transaction not found: {}", transaction_id))
            })
    }

    /// Apply write operation to database
    async fn apply_write_operation(&self, write_op: &WriteOperation) -> Result<()> {
        match (&write_op.resource_type, &write_op.operation_type) {
            (ResourceType::Node, OperationType::Create) => {
                if let Some(data) = &write_op.new_value {
                    let node: Node = bincode::deserialize(data).map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to deserialize node: {}", e))
                    })?;
                    self.client.create_node(node).await?;
                }
            }
            (ResourceType::Node, OperationType::Update) => {
                if let Some(data) = &write_op.new_value {
                    let node: Node = bincode::deserialize(data).map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to deserialize node: {}", e))
                    })?;
                    self.client.update_node(&node).await?;
                }
            }
            (ResourceType::Node, OperationType::Delete) => {
                let node_id = write_op
                    .resource_id
                    .strip_prefix("node:")
                    .unwrap_or(&write_op.resource_id);
                self.client.delete_node(node_id).await?;
            }
            (ResourceType::Edge, OperationType::Create) => {
                if let Some(data) = &write_op.new_value {
                    let edge: Edge = bincode::deserialize(data).map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to deserialize edge: {}", e))
                    })?;
                    self.client.create_edge(edge).await?;
                }
            }
            (ResourceType::Edge, OperationType::Update) => {
                if let Some(data) = &write_op.new_value {
                    let edge: Edge = bincode::deserialize(data).map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to deserialize edge: {}", e))
                    })?;
                    self.client.update_edge(edge).await?;
                }
            }
            (ResourceType::Edge, OperationType::Delete) => {
                let edge_id = write_op
                    .resource_id
                    .strip_prefix("edge:")
                    .unwrap_or(&write_op.resource_id);
                self.client.delete_edge(edge_id).await?;
            }
        }
        Ok(())
    }

    /// Cleanup expired transactions
    async fn cleanup_expired_transactions(
        active_transactions: &DashMap<String, Transaction>,
        lock_manager: &LockManager,
        wal: &WriteAheadLog,
    ) {
        let mut expired_transactions = Vec::new();

        for entry in active_transactions.iter() {
            if entry.value().is_expired() {
                expired_transactions.push(entry.key().clone());
            }
        }

        for transaction_id in expired_transactions {
            warn!("Cleaning up expired transaction: {}", transaction_id);

            // Log abort
            if let Err(e) = wal.log_abort(transaction_id.clone()).await {
                error!(
                    "Failed to log abort for expired transaction {}: {}",
                    transaction_id, e
                );
            }

            // Release locks
            if let Err(e) = lock_manager
                .release_transaction_locks(&transaction_id)
                .await
            {
                error!(
                    "Failed to release locks for expired transaction {}: {}",
                    transaction_id, e
                );
            }

            // Remove from active transactions
            active_transactions.remove(&transaction_id);
        }
    }

    /// Recover from write-ahead log
    async fn recover(&self) -> Result<()> {
        info!("Starting transaction recovery from WAL");

        let log_records = self.wal.read_all_records().await?;
        let mut committed_transactions = std::collections::HashSet::new();
        let mut aborted_transactions = std::collections::HashSet::new();

        // First pass: identify committed and aborted transactions
        for record in &log_records {
            match record.record_type {
                LogRecordType::Commit => {
                    committed_transactions.insert(record.transaction_id.clone());
                }
                LogRecordType::Abort => {
                    aborted_transactions.insert(record.transaction_id.clone());
                }
                _ => {}
            }
        }

        // Second pass: redo committed transactions, undo uncommitted ones
        for record in &log_records {
            match record.record_type {
                LogRecordType::Write => {
                    if committed_transactions.contains(&record.transaction_id) {
                        // Redo: apply the write operation
                        if let Some(write_op) = &record.write_operation {
                            if let Err(e) = self.apply_write_operation(write_op).await {
                                error!("Failed to redo write operation during recovery: {}", e);
                            }
                        }
                    }
                    // For uncommitted transactions, we don't need to undo since
                    // changes weren't applied to the database yet (WAL approach)
                }
                _ => {}
            }
        }

        info!("Transaction recovery completed");
        Ok(())
    }

    /// Clone transaction manager for a specific client
    pub fn clone_for_client(&self, client: DatabaseClient) -> Self {
        Self {
            client,
            active_transactions: self.active_transactions.clone(),
            lock_manager: self.lock_manager.clone(),
            wal: self.wal.clone(),
            cleanup_task: self.cleanup_task.clone(), // This is a handle, safe to clone
            checkpoint_task: self.checkpoint_task.clone(),
            config: self.config.clone(),
            statistics: self.statistics.clone(),
        }
    }

    /// Perform checkpoint operation
    async fn perform_checkpoint(wal: &WriteAheadLog) -> Result<()> {
        debug!("Performing WAL checkpoint");

        // Log checkpoint start
        let checkpoint_lsn = wal.log_checkpoint().await?;

        // Force sync to disk
        wal.sync().await?;

        // Log checkpoint end
        wal.log_checkpoint_end().await?;

        // Optionally truncate old WAL entries (if configured)
        // wal.truncate_before_lsn(checkpoint_lsn).await?;

        info!("Checkpoint completed at LSN {}", checkpoint_lsn);
        Ok(())
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down TransactionManager");

        // Abort cleanup task
        self.cleanup_task.abort();

        // Abort checkpoint task if it exists
        if let Some(ref checkpoint_task) = self.checkpoint_task {
            checkpoint_task.abort();
        }

        // Abort all active transactions
        let active_tx_ids: Vec<String> = self
            .active_transactions
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for tx_id in active_tx_ids {
            if let Err(e) = self.abort_transaction(&tx_id).await {
                warn!(
                    "Failed to abort transaction {} during shutdown: {}",
                    tx_id, e
                );
            }
        }

        // Final checkpoint
        Self::perform_checkpoint(&self.wal).await?;

        info!("TransactionManager shutdown completed");
        Ok(())
    }

    /// Get transaction manager statistics
    pub async fn get_statistics(&self) -> TransactionManagerStatistics {
        self.statistics.read().await.clone()
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        self.cleanup_task.abort();
        if let Some(ref checkpoint_task) = self.checkpoint_task {
            checkpoint_task.abort();
        }
    }
}

/// Transaction manager statistics
#[derive(Debug, Clone, Default)]
pub struct TransactionManagerStatistics {
    pub total_transactions_started: u64,
    pub total_transactions_committed: u64,
    pub total_transactions_aborted: u64,
    pub currently_active_transactions: usize,
    pub average_transaction_duration: Duration,
    pub total_locks_acquired: u64,
    pub total_deadlocks_detected: u64,
    pub wal_records_written: u64,
    pub checkpoints_performed: u64,
}

impl TransactionManagerStatistics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.total_transactions_committed + self.total_transactions_aborted;
        if total == 0 {
            0.0
        } else {
            (self.total_transactions_committed as f64 / total as f64) * 100.0
        }
    }
}
