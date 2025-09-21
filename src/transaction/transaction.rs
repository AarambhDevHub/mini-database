use crate::utils::error::{DatabaseError, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum TransactionIsolationLevel {
    /// Lowest level - allows dirty reads, non-repeatable reads, phantom reads
    ReadUncommitted,
    /// Prevents dirty reads, allows non-repeatable reads and phantom reads
    ReadCommitted,
    /// Prevents dirty reads and non-repeatable reads, allows phantom reads
    RepeatableRead,
    /// Highest level - prevents all phenomena
    Serializable,
}

impl Default for TransactionIsolationLevel {
    fn default() -> Self {
        Self::ReadCommitted
    }
}

/// Transaction state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
    Failed,
}

/// Database transaction with ACID properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Unique transaction ID
    pub id: String,

    /// Transaction state
    pub state: TransactionState,

    /// Isolation level
    pub isolation_level: TransactionIsolationLevel,

    /// Transaction start time
    pub start_time: DateTime<Utc>,

    /// Transaction end time
    pub end_time: Option<DateTime<Utc>>,

    /// Read set - tracks nodes/edges read during transaction
    pub read_set: HashMap<String, u64>, // resource_id -> timestamp

    /// Write set - tracks nodes/edges modified during transaction
    pub write_set: HashMap<String, WriteOperation>,

    /// Savepoints for nested transactions
    pub savepoints: Vec<Savepoint>,

    /// Transaction timeout in seconds
    pub timeout_seconds: Option<u64>,
}

/// Write operation in a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteOperation {
    pub operation_type: OperationType,
    pub resource_type: ResourceType,
    pub resource_id: String,
    pub old_value: Option<Vec<u8>>, // For rollback
    pub new_value: Option<Vec<u8>>,
    pub timestamp: DateTime<Utc>,
}

/// Type of operation performed
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OperationType {
    Create,
    Update,
    Delete,
}

/// Type of resource being operated on
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ResourceType {
    Node,
    Edge,
}

/// Savepoint for nested transactions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Savepoint {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub write_set_size: usize,
    pub read_set_size: usize,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(isolation_level: TransactionIsolationLevel) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            state: TransactionState::Active,
            isolation_level,
            start_time: Utc::now(),
            end_time: None,
            read_set: HashMap::new(),
            write_set: HashMap::new(),
            savepoints: Vec::new(),
            timeout_seconds: Some(30), // 30 second default timeout
        }
    }

    /// Create a transaction with custom timeout
    pub fn with_timeout(isolation_level: TransactionIsolationLevel, timeout_seconds: u64) -> Self {
        let mut tx = Self::new(isolation_level);
        tx.timeout_seconds = Some(timeout_seconds);
        tx
    }

    /// Add a read operation to the transaction
    pub fn add_read(&mut self, resource_id: String) {
        let timestamp = Utc::now().timestamp_micros() as u64;
        self.read_set.insert(resource_id, timestamp);
    }

    /// Add a write operation to the transaction
    pub fn add_write(
        &mut self,
        operation_type: OperationType,
        resource_type: ResourceType,
        resource_id: String,
        old_value: Option<Vec<u8>>,
        new_value: Option<Vec<u8>>,
    ) {
        let write_op = WriteOperation {
            operation_type,
            resource_type,
            resource_id: resource_id.clone(),
            old_value,
            new_value,
            timestamp: Utc::now(),
        };

        self.write_set.insert(resource_id, write_op);
    }

    /// Create a savepoint
    pub fn create_savepoint(&mut self, name: String) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(DatabaseError::Transaction(
                "Cannot create savepoint in non-active transaction".to_string(),
            ));
        }

        // Check if savepoint already exists
        if self.savepoints.iter().any(|sp| sp.name == name) {
            return Err(DatabaseError::Transaction(format!(
                "Savepoint '{}' already exists",
                name
            )));
        }

        let savepoint = Savepoint {
            name,
            created_at: Utc::now(),
            write_set_size: self.write_set.len(),
            read_set_size: self.read_set.len(),
        };

        self.savepoints.push(savepoint);
        Ok(())
    }

    /// Rollback to a savepoint
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(DatabaseError::Transaction(
                "Cannot rollback in non-active transaction".to_string(),
            ));
        }

        if let Some(savepoint_index) = self.savepoints.iter().position(|sp| sp.name == name) {
            let savepoint = &self.savepoints[savepoint_index];

            // Truncate write and read sets to savepoint state
            let write_keys: Vec<String> = self
                .write_set
                .keys()
                .skip(savepoint.write_set_size)
                .cloned()
                .collect();

            for key in write_keys {
                self.write_set.remove(&key);
            }

            let read_keys: Vec<String> = self
                .read_set
                .keys()
                .skip(savepoint.read_set_size)
                .cloned()
                .collect();

            for key in read_keys {
                self.read_set.remove(&key);
            }

            // Remove savepoints created after this one
            self.savepoints.truncate(savepoint_index + 1);

            Ok(())
        } else {
            Err(DatabaseError::Transaction(format!(
                "Savepoint '{}' not found",
                name
            )))
        }
    }

    /// Release a savepoint
    pub fn release_savepoint(&mut self, name: &str) -> Result<()> {
        if let Some(index) = self.savepoints.iter().position(|sp| sp.name == name) {
            self.savepoints.remove(index);
            Ok(())
        } else {
            Err(DatabaseError::Transaction(format!(
                "Savepoint '{}' not found",
                name
            )))
        }
    }

    /// Check if transaction has expired
    pub fn is_expired(&self) -> bool {
        if let Some(timeout) = self.timeout_seconds {
            let elapsed = Utc::now().signed_duration_since(self.start_time);
            elapsed.num_seconds() > timeout as i64
        } else {
            false
        }
    }

    /// Mark transaction as preparing for commit
    pub fn prepare(&mut self) -> Result<()> {
        match self.state {
            TransactionState::Active => {
                self.state = TransactionState::Preparing;
                Ok(())
            }
            _ => Err(DatabaseError::Transaction(format!(
                "Cannot prepare transaction in state {:?}",
                self.state
            ))),
        }
    }

    /// Commit the transaction
    pub fn commit(&mut self) -> Result<()> {
        match self.state {
            TransactionState::Active | TransactionState::Preparing => {
                self.state = TransactionState::Committed;
                self.end_time = Some(Utc::now());
                Ok(())
            }
            _ => Err(DatabaseError::Transaction(format!(
                "Cannot commit transaction in state {:?}",
                self.state
            ))),
        }
    }

    /// Abort the transaction
    pub fn abort(&mut self) -> Result<()> {
        match self.state {
            TransactionState::Active | TransactionState::Preparing => {
                self.state = TransactionState::Aborted;
                self.end_time = Some(Utc::now());
                Ok(())
            }
            _ => Err(DatabaseError::Transaction(format!(
                "Cannot abort transaction in state {:?}",
                self.state
            ))),
        }
    }

    /// Get transaction duration
    pub fn duration(&self) -> chrono::Duration {
        let end = self.end_time.unwrap_or_else(Utc::now);
        end.signed_duration_since(self.start_time)
    }

    /// Check if transaction conflicts with another transaction
    pub fn conflicts_with(&self, other: &Transaction) -> bool {
        // Check for read-write conflicts
        for read_resource in self.read_set.keys() {
            if other.write_set.contains_key(read_resource) {
                return true;
            }
        }

        // Check for write-read conflicts
        for write_resource in self.write_set.keys() {
            if other.read_set.contains_key(write_resource) {
                return true;
            }
        }

        // Check for write-write conflicts
        for write_resource in self.write_set.keys() {
            if other.write_set.contains_key(write_resource) {
                return true;
            }
        }

        false
    }

    /// Get transaction statistics
    pub fn statistics(&self) -> TransactionStats {
        TransactionStats {
            id: self.id.clone(),
            state: self.state.clone(),
            isolation_level: self.isolation_level,
            duration: self.duration(),
            reads_count: self.read_set.len(),
            writes_count: self.write_set.len(),
            savepoints_count: self.savepoints.len(),
        }
    }
}

/// Transaction statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionStats {
    pub id: String,
    pub state: TransactionState,
    pub isolation_level: TransactionIsolationLevel,
    pub duration: chrono::Duration,
    pub reads_count: usize,
    pub writes_count: usize,
    pub savepoints_count: usize,
}
