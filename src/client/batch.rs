use crate::transaction::TransactionIsolationLevel;
use crate::types::{Edge, Node, Value};
use crate::utils::error::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Batch operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchOperation {
    CreateNode(Node),
    UpdateNode(Node),
    DeleteNode(String),
    CreateEdge(Edge),
    UpdateEdge(Edge),
    DeleteEdge(String),
    Custom(CustomBatchOperation),
}

impl BatchOperation {
    pub fn operation_type(&self) -> String {
        match self {
            Self::CreateNode(_) => "create_node".to_string(),
            Self::UpdateNode(_) => "update_node".to_string(),
            Self::DeleteNode(_) => "delete_node".to_string(),
            Self::CreateEdge(_) => "create_edge".to_string(),
            Self::UpdateEdge(_) => "update_edge".to_string(),
            Self::DeleteEdge(_) => "delete_edge".to_string(),
            Self::Custom(op) => op.operation_type.clone(),
        }
    }
}

/// Custom batch operation for extensibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomBatchOperation {
    pub operation_type: String,
    pub parameters: serde_json::Value,
}

/// Batch execution modes
#[derive(Debug, Clone, PartialEq)]
pub enum BatchExecutionMode {
    /// All operations in single transaction (default)
    Transaction,
    /// Each operation commits individually
    AutoCommit,
    /// Memory-efficient streaming processing
    Streaming,
    /// Parallel execution with controlled concurrency
    Parallel,
}

/// Batch configuration
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Execution mode
    pub execution_mode: BatchExecutionMode,

    /// Number of operations per batch chunk
    pub batch_size: usize,

    /// Maximum concurrent operations (for parallel/streaming modes)
    pub max_concurrent_operations: usize,

    /// Transaction isolation level
    pub isolation_level: TransactionIsolationLevel,

    /// Transaction timeout
    pub transaction_timeout: Duration,

    /// Individual operation timeout
    pub operation_timeout: Duration,

    /// Delay between chunks
    pub inter_chunk_delay: Duration,

    /// Delay between individual operations
    pub inter_operation_delay: Duration,

    /// Stop on first error
    pub fail_fast: bool,

    /// Fail entire batch if any operation fails
    pub fail_on_any_error: bool,

    /// Enable savepoints for partial rollback
    pub enable_savepoints: bool,

    /// Cancellation token for early termination
    pub cancellation_token: Option<CancellationToken>,

    /// Retry configuration
    pub retry_config: Option<RetryConfig>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            execution_mode: BatchExecutionMode::Transaction,
            batch_size: 1000,
            max_concurrent_operations: 10,
            isolation_level: TransactionIsolationLevel::ReadCommitted,
            transaction_timeout: Duration::from_secs(300), // 5 minutes
            operation_timeout: Duration::from_secs(30),
            inter_chunk_delay: Duration::from_millis(0),
            inter_operation_delay: Duration::from_millis(0),
            fail_fast: false,
            fail_on_any_error: false,
            enable_savepoints: true,
            cancellation_token: None,
            retry_config: None,
        }
    }
}

/// Retry configuration for failed operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
    pub retry_on_timeout: bool,
    pub retry_on_connection_error: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            retry_on_timeout: true,
            retry_on_connection_error: true,
        }
    }
}

/// Batch operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BatchOperationResult {
    NodeCreated {
        id: String,
    },
    NodeUpdated {
        id: String,
        updated: bool,
    },
    NodeDeleted {
        id: String,
        deleted: bool,
    },
    EdgeCreated {
        id: String,
    },
    EdgeUpdated {
        id: String,
        updated: bool,
    },
    EdgeDeleted {
        id: String,
        deleted: bool,
    },
    Custom {
        operation_type: String,
        result: serde_json::Value,
    },
}

/// Progress information for batch operations
#[derive(Debug, Clone)]
pub struct BatchProgress {
    pub processed: usize,
    pub total: usize,
    pub current_operation: String,
    pub percentage: f64,
    pub errors: usize,
}

/// Error information for failed batch operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchError {
    pub operation_index: usize,
    pub operation_type: String,
    pub error_message: String,
    pub is_retryable: bool,
}

/// Complete batch execution result
#[derive(Debug, Clone)]
pub struct BatchResult {
    pub successful_operations: usize,
    pub failed_operations: usize,
    pub total_operations: usize,
    pub operation_results: Vec<BatchOperationResult>,
    pub errors: Vec<BatchError>,
    pub total_duration: Duration,
    pub average_operation_time: Duration,
    pub operations_per_second: f64,
}

impl BatchResult {
    pub fn new(total_operations: usize) -> Self {
        Self {
            successful_operations: 0,
            failed_operations: 0,
            total_operations,
            operation_results: Vec::new(),
            errors: Vec::new(),
            total_duration: Duration::from_secs(0),
            average_operation_time: Duration::from_secs(0),
            operations_per_second: 0.0,
        }
    }

    pub fn calculate_statistics(&mut self) {
        if self.successful_operations > 0 {
            self.average_operation_time = self.total_duration / self.successful_operations as u32;
            self.operations_per_second =
                self.successful_operations as f64 / self.total_duration.as_secs_f64();
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            (self.successful_operations as f64 / self.total_operations as f64) * 100.0
        }
    }

    pub fn is_successful(&self) -> bool {
        self.failed_operations == 0
    }

    pub fn get_retryable_errors(&self) -> Vec<&BatchError> {
        self.errors.iter().filter(|e| e.is_retryable).collect()
    }
}
