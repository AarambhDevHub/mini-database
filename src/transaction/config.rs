use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for transaction manager
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionManagerConfig {
    /// Path to write-ahead log file
    pub wal_path: PathBuf,

    /// Force sync WAL writes to disk
    pub force_sync: bool,

    /// Transaction timeout in seconds
    pub default_transaction_timeout: Duration,

    /// Lock timeout in seconds
    pub lock_timeout: Duration,

    /// Maximum number of active transactions
    pub max_active_transactions: usize,

    /// Cleanup interval for expired transactions
    pub cleanup_interval: Duration,

    /// WAL checkpoint interval
    pub checkpoint_interval: Duration,

    /// Maximum WAL file size before rotation
    pub max_wal_size: u64,

    /// Enable detailed transaction logging
    pub enable_detailed_logging: bool,

    /// Enable transaction statistics collection
    pub enable_statistics: bool,
}

impl Default for TransactionManagerConfig {
    fn default() -> Self {
        Self {
            wal_path: PathBuf::from("./data/transaction.wal"),
            force_sync: true,
            default_transaction_timeout: Duration::from_secs(300), // 5 minutes
            lock_timeout: Duration::from_secs(30),
            max_active_transactions: 1000,
            cleanup_interval: Duration::from_secs(60), // 1 minute
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            max_wal_size: 100 * 1024 * 1024,           // 100MB
            enable_detailed_logging: false,
            enable_statistics: true,
        }
    }
}

impl TransactionManagerConfig {
    /// Create configuration for development environment
    pub fn development() -> Self {
        Self {
            force_sync: false, // Faster for development
            enable_detailed_logging: true,
            cleanup_interval: Duration::from_secs(30),
            ..Default::default()
        }
    }

    /// Create configuration for production environment
    pub fn production() -> Self {
        Self {
            force_sync: true,                      // Ensure durability
            enable_detailed_logging: false,        // Reduce overhead
            max_active_transactions: 10000,        // Higher capacity
            lock_timeout: Duration::from_secs(60), // Longer timeout
            ..Default::default()
        }
    }

    /// Create configuration for testing
    pub fn testing() -> Self {
        Self {
            wal_path: PathBuf::from(":memory:"), // In-memory for tests
            force_sync: false,
            cleanup_interval: Duration::from_secs(5),
            checkpoint_interval: Duration::from_secs(10),
            enable_detailed_logging: true,
            ..Default::default()
        }
    }
}
