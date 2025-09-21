use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::transaction::TransactionManagerConfig;

/// Database configuration settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database data directory
    pub data_dir: PathBuf,

    /// Cache size in MB
    pub cache_size_mb: usize,

    /// Maximum file size for rotation
    pub max_file_size_mb: usize,

    /// Enable compression
    pub compression: bool,

    /// Sync writes to disk
    pub sync_writes: bool,

    /// Memory map threshold in bytes
    pub mmap_threshold: usize,

    /// Index page size
    pub index_page_size: usize,

    /// Transaction system configuration
    pub transaction_config: Option<TransactionManagerConfig>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            cache_size_mb: 128,
            max_file_size_mb: 256,
            compression: true,
            sync_writes: false,
            mmap_threshold: 64 * 1024, // 64KB
            index_page_size: 4096,
            transaction_config: None,
        }
    }
}

impl DatabaseConfig {
    /// Create a new configuration with custom data directory
    pub fn new<P: Into<PathBuf>>(data_dir: P) -> Self {
        Self {
            data_dir: data_dir.into(),
            ..Default::default()
        }
    }

    /// Set cache size in MB
    pub fn with_cache_size(mut self, size_mb: usize) -> Self {
        self.cache_size_mb = size_mb;
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }

    /// Enable or disable sync writes
    pub fn with_sync_writes(mut self, enabled: bool) -> Self {
        self.sync_writes = enabled;
        self
    }

    pub fn with_transactions(mut self, config: TransactionManagerConfig) -> Self {
        self.transaction_config = Some(config);
        self
    }
}
