use crate::core::DatabaseConfig;
use crate::storage::{Cache, EdgeStore, FileReader, Index, NodeStore};
use crate::utils::error::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Main database instance managing all storage components
#[derive(Clone)] // Add Serialize, Deserialize
pub struct Database {
    pub config: Arc<DatabaseConfig>,
    node_store: Arc<NodeStore>,
    edge_store: Arc<EdgeStore>,
    index: Arc<RwLock<Index>>,
    cache: Arc<Cache>,
    file_reader: Arc<FileReader>,
}

impl Database {
    /// Create a new database instance
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        info!("Initializing database with config: {:?}", config);

        // Ensure data directory exists
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to create data directory: {}", e)))?;

        let config = Arc::new(config);

        // Initialize storage components
        let file_reader = Arc::new(FileReader::new(config.clone())?);
        let cache = Arc::new(Cache::new(config.cache_size_mb * 1024 * 1024));
        let index = Arc::new(RwLock::new(Index::new(config.clone()).await?));
        let node_store =
            Arc::new(NodeStore::new(config.clone(), file_reader.clone(), cache.clone()).await?);
        let edge_store =
            Arc::new(EdgeStore::new(config.clone(), file_reader.clone(), cache.clone()).await?);

        debug!("Database initialization complete");

        Ok(Self {
            config,
            node_store,
            edge_store,
            index,
            cache,
            file_reader,
        })
    }

    /// Get node store reference
    pub fn node_store(&self) -> &Arc<NodeStore> {
        &self.node_store
    }

    /// Get edge store reference
    pub fn edge_store(&self) -> &Arc<EdgeStore> {
        &self.edge_store
    }

    /// Get index reference
    pub fn index(&self) -> &Arc<RwLock<Index>> {
        &self.index
    }

    /// Get cache reference
    pub fn cache(&self) -> &Arc<Cache> {
        &self.cache
    }

    /// Get file reader reference
    pub fn file_reader(&self) -> &Arc<FileReader> {
        &self.file_reader
    }

    /// Get database configuration
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        debug!("Flushing database to disk");

        self.node_store.flush().await?;
        self.edge_store.flush().await?;

        let mut index = self.index.write().await; // Get mutable reference
        index.flush().await?;

        info!("Database flush completed");
        Ok(())
    }

    /// Get database statistics
    pub async fn stats(&self) -> DatabaseStats {
        let node_count = self.node_store.count().await;
        let edge_count = self.edge_store.count().await;
        let cache_stats = self.cache.stats();

        DatabaseStats {
            node_count,
            edge_count,
            cache_hit_rate: cache_stats.hit_rate(),
            cache_size: cache_stats.current_size,
            cache_capacity: cache_stats.capacity,
        }
    }
}

/// Database statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub cache_hit_rate: f64,
    pub cache_size: usize,
    pub cache_capacity: usize,
}
