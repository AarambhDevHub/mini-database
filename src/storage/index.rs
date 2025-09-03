use crate::core::DatabaseConfig;
use crate::utils::error::{DatabaseError, Result};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tracing::{debug, trace};

/// B-tree indexing for fast lookups and range queries
pub struct Index {
    config: Arc<DatabaseConfig>,
    indices: BTreeMap<String, BTreeMap<String, Vec<String>>>, // index_name -> key -> [record_ids]
    index_file: File,
    data_path: PathBuf,
}

impl Index {
    /// Create a new index
    pub async fn new(config: Arc<DatabaseConfig>) -> Result<Self> {
        let data_path = config.data_dir.join("index.db");

        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open index file: {}", e)))?;

        let mut index = Self {
            config,
            indices: BTreeMap::new(),
            index_file,
            data_path,
        };

        // Load existing indices
        index.load_indices().await?;

        debug!("Index initialized with {} indices", index.indices.len());
        Ok(index)
    }

    /// Create a new index
    pub fn create_index(&mut self, index_name: &str) {
        self.indices.insert(index_name.to_string(), BTreeMap::new());
        debug!("Created index: {}", index_name);
    }

    /// Add entry to index
    pub fn add_to_index(&mut self, index_name: &str, key: &str, record_id: &str) {
        let index = self
            .indices
            .entry(index_name.to_string())
            .or_insert_with(BTreeMap::new);

        let records = index.entry(key.to_string()).or_insert_with(Vec::new);

        if !records.contains(&record_id.to_string()) {
            records.push(record_id.to_string());
            trace!("Added to index {}: {} -> {}", index_name, key, record_id);
        }
    }

    /// Remove entry from index
    pub fn remove_from_index(&mut self, index_name: &str, key: &str, record_id: &str) {
        if let Some(index) = self.indices.get_mut(index_name) {
            if let Some(records) = index.get_mut(key) {
                records.retain(|id| id != record_id);
                if records.is_empty() {
                    index.remove(key);
                }
                trace!(
                    "Removed from index {}: {} -> {}",
                    index_name, key, record_id
                );
            }
        }
    }

    /// Lookup records by key
    pub fn lookup(&self, index_name: &str, key: &str) -> Vec<String> {
        self.indices
            .get(index_name)
            .and_then(|index| index.get(key))
            .map(|records| records.clone())
            .unwrap_or_default()
    }

    /// Range query on index
    pub fn range_query(&self, index_name: &str, start_key: &str, end_key: &str) -> Vec<String> {
        let mut results = Vec::new();

        if let Some(index) = self.indices.get(index_name) {
            for (key, records) in index.range(start_key.to_string()..=end_key.to_string()) {
                results.extend(records.clone());
                trace!("Range query result: {} -> {} records", key, records.len());
            }
        }

        debug!(
            "Range query in index {} returned {} results",
            index_name,
            results.len()
        );
        results
    }

    /// Get all keys in index
    pub fn get_keys(&self, index_name: &str) -> Vec<String> {
        self.indices
            .get(index_name)
            .map(|index| index.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Check if index exists
    pub fn has_index(&self, index_name: &str) -> bool {
        self.indices.contains_key(index_name)
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> bool {
        if self.indices.remove(index_name).is_some() {
            debug!("Dropped index: {}", index_name);
            true
        } else {
            false
        }
    }

    /// Get index statistics
    pub fn get_stats(&self, index_name: &str) -> IndexStats {
        if let Some(index) = self.indices.get(index_name) {
            let total_records: usize = index.values().map(|records| records.len()).sum();
            IndexStats {
                key_count: index.len(),
                record_count: total_records,
                avg_records_per_key: if index.is_empty() {
                    0.0
                } else {
                    total_records as f64 / index.len() as f64
                },
            }
        } else {
            IndexStats::default()
        }
    }

    /// Flush indices to disk
    pub async fn flush(&mut self) -> Result<()> {
        // Change &self to &mut self
        // For simplicity, we'll serialize the entire index structure
        let serialized = bincode::serialize(&self.indices).map_err(|e| {
            DatabaseError::Serialization(format!("Failed to serialize indices: {}", e))
        })?;

        self.index_file
            .seek(SeekFrom::Start(0))
            .await // Remove the & since self is already mutable
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        self.index_file
            .write_all(&serialized)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to write indices: {}", e)))?;

        self.index_file
            .sync_all()
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to sync: {}", e)))?;

        debug!("Flushed {} indices to disk", self.indices.len());
        Ok(())
    }

    /// Load indices from disk
    async fn load_indices(&mut self) -> Result<()> {
        let file_size = self
            .index_file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to get file size: {}", e)))?;

        if file_size == 0 {
            return Ok(());
        }

        self.index_file
            .seek(SeekFrom::Start(0))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        let mut buffer = Vec::new();
        self.index_file
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read indices: {}", e)))?;

        if !buffer.is_empty() {
            self.indices = bincode::deserialize(&buffer).map_err(|e| {
                DatabaseError::Serialization(format!("Failed to deserialize indices: {}", e))
            })?;

            debug!("Loaded {} indices from disk", self.indices.len());
        }

        Ok(())
    }
}

/// Index statistics
#[derive(Debug, Clone, Default)]
pub struct IndexStats {
    pub key_count: usize,
    pub record_count: usize,
    pub avg_records_per_key: f64,
}
