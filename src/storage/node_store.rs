use crate::core::DatabaseConfig;
use crate::storage::{Cache, FileReader};
use crate::types::{Node, Value};
use crate::utils::error::{DatabaseError, Result};
use crate::utils::serde::{deserialize, serialize};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::RwLock;
use tracing::{debug, trace, warn};
use uuid::Uuid;

/// Node storage with serialization, indexing, and efficient retrieval
pub struct NodeStore {
    config: Arc<DatabaseConfig>,
    file_reader: Arc<FileReader>,
    cache: Arc<Cache>,
    node_file: Arc<RwLock<File>>,
    node_index: DashMap<String, (u64, u32)>, // node_id -> (file_offset, data_length)
    data_path: PathBuf,
}

impl NodeStore {
    /// Create a new node store
    pub async fn new(
        config: Arc<DatabaseConfig>,
        file_reader: Arc<FileReader>,
        cache: Arc<Cache>,
    ) -> Result<Self> {
        let data_path = config.data_dir.join("nodes.db");

        let node_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open node file: {}", e)))?;

        let store = Self {
            config,
            file_reader,
            cache,
            node_file: Arc::new(RwLock::new(node_file)),
            node_index: DashMap::new(),
            data_path,
        };

        // Load existing index - this is crucial for persistence
        store.load_index().await?;

        debug!(
            "Node store initialized with {} nodes",
            store.node_index.len()
        );
        Ok(store)
    }

    /// Store a node and return its ID
    pub async fn store(&self, mut node: Node) -> Result<String> {
        if node.id.is_empty() {
            node.id = Uuid::new_v4().to_string();
        }

        let serialized = serialize(&node)?;
        let compressed = if self.config.compression {
            lz4_flex::compress_prepend_size(&serialized)
        } else {
            serialized
        };

        // Add length prefix for proper record boundaries
        let data_length = compressed.len() as u32;
        let mut record_data = Vec::with_capacity(4 + compressed.len());
        record_data.extend_from_slice(&data_length.to_le_bytes());
        record_data.extend_from_slice(&compressed);

        let mut file = self.node_file.write().await;
        let offset = file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        file.write_all(&record_data)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to write node: {}", e)))?;

        if self.config.sync_writes {
            file.sync_all()
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to sync: {}", e)))?;
        }

        // Update index with offset and actual data length
        self.node_index
            .insert(node.id.clone(), (offset, data_length));
        self.cache
            .put(format!("node:{}", node.id), compressed.clone());
        self.cache.put_index(format!("node:{}", node.id), offset);

        trace!(
            "Stored node {} at offset {} with length {}",
            node.id, offset, data_length
        );
        Ok(node.id)
    }

    /// Retrieve a node by ID
    pub async fn get(&self, node_id: &str) -> Result<Option<Node>> {
        let cache_key = format!("node:{}", node_id);

        // Try cache first
        if let Some(data) = self.cache.get(&cache_key) {
            trace!("Node {} found in cache", node_id);
            let decompressed = if self.config.compression {
                lz4_flex::decompress_size_prepended(&data).map_err(|e| {
                    DatabaseError::Serialization(format!("Decompression failed: {}", e))
                })?
            } else {
                data
            };
            return Ok(Some(deserialize(&decompressed)?));
        }

        // Get from file using index
        if let Some((offset, length)) = self.node_index.get(node_id).map(|v| *v) {
            trace!(
                "Loading node {} from offset {} with length {}",
                node_id, offset, length
            );

            let data = self.read_node_at_offset(offset, length).await?;
            let decompressed = if self.config.compression {
                lz4_flex::decompress_size_prepended(&data).map_err(|e| {
                    DatabaseError::Serialization(format!("Decompression failed: {}", e))
                })?
            } else {
                data.clone()
            };

            // Cache for future access
            self.cache.put(cache_key, data);

            Ok(Some(deserialize(&decompressed)?))
        } else {
            Ok(None)
        }
    }

    /// Update an existing node
    pub async fn update(&self, node: &Node) -> Result<bool> {
        if !self.node_index.contains_key(&node.id) {
            return Ok(false);
        }

        // For simplicity, we append the updated node (copy-on-write)
        self.store(node.clone()).await?;
        Ok(true)
    }

    /// Delete a node
    pub async fn delete(&self, node_id: &str) -> Result<bool> {
        if let Some((_, _offset)) = self.node_index.remove(node_id) {
            self.cache.remove(&format!("node:{}", node_id));
            debug!("Deleted node {}", node_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get all node IDs
    pub fn get_all_ids(&self) -> Vec<String> {
        self.node_index
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get node count
    pub async fn count(&self) -> u64 {
        self.node_index.len() as u64
    }

    /// Flush pending writes
    pub async fn flush(&self) -> Result<()> {
        let file = self.node_file.write().await;
        file.sync_all()
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to flush: {}", e)))?;
        Ok(())
    }

    /// Find nodes by label
    pub async fn find_by_label(&self, label: &str) -> Result<Vec<Node>> {
        let mut nodes = Vec::new();

        for entry in self.node_index.iter() {
            let node_id = entry.key();
            if let Some(node) = self.get(node_id).await? {
                if node.label == label {
                    nodes.push(node);
                }
            }
        }

        debug!("Found {} nodes with label '{}'", nodes.len(), label);
        Ok(nodes)
    }

    /// Find nodes by property
    pub async fn find_by_property(&self, key: &str, value: &Value) -> Result<Vec<Node>> {
        let mut nodes = Vec::new();

        for entry in self.node_index.iter() {
            let node_id = entry.key();
            if let Some(node) = self.get(node_id).await? {
                if let Some(prop_value) = node.properties.get(key) {
                    if prop_value == value {
                        nodes.push(node);
                    }
                }
            }
        }

        debug!(
            "Found {} nodes with property {}={:?}",
            nodes.len(),
            key,
            value
        );
        Ok(nodes)
    }

    /// Load index from file
    async fn load_index(&self) -> Result<()> {
        if !self.file_reader.exists(&self.data_path).await {
            debug!("No existing node file found, starting with empty index");
            return Ok(());
        }

        let file_size = self.file_reader.file_size(&self.data_path).await?;
        if file_size == 0 {
            debug!("Node file is empty, no index to load");
            return Ok(());
        }

        let mut file = File::open(&self.data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        let mut offset = 0u64;
        let mut loaded_nodes = 0;

        while offset < file_size {
            // Seek to current position
            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

            // Read length prefix (4 bytes)
            let mut length_bytes = [0u8; 4];
            match file.read_exact(&mut length_bytes).await {
                Ok(_) => {}
                Err(_) => {
                    // End of file or corrupted data
                    warn!(
                        "Could not read length prefix at offset {}, stopping index load",
                        offset
                    );
                    break;
                }
            }

            let data_length = u32::from_le_bytes(length_bytes);
            if data_length == 0 || data_length > 10_000_000 {
                // Sanity check: prevent reading huge chunks
                warn!(
                    "Invalid data length {} at offset {}, stopping index load",
                    data_length, offset
                );
                break;
            }

            // Read the actual data
            let mut data = vec![0u8; data_length as usize];
            match file.read_exact(&mut data).await {
                Ok(_) => {}
                Err(_) => {
                    warn!(
                        "Could not read data of length {} at offset {}, stopping index load",
                        data_length, offset
                    );
                    break;
                }
            }

            // Try to deserialize the node
            match self.deserialize_node_data(&data) {
                Ok(node) => {
                    self.node_index
                        .insert(node.id.clone(), (offset + 4, data_length)); // +4 for length prefix
                    loaded_nodes += 1;
                    trace!("Loaded node {} from offset {}", node.id, offset);
                }
                Err(e) => {
                    warn!("Failed to deserialize node at offset {}: {}", offset, e);
                    break;
                }
            }

            // Move to next record (4 bytes for length + data length)
            offset += 4 + data_length as u64;
        }

        debug!("Loaded {} nodes from index file", loaded_nodes);
        Ok(())
    }

    /// **NEW** Helper method to deserialize node data (handles compression)
    fn deserialize_node_data(&self, data: &[u8]) -> Result<Node> {
        let decompressed = if self.config.compression {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| DatabaseError::Serialization(format!("Decompression failed: {}", e)))?
        } else {
            data.to_vec()
        };
        deserialize(&decompressed)
    }

    /// **FIXED** Read node data at specific offset with known length
    async fn read_node_at_offset(&self, offset: u64, length: u32) -> Result<Vec<u8>> {
        let mut file = File::open(&self.data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        let mut buffer = vec![0u8; length as usize];
        file.read_exact(&mut buffer)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to read data: {}", e)))?;

        Ok(buffer)
    }
}
