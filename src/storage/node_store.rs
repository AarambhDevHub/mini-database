use crate::core::DatabaseConfig;
use crate::storage::{Cache, FileReader};
use crate::types::{Node, Value};
use crate::utils::error::{DatabaseError, Result};
use crate::utils::serde::{deserialize, serialize};
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::RwLock;
use tracing::{debug, trace};
use uuid::Uuid;

/// Node storage with serialization, indexing, and efficient retrieval
pub struct NodeStore {
    config: Arc<DatabaseConfig>,
    file_reader: Arc<FileReader>,
    cache: Arc<Cache>,
    node_file: Arc<RwLock<File>>,
    node_index: DashMap<String, u64>, // node_id -> file_offset
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

        // Load existing index
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

        let mut file = self.node_file.write().await;
        let offset = file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        file.write_all(&compressed)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to write node: {}", e)))?;

        if self.config.sync_writes {
            file.sync_all()
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to sync: {}", e)))?;
        }

        // Update index and cache
        self.node_index.insert(node.id.clone(), offset);
        self.cache
            .put(format!("node:{}", node.id), compressed.clone());
        self.cache.put_index(format!("node:{}", node.id), offset);

        trace!("Stored node {} at offset {}", node.id, offset);
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

        // Get from file
        if let Some(offset) = self.node_index.get(node_id) {
            let offset = *offset;
            trace!("Loading node {} from offset {}", node_id, offset);

            let data = self.read_node_at_offset(offset).await?;
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
            return Ok(());
        }

        let mut file = File::open(&self.data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        let mut offset = 0u64;

        loop {
            file.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

            // Try to read node at current offset
            match self.read_node_at_offset(offset).await {
                Ok(data) => {
                    let decompressed = if self.config.compression {
                        lz4_flex::decompress_size_prepended(&data).map_err(|e| {
                            DatabaseError::Serialization(format!("Decompression failed: {}", e))
                        })?
                    } else {
                        data.clone()
                    };

                    if let Ok(node) = deserialize::<Node>(&decompressed) {
                        self.node_index.insert(node.id.clone(), offset);
                        offset += if self.config.compression {
                            data.len() as u64
                        } else {
                            decompressed.len() as u64
                        };
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        debug!("Loaded {} nodes from index", self.node_index.len());
        Ok(())
    }

    /// Read node data at specific offset
    async fn read_node_at_offset(&self, offset: u64) -> Result<Vec<u8>> {
        // For simplicity, read a reasonable chunk and find the node boundary
        const CHUNK_SIZE: usize = 8192;
        self.file_reader
            .read_chunk(&self.data_path, offset, CHUNK_SIZE)
            .await
    }
}
