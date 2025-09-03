use crate::core::DatabaseConfig;
use crate::storage::{Cache, FileReader};
use crate::types::Edge;
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

/// Edge storage managing relationships and adjacency lists
pub struct EdgeStore {
    config: Arc<DatabaseConfig>,
    file_reader: Arc<FileReader>,
    cache: Arc<Cache>,
    edge_file: Arc<RwLock<File>>,
    pub edge_index: DashMap<String, u64>, // edge_id -> file_offset
    adjacency_index: DashMap<String, Vec<String>>, // node_id -> [edge_ids]
    data_path: PathBuf,
}

impl EdgeStore {
    /// Create a new edge store
    pub async fn new(
        config: Arc<DatabaseConfig>,
        file_reader: Arc<FileReader>,
        cache: Arc<Cache>,
    ) -> Result<Self> {
        let data_path = config.data_dir.join("edges.db");

        let edge_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open edge file: {}", e)))?;

        let store = Self {
            config,
            file_reader,
            cache,
            edge_file: Arc::new(RwLock::new(edge_file)),
            edge_index: DashMap::new(),
            adjacency_index: DashMap::new(),
            data_path,
        };

        // Load existing index
        store.load_index().await?;

        debug!(
            "Edge store initialized with {} edges",
            store.edge_index.len()
        );
        Ok(store)
    }

    /// Store an edge and return its ID
    pub async fn store(&self, mut edge: Edge) -> Result<String> {
        if edge.id.is_empty() {
            edge.id = Uuid::new_v4().to_string();
        }

        let serialized = serialize(&edge)?;
        let compressed = if self.config.compression {
            lz4_flex::compress_prepend_size(&serialized)
        } else {
            serialized
        };

        let mut file = self.edge_file.write().await;
        let offset = file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        file.write_all(&compressed)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to write edge: {}", e)))?;

        if self.config.sync_writes {
            file.sync_all()
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to sync: {}", e)))?;
        }

        // Update indices and cache
        self.edge_index.insert(edge.id.clone(), offset);
        self.update_adjacency_index(&edge);
        self.cache
            .put(format!("edge:{}", edge.id), compressed.clone());
        self.cache.put_index(format!("edge:{}", edge.id), offset);

        trace!("Stored edge {} at offset {}", edge.id, offset);
        Ok(edge.id)
    }

    /// Retrieve an edge by ID
    pub async fn get(&self, edge_id: &str) -> Result<Option<Edge>> {
        let cache_key = format!("edge:{}", edge_id);

        // Try cache first
        if let Some(data) = self.cache.get(&cache_key) {
            trace!("Edge {} found in cache", edge_id);
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
        if let Some(offset) = self.edge_index.get(edge_id) {
            let offset = *offset;
            trace!("Loading edge {} from offset {}", edge_id, offset);

            let data = self.read_edge_at_offset(offset).await?;
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

    /// Get all edges connected to a node
    pub async fn get_node_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let mut edges = Vec::new();

        if let Some(edge_ids) = self.adjacency_index.get(node_id) {
            for edge_id in edge_ids.value() {
                if let Some(edge) = self.get(edge_id).await? {
                    edges.push(edge);
                }
            }
        }

        debug!("Found {} edges for node {}", edges.len(), node_id);
        Ok(edges)
    }

    /// Get outgoing edges from a node
    pub async fn get_outgoing_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let edges = self.get_node_edges(node_id).await?;
        Ok(edges.into_iter().filter(|e| e.source == node_id).collect())
    }

    /// Get incoming edges to a node
    pub async fn get_incoming_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let edges = self.get_node_edges(node_id).await?;
        Ok(edges.into_iter().filter(|e| e.target == node_id).collect())
    }

    /// Find edges by label
    pub async fn find_by_label(&self, label: &str) -> Result<Vec<Edge>> {
        let mut edges = Vec::new();

        for entry in self.edge_index.iter() {
            let edge_id = entry.key();
            if let Some(edge) = self.get(edge_id).await? {
                if edge.label == label {
                    edges.push(edge);
                }
            }
        }

        debug!("Found {} edges with label '{}'", edges.len(), label);
        Ok(edges)
    }

    /// Delete an edge
    pub async fn delete(&self, edge_id: &str) -> Result<bool> {
        if let Some(edge) = self.get(edge_id).await? {
            // Remove from indices
            self.edge_index.remove(edge_id);
            self.remove_from_adjacency_index(&edge);
            self.cache.remove(&format!("edge:{}", edge_id));

            debug!("Deleted edge {}", edge_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get edge count
    pub async fn count(&self) -> u64 {
        self.edge_index.len() as u64
    }

    /// Flush pending writes
    pub async fn flush(&self) -> Result<()> {
        let file = self.edge_file.write().await;
        file.sync_all()
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to flush: {}", e)))?;
        Ok(())
    }

    /// Update adjacency index for an edge
    fn update_adjacency_index(&self, edge: &Edge) {
        // Add edge to source node's adjacency list
        self.adjacency_index
            .entry(edge.source.clone())
            .or_insert_with(Vec::new)
            .push(edge.id.clone());

        // Add edge to target node's adjacency list
        self.adjacency_index
            .entry(edge.target.clone())
            .or_insert_with(Vec::new)
            .push(edge.id.clone());
    }

    /// Remove edge from adjacency index
    fn remove_from_adjacency_index(&self, edge: &Edge) {
        if let Some(mut source_edges) = self.adjacency_index.get_mut(&edge.source) {
            source_edges.retain(|id| id != &edge.id);
        }

        if let Some(mut target_edges) = self.adjacency_index.get_mut(&edge.target) {
            target_edges.retain(|id| id != &edge.id);
        }
    }

    /// Load index from file
    async fn load_index(&self) -> Result<()> {
        if !self.file_reader.exists(&self.data_path).await {
            return Ok(());
        }

        let mut offset = 0u64;

        loop {
            match self.read_edge_at_offset(offset).await {
                Ok(data) => {
                    let decompressed = if self.config.compression {
                        lz4_flex::decompress_size_prepended(&data).map_err(|e| {
                            DatabaseError::Serialization(format!("Decompression failed: {}", e))
                        })?
                    } else {
                        data.clone()
                    };

                    if let Ok(edge) = deserialize::<Edge>(&decompressed) {
                        self.edge_index.insert(edge.id.clone(), offset);
                        self.update_adjacency_index(&edge);
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

        debug!("Loaded {} edges from index", self.edge_index.len());
        Ok(())
    }

    /// Read edge data at specific offset
    async fn read_edge_at_offset(&self, offset: u64) -> Result<Vec<u8>> {
        const CHUNK_SIZE: usize = 8192;
        self.file_reader
            .read_chunk(&self.data_path, offset, CHUNK_SIZE)
            .await
    }
}
