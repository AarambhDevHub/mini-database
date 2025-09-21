use crate::core::DatabaseConfig;
use crate::storage::{Cache, FileReader};
use crate::types::Edge;
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

/// Edge storage managing relationships and adjacency lists
pub struct EdgeStore {
    config: Arc<DatabaseConfig>,
    file_reader: Arc<FileReader>,
    cache: Arc<Cache>,
    edge_file: Arc<RwLock<File>>,
    pub edge_index: DashMap<String, (u64, u32)>, // edge_id -> (file_offset, data_length)
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

        // Load existing index - crucial for persistence
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

        // Add length prefix for proper record boundaries
        let data_length = compressed.len() as u32;
        let mut record_data = Vec::with_capacity(4 + compressed.len());
        record_data.extend_from_slice(&data_length.to_le_bytes());
        record_data.extend_from_slice(&compressed);

        let mut file = self.edge_file.write().await;
        let offset = file
            .seek(SeekFrom::End(0))
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to seek: {}", e)))?;

        file.write_all(&record_data)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to write edge: {}", e)))?;

        if self.config.sync_writes {
            file.sync_all()
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to sync: {}", e)))?;
        }

        // Update indices and cache - store actual data offset (after length prefix)
        self.edge_index
            .insert(edge.id.clone(), (offset + 4, data_length));
        self.update_adjacency_index(&edge);
        self.cache
            .put(format!("edge:{}", edge.id), compressed.clone());
        self.cache
            .put_index(format!("edge:{}", edge.id), offset + 4);

        trace!(
            "Stored edge {} at offset {} with length {}",
            edge.id, offset, data_length
        );
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

        // Get from file using index
        if let Some((offset, length)) = self.edge_index.get(edge_id).map(|v| *v) {
            trace!(
                "Loading edge {} from offset {} with length {}",
                edge_id, offset, length
            );

            let data = self.read_edge_at_offset(offset, length).await?;
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

    /// Get all edge IDs
    pub fn get_all_ids(&self) -> Vec<String> {
        self.edge_index
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
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

        // Add edge to target node's adjacency list (if different from source)
        if edge.source != edge.target {
            self.adjacency_index
                .entry(edge.target.clone())
                .or_insert_with(Vec::new)
                .push(edge.id.clone());
        }
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

    /// **FIXED** Load index from file with proper record boundary handling
    async fn load_index(&self) -> Result<()> {
        if !self.file_reader.exists(&self.data_path).await {
            debug!("No existing edge file found, starting with empty index");
            return Ok(());
        }

        let file_size = self.file_reader.file_size(&self.data_path).await?;
        if file_size == 0 {
            debug!("Edge file is empty, no index to load");
            return Ok(());
        }

        let mut file = File::open(&self.data_path)
            .await
            .map_err(|e| DatabaseError::Io(format!("Failed to open file: {}", e)))?;

        let mut offset = 0u64;
        let mut loaded_edges = 0;

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

            // Try to deserialize the edge
            match self.deserialize_edge_data(&data) {
                Ok(edge) => {
                    // Store index entry: data starts after the 4-byte length prefix
                    self.edge_index
                        .insert(edge.id.clone(), (offset + 4, data_length));
                    self.update_adjacency_index(&edge);
                    loaded_edges += 1;
                    trace!("Loaded edge {} from offset {}", edge.id, offset);
                }
                Err(e) => {
                    warn!("Failed to deserialize edge at offset {}: {}", offset, e);
                    break;
                }
            }

            // Move to next record (4 bytes for length prefix + data length)
            offset += 4 + data_length as u64;
        }

        debug!("Loaded {} edges from index file", loaded_edges);
        Ok(())
    }

    /// **NEW** Helper method to deserialize edge data (handles compression)
    fn deserialize_edge_data(&self, data: &[u8]) -> Result<Edge> {
        let decompressed = if self.config.compression {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| DatabaseError::Serialization(format!("Decompression failed: {}", e)))?
        } else {
            data.to_vec()
        };
        deserialize(&decompressed)
    }

    /// **FIXED** Read edge data at specific offset with known length
    async fn read_edge_at_offset(&self, offset: u64, length: u32) -> Result<Vec<u8>> {
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

    /// Update an existing edge
    pub async fn update(&self, edge: Edge) -> Result<bool> {
        if !self.edge_index.contains_key(&edge.id) {
            return Ok(false);
        }

        // Remove from adjacency index first (with old edge data)
        if let Some(old_edge) = self.get(&edge.id).await? {
            self.remove_from_adjacency_index(&old_edge);
        }

        // Store the updated edge (this appends the new version)
        self.store(edge.clone()).await?;

        // Update adjacency index with new edge data
        self.update_adjacency_index(&edge);

        debug!("Updated edge: {}", edge.id);
        Ok(true)
    }

    /// Find edges between two specific nodes
    pub async fn find_edges_between(&self, source_id: &str, target_id: &str) -> Result<Vec<Edge>> {
        let mut result = Vec::new();

        if let Some(source_edges) = self.adjacency_index.get(source_id) {
            for edge_id in source_edges.value() {
                if let Some(edge) = self.get(edge_id).await? {
                    if (edge.source == source_id && edge.target == target_id)
                        || (edge.source == target_id && edge.target == source_id)
                    {
                        result.push(edge);
                    }
                }
            }
        }

        debug!(
            "Found {} edges between {} and {}",
            result.len(),
            source_id,
            target_id
        );
        Ok(result)
    }

    /// Get all edges by property value
    pub async fn find_by_property(
        &self,
        key: &str,
        value: &crate::types::Value,
    ) -> Result<Vec<Edge>> {
        let mut edges = Vec::new();

        for entry in self.edge_index.iter() {
            let edge_id = entry.key();
            if let Some(edge) = self.get(edge_id).await? {
                if let Some(prop_value) = edge.properties.get(key) {
                    if prop_value == value {
                        edges.push(edge);
                    }
                }
            }
        }

        debug!(
            "Found {} edges with property {}={:?}",
            edges.len(),
            key,
            value
        );
        Ok(edges)
    }

    /// Get adjacency statistics for debugging
    pub async fn get_adjacency_stats(&self) -> AdjacencyStats {
        let total_nodes = self.adjacency_index.len();
        let total_adjacencies: usize = self
            .adjacency_index
            .iter()
            .map(|entry| entry.value().len())
            .sum();

        let avg_degree = if total_nodes > 0 {
            total_adjacencies as f64 / total_nodes as f64
        } else {
            0.0
        };

        AdjacencyStats {
            total_nodes_with_edges: total_nodes,
            total_adjacencies,
            average_degree: avg_degree,
        }
    }
}

/// Adjacency index statistics
#[derive(Debug, Clone)]
pub struct AdjacencyStats {
    pub total_nodes_with_edges: usize,
    pub total_adjacencies: usize,
    pub average_degree: f64,
}
