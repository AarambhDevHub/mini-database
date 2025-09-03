use crate::client::QueryResult;
use crate::core::Database;
use crate::query::{QueryBuilder, QueryExecutor};
use crate::types::{Edge, Node, Value};
use crate::utils::error::Result;
use tracing::{debug, info};

/// Main client API with CRUD operations and transaction support
#[derive(Clone)]
pub struct DatabaseClient {
    database: Database,
    executor: QueryExecutor,
}

impl DatabaseClient {
    /// Create a new database client
    pub fn new(database: Database) -> Self {
        let executor = QueryExecutor::new(database.clone());
        Self { database, executor }
    }

    /// Create a new node
    pub async fn create_node(&self, node: Node) -> Result<String> {
        let node_id = self.database.node_store().store(node).await?;
        debug!("Client: Created node {}", node_id);
        Ok(node_id)
    }

    /// Get a node by ID
    pub async fn get_node(&self, node_id: &str) -> Result<Option<Node>> {
        let node = self.database.node_store().get(node_id).await?;
        debug!("Client: Retrieved node {}: {:?}", node_id, node.is_some());
        Ok(node)
    }

    /// Update a node
    pub async fn update_node(&self, node: &Node) -> Result<bool> {
        let updated = self.database.node_store().update(node).await?;
        debug!("Client: Updated node {}: {}", node.id, updated);
        Ok(updated)
    }

    /// Delete a node
    pub async fn delete_node(&self, node_id: &str) -> Result<bool> {
        let deleted = self.database.node_store().delete(node_id).await?;
        debug!("Client: Deleted node {}: {}", node_id, deleted);
        Ok(deleted)
    }

    /// Create a new edge
    pub async fn create_edge(&self, edge: Edge) -> Result<String> {
        let edge_id = self.database.edge_store().store(edge).await?;
        debug!("Client: Created edge {}", edge_id);
        Ok(edge_id)
    }

    /// Get an edge by ID
    pub async fn get_edge(&self, edge_id: &str) -> Result<Option<Edge>> {
        let edge = self.database.edge_store().get(edge_id).await?;
        debug!("Client: Retrieved edge {}: {:?}", edge_id, edge.is_some());
        Ok(edge)
    }

    /// Delete an edge
    pub async fn delete_edge(&self, edge_id: &str) -> Result<bool> {
        let deleted = self.database.edge_store().delete(edge_id).await?;
        debug!("Client: Deleted edge {}: {}", edge_id, deleted);
        Ok(deleted)
    }

    /// Get all edges connected to a node
    pub async fn get_node_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let edges = self.database.edge_store().get_node_edges(node_id).await?;
        debug!("Client: Found {} edges for node {}", edges.len(), node_id);
        Ok(edges)
    }

    /// Get outgoing edges from a node
    pub async fn get_outgoing_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let edges = self
            .database
            .edge_store()
            .get_outgoing_edges(node_id)
            .await?;
        debug!(
            "Client: Found {} outgoing edges for node {}",
            edges.len(),
            node_id
        );
        Ok(edges)
    }

    /// Get incoming edges to a node
    pub async fn get_incoming_edges(&self, node_id: &str) -> Result<Vec<Edge>> {
        let edges = self
            .database
            .edge_store()
            .get_incoming_edges(node_id)
            .await?;
        debug!(
            "Client: Found {} incoming edges for node {}",
            edges.len(),
            node_id
        );
        Ok(edges)
    }

    /// Find nodes by label
    pub async fn find_nodes_by_label(&self, label: &str) -> Result<Vec<Node>> {
        let nodes = self.database.node_store().find_by_label(label).await?;
        debug!("Client: Found {} nodes with label '{}'", nodes.len(), label);
        Ok(nodes)
    }

    /// Find nodes by property
    pub async fn find_nodes_by_property(&self, key: &str, value: &Value) -> Result<Vec<Node>> {
        let nodes = self
            .database
            .node_store()
            .find_by_property(key, value)
            .await?;
        debug!(
            "Client: Found {} nodes with property {}={:?}",
            nodes.len(),
            key,
            value
        );
        Ok(nodes)
    }

    /// Find edges by label
    pub async fn find_edges_by_label(&self, label: &str) -> Result<Vec<Edge>> {
        let edges = self.database.edge_store().find_by_label(label).await?;
        debug!("Client: Found {} edges with label '{}'", edges.len(), label);
        Ok(edges)
    }

    /// Execute a query using the query builder
    pub async fn execute_query(&self, query: QueryBuilder) -> Result<QueryResult> {
        info!("Client: Executing query");
        let result = self.executor.execute(query).await?;
        debug!("Client: Query executed successfully");
        Ok(result)
    }

    /// Start a query builder for nodes
    pub fn query_nodes(&self) -> QueryBuilder {
        QueryBuilder::new().select_nodes()
    }

    /// Start a query builder for edges
    pub fn query_edges(&self) -> QueryBuilder {
        QueryBuilder::new().select_edges()
    }

    /// Perform breadth-first search
    pub async fn bfs(&self, start_node_id: &str, max_depth: usize) -> Result<Vec<Node>> {
        use crate::query::GraphOps;
        let graph_ops = GraphOps::new(self.database.clone());
        let result = graph_ops
            .breadth_first_search(start_node_id, max_depth)
            .await?;
        debug!(
            "Client: BFS from {} found {} nodes",
            start_node_id,
            result.len()
        );
        Ok(result)
    }

    /// Perform depth-first search
    pub async fn dfs(&self, start_node_id: &str, max_depth: usize) -> Result<Vec<Node>> {
        use crate::query::GraphOps;
        let graph_ops = GraphOps::new(self.database.clone());
        let result = graph_ops
            .depth_first_search(start_node_id, max_depth)
            .await?;
        debug!(
            "Client: DFS from {} found {} nodes",
            start_node_id,
            result.len()
        );
        Ok(result)
    }

    /// Find shortest path between two nodes
    pub async fn shortest_path(
        &self,
        start_node_id: &str,
        end_node_id: &str,
    ) -> Result<Option<Vec<Node>>> {
        use crate::query::GraphOps;
        let graph_ops = GraphOps::new(self.database.clone());
        let result = graph_ops.shortest_path(start_node_id, end_node_id).await?;
        debug!(
            "Client: Shortest path from {} to {}: {:?}",
            start_node_id,
            end_node_id,
            result.is_some()
        );
        Ok(result)
    }

    /// Get database statistics
    pub async fn get_stats(&self) -> Result<crate::core::DatabaseStats> {
        let stats = self.database.stats().await;
        info!(
            "Client: Retrieved database stats: {} nodes, {} edges",
            stats.node_count, stats.edge_count
        );
        Ok(stats)
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        self.database.flush().await?;
        info!("Client: Database flushed to disk");
        Ok(())
    }

    /// Batch create nodes
    pub async fn batch_create_nodes(&self, nodes: Vec<Node>) -> Result<Vec<String>> {
        let mut node_ids = Vec::new();

        for node in nodes {
            let node_id = self.create_node(node).await?;
            node_ids.push(node_id);
        }

        info!("Client: Batch created {} nodes", node_ids.len());
        Ok(node_ids)
    }

    /// Batch create edges
    pub async fn batch_create_edges(&self, edges: Vec<Edge>) -> Result<Vec<String>> {
        let mut edge_ids = Vec::new();

        for edge in edges {
            let edge_id = self.create_edge(edge).await?;
            edge_ids.push(edge_id);
        }

        info!("Client: Batch created {} edges", edge_ids.len());
        Ok(edge_ids)
    }

    /// Get neighbors of a node within specified distance
    pub async fn get_neighbors(&self, node_id: &str, max_distance: usize) -> Result<Vec<Node>> {
        use crate::query::GraphOps;
        let graph_ops = GraphOps::new(self.database.clone());
        let result = graph_ops.get_neighbors(node_id, max_distance).await?;
        debug!(
            "Client: Found {} neighbors for node {}",
            result.len(),
            node_id
        );
        Ok(result)
    }

    /// Count total nodes in database
    pub async fn count_nodes(&self) -> Result<u64> {
        let count = self.database.node_store().count().await;
        debug!("Client: Node count: {}", count);
        Ok(count)
    }

    /// Count total edges in database
    pub async fn count_edges(&self) -> Result<u64> {
        let count = self.database.edge_store().count().await;
        debug!("Client: Edge count: {}", count);
        Ok(count)
    }

    /// Advanced query with custom filtering
    pub async fn advanced_node_query<F>(&self, filter: F) -> Result<Vec<Node>>
    where
        F: Fn(&Node) -> bool + Send + Sync,
    {
        let mut result_nodes = Vec::new();
        let all_node_ids = self.database.node_store().get_all_ids();

        for node_id in all_node_ids {
            if let Some(node) = self.database.node_store().get(&node_id).await? {
                if filter(&node) {
                    result_nodes.push(node);
                }
            }
        }

        debug!("Client: Advanced query found {} nodes", result_nodes.len());
        Ok(result_nodes)
    }

    /// Advanced query with custom edge filtering
    pub async fn advanced_edge_query<F>(&self, filter: F) -> Result<Vec<Edge>>
    where
        F: Fn(&Edge) -> bool + Send + Sync,
    {
        let mut result_edges = Vec::new();

        for entry in self.database.edge_store().edge_index.iter() {
            let edge_id = entry.key();
            if let Some(edge) = self.database.edge_store().get(edge_id).await? {
                if filter(&edge) {
                    result_edges.push(edge);
                }
            }
        }

        debug!(
            "Client: Advanced edge query found {} edges",
            result_edges.len()
        );
        Ok(result_edges)
    }

    /// Get connected components in the graph
    pub async fn get_connected_components(&self) -> Result<Vec<Vec<Node>>> {
        use crate::query::GraphOps;
        let graph_ops = GraphOps::new(self.database.clone());
        let components = graph_ops.connected_components().await?;
        info!("Client: Found {} connected components", components.len());
        Ok(components)
    }

    /// Check if two nodes are connected
    pub async fn are_nodes_connected(&self, node1_id: &str, node2_id: &str) -> Result<bool> {
        let path = self.shortest_path(node1_id, node2_id).await?;
        Ok(path.is_some())
    }

    /// Get node degree (number of connections)
    pub async fn get_node_degree(&self, node_id: &str) -> Result<usize> {
        let edges = self.get_node_edges(node_id).await?;
        Ok(edges.len())
    }

    /// Get nodes with highest degree (most connections)
    pub async fn get_most_connected_nodes(&self, limit: usize) -> Result<Vec<(Node, usize)>> {
        let mut node_degrees = Vec::new();
        let all_node_ids = self.database.node_store().get_all_ids();

        for node_id in all_node_ids {
            if let Some(node) = self.database.node_store().get(&node_id).await? {
                let degree = self.get_node_degree(&node_id).await?;
                node_degrees.push((node, degree));
            }
        }

        // Sort by degree (descending) and take top N
        node_degrees.sort_by(|a, b| b.1.cmp(&a.1));
        node_degrees.truncate(limit);

        debug!("Client: Found {} most connected nodes", node_degrees.len());
        Ok(node_degrees)
    }

    /// Create a transaction-like batch operation
    pub async fn execute_batch<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(
                &DatabaseClient,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + '_>>
            + Send,
        T: Send,
    {
        // For now, we'll execute the operation directly
        // In a full implementation, this would involve transaction management
        info!("Client: Starting batch operation");
        let result = operation(self).await;

        match &result {
            Ok(_) => {
                self.flush().await?;
                info!("Client: Batch operation completed successfully");
            }
            Err(e) => {
                info!("Client: Batch operation failed: {}", e);
            }
        }

        result
    }

    /// Export data to JSON format
    pub async fn export_to_json(&self) -> Result<String> {
        use std::collections::HashMap as StdHashMap;

        let _stats = self.get_stats().await?;
        let all_node_ids = self.database.node_store().get_all_ids();
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Collect all nodes
        for node_id in all_node_ids {
            if let Some(node) = self.database.node_store().get(&node_id).await? {
                nodes.push(node);
            }
        }

        // Collect all edges
        for entry in self.database.edge_store().edge_index.iter() {
            let edge_id = entry.key();
            if let Some(edge) = self.database.edge_store().get(edge_id).await? {
                edges.push(edge);
            }
        }

        // Create export data structure manually to avoid serde_json macro
        let mut export_data = StdHashMap::new();

        let mut metadata = StdHashMap::new();
        metadata.insert(
            "exported_at".to_string(),
            serde_json::Value::String(chrono::Utc::now().to_rfc3339()),
        );
        metadata.insert(
            "node_count".to_string(),
            serde_json::Value::Number(serde_json::Number::from(nodes.len())),
        );
        metadata.insert(
            "edge_count".to_string(),
            serde_json::Value::Number(serde_json::Number::from(edges.len())),
        );

        export_data.insert(
            "metadata".to_string(),
            serde_json::Value::Object(metadata.into_iter().collect()),
        );
        export_data.insert("nodes".to_string(), serde_json::to_value(&nodes)?);
        export_data.insert("edges".to_string(), serde_json::to_value(&edges)?);

        let json_str = serde_json::to_string_pretty(&export_data)?;
        info!(
            "Client: Exported {} nodes and {} edges to JSON",
            nodes.len(),
            edges.len()
        );
        Ok(json_str)
    }
}
