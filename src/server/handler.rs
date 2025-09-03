use crate::client::DatabaseClient;
use crate::core::Database;
use crate::server::{ConnectionPool, Request, Response};
use crate::types::{Edge, Node};

use serde_json;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Request handler for database operations
pub struct RequestHandler {
    client: Arc<DatabaseClient>,
    connection_pool: Arc<ConnectionPool>,
}

impl RequestHandler {
    /// Create new request handler
    pub fn new(database: Arc<Database>, connection_pool: Arc<ConnectionPool>) -> Self {
        let client = Arc::new(DatabaseClient::new(database.as_ref().clone()));
        Self {
            client,
            connection_pool,
        }
    }

    /// Handle incoming request and return response
    pub async fn handle_request(&self, request: Request, connection_id: &str) -> Response {
        // Update connection activity
        self.connection_pool.update_activity(connection_id).await;

        debug!("Processing request from {}: {:?}", connection_id, request);

        match request {
            // Connection management
            Request::Connect { client_id } => {
                info!(
                    "Client {} connected with session {}",
                    connection_id, client_id
                );
                Response::Connected {
                    session_id: client_id,
                }
            }

            Request::Disconnect { client_id: _ } => {
                info!("Client {} disconnecting", connection_id);
                Response::Disconnected
            }

            Request::Ping => {
                debug!("Ping from {}", connection_id);
                Response::Pong
            }

            // Node operations
            Request::CreateNode { node } => self.handle_create_node(node, connection_id).await,

            Request::GetNode { node_id } => self.handle_get_node(&node_id, connection_id).await,

            Request::UpdateNode { node } => self.handle_update_node(node, connection_id).await,

            Request::DeleteNode { node_id } => {
                self.handle_delete_node(&node_id, connection_id).await
            }

            Request::FindNodesByLabel { label } => {
                self.handle_find_nodes_by_label(&label, connection_id).await
            }

            // Edge operations
            Request::CreateEdge { edge } => self.handle_create_edge(edge, connection_id).await,

            Request::GetEdge { edge_id } => self.handle_get_edge(&edge_id, connection_id).await,

            Request::DeleteEdge { edge_id } => {
                self.handle_delete_edge(&edge_id, connection_id).await
            }

            Request::GetNodeEdges { node_id } => {
                self.handle_get_node_edges(&node_id, connection_id).await
            }

            // Graph operations
            Request::BreadthFirstSearch {
                start_node,
                max_depth,
            } => self.handle_bfs(&start_node, max_depth, connection_id).await,

            Request::DepthFirstSearch {
                start_node,
                max_depth,
            } => self.handle_dfs(&start_node, max_depth, connection_id).await,

            Request::ShortestPath {
                start_node,
                end_node,
            } => {
                self.handle_shortest_path(&start_node, &end_node, connection_id)
                    .await
            }

            // Query operations
            Request::ExecuteQuery { query } => {
                self.handle_execute_query(&query, connection_id).await
            }

            // Management operations
            Request::GetStats => self.handle_get_stats(connection_id).await,

            Request::Flush => self.handle_flush(connection_id).await,

            Request::Export => self.handle_export(connection_id).await,

            // Transaction operations (future implementation)
            Request::BeginTransaction => {
                warn!("Transaction support not yet implemented");
                Response::Error {
                    message: "Transactions not yet implemented".to_string(),
                    code: 501,
                }
            }

            Request::CommitTransaction => Response::Error {
                message: "Transactions not yet implemented".to_string(),
                code: 501,
            },

            Request::RollbackTransaction => Response::Error {
                message: "Transactions not yet implemented".to_string(),
                code: 501,
            },
        }
    }

    /// Handle create node request
    async fn handle_create_node(&self, node: Node, connection_id: &str) -> Response {
        match self.client.create_node(node.clone()).await {
            Ok(node_id) => {
                info!("Created node {} for client {}", node_id, connection_id);
                Response::NodeCreated { node_id }
            }
            Err(e) => {
                error!("Failed to create node for client {}: {}", connection_id, e);
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle get node request
    async fn handle_get_node(&self, node_id: &str, connection_id: &str) -> Response {
        match self.client.get_node(node_id).await {
            Ok(node) => {
                debug!("Retrieved node {} for client {}", node_id, connection_id);
                Response::NodeData { node }
            }
            Err(e) => {
                error!(
                    "Failed to get node {} for client {}: {}",
                    node_id, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle update node request
    async fn handle_update_node(&self, node: Node, connection_id: &str) -> Response {
        match self.client.update_node(&node).await {
            Ok(updated) => {
                if updated {
                    info!("Updated node {} for client {}", node.id, connection_id);
                    Response::Ok
                } else {
                    Response::NotFound {
                        resource: format!("Node {}", node.id),
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to update node {} for client {}: {}",
                    node.id, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle delete node request
    async fn handle_delete_node(&self, node_id: &str, connection_id: &str) -> Response {
        match self.client.delete_node(node_id).await {
            Ok(deleted) => {
                if deleted {
                    info!("Deleted node {} for client {}", node_id, connection_id);
                    Response::Ok
                } else {
                    Response::NotFound {
                        resource: format!("Node {}", node_id),
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to delete node {} for client {}: {}",
                    node_id, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle find nodes by label request
    async fn handle_find_nodes_by_label(&self, label: &str, connection_id: &str) -> Response {
        match self.client.find_nodes_by_label(label).await {
            Ok(nodes) => {
                debug!(
                    "Found {} nodes with label '{}' for client {}",
                    nodes.len(),
                    label,
                    connection_id
                );
                Response::NodesData { nodes }
            }
            Err(e) => {
                error!(
                    "Failed to find nodes by label '{}' for client {}: {}",
                    label, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle create edge request
    async fn handle_create_edge(&self, edge: Edge, connection_id: &str) -> Response {
        match self.client.create_edge(edge.clone()).await {
            Ok(edge_id) => {
                info!("Created edge {} for client {}", edge_id, connection_id);
                Response::EdgeCreated { edge_id }
            }
            Err(e) => {
                error!("Failed to create edge for client {}: {}", connection_id, e);
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle get edge request
    async fn handle_get_edge(&self, edge_id: &str, connection_id: &str) -> Response {
        match self.client.get_edge(edge_id).await {
            Ok(edge) => {
                debug!("Retrieved edge {} for client {}", edge_id, connection_id);
                Response::EdgeData { edge }
            }
            Err(e) => {
                error!(
                    "Failed to get edge {} for client {}: {}",
                    edge_id, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle delete edge request
    async fn handle_delete_edge(&self, edge_id: &str, connection_id: &str) -> Response {
        match self.client.delete_edge(edge_id).await {
            Ok(deleted) => {
                if deleted {
                    info!("Deleted edge {} for client {}", edge_id, connection_id);
                    Response::Ok
                } else {
                    Response::NotFound {
                        resource: format!("Edge {}", edge_id),
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to delete edge {} for client {}: {}",
                    edge_id, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle get node edges request
    async fn handle_get_node_edges(&self, node_id: &str, connection_id: &str) -> Response {
        match self.client.get_node_edges(node_id).await {
            Ok(edges) => {
                debug!(
                    "Retrieved {} edges for node {} for client {}",
                    edges.len(),
                    node_id,
                    connection_id
                );
                Response::EdgesData { edges }
            }
            Err(e) => {
                error!(
                    "Failed to get edges for node {} for client {}: {}",
                    node_id, connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle breadth-first search request
    async fn handle_bfs(
        &self,
        start_node: &str,
        max_depth: usize,
        connection_id: &str,
    ) -> Response {
        match self.client.bfs(start_node, max_depth).await {
            Ok(nodes) => {
                info!(
                    "BFS from {} found {} nodes for client {}",
                    start_node,
                    nodes.len(),
                    connection_id
                );
                Response::TraversalResult { nodes }
            }
            Err(e) => {
                error!("BFS failed for client {}: {}", connection_id, e);
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle depth-first search request
    async fn handle_dfs(
        &self,
        start_node: &str,
        max_depth: usize,
        connection_id: &str,
    ) -> Response {
        match self.client.dfs(start_node, max_depth).await {
            Ok(nodes) => {
                info!(
                    "DFS from {} found {} nodes for client {}",
                    start_node,
                    nodes.len(),
                    connection_id
                );
                Response::TraversalResult { nodes }
            }
            Err(e) => {
                error!("DFS failed for client {}: {}", connection_id, e);
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle shortest path request
    async fn handle_shortest_path(
        &self,
        start_node: &str,
        end_node: &str,
        connection_id: &str,
    ) -> Response {
        match self.client.shortest_path(start_node, end_node).await {
            Ok(Some(nodes)) => {
                info!(
                    "Shortest path from {} to {} found ({} steps) for client {}",
                    start_node,
                    end_node,
                    nodes.len() - 1,
                    connection_id
                );
                Response::PathFound { nodes }
            }
            Ok(None) => {
                debug!(
                    "No path found from {} to {} for client {}",
                    start_node, end_node, connection_id
                );
                Response::NoPathFound
            }
            Err(e) => {
                error!(
                    "Shortest path search failed for client {}: {}",
                    connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle execute query request
    async fn handle_execute_query(&self, query: &str, connection_id: &str) -> Response {
        // For now, we'll implement a simple JSON-based query format
        // In a full implementation, you'd have a proper query parser
        match serde_json::from_str::<serde_json::Value>(query) {
            Ok(query_json) => {
                // This is a simplified query handler
                // You could implement a proper query DSL here
                debug!("Executing query for client {}: {}", connection_id, query);

                // Example: {"type": "find_nodes", "label": "person"}
                if let Some(query_type) = query_json.get("type").and_then(|t| t.as_str()) {
                    match query_type {
                        "find_nodes" => {
                            if let Some(label) = query_json.get("label").and_then(|l| l.as_str()) {
                                return self.handle_find_nodes_by_label(label, connection_id).await;
                            }
                        }
                        _ => {
                            return Response::Error {
                                message: format!("Unknown query type: {}", query_type),
                                code: 400,
                            };
                        }
                    }
                }

                Response::Error {
                    message: "Invalid query format".to_string(),
                    code: 400,
                }
            }
            Err(e) => {
                error!("Failed to parse query for client {}: {}", connection_id, e);
                Response::Error {
                    message: format!("Query parsing error: {}", e),
                    code: 400,
                }
            }
        }
    }

    /// Handle get stats request
    async fn handle_get_stats(&self, connection_id: &str) -> Response {
        match self.client.get_stats().await {
            Ok(stats) => {
                debug!("Retrieved stats for client {}", connection_id);
                Response::Stats { stats }
            }
            Err(e) => {
                error!("Failed to get stats for client {}: {}", connection_id, e);
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle flush request
    async fn handle_flush(&self, connection_id: &str) -> Response {
        match self.client.flush().await {
            Ok(_) => {
                info!("Database flushed for client {}", connection_id);
                Response::Ok
            }
            Err(e) => {
                error!(
                    "Failed to flush database for client {}: {}",
                    connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Handle export request
    async fn handle_export(&self, connection_id: &str) -> Response {
        match self.client.export_to_json().await {
            Ok(data) => {
                info!("Database exported for client {}", connection_id);
                Response::ExportData { data }
            }
            Err(e) => {
                error!(
                    "Failed to export database for client {}: {}",
                    connection_id, e
                );
                Response::Error {
                    message: e.to_string(),
                    code: 500,
                }
            }
        }
    }

    /// Get handler statistics
    pub async fn get_handler_stats(&self) -> HandlerStats {
        let pool_stats = self.connection_pool.get_stats().await;
        let db_stats = self.client.get_stats().await.unwrap_or_default();

        HandlerStats {
            active_connections: pool_stats.active_connections,
            total_queries: pool_stats.total_queries,
            node_count: db_stats.node_count,
            edge_count: db_stats.edge_count,
        }
    }
}

/// Handler statistics
#[derive(Debug, Clone)]
pub struct HandlerStats {
    pub active_connections: usize,
    pub total_queries: u64,
    pub node_count: u64,
    pub edge_count: u64,
}

impl Default for crate::core::DatabaseStats {
    fn default() -> Self {
        Self {
            node_count: 0,
            edge_count: 0,
            cache_hit_rate: 0.0,
            cache_size: 0,
            cache_capacity: 0,
        }
    }
}
