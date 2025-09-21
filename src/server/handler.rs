use crate::client::DatabaseClient;
use crate::core::Database;
use crate::server::{ConnectionPool, Request, Response};
use crate::transaction::{TransactionIsolationLevel, TransactionManager};
use crate::types::{Edge, Node};
use crate::utils::error::Result;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Request handler for database operations with transaction support
pub struct RequestHandler {
    client: Arc<DatabaseClient>,
    connection_pool: Arc<ConnectionPool>,
    transaction_manager: Arc<RwLock<Option<TransactionManager>>>,
    active_transactions: Arc<RwLock<HashMap<String, String>>>, // connection_id -> transaction_id
}

impl RequestHandler {
    /// Create new request handler with transaction support
    pub fn new(database: Arc<Database>, connection_pool: Arc<ConnectionPool>) -> Self {
        let client = Arc::new(DatabaseClient::new(database.as_ref().clone()));
        Self {
            client,
            connection_pool,
            transaction_manager: Arc::new(RwLock::new(None)),
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
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
                    client_id, connection_id
                );
                Response::Connected {
                    session_id: client_id,
                }
            }
            Request::Disconnect { client_id } => {
                // End any active transaction for this connection
                if let Some(tx_id) = self.active_transactions.read().await.get(connection_id) {
                    if let Some(tx_manager) = &*self.transaction_manager.read().await {
                        if let Err(e) = tx_manager.abort_transaction(tx_id).await {
                            warn!(
                                "Failed to abort transaction {} during disconnect: {}",
                                tx_id, e
                            );
                        }
                    }
                    self.active_transactions.write().await.remove(connection_id);
                }
                info!("Client {} disconnecting", connection_id);
                Response::Disconnected
            }
            Request::Ping => {
                debug!("Ping from {}", connection_id);
                Response::Pong
            }

            // Transaction operations
            Request::BeginTransaction => self.handle_begin_transaction(connection_id).await,
            Request::CommitTransaction => self.handle_commit_transaction(connection_id).await,
            Request::RollbackTransaction => self.handle_rollback_transaction(connection_id).await,

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
        }
    }

    /// Handle begin transaction request
    async fn handle_begin_transaction(&self, connection_id: &str) -> Response {
        // Check if there's already an active transaction for this connection
        if self
            .active_transactions
            .read()
            .await
            .contains_key(connection_id)
        {
            return Response::Error {
                message: "Transaction already active for this connection".to_string(),
                code: 400,
            };
        }

        match &*self.transaction_manager.read().await {
            Some(tx_manager) => {
                match tx_manager
                    .begin_transaction(TransactionIsolationLevel::ReadCommitted)
                    .await
                {
                    Ok(transaction_id) => {
                        // Associate transaction with connection
                        self.active_transactions
                            .write()
                            .await
                            .insert(connection_id.to_string(), transaction_id.clone());

                        info!(
                            "Started transaction {} for connection {}",
                            transaction_id, connection_id
                        );
                        Response::Ok
                    }
                    Err(e) => {
                        error!(
                            "Failed to begin transaction for connection {}: {}",
                            connection_id, e
                        );
                        Response::Error {
                            message: format!("Failed to begin transaction: {}", e),
                            code: 500,
                        }
                    }
                }
            }
            None => {
                error!("Transaction manager not initialized");
                Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                }
            }
        }
    }

    /// Handle commit transaction request
    async fn handle_commit_transaction(&self, connection_id: &str) -> Response {
        let transaction_id = {
            match self.active_transactions.write().await.remove(connection_id) {
                Some(tx_id) => tx_id,
                None => {
                    return Response::Error {
                        message: "No active transaction for this connection".to_string(),
                        code: 400,
                    };
                }
            }
        };

        match &*self.transaction_manager.read().await {
            Some(tx_manager) => match tx_manager.commit_transaction(&transaction_id).await {
                Ok(()) => {
                    info!(
                        "Committed transaction {} for connection {}",
                        transaction_id, connection_id
                    );
                    Response::Ok
                }
                Err(e) => {
                    error!(
                        "Failed to commit transaction {} for connection {}: {}",
                        transaction_id, connection_id, e
                    );
                    Response::Error {
                        message: format!("Failed to commit transaction: {}", e),
                        code: 500,
                    }
                }
            },
            None => {
                // Re-add transaction back to active list since manager is not available
                self.active_transactions
                    .write()
                    .await
                    .insert(connection_id.to_string(), transaction_id);
                Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                }
            }
        }
    }

    /// Handle rollback transaction request
    async fn handle_rollback_transaction(&self, connection_id: &str) -> Response {
        let transaction_id = {
            match self.active_transactions.write().await.remove(connection_id) {
                Some(tx_id) => tx_id,
                None => {
                    return Response::Error {
                        message: "No active transaction for this connection".to_string(),
                        code: 400,
                    };
                }
            }
        };

        match &*self.transaction_manager.read().await {
            Some(tx_manager) => match tx_manager.abort_transaction(&transaction_id).await {
                Ok(()) => {
                    info!(
                        "Rolled back transaction {} for connection {}",
                        transaction_id, connection_id
                    );
                    Response::Ok
                }
                Err(e) => {
                    error!(
                        "Failed to rollback transaction {} for connection {}: {}",
                        transaction_id, connection_id, e
                    );
                    Response::Error {
                        message: format!("Failed to rollback transaction: {}", e),
                        code: 500,
                    }
                }
            },
            None => {
                // Re-add transaction back to active list since manager is not available
                self.active_transactions
                    .write()
                    .await
                    .insert(connection_id.to_string(), transaction_id);
                Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                }
            }
        }
    }

    /// Handle create node request (with transaction support)
    async fn handle_create_node(&self, node: Node, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => {
                    match tx_manager
                        .create_node_tx(transaction_id, node.clone())
                        .await
                    {
                        Ok(node_id) => {
                            info!(
                                "Created node {} in transaction {} for client {}",
                                node_id, transaction_id, connection_id
                            );
                            Response::NodeCreated { node_id }
                        }
                        Err(e) => {
                            error!(
                                "Failed to create node in transaction {} for client {}: {}",
                                transaction_id, connection_id, e
                            );
                            Response::Error {
                                message: e.to_string(),
                                code: 500,
                            }
                        }
                    }
                }
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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
    }

    /// Handle get node request (with transaction support)
    async fn handle_get_node(&self, node_id: &str, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => match tx_manager.get_node_tx(transaction_id, node_id).await {
                    Ok(node) => {
                        debug!(
                            "Retrieved node {} in transaction {} for client {}",
                            node_id, transaction_id, connection_id
                        );
                        Response::NodeData { node }
                    }
                    Err(e) => {
                        error!(
                            "Failed to get node {} in transaction {} for client {}: {}",
                            node_id, transaction_id, connection_id, e
                        );
                        Response::Error {
                            message: e.to_string(),
                            code: 500,
                        }
                    }
                },
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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
    }

    /// Handle update node request (with transaction support)
    async fn handle_update_node(&self, node: Node, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => {
                    match tx_manager
                        .update_node_tx(transaction_id, node.clone())
                        .await
                    {
                        Ok(updated) => {
                            if updated {
                                info!(
                                    "Updated node {} in transaction {} for client {}",
                                    node.id, transaction_id, connection_id
                                );
                                Response::Ok
                            } else {
                                Response::NotFound {
                                    resource: format!("Node {}", node.id),
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "Failed to update node {} in transaction {} for client {}: {}",
                                node.id, transaction_id, connection_id, e
                            );
                            Response::Error {
                                message: e.to_string(),
                                code: 500,
                            }
                        }
                    }
                }
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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
    }

    /// Handle delete node request (with transaction support)
    async fn handle_delete_node(&self, node_id: &str, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => {
                    match tx_manager.delete_node_tx(transaction_id, node_id).await {
                        Ok(deleted) => {
                            if deleted {
                                info!(
                                    "Deleted node {} in transaction {} for client {}",
                                    node_id, transaction_id, connection_id
                                );
                                Response::Ok
                            } else {
                                Response::NotFound {
                                    resource: format!("Node {}", node_id),
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "Failed to delete node {} in transaction {} for client {}: {}",
                                node_id, transaction_id, connection_id, e
                            );
                            Response::Error {
                                message: e.to_string(),
                                code: 500,
                            }
                        }
                    }
                }
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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

    /// Handle create edge request (with transaction support)
    async fn handle_create_edge(&self, edge: Edge, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => {
                    match tx_manager
                        .create_edge_tx(transaction_id, edge.clone())
                        .await
                    {
                        Ok(edge_id) => {
                            info!(
                                "Created edge {} in transaction {} for client {}",
                                edge_id, transaction_id, connection_id
                            );
                            Response::EdgeCreated { edge_id }
                        }
                        Err(e) => {
                            error!(
                                "Failed to create edge in transaction {} for client {}: {}",
                                transaction_id, connection_id, e
                            );
                            Response::Error {
                                message: e.to_string(),
                                code: 500,
                            }
                        }
                    }
                }
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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
    }

    /// Handle get edge request (with transaction support)
    async fn handle_get_edge(&self, edge_id: &str, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => match tx_manager.get_edge_tx(transaction_id, edge_id).await {
                    Ok(edge) => {
                        debug!(
                            "Retrieved edge {} in transaction {} for client {}",
                            edge_id, transaction_id, connection_id
                        );
                        Response::EdgeData { edge }
                    }
                    Err(e) => {
                        error!(
                            "Failed to get edge {} in transaction {} for client {}: {}",
                            edge_id, transaction_id, connection_id, e
                        );
                        Response::Error {
                            message: e.to_string(),
                            code: 500,
                        }
                    }
                },
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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
    }

    /// Handle delete edge request (with transaction support)
    async fn handle_delete_edge(&self, edge_id: &str, connection_id: &str) -> Response {
        // Check if there's an active transaction
        if let Some(transaction_id) = self.active_transactions.read().await.get(connection_id) {
            // Use transactional operation
            match &*self.transaction_manager.read().await {
                Some(tx_manager) => {
                    match tx_manager.delete_edge_tx(transaction_id, edge_id).await {
                        Ok(deleted) => {
                            if deleted {
                                info!(
                                    "Deleted edge {} in transaction {} for client {}",
                                    edge_id, transaction_id, connection_id
                                );
                                Response::Ok
                            } else {
                                Response::NotFound {
                                    resource: format!("Edge {}", edge_id),
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "Failed to delete edge {} in transaction {} for client {}: {}",
                                edge_id, transaction_id, connection_id, e
                            );
                            Response::Error {
                                message: e.to_string(),
                                code: 500,
                            }
                        }
                    }
                }
                None => Response::Error {
                    message: "Transaction manager not available".to_string(),
                    code: 503,
                },
            }
        } else {
            // Use non-transactional operation
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
                    "Shortest path from {} to {} found {} steps for client {}",
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
                            return Response::Error {
                                message: format!("Unknown query type: {}", query_type),
                                code: 400,
                            };
                        }
                        _ => {}
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
            Ok(()) => {
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
        let active_tx_count = self.active_transactions.read().await.len();
        let db_stats = self.client.get_stats().await.unwrap();

        HandlerStats {
            active_connections: pool_stats.active_connections,
            total_queries: pool_stats.total_queries,
            node_count: db_stats.node_count,
            edge_count: db_stats.edge_count,
            active_transactions: active_tx_count,
        }
    }

    /// Get active transaction for connection (if any)
    pub async fn get_active_transaction(&self, connection_id: &str) -> Option<String> {
        self.active_transactions
            .read()
            .await
            .get(connection_id)
            .cloned()
    }

    /// Clean up connection (abort any active transaction)
    pub async fn cleanup_connection(&self, connection_id: &str) -> Result<()> {
        if let Some(tx_id) = self.active_transactions.write().await.remove(connection_id) {
            if let Some(tx_manager) = &*self.transaction_manager.read().await {
                tx_manager.abort_transaction(&tx_id).await?;
                warn!(
                    "Aborted transaction {} for disconnected connection {}",
                    tx_id, connection_id
                );
            }
        }
        Ok(())
    }
}

/// Handler statistics
#[derive(Debug, Clone)]
pub struct HandlerStats {
    pub active_connections: usize,
    pub total_queries: u64,
    pub node_count: u64,
    pub edge_count: u64,
    pub active_transactions: usize,
}
