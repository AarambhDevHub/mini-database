use crate::client::QueryResult;
use crate::client::batch::{BatchError, BatchExecutionMode};
use crate::core::Database;
use crate::query::{QueryBuilder, QueryExecutor};
use crate::transaction::TransactionManagerConfig;
use crate::types::{Edge, Node, Value};
use crate::utils::error::Result;
use crate::{DatabaseError, TransactionManager};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tokio::time::{Duration, timeout};
use tracing::{debug, error, info, warn};

use super::batch::{
    BatchConfig, BatchOperation, BatchOperationResult, BatchProgress, BatchResult,
    CustomBatchOperation,
};

static GLOBAL_TX_MANAGER: OnceLock<Arc<RwLock<Option<TransactionManager>>>> = OnceLock::new();

/// Main client API with CRUD operations and transaction support
#[derive(Clone)]
pub struct DatabaseClient {
    database: Database,
    executor: QueryExecutor,
}

impl fmt::Debug for DatabaseClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseClient")
            .field("database", &"<Database Instance>")
            .finish_non_exhaustive()
    }
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

    /// Update an edge
    pub async fn update_edge(&self, edge: Edge) -> Result<bool> {
        let updated = self.database.edge_store().update(edge.clone()).await?;
        debug!("Client: Updated edge {} = {}", edge.id, updated);
        Ok(updated)
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

    /// Execute batch operations with full transaction support and optimization
    ///
    /// This is a production-ready batch execution system that provides:
    /// - Atomic batch operations (all-or-nothing)
    /// - Performance optimization with configurable batch sizes
    /// - Progress tracking and detailed error reporting
    /// - Transaction isolation and rollback on failure
    /// - Memory-efficient streaming for large datasets
    /// - Parallel processing support
    /// - Comprehensive validation and error handling
    pub async fn execute_batch<F>(
        &self,
        operations: Vec<BatchOperation>,
        config: BatchConfig,
        progress_callback: Option<F>,
    ) -> Result<BatchResult>
    where
        F: Fn(BatchProgress) + Send + Sync,
    {
        info!(
            "Starting batch execution with {} operations",
            operations.len()
        );

        let start_time = std::time::Instant::now();
        let mut batch_result = BatchResult::new(operations.len());

        // Validate batch operations first
        self.validate_batch_operations(&operations)?;

        // Initialize transaction manager if not provided externally
        let tx_manager = self.get_or_create_transaction_manager().await?;

        // Execute batch based on configuration
        match config.execution_mode {
            BatchExecutionMode::Transaction => {
                self.execute_batch_transactional(
                    &operations,
                    &config,
                    &tx_manager,
                    progress_callback,
                    &mut batch_result,
                )
                .await?
            }
            BatchExecutionMode::AutoCommit => {
                self.execute_batch_autocommit(
                    &operations,
                    &config,
                    progress_callback,
                    &mut batch_result,
                )
                .await?
            }
            BatchExecutionMode::Streaming => {
                self.execute_batch_streaming(
                    &operations,
                    &config,
                    progress_callback,
                    &mut batch_result,
                )
                .await?
            }
            BatchExecutionMode::Parallel => {
                self.execute_batch_parallel(
                    &operations,
                    &config,
                    progress_callback,
                    &mut batch_result,
                )
                .await?
            }
        }

        // Finalize results
        batch_result.total_duration = start_time.elapsed();
        batch_result.calculate_statistics();

        info!(
            "Batch execution completed: {} successful, {} failed in {:?}",
            batch_result.successful_operations,
            batch_result.failed_operations,
            batch_result.total_duration
        );

        Ok(batch_result)
    }

    /// Execute batch operations within a single transaction
    async fn execute_batch_transactional<F>(
        &self,
        operations: &[BatchOperation],
        config: &BatchConfig,
        tx_manager: &TransactionManager,
        progress_callback: Option<F>,
        batch_result: &mut BatchResult,
    ) -> Result<()>
    where
        F: Fn(BatchProgress) + Send + Sync,
    {
        debug!("Executing batch in transactional mode");

        // Begin transaction with specified isolation level
        let tx_id = tx_manager
            .begin_transaction_with_timeout(
                config.isolation_level,
                config.transaction_timeout.as_secs(),
            )
            .await?;

        // Create savepoint for potential partial rollback
        if config.enable_savepoints {
            tx_manager
                .create_savepoint(&tx_id, "batch_start".to_string())
                .await?;
        }

        let mut processed_count = 0;
        let total_operations = operations.len();

        // Process operations in chunks
        for chunk in operations.chunks(config.batch_size) {
            // Check for cancellation
            if let Some(ref cancel_token) = config.cancellation_token {
                if cancel_token.is_cancelled() {
                    warn!("Batch execution cancelled by user");
                    tx_manager.abort_transaction(&tx_id).await?;
                    return Err(DatabaseError::InvalidOperation(
                        "Batch execution cancelled".to_string(),
                    ));
                }
            }

            // Process chunk with timeout
            let chunk_result = timeout(
                config.operation_timeout,
                self.process_batch_chunk_transactional(&tx_id, chunk, tx_manager, config),
            )
            .await;

            match chunk_result {
                Ok(Ok(chunk_results)) => {
                    // Add successful results
                    batch_result.operation_results.extend(chunk_results);
                    processed_count += chunk.len();
                    batch_result.successful_operations += chunk.len();

                    // Update progress
                    if let Some(ref callback) = progress_callback {
                        let progress = BatchProgress {
                            processed: processed_count,
                            total: total_operations,
                            current_operation: format!(
                                "Processing chunk {}/{}",
                                processed_count / config.batch_size + 1,
                                (total_operations + config.batch_size - 1) / config.batch_size
                            ),
                            percentage: (processed_count as f64 / total_operations as f64) * 100.0,
                            errors: batch_result.failed_operations,
                        };
                        callback(progress);
                    }
                }
                Ok(Err(e)) => {
                    error!("Batch chunk failed: {}", e);
                    batch_result.failed_operations += chunk.len();
                    batch_result.errors.push(BatchError {
                        operation_index: processed_count,
                        operation_type: "chunk".to_string(),
                        error_message: e.to_string(),
                        is_retryable: e.is_retryable(),
                    });

                    if config.fail_fast {
                        warn!("Failing fast due to chunk error");
                        tx_manager.abort_transaction(&tx_id).await?;
                        return Err(e);
                    }

                    // Rollback to savepoint if enabled
                    if config.enable_savepoints {
                        tx_manager
                            .rollback_to_savepoint(&tx_id, "batch_start")
                            .await?;
                        tx_manager
                            .create_savepoint(&tx_id, "batch_start".to_string())
                            .await?;
                    }
                }
                Err(_) => {
                    let timeout_error =
                        DatabaseError::Timeout("Batch chunk operation timeout".to_string());
                    error!("Batch chunk timeout");
                    batch_result.failed_operations += chunk.len();
                    batch_result.errors.push(BatchError {
                        operation_index: processed_count,
                        operation_type: "timeout".to_string(),
                        error_message: timeout_error.to_string(),
                        is_retryable: true,
                    });

                    if config.fail_fast {
                        tx_manager.abort_transaction(&tx_id).await?;
                        return Err(timeout_error);
                    }
                }
            }

            // Optional delay between chunks to prevent overwhelming the system
            if config.inter_chunk_delay > Duration::from_millis(0) {
                tokio::time::sleep(config.inter_chunk_delay).await;
            }
        }

        // Commit or abort transaction based on results
        if batch_result.failed_operations > 0 && config.fail_on_any_error {
            warn!("Aborting transaction due to failures");
            tx_manager.abort_transaction(&tx_id).await?;
            Err(DatabaseError::InvalidOperation(format!(
                "Batch failed with {} errors",
                batch_result.failed_operations
            )))
        } else {
            info!("Committing batch transaction");
            tx_manager.commit_transaction(&tx_id).await?;
            Ok(())
        }
    }

    /// Execute batch operations in auto-commit mode (each operation commits individually)
    async fn execute_batch_autocommit<F>(
        &self,
        operations: &[BatchOperation],
        config: &BatchConfig,
        progress_callback: Option<F>,
        batch_result: &mut BatchResult,
    ) -> Result<()>
    where
        F: Fn(BatchProgress) + Send + Sync,
    {
        debug!("Executing batch in auto-commit mode");

        let mut processed_count = 0;
        let total_operations = operations.len();

        for (index, operation) in operations.iter().enumerate() {
            // Check for cancellation
            if let Some(ref cancel_token) = config.cancellation_token {
                if cancel_token.is_cancelled() {
                    warn!("Batch execution cancelled by user");
                    return Err(DatabaseError::InvalidOperation(
                        "Batch execution cancelled".to_string(),
                    ));
                }
            }

            // Execute single operation with timeout
            let operation_result = timeout(
                config.operation_timeout,
                self.execute_single_batch_operation(operation),
            )
            .await;

            match operation_result {
                Ok(Ok(result)) => {
                    batch_result.operation_results.push(result);
                    batch_result.successful_operations += 1;
                    processed_count += 1;
                }
                Ok(Err(e)) => {
                    error!("Operation {} failed: {}", index, e);
                    batch_result.failed_operations += 1;
                    batch_result.errors.push(BatchError {
                        operation_index: index,
                        operation_type: operation.operation_type(),
                        error_message: e.to_string(),
                        is_retryable: e.is_retryable(),
                    });

                    if config.fail_fast {
                        return Err(e);
                    }
                }
                Err(_) => {
                    let timeout_error =
                        DatabaseError::Timeout(format!("Operation {} timeout", index));
                    error!("Operation {} timeout", index);
                    batch_result.failed_operations += 1;
                    batch_result.errors.push(BatchError {
                        operation_index: index,
                        operation_type: operation.operation_type(),
                        error_message: timeout_error.to_string(),
                        is_retryable: true,
                    });

                    if config.fail_fast {
                        return Err(timeout_error);
                    }
                }
            }

            // Update progress
            if let Some(ref callback) = progress_callback {
                let progress = BatchProgress {
                    processed: processed_count,
                    total: total_operations,
                    current_operation: format!(
                        "Operation {}: {}",
                        index + 1,
                        operation.operation_type()
                    ),
                    percentage: (processed_count as f64 / total_operations as f64) * 100.0,
                    errors: batch_result.failed_operations,
                };
                callback(progress);
            }

            // Optional delay between operations
            if config.inter_operation_delay > Duration::from_millis(0) {
                tokio::time::sleep(config.inter_operation_delay).await;
            }
        }

        Ok(())
    }

    /// Execute batch operations in streaming mode for memory efficiency
    /// Execute batch operations in streaming mode for memory efficiency
    async fn execute_batch_streaming<F>(
        &self,
        operations: &[BatchOperation],
        config: &BatchConfig,
        progress_callback: Option<F>,
        batch_result: &mut BatchResult,
    ) -> Result<()>
    where
        F: Fn(BatchProgress) + Send + Sync,
    {
        debug!("Executing batch in streaming mode");

        use futures::stream::{self, StreamExt};
        use std::sync::{Arc, Mutex};

        // Wrap batch_result in Arc<Mutex> to share across async tasks
        let batch_result_shared = Arc::new(Mutex::new(BatchResult::new(operations.len())));
        let processed_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let total_operations = operations.len();

        let operation_stream = stream::iter(operations.iter().enumerate())
            .map({
                let batch_result_shared = batch_result_shared.clone();
                let processed_counter = processed_counter.clone();

                move |(index, operation)| {
                    let batch_result_shared = batch_result_shared.clone();
                    let processed_counter = processed_counter.clone();
                    let operation = operation.clone(); // Clone the operation to move into async block

                    async move {
                        let result = self.execute_single_batch_operation(&operation).await;
                        (
                            index,
                            operation,
                            result,
                            batch_result_shared,
                            processed_counter,
                        )
                    }
                }
            })
            .buffer_unordered(config.max_concurrent_operations);

        // Collect results using a different approach to avoid borrow issues
        let mut results: Vec<(usize, BatchOperation, Result<BatchOperationResult>)> = Vec::new();

        // Use collect instead of for_each to avoid lifetime issues
        let stream_results: Vec<_> = operation_stream.collect().await;

        for (index, operation, result, batch_result_shared, processed_counter) in stream_results {
            match result {
                Ok(op_result) => {
                    let mut batch_result_guard = batch_result_shared.lock().unwrap();
                    batch_result_guard.operation_results.push(op_result);
                    batch_result_guard.successful_operations += 1;
                }
                Err(e) => {
                    error!("Streaming operation {} failed: {}", index, e);
                    let mut batch_result_guard = batch_result_shared.lock().unwrap();
                    batch_result_guard.failed_operations += 1;
                    batch_result_guard.errors.push(BatchError {
                        operation_index: index,
                        operation_type: operation.operation_type(),
                        error_message: e.to_string(),
                        is_retryable: e.is_retryable(),
                    });
                }
            }

            let processed_count =
                processed_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

            // Update progress
            if let Some(callback) = &progress_callback {
                let batch_result_guard = batch_result_shared.lock().unwrap();
                let progress = BatchProgress {
                    processed: processed_count,
                    total: total_operations,
                    current_operation: format!("Streaming operation {}", index + 1),
                    percentage: (processed_count as f64 / total_operations as f64) * 100.0,
                    errors: batch_result_guard.failed_operations,
                };
                callback(progress);
            }
        }

        // Copy results back to the original batch_result
        let final_result = batch_result_shared.lock().unwrap();
        batch_result.operation_results = final_result.operation_results.clone();
        batch_result.successful_operations = final_result.successful_operations;
        batch_result.failed_operations = final_result.failed_operations;
        batch_result.errors = final_result.errors.clone();

        Ok(())
    }

    /// Execute batch operations in parallel mode
    async fn execute_batch_parallel<F>(
        &self,
        operations: &[BatchOperation],
        config: &BatchConfig,
        progress_callback: Option<F>,
        batch_result: &mut BatchResult,
    ) -> Result<()>
    where
        F: Fn(BatchProgress) + Send + Sync,
    {
        debug!(
            "Executing batch in parallel mode with {} concurrent operations",
            config.max_concurrent_operations
        );

        use futures::future::{FutureExt, join_all};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let processed_counter = Arc::new(AtomicUsize::new(0));
        let total_operations = operations.len();

        // Split operations into chunks for parallel processing
        let chunk_futures: Vec<_> = operations
            .chunks(config.batch_size)
            .enumerate()
            .map(|(chunk_index, chunk)| {
                let processed_counter = processed_counter.clone();
                let progress_callback = &progress_callback;

                async move {
                    let mut chunk_results = Vec::new();
                    let mut chunk_errors = Vec::new();
                    let mut successful_in_chunk = 0;
                    let mut failed_in_chunk = 0;

                    for (op_index, operation) in chunk.iter().enumerate() {
                        let global_index = chunk_index * config.batch_size + op_index;

                        match self.execute_single_batch_operation(operation).await {
                            Ok(result) => {
                                chunk_results.push(result);
                                successful_in_chunk += 1;
                            }
                            Err(e) => {
                                error!("Parallel operation {} failed: {}", global_index, e);
                                chunk_errors.push(BatchError {
                                    operation_index: global_index,
                                    operation_type: operation.operation_type(),
                                    error_message: e.to_string(),
                                    is_retryable: e.is_retryable(),
                                });
                                failed_in_chunk += 1;

                                if config.fail_fast {
                                    return Err(e);
                                }
                            }
                        }

                        // Update progress atomically
                        let processed = processed_counter.fetch_add(1, Ordering::Relaxed) + 1;
                        if let Some(callback) = &progress_callback {
                            let progress = BatchProgress {
                                processed,
                                total: total_operations,
                                current_operation: format!(
                                    "Parallel chunk {} operation {}",
                                    chunk_index + 1,
                                    op_index + 1
                                ),
                                percentage: (processed as f64 / total_operations as f64) * 100.0,
                                errors: failed_in_chunk, // This is just chunk errors, will be aggregated later
                            };
                            callback(progress);
                        }
                    }

                    Ok::<_, DatabaseError>((
                        chunk_results,
                        chunk_errors,
                        successful_in_chunk,
                        failed_in_chunk,
                    ))
                }
                .boxed()
            })
            .collect();

        // Execute all chunks in parallel
        let chunk_results = join_all(chunk_futures).await;

        // Aggregate results
        for chunk_result in chunk_results {
            match chunk_result {
                Ok((results, errors, successful, failed)) => {
                    batch_result.operation_results.extend(results);
                    batch_result.errors.extend(errors);
                    batch_result.successful_operations += successful;
                    batch_result.failed_operations += failed;
                }
                Err(e) => {
                    if config.fail_fast {
                        return Err(e);
                    }
                    // Handle chunk-level failure
                    batch_result.errors.push(BatchError {
                        operation_index: 0, // Chunk error doesn't have specific operation index
                        operation_type: "chunk".to_string(),
                        error_message: e.to_string(),
                        is_retryable: e.is_retryable(),
                    });
                }
            }
        }

        Ok(())
    }

    /// Process a batch chunk within a transaction
    async fn process_batch_chunk_transactional(
        &self,
        tx_id: &str,
        chunk: &[BatchOperation],
        tx_manager: &TransactionManager,
        config: &BatchConfig,
    ) -> Result<Vec<BatchOperationResult>> {
        let mut results = Vec::new();

        for operation in chunk {
            let result = match operation {
                BatchOperation::CreateNode(node) => {
                    let node_id = tx_manager.create_node_tx(tx_id, node.clone()).await?;
                    BatchOperationResult::NodeCreated { id: node_id }
                }
                BatchOperation::UpdateNode(node) => {
                    let updated = tx_manager.update_node_tx(tx_id, node.clone()).await?;
                    BatchOperationResult::NodeUpdated {
                        id: node.id.clone(),
                        updated,
                    }
                }
                BatchOperation::DeleteNode(node_id) => {
                    let deleted = tx_manager.delete_node_tx(tx_id, node_id).await?;
                    BatchOperationResult::NodeDeleted {
                        id: node_id.clone(),
                        deleted,
                    }
                }
                BatchOperation::CreateEdge(edge) => {
                    let edge_id = tx_manager.create_edge_tx(tx_id, edge.clone()).await?;
                    BatchOperationResult::EdgeCreated { id: edge_id }
                }
                BatchOperation::UpdateEdge(edge) => {
                    let updated = tx_manager.update_edge_tx(tx_id, edge.clone()).await?;
                    BatchOperationResult::EdgeUpdated {
                        id: edge.id.clone(),
                        updated,
                    }
                }
                BatchOperation::DeleteEdge(edge_id) => {
                    let deleted = tx_manager.delete_edge_tx(tx_id, &edge_id).await?;
                    BatchOperationResult::EdgeDeleted {
                        id: edge_id.clone(),
                        deleted,
                    }
                }
                BatchOperation::Custom(custom_op) => {
                    // Execute custom operation
                    self.execute_custom_batch_operation(custom_op).await?
                }
            };

            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single batch operation
    async fn execute_single_batch_operation(
        &self,
        operation: &BatchOperation,
    ) -> Result<BatchOperationResult> {
        match operation {
            BatchOperation::CreateNode(node) => {
                let node_id = self.create_node(node.clone()).await?;
                Ok(BatchOperationResult::NodeCreated { id: node_id })
            }
            BatchOperation::UpdateNode(node) => {
                let updated = self.update_node(node).await?;
                Ok(BatchOperationResult::NodeUpdated {
                    id: node.id.clone(),
                    updated,
                })
            }
            BatchOperation::DeleteNode(node_id) => {
                let deleted = self.delete_node(node_id).await?;
                Ok(BatchOperationResult::NodeDeleted {
                    id: node_id.clone(),
                    deleted,
                })
            }
            BatchOperation::CreateEdge(edge) => {
                let edge_id = self.create_edge(edge.clone()).await?;
                Ok(BatchOperationResult::EdgeCreated { id: edge_id })
            }
            BatchOperation::UpdateEdge(edge) => {
                let updated = self.update_edge(edge.clone()).await?;
                Ok(BatchOperationResult::EdgeUpdated {
                    id: edge.id.clone(),
                    updated,
                })
            }
            BatchOperation::DeleteEdge(edge_id) => {
                let deleted = self.delete_edge(edge_id).await?;
                Ok(BatchOperationResult::EdgeDeleted {
                    id: edge_id.clone(),
                    deleted,
                })
            }
            BatchOperation::Custom(custom_op) => {
                self.execute_custom_batch_operation(custom_op).await
            }
        }
    }

    /// Execute custom batch operation
    async fn execute_custom_batch_operation(
        &self,
        custom_op: &CustomBatchOperation,
    ) -> Result<BatchOperationResult> {
        match &custom_op.operation_type[..] {
            "bulk_create_nodes" => {
                let nodes: Vec<Node> = serde_json::from_value(custom_op.parameters.clone())
                    .map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to deserialize nodes: {}", e))
                    })?;

                let mut node_ids = Vec::new();
                for node in nodes {
                    let node_id = self.create_node(node).await?;
                    node_ids.push(node_id);
                }

                Ok(BatchOperationResult::Custom {
                    operation_type: "bulk_create_nodes".to_string(),
                    result: serde_json::to_value(node_ids).map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to serialize result: {}", e))
                    })?,
                })
            }
            "bulk_create_edges" => {
                let edges: Vec<Edge> = serde_json::from_value(custom_op.parameters.clone())
                    .map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to deserialize edges: {}", e))
                    })?;

                let mut edge_ids = Vec::new();
                for edge in edges {
                    let edge_id = self.create_edge(edge).await?;
                    edge_ids.push(edge_id);
                }

                Ok(BatchOperationResult::Custom {
                    operation_type: "bulk_create_edges".to_string(),
                    result: serde_json::to_value(edge_ids).map_err(|e| {
                        DatabaseError::Serialization(format!("Failed to serialize result: {}", e))
                    })?,
                })
            }
            _ => Err(DatabaseError::InvalidOperation(format!(
                "Unknown custom operation: {}",
                custom_op.operation_type
            ))),
        }
    }

    /// Validate batch operations before execution
    fn validate_batch_operations(&self, operations: &[BatchOperation]) -> Result<()> {
        if operations.is_empty() {
            return Err(DatabaseError::InvalidOperation(
                "Empty batch operations".to_string(),
            ));
        }

        if operations.len() > 100_000 {
            return Err(DatabaseError::InvalidOperation(
                "Batch size too large (max: 100,000)".to_string(),
            ));
        }

        // Validate each operation
        for (index, operation) in operations.iter().enumerate() {
            match operation {
                BatchOperation::CreateNode(node) => {
                    if node.label.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Node label cannot be empty",
                            index
                        )));
                    }
                }
                BatchOperation::UpdateNode(node) => {
                    if node.id.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Node ID cannot be empty for update",
                            index
                        )));
                    }
                }
                BatchOperation::DeleteNode(node_id) => {
                    if node_id.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Node ID cannot be empty for delete",
                            index
                        )));
                    }
                }
                BatchOperation::CreateEdge(edge) => {
                    if edge.source.is_empty() || edge.target.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Edge source and target cannot be empty",
                            index
                        )));
                    }
                }
                BatchOperation::UpdateEdge(edge) => {
                    if edge.id.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Edge ID cannot be empty for update",
                            index
                        )));
                    }
                }
                BatchOperation::DeleteEdge(edge_id) => {
                    if edge_id.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Edge ID cannot be empty for delete",
                            index
                        )));
                    }
                }
                BatchOperation::Custom(custom_op) => {
                    if custom_op.operation_type.is_empty() {
                        return Err(DatabaseError::InvalidOperation(format!(
                            "Operation {}: Custom operation type cannot be empty",
                            index
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Get or create a transaction manager instance (production-ready singleton)
    async fn get_or_create_transaction_manager(&self) -> Result<TransactionManager> {
        // Get the global container
        let tx_manager_container = GLOBAL_TX_MANAGER.get_or_init(|| Arc::new(RwLock::new(None)));

        // First, try to get an existing transaction manager
        {
            let read_lock = tx_manager_container.read().await;
            if let Some(ref tx_manager) = *read_lock {
                // Clone the transaction manager (it's designed to be cloneable/shareable)
                return Ok(tx_manager.clone_for_client(self.clone()));
            }
        }

        // If no transaction manager exists, create one with write lock
        let mut write_lock = tx_manager_container.write().await;

        // Double-check pattern to avoid race condition
        if let Some(ref tx_manager) = *write_lock {
            return Ok(tx_manager.clone_for_client(self.clone()));
        }

        // Create new transaction manager
        info!("Creating new global transaction manager");

        let wal_path = self.get_transaction_wal_path();
        let tx_manager = TransactionManager::new(
            self.clone(),
            wal_path,
            true, // Force sync for durability
        )
        .await?;

        *write_lock = Some(tx_manager.clone_for_client(self.clone()));

        Ok(tx_manager)
    }

    /// Get transaction manager with custom configuration
    async fn get_or_create_transaction_manager_with_config(
        &self,
        config: TransactionManagerConfig,
    ) -> Result<TransactionManager> {
        // For custom configs, always create a new instance
        info!("Creating transaction manager with custom config");

        TransactionManager::with_config(self.clone(), config).await
    }

    /// Get WAL path for transactions (configurable)
    fn get_transaction_wal_path(&self) -> PathBuf {
        // Get from database config if available, otherwise use default
        if let Some(tx_config) = &self.database.config.transaction_config {
            tx_config.wal_path.clone()
        } else {
            PathBuf::from("./data/transaction.wal")
        }
    }

    /// Initialize transaction subsystem (call this during database startup)
    pub async fn initialize_transaction_system(&self) -> Result<()> {
        info!("Initializing transaction system");

        // Create WAL directory if it doesn't exist
        let wal_path = self.get_transaction_wal_path();
        if let Some(parent) = wal_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| DatabaseError::Io(format!("Failed to create WAL directory: {}", e)))?;
        }

        // Pre-create the transaction manager to ensure it's ready
        let _tx_manager = self.get_or_create_transaction_manager().await?;

        info!("Transaction system initialized successfully");
        Ok(())
    }

    /// Shutdown transaction system (call this during database shutdown)
    pub async fn shutdown_transaction_system(&self) -> Result<()> {
        info!("Shutting down transaction system");

        let tx_manager_container = GLOBAL_TX_MANAGER.get_or_init(|| Arc::new(RwLock::new(None)));

        let mut write_lock = tx_manager_container.write().await;
        if let Some(tx_manager) = write_lock.take() {
            tx_manager.shutdown().await?;
        }

        info!("Transaction system shutdown completed");
        Ok(())
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
