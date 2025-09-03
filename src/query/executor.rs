use crate::client::QueryResult;
use crate::core::Database;
use crate::query::{QueryBuilder, QueryType, SortOrder};
use crate::types::{Edge, Node, Value};
use crate::utils::error::{DatabaseError, Result};
use tracing::debug;

/// Query execution engine with optimization and planning
#[derive(Clone)]
pub struct QueryExecutor {
    database: Database,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Execute a query and return results
    pub async fn execute(&self, query: QueryBuilder) -> Result<QueryResult> {
        debug!("Executing query: {:?}", query.query_type());

        match query.query_type() {
            QueryType::SelectNodes => self.execute_select_nodes(query).await,
            QueryType::SelectEdges => self.execute_select_edges(query).await,
            QueryType::CreateNode => self.execute_create_node(query).await,
            QueryType::CreateEdge { source, target } => {
                self.execute_create_edge(query.clone(), source.clone(), target.clone())
                    .await
            }
            QueryType::UpdateNode { id } => {
                self.execute_update_node(query.clone(), id.clone()).await
            }
            QueryType::UpdateEdge { id } => {
                self.execute_update_edge(query.clone(), id.clone()).await
            }
            QueryType::DeleteNode { id } => self.execute_delete_node(id.clone()).await,
            QueryType::DeleteEdge { id } => self.execute_delete_edge(id.clone()).await,
            QueryType::GraphTraversal {
                start_node,
                max_depth,
            } => {
                self.execute_graph_traversal(start_node.clone(), *max_depth)
                    .await
            }
        }
    }

    /// Execute node selection query
    async fn execute_select_nodes(&self, query: QueryBuilder) -> Result<QueryResult> {
        let mut nodes = Vec::new();

        // Get all node IDs
        let node_ids = self.database.node_store().get_all_ids();

        // Filter nodes based on conditions
        for node_id in node_ids {
            if let Some(node) = self.database.node_store().get(&node_id).await? {
                if self.matches_node_conditions(&node, &query) {
                    nodes.push(node);
                }
            }
        }

        // Apply sorting
        if let Some((sort_key, order)) = query.order_by() {
            self.sort_nodes(&mut nodes, sort_key, order);
        }

        // Apply offset and limit
        let total_count = nodes.len();
        if let Some(offset) = query.offset_value() {
            if offset < nodes.len() {
                nodes = nodes.into_iter().skip(offset).collect();
            } else {
                nodes.clear();
            }
        }

        if let Some(limit) = query.limit_value() {
            nodes.truncate(limit);
        }

        debug!("Selected {} nodes (total: {})", nodes.len(), total_count);

        Ok(QueryResult::Nodes { nodes, total_count })
    }

    /// Execute edge selection query
    async fn execute_select_edges(&self, query: QueryBuilder) -> Result<QueryResult> {
        let mut edges = Vec::new();

        // For simplicity, we'll iterate through all edges
        // In a production system, we'd use proper indexing
        for entry in self.database.edge_store().edge_index.iter() {
            let edge_id = entry.key();
            if let Some(edge) = self.database.edge_store().get(edge_id).await? {
                if self.matches_edge_conditions(&edge, &query) {
                    edges.push(edge);
                }
            }
        }

        // Apply sorting and limits similar to nodes
        let total_count = edges.len();
        if let Some(offset) = query.offset_value() {
            if offset < edges.len() {
                edges = edges.into_iter().skip(offset).collect();
            } else {
                edges.clear();
            }
        }

        if let Some(limit) = query.limit_value() {
            edges.truncate(limit);
        }

        debug!("Selected {} edges (total: {})", edges.len(), total_count);

        Ok(QueryResult::Edges { edges, total_count })
    }

    /// Execute node creation query
    async fn execute_create_node(&self, query: QueryBuilder) -> Result<QueryResult> {
        let label = query.label().cloned().unwrap_or_else(|| "node".to_string());
        let properties = query.properties().clone();

        let node = Node::new(&label).with_properties(properties);
        let node_id = self.database.node_store().store(node.clone()).await?;

        debug!("Created node with ID: {}", node_id);

        Ok(QueryResult::Created {
            id: node_id,
            record_type: "node".to_string(),
        })
    }

    /// Execute edge creation query
    async fn execute_create_edge(
        &self,
        query: QueryBuilder,
        source: String,
        target: String,
    ) -> Result<QueryResult> {
        let label = query.label().cloned().unwrap_or_else(|| "edge".to_string());
        let properties = query.properties().clone();

        let edge = Edge::new(&source, &target, &label).with_properties(properties);
        let edge_id = self.database.edge_store().store(edge).await?;

        debug!("Created edge with ID: {}", edge_id);

        Ok(QueryResult::Created {
            id: edge_id,
            record_type: "edge".to_string(),
        })
    }

    /// Execute node update query
    async fn execute_update_node(
        &self,
        query: QueryBuilder,
        node_id: String,
    ) -> Result<QueryResult> {
        if let Some(mut node) = self.database.node_store().get(&node_id).await? {
            // Update properties
            for (key, value) in query.properties() {
                node.properties.insert(key.clone(), value.clone());
            }

            self.database.node_store().update(&node).await?;
            debug!("Updated node: {}", node_id);

            Ok(QueryResult::Updated {
                id: node_id,
                record_type: "node".to_string(),
            })
        } else {
            Err(DatabaseError::NotFound(format!(
                "Node not found: {}",
                node_id
            )))
        }
    }

    /// Execute edge update query
    async fn execute_update_edge(
        &self,
        query: QueryBuilder,
        edge_id: String,
    ) -> Result<QueryResult> {
        if let Some(mut edge) = self.database.edge_store().get(&edge_id).await? {
            // Update properties
            for (key, value) in query.properties() {
                edge.properties.insert(key.clone(), value.clone());
            }

            self.database.edge_store().store(edge).await?;
            debug!("Updated edge: {}", edge_id);

            Ok(QueryResult::Updated {
                id: edge_id,
                record_type: "edge".to_string(),
            })
        } else {
            Err(DatabaseError::NotFound(format!(
                "Edge not found: {}",
                edge_id
            )))
        }
    }

    /// Execute node deletion query
    async fn execute_delete_node(&self, node_id: String) -> Result<QueryResult> {
        if self.database.node_store().delete(&node_id).await? {
            debug!("Deleted node: {}", node_id);

            Ok(QueryResult::Deleted {
                id: node_id,
                record_type: "node".to_string(),
            })
        } else {
            Err(DatabaseError::NotFound(format!(
                "Node not found: {}",
                node_id
            )))
        }
    }

    /// Execute edge deletion query
    async fn execute_delete_edge(&self, edge_id: String) -> Result<QueryResult> {
        if self.database.edge_store().delete(&edge_id).await? {
            debug!("Deleted edge: {}", edge_id);

            Ok(QueryResult::Deleted {
                id: edge_id,
                record_type: "edge".to_string(),
            })
        } else {
            Err(DatabaseError::NotFound(format!(
                "Edge not found: {}",
                edge_id
            )))
        }
    }

    /// Execute graph traversal query
    async fn execute_graph_traversal(
        &self,
        start_node: String,
        max_depth: Option<usize>,
    ) -> Result<QueryResult> {
        use crate::query::GraphOps;

        let graph_ops = GraphOps::new(self.database.clone());
        let result = graph_ops
            .breadth_first_search(&start_node, max_depth.unwrap_or(10))
            .await?;

        debug!(
            "Graph traversal from {} found {} nodes",
            start_node,
            result.len()
        );

        Ok(QueryResult::Traversal {
            nodes: result,
            start_node,
        })
    }

    /// Check if node matches query conditions
    fn matches_node_conditions(&self, node: &Node, query: &QueryBuilder) -> bool {
        // Check label filter
        if let Some(label) = query.label() {
            if &node.label != label {
                return false;
            }
        }

        // Check all conditions
        for condition in query.conditions() {
            if !condition.matches_node(node) {
                return false;
            }
        }

        true
    }

    /// Check if edge matches query conditions
    fn matches_edge_conditions(&self, edge: &Edge, query: &QueryBuilder) -> bool {
        // Check label filter
        if let Some(label) = query.label() {
            if &edge.label != label {
                return false;
            }
        }

        // Check all conditions
        for condition in query.conditions() {
            if !condition.matches_edge(edge) {
                return false;
            }
        }

        true
    }

    /// Sort nodes by property
    fn sort_nodes(&self, nodes: &mut Vec<Node>, sort_key: &str, order: &SortOrder) {
        nodes.sort_by(|a, b| {
            let a_val = if sort_key == "label" {
                Value::String(a.label.clone()) // Remove the reference wrapping
            } else {
                a.properties.get(sort_key).cloned().unwrap_or(Value::Null)
            };

            let b_val = if sort_key == "label" {
                Value::String(b.label.clone()) // Remove the reference wrapping
            } else {
                b.properties.get(sort_key).cloned().unwrap_or(Value::Null)
            };

            match order {
                SortOrder::Asc => a_val
                    .partial_cmp(&b_val)
                    .unwrap_or(std::cmp::Ordering::Equal),
                SortOrder::Desc => b_val
                    .partial_cmp(&a_val)
                    .unwrap_or(std::cmp::Ordering::Equal),
            }
        });
    }
}
