use crate::core::Database;
use crate::types::{Edge, Node};
use crate::utils::error::{DatabaseError, Result};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use tracing::{debug, trace};

/// Graph traversal algorithms (BFS, DFS, shortest path)
pub struct GraphOps {
    database: Database,
}

impl GraphOps {
    /// Create a new graph operations instance
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Breadth-first search from a starting node
    pub async fn breadth_first_search(
        &self,
        start_node_id: &str,
        max_depth: usize,
    ) -> Result<Vec<Node>> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        // Add starting node
        if let Some(start_node) = self.database.node_store().get(start_node_id).await? {
            queue.push_back((start_node.clone(), 0));
            visited.insert(start_node_id.to_string());
            result.push(start_node);
        } else {
            return Err(DatabaseError::NotFound(format!(
                "Start node not found: {}",
                start_node_id
            )));
        }

        while let Some((current_node, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            // Get all edges connected to current node
            let edges = self
                .database
                .edge_store()
                .get_node_edges(&current_node.id)
                .await?;

            for edge in edges {
                // Determine the neighbor node
                let neighbor_id = if edge.source == current_node.id {
                    &edge.target
                } else {
                    &edge.source
                };

                if !visited.contains(neighbor_id) {
                    if let Some(neighbor) = self.database.node_store().get(neighbor_id).await? {
                        visited.insert(neighbor_id.clone());
                        queue.push_back((neighbor.clone(), depth + 1));
                        result.push(neighbor);
                        trace!("BFS visited node {} at depth {}", neighbor_id, depth + 1);
                    }
                }
            }
        }

        debug!("BFS from {} found {} nodes", start_node_id, result.len());
        Ok(result)
    }

    /// Depth-first search from a starting node
    pub async fn depth_first_search(
        &self,
        start_node_id: &str,
        max_depth: usize,
    ) -> Result<Vec<Node>> {
        let mut visited = HashSet::new();
        let mut result = Vec::new();

        if let Some(start_node) = self.database.node_store().get(start_node_id).await? {
            self.dfs_recursive(&start_node, &mut visited, &mut result, 0, max_depth)
                .await?;
        } else {
            return Err(DatabaseError::NotFound(format!(
                "Start node not found: {}",
                start_node_id
            )));
        }

        debug!("DFS from {} found {} nodes", start_node_id, result.len());
        Ok(result)
    }

    /// Recursive DFS helper
    fn dfs_recursive<'a>(
        &'a self,
        current_node: &'a Node,
        visited: &'a mut HashSet<String>,
        result: &'a mut Vec<Node>,
        depth: usize,
        max_depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if depth > max_depth || visited.contains(&current_node.id) {
                return Ok(());
            }

            visited.insert(current_node.id.clone());
            result.push(current_node.clone());
            trace!("DFS visited node {} at depth {}", current_node.id, depth);

            // Get all edges connected to current node
            let edges = self
                .database
                .edge_store()
                .get_node_edges(&current_node.id)
                .await?;

            for edge in edges {
                // Determine the neighbor node
                let neighbor_id = if edge.source == current_node.id {
                    &edge.target
                } else {
                    &edge.source
                };

                if !visited.contains(neighbor_id) {
                    if let Some(neighbor) = self.database.node_store().get(neighbor_id).await? {
                        self.dfs_recursive(&neighbor, visited, result, depth + 1, max_depth)
                            .await?;
                    }
                }
            }

            Ok(())
        })
    }

    /// Find shortest path between two nodes using Dijkstra's algorithm
    pub async fn shortest_path(
        &self,
        start_node_id: &str,
        end_node_id: &str,
    ) -> Result<Option<Vec<Node>>> {
        let mut distances: HashMap<String, f64> = HashMap::new();
        let mut previous: HashMap<String, String> = HashMap::new();
        let mut unvisited = HashSet::new();

        // Initialize distances
        let all_node_ids = self.database.node_store().get_all_ids();
        for node_id in &all_node_ids {
            distances.insert(node_id.clone(), f64::INFINITY);
            unvisited.insert(node_id.clone());
        }
        distances.insert(start_node_id.to_string(), 0.0);

        while !unvisited.is_empty() {
            // Find unvisited node with minimum distance
            let current_node_id = unvisited
                .iter()
                .min_by(|a, b| {
                    distances[*a]
                        .partial_cmp(&distances[*b])
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .cloned();

            if let Some(current_id) = current_node_id {
                if distances[&current_id] == f64::INFINITY {
                    break; // No path exists
                }

                unvisited.remove(&current_id);

                if current_id == end_node_id {
                    // Found shortest path, reconstruct it
                    return self
                        .reconstruct_path(&previous, start_node_id, end_node_id)
                        .await;
                }

                // Check neighbors
                let edges = self
                    .database
                    .edge_store()
                    .get_outgoing_edges(&current_id)
                    .await?;
                for edge in edges {
                    let neighbor_id = &edge.target;
                    if unvisited.contains(neighbor_id) {
                        let weight = self.get_edge_weight(&edge);
                        let alt_distance = distances[&current_id] + weight;

                        if alt_distance < distances[neighbor_id] {
                            distances.insert(neighbor_id.clone(), alt_distance);
                            previous.insert(neighbor_id.clone(), current_id.clone());
                        }
                    }
                }
            } else {
                break;
            }
        }

        debug!("No path found from {} to {}", start_node_id, end_node_id);
        Ok(None)
    }

    /// Get all nodes within a certain distance
    pub async fn get_neighbors(&self, node_id: &str, max_distance: usize) -> Result<Vec<Node>> {
        self.breadth_first_search(node_id, max_distance).await
    }

    /// Find all connected components in the graph
    pub async fn connected_components(&self) -> Result<Vec<Vec<Node>>> {
        let mut visited = HashSet::new();
        let mut components = Vec::new();
        let all_node_ids = self.database.node_store().get_all_ids();

        for node_id in all_node_ids {
            if !visited.contains(&node_id) {
                let component = self.get_connected_component(&node_id, &mut visited).await?;
                if !component.is_empty() {
                    components.push(component);
                }
            }
        }

        debug!("Found {} connected components", components.len());
        Ok(components)
    }

    /// Get all nodes in the same connected component
    async fn get_connected_component(
        &self,
        start_node_id: &str,
        global_visited: &mut HashSet<String>,
    ) -> Result<Vec<Node>> {
        let mut component = Vec::new();
        let mut queue = VecDeque::new();
        let mut local_visited = HashSet::new();

        if let Some(start_node) = self.database.node_store().get(start_node_id).await? {
            queue.push_back(start_node.clone());
            local_visited.insert(start_node_id.to_string());
            component.push(start_node);
        }

        while let Some(current_node) = queue.pop_front() {
            global_visited.insert(current_node.id.clone());

            let edges = self
                .database
                .edge_store()
                .get_node_edges(&current_node.id)
                .await?;
            for edge in edges {
                let neighbor_id = if edge.source == current_node.id {
                    &edge.target
                } else {
                    &edge.source
                };

                if !local_visited.contains(neighbor_id) {
                    if let Some(neighbor) = self.database.node_store().get(neighbor_id).await? {
                        local_visited.insert(neighbor_id.clone());
                        queue.push_back(neighbor.clone());
                        component.push(neighbor);
                    }
                }
            }
        }

        Ok(component)
    }

    /// Reconstruct path from previous node map
    async fn reconstruct_path(
        &self,
        previous: &HashMap<String, String>,
        start_node_id: &str,
        end_node_id: &str,
    ) -> Result<Option<Vec<Node>>> {
        let mut path = Vec::new();
        let mut current_id = end_node_id.to_string();

        while let Some(prev_id) = previous.get(&current_id) {
            if let Some(node) = self.database.node_store().get(&current_id).await? {
                path.push(node);
            }
            current_id = prev_id.clone();

            if current_id == start_node_id {
                if let Some(start_node) = self.database.node_store().get(start_node_id).await? {
                    path.push(start_node);
                }
                break;
            }
        }

        path.reverse();
        debug!("Reconstructed path with {} nodes", path.len());
        Ok(Some(path))
    }

    /// Get edge weight (default to 1.0 if no weight property)
    fn get_edge_weight(&self, edge: &Edge) -> f64 {
        edge.properties
            .get("weight")
            .and_then(|v| match v {
                crate::types::Value::Float(f) => Some(*f),
                crate::types::Value::Integer(i) => Some(*i as f64),
                _ => None,
            })
            .unwrap_or(1.0)
    }
}
