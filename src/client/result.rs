use crate::types::{Edge, Node};
use serde::{Deserialize, Serialize};

/// Query result handling with iterators and cursors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    /// Node query results
    Nodes {
        nodes: Vec<Node>,
        total_count: usize,
    },
    /// Edge query results
    Edges {
        edges: Vec<Edge>,
        total_count: usize,
    },
    /// Created record result
    Created { id: String, record_type: String },
    /// Updated record result
    Updated { id: String, record_type: String },
    /// Deleted record result
    Deleted { id: String, record_type: String },
    /// Graph traversal result
    Traversal {
        nodes: Vec<Node>,
        start_node: String,
    },
    /// Empty result
    Empty,
}

impl QueryResult {
    /// Get nodes from result
    pub fn nodes(&self) -> Option<&Vec<Node>> {
        match self {
            QueryResult::Nodes { nodes, .. } => Some(nodes),
            QueryResult::Traversal { nodes, .. } => Some(nodes),
            _ => None,
        }
    }

    /// Get edges from result
    pub fn edges(&self) -> Option<&Vec<Edge>> {
        match self {
            QueryResult::Edges { edges, .. } => Some(edges),
            _ => None,
        }
    }

    /// Get record ID from create/update/delete results
    pub fn record_id(&self) -> Option<&String> {
        match self {
            QueryResult::Created { id, .. }
            | QueryResult::Updated { id, .. }
            | QueryResult::Deleted { id, .. } => Some(id),
            _ => None,
        }
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        match self {
            QueryResult::Nodes { nodes, .. } => nodes.is_empty(),
            QueryResult::Edges { edges, .. } => edges.is_empty(),
            QueryResult::Traversal { nodes, .. } => nodes.is_empty(),
            QueryResult::Empty => true,
            _ => false,
        }
    }

    /// Get result count
    pub fn count(&self) -> usize {
        match self {
            QueryResult::Nodes { nodes, .. } => nodes.len(),
            QueryResult::Edges { edges, .. } => edges.len(),
            QueryResult::Traversal { nodes, .. } => nodes.len(),
            QueryResult::Created { .. }
            | QueryResult::Updated { .. }
            | QueryResult::Deleted { .. } => 1,
            QueryResult::Empty => 0,
        }
    }

    /// Get total count (before pagination)
    pub fn total_count(&self) -> usize {
        match self {
            QueryResult::Nodes { total_count, .. } | QueryResult::Edges { total_count, .. } => {
                *total_count
            }
            _ => self.count(),
        }
    }

    /// Convert to iterator over nodes
    pub fn into_node_iter(self) -> Option<std::vec::IntoIter<Node>> {
        match self {
            QueryResult::Nodes { nodes, .. } => Some(nodes.into_iter()),
            QueryResult::Traversal { nodes, .. } => Some(nodes.into_iter()),
            _ => None,
        }
    }

    /// Convert to iterator over edges
    pub fn into_edge_iter(self) -> Option<std::vec::IntoIter<Edge>> {
        match self {
            QueryResult::Edges { edges, .. } => Some(edges.into_iter()),
            _ => None,
        }
    }
}

/// Iterator wrapper for query results
pub struct ResultIterator<T> {
    inner: std::vec::IntoIter<T>,
}

impl<T> ResultIterator<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self {
            inner: items.into_iter(),
        }
    }
}

impl<T> Iterator for ResultIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<T> ExactSizeIterator for ResultIterator<T> {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Cursor for paginated results
#[derive(Debug, Clone)]
pub struct QueryCursor {
    pub offset: usize,
    pub limit: usize,
    pub total_count: usize,
    pub has_more: bool,
}

impl QueryCursor {
    /// Create a new cursor
    pub fn new(offset: usize, limit: usize, total_count: usize) -> Self {
        let has_more = offset + limit < total_count;
        Self {
            offset,
            limit,
            total_count,
            has_more,
        }
    }

    /// Get next page cursor
    pub fn next_page(&self) -> Option<Self> {
        if self.has_more {
            Some(Self::new(
                self.offset + self.limit,
                self.limit,
                self.total_count,
            ))
        } else {
            None
        }
    }

    /// Get previous page cursor
    pub fn prev_page(&self) -> Option<Self> {
        if self.offset > 0 {
            let new_offset = if self.offset >= self.limit {
                self.offset - self.limit
            } else {
                0
            };
            Some(Self::new(new_offset, self.limit, self.total_count))
        } else {
            None
        }
    }

    /// Get current page number (1-indexed)
    pub fn page_number(&self) -> usize {
        (self.offset / self.limit) + 1
    }

    /// Get total number of pages
    pub fn total_pages(&self) -> usize {
        (self.total_count + self.limit - 1) / self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_query_result() {
        let nodes = vec![
            Node::new("person").with_property("name", Value::String("Alice".to_string())),
            Node::new("person").with_property("name", Value::String("Bob".to_string())),
        ];

        let result = QueryResult::Nodes {
            nodes: nodes.clone(),
            total_count: 2,
        };

        assert_eq!(result.count(), 2);
        assert_eq!(result.total_count(), 2);
        assert!(!result.is_empty());
        assert!(result.nodes().is_some());
    }

    #[test]
    fn test_cursor() {
        let cursor = QueryCursor::new(0, 10, 25);

        assert_eq!(cursor.page_number(), 1);
        assert_eq!(cursor.total_pages(), 3);
        assert!(cursor.has_more);

        let next = cursor.next_page().unwrap();
        assert_eq!(next.offset, 10);
        assert_eq!(next.page_number(), 2);
    }
}
