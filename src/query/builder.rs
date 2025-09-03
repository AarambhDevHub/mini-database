use crate::types::{Edge, Node, Value};
use std::collections::HashMap;

/// Fluent API for building SQL-like and graph queries
#[derive(Debug, Clone)]
pub struct QueryBuilder {
    query_type: QueryType,
    conditions: Vec<Condition>,
    properties: HashMap<String, Value>,
    label: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    order_by: Option<(String, SortOrder)>,
}

#[derive(Debug, Clone)]
pub enum QueryType {
    SelectNodes,
    SelectEdges,
    CreateNode,
    CreateEdge {
        source: String,
        target: String,
    },
    UpdateNode {
        id: String,
    },
    UpdateEdge {
        id: String,
    },
    DeleteNode {
        id: String,
    },
    DeleteEdge {
        id: String,
    },
    GraphTraversal {
        start_node: String,
        max_depth: Option<usize>,
    },
}

#[derive(Debug, Clone)]
pub enum Condition {
    Equals { key: String, value: Value },
    NotEquals { key: String, value: Value },
    GreaterThan { key: String, value: Value },
    LessThan { key: String, value: Value },
    Contains { key: String, value: String },
    StartsWith { key: String, value: String },
    EndsWith { key: String, value: String },
    In { key: String, values: Vec<Value> },
    IsNull { key: String },
    IsNotNull { key: String },
}

#[derive(Debug, Clone)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl QueryBuilder {
    /// Create a new query builder
    pub fn new() -> Self {
        Self {
            query_type: QueryType::SelectNodes,
            conditions: Vec::new(),
            properties: HashMap::new(),
            label: None,
            limit: None,
            offset: None,
            order_by: None,
        }
    }

    /// Select nodes query
    pub fn select_nodes(mut self) -> Self {
        self.query_type = QueryType::SelectNodes;
        self
    }

    /// Select edges query
    pub fn select_edges(mut self) -> Self {
        self.query_type = QueryType::SelectEdges;
        self
    }

    /// Create node query
    pub fn create_node(mut self) -> Self {
        self.query_type = QueryType::CreateNode;
        self
    }

    /// Create edge query
    pub fn create_edge(mut self, source: String, target: String) -> Self {
        self.query_type = QueryType::CreateEdge { source, target };
        self
    }

    /// Update node query
    pub fn update_node(mut self, id: String) -> Self {
        self.query_type = QueryType::UpdateNode { id };
        self
    }

    /// Update edge query
    pub fn update_edge(mut self, id: String) -> Self {
        self.query_type = QueryType::UpdateEdge { id };
        self
    }

    /// Delete node query
    pub fn delete_node(mut self, id: String) -> Self {
        self.query_type = QueryType::DeleteNode { id };
        self
    }

    /// Delete edge query
    pub fn delete_edge(mut self, id: String) -> Self {
        self.query_type = QueryType::DeleteEdge { id };
        self
    }

    /// Graph traversal query
    pub fn traverse_from(mut self, start_node: String) -> Self {
        self.query_type = QueryType::GraphTraversal {
            start_node,
            max_depth: None,
        };
        self
    }

    /// Set maximum depth for graph traversal
    pub fn max_depth(mut self, depth: usize) -> Self {
        if let QueryType::GraphTraversal { start_node, .. } = self.query_type {
            self.query_type = QueryType::GraphTraversal {
                start_node,
                max_depth: Some(depth),
            };
        }
        self
    }

    /// Add equals condition
    pub fn where_eq(mut self, key: &str, value: Value) -> Self {
        self.conditions.push(Condition::Equals {
            key: key.to_string(),
            value,
        });
        self
    }

    /// Add not equals condition
    pub fn where_ne(mut self, key: &str, value: Value) -> Self {
        self.conditions.push(Condition::NotEquals {
            key: key.to_string(),
            value,
        });
        self
    }

    /// Add greater than condition
    pub fn where_gt(mut self, key: &str, value: Value) -> Self {
        self.conditions.push(Condition::GreaterThan {
            key: key.to_string(),
            value,
        });
        self
    }

    /// Add less than condition
    pub fn where_lt(mut self, key: &str, value: Value) -> Self {
        self.conditions.push(Condition::LessThan {
            key: key.to_string(),
            value,
        });
        self
    }

    /// Add contains condition
    pub fn where_contains(mut self, key: &str, value: &str) -> Self {
        self.conditions.push(Condition::Contains {
            key: key.to_string(),
            value: value.to_string(),
        });
        self
    }

    /// Add starts with condition
    pub fn where_starts_with(mut self, key: &str, value: &str) -> Self {
        self.conditions.push(Condition::StartsWith {
            key: key.to_string(),
            value: value.to_string(),
        });
        self
    }

    /// Add in condition
    pub fn where_in(mut self, key: &str, values: Vec<Value>) -> Self {
        self.conditions.push(Condition::In {
            key: key.to_string(),
            values,
        });
        self
    }

    /// Add is null condition
    pub fn where_null(mut self, key: &str) -> Self {
        self.conditions.push(Condition::IsNull {
            key: key.to_string(),
        });
        self
    }

    /// Filter by label
    pub fn with_label(mut self, label: &str) -> Self {
        self.label = Some(label.to_string());
        self
    }

    /// Set property for create/update operations
    pub fn set_property(mut self, key: &str, value: Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self
    }

    /// Set multiple properties
    pub fn set_properties(mut self, properties: HashMap<String, Value>) -> Self {
        self.properties.extend(properties);
        self
    }

    /// Set result limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set result offset
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Order by property ascending
    pub fn order_by_asc(mut self, property: &str) -> Self {
        self.order_by = Some((property.to_string(), SortOrder::Asc));
        self
    }

    /// Order by property descending
    pub fn order_by_desc(mut self, property: &str) -> Self {
        self.order_by = Some((property.to_string(), SortOrder::Desc));
        self
    }

    /// Get query type
    pub fn query_type(&self) -> &QueryType {
        &self.query_type
    }

    /// Get conditions
    pub fn conditions(&self) -> &[Condition] {
        &self.conditions
    }

    /// Get properties
    pub fn properties(&self) -> &HashMap<String, Value> {
        &self.properties
    }

    /// Get label filter
    pub fn label(&self) -> Option<&String> {
        self.label.as_ref()
    }

    /// Get limit
    pub fn limit_value(&self) -> Option<usize> {
        self.limit
    }

    /// Get offset
    pub fn offset_value(&self) -> Option<usize> {
        self.offset
    }

    /// Get order by
    pub fn order_by(&self) -> Option<&(String, SortOrder)> {
        self.order_by.as_ref()
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Condition {
    /// Check if a node matches this condition
    pub fn matches_node(&self, node: &Node) -> bool {
        match self {
            Condition::Equals { key, value } => {
                if key == "label" {
                    match value {
                        Value::String(v) => &node.label == v,
                        _ => false,
                    }
                } else {
                    node.properties.get(key).map_or(false, |v| v == value)
                }
            }
            Condition::NotEquals { key, value } => {
                if key == "label" {
                    match value {
                        Value::String(v) => &node.label != v,
                        _ => true,
                    }
                } else {
                    node.properties.get(key).map_or(true, |v| v != value)
                }
            }
            Condition::GreaterThan { key, value } => {
                node.properties.get(key).map_or(false, |v| v > value)
            }
            Condition::LessThan { key, value } => {
                node.properties.get(key).map_or(false, |v| v < value)
            }
            Condition::Contains { key, value } => node.properties.get(key).map_or(false, |v| {
                if let Value::String(s) = v {
                    s.contains(value)
                } else {
                    false
                }
            }),
            Condition::StartsWith { key, value } => node.properties.get(key).map_or(false, |v| {
                if let Value::String(s) = v {
                    s.starts_with(value)
                } else {
                    false
                }
            }),
            Condition::EndsWith { key, value } => node.properties.get(key).map_or(false, |v| {
                if let Value::String(s) = v {
                    s.ends_with(value)
                } else {
                    false
                }
            }),
            Condition::In { key, values } => node
                .properties
                .get(key)
                .map_or(false, |v| values.contains(v)),
            Condition::IsNull { key } => !node.properties.contains_key(key),
            Condition::IsNotNull { key } => node.properties.contains_key(key),
        }
    }

    /// Check if an edge matches this condition
    pub fn matches_edge(&self, edge: &Edge) -> bool {
        match self {
            Condition::Equals { key, value } => {
                if key == "label" {
                    match value {
                        Value::String(v) => &edge.label == v,
                        _ => false,
                    }
                } else if key == "source" {
                    match value {
                        Value::String(v) => &edge.source == v,
                        _ => false,
                    }
                } else if key == "target" {
                    match value {
                        Value::String(v) => &edge.target == v,
                        _ => false,
                    }
                } else if key == "id" {
                    match value {
                        Value::String(v) => &edge.id == v,
                        _ => false,
                    }
                } else if key == "created_at" {
                    match value {
                        Value::Integer(v) => edge.created_at == *v as u64,
                        _ => false,
                    }
                } else if key == "updated_at" {
                    match value {
                        Value::Integer(v) => edge.updated_at == *v as u64,
                        _ => false,
                    }
                } else {
                    edge.properties.get(key).map_or(false, |v| v == value)
                }
            }
            Condition::NotEquals { key, value } => {
                if key == "label" {
                    match value {
                        Value::String(v) => &edge.label != v,
                        _ => true,
                    }
                } else if key == "source" {
                    match value {
                        Value::String(v) => &edge.source != v,
                        _ => true,
                    }
                } else if key == "target" {
                    match value {
                        Value::String(v) => &edge.target != v,
                        _ => true,
                    }
                } else if key == "id" {
                    match value {
                        Value::String(v) => &edge.id != v,
                        _ => true,
                    }
                } else if key == "created_at" {
                    match value {
                        Value::Integer(v) => edge.created_at != *v as u64,
                        _ => true,
                    }
                } else if key == "updated_at" {
                    match value {
                        Value::Integer(v) => edge.updated_at != *v as u64,
                        _ => true,
                    }
                } else {
                    edge.properties.get(key).map_or(true, |v| v != value)
                }
            }
            Condition::GreaterThan { key, value } => {
                if key == "created_at" {
                    match value {
                        Value::Integer(v) => edge.created_at > *v as u64,
                        _ => false,
                    }
                } else if key == "updated_at" {
                    match value {
                        Value::Integer(v) => edge.updated_at > *v as u64,
                        _ => false,
                    }
                } else {
                    edge.properties.get(key).map_or(false, |v| v > value)
                }
            }
            Condition::LessThan { key, value } => {
                if key == "created_at" {
                    match value {
                        Value::Integer(v) => edge.created_at < *v as u64,
                        _ => false,
                    }
                } else if key == "updated_at" {
                    match value {
                        Value::Integer(v) => edge.updated_at < *v as u64,
                        _ => false,
                    }
                } else {
                    edge.properties.get(key).map_or(false, |v| v < value)
                }
            }
            Condition::Contains { key, value } => {
                if key == "label" {
                    edge.label.contains(value)
                } else if key == "source" {
                    edge.source.contains(value)
                } else if key == "target" {
                    edge.target.contains(value)
                } else if key == "id" {
                    edge.id.contains(value)
                } else {
                    edge.properties.get(key).map_or(false, |v| {
                        if let Value::String(s) = v {
                            s.contains(value)
                        } else {
                            false
                        }
                    })
                }
            }
            Condition::StartsWith { key, value } => {
                if key == "label" {
                    edge.label.starts_with(value)
                } else if key == "source" {
                    edge.source.starts_with(value)
                } else if key == "target" {
                    edge.target.starts_with(value)
                } else if key == "id" {
                    edge.id.starts_with(value)
                } else {
                    edge.properties.get(key).map_or(false, |v| {
                        if let Value::String(s) = v {
                            s.starts_with(value)
                        } else {
                            false
                        }
                    })
                }
            }
            Condition::EndsWith { key, value } => {
                if key == "label" {
                    edge.label.ends_with(value)
                } else if key == "source" {
                    edge.source.ends_with(value)
                } else if key == "target" {
                    edge.target.ends_with(value)
                } else if key == "id" {
                    edge.id.ends_with(value)
                } else {
                    edge.properties.get(key).map_or(false, |v| {
                        if let Value::String(s) = v {
                            s.ends_with(value)
                        } else {
                            false
                        }
                    })
                }
            }
            Condition::In { key, values } => {
                if key == "label" {
                    values.contains(&Value::String(edge.label.clone()))
                } else if key == "source" {
                    values.contains(&Value::String(edge.source.clone()))
                } else if key == "target" {
                    values.contains(&Value::String(edge.target.clone()))
                } else if key == "id" {
                    values.contains(&Value::String(edge.id.clone()))
                } else if key == "created_at" {
                    values.contains(&Value::Integer(edge.created_at as i64))
                } else if key == "updated_at" {
                    values.contains(&Value::Integer(edge.updated_at as i64))
                } else {
                    edge.properties
                        .get(key)
                        .map_or(false, |v| values.contains(v))
                }
            }
            Condition::IsNull { key } => {
                if key == "label"
                    || key == "source"
                    || key == "target"
                    || key == "id"
                    || key == "created_at"
                    || key == "updated_at"
                {
                    // Built-in fields are never null
                    false
                } else {
                    !edge.properties.contains_key(key)
                }
            }
            Condition::IsNotNull { key } => {
                if key == "label"
                    || key == "source"
                    || key == "target"
                    || key == "id"
                    || key == "created_at"
                    || key == "updated_at"
                {
                    // Built-in fields are always present
                    true
                } else {
                    edge.properties.contains_key(key)
                }
            }
        }
    }
}
