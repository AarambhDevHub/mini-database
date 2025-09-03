use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Edge data structure with source, target, and relationship data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    /// Unique edge identifier
    pub id: String,
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Edge label/type
    pub label: String,
    /// Edge properties
    pub properties: HashMap<String, Value>,
    /// Creation timestamp (Unix timestamp)
    pub created_at: u64,
    /// Last update timestamp (Unix timestamp)
    pub updated_at: u64,
}

impl Edge {
    /// Create a new edge
    pub fn new(source: &str, target: &str, label: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id: Uuid::new_v4().to_string(),
            source: source.to_string(),
            target: target.to_string(),
            label: label.to_string(),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create an edge with a specific ID
    pub fn with_id(source: &str, target: &str, label: &str, id: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id,
            source: source.to_string(),
            target: target.to_string(),
            label: label.to_string(),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a property to the edge
    pub fn with_property(mut self, key: &str, value: Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self.update_timestamp();
        self
    }

    /// Add multiple properties to the edge
    pub fn with_properties(mut self, properties: HashMap<String, Value>) -> Self {
        self.properties.extend(properties);
        self.update_timestamp();
        self
    }

    /// Set a property
    pub fn set_property(&mut self, key: &str, value: Value) {
        self.properties.insert(key.to_string(), value);
        self.update_timestamp();
    }

    /// Get a property by key
    pub fn get_property(&self, key: &str) -> Option<&Value> {
        self.properties.get(key)
    }

    /// Remove a property
    pub fn remove_property(&mut self, key: &str) -> Option<Value> {
        let result = self.properties.remove(key);
        if result.is_some() {
            self.update_timestamp();
        }
        result
    }

    /// Check if edge has a property
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Get all property keys
    pub fn property_keys(&self) -> Vec<&String> {
        self.properties.keys().collect()
    }

    /// Clear all properties
    pub fn clear_properties(&mut self) {
        self.properties.clear();
        self.update_timestamp();
    }

    /// Get property count
    pub fn property_count(&self) -> usize {
        self.properties.len()
    }

    /// Check if edge matches a label
    pub fn has_label(&self, label: &str) -> bool {
        self.label == label
    }

    /// Update the label
    pub fn set_label(&mut self, label: &str) {
        self.label = label.to_string();
        self.update_timestamp();
    }

    /// Check if this is a self-loop
    pub fn is_self_loop(&self) -> bool {
        self.source == self.target
    }

    /// Get the other node ID given one node ID
    pub fn get_other_node(&self, node_id: &str) -> Option<&String> {
        if self.source == node_id {
            Some(&self.target)
        } else if self.target == node_id {
            Some(&self.source)
        } else {
            None
        }
    }

    /// Check if edge connects two specific nodes
    pub fn connects(&self, node1: &str, node2: &str) -> bool {
        (self.source == node1 && self.target == node2)
            || (self.source == node2 && self.target == node1)
    }

    /// Check if edge is directed from source to target
    pub fn is_directed_from(&self, source: &str, target: &str) -> bool {
        self.source == source && self.target == target
    }

    /// Get edge weight (default to 1.0 if no weight property)
    pub fn weight(&self) -> f64 {
        self.properties
            .get("weight")
            .and_then(|v| match v {
                Value::Float(f) => Some(*f),
                Value::Integer(i) => Some(*i as f64),
                _ => None,
            })
            .unwrap_or(1.0)
    }

    /// Set edge weight
    pub fn set_weight(&mut self, weight: f64) {
        self.set_property("weight", Value::Float(weight));
    }

    /// Update the timestamp
    fn update_timestamp(&mut self) {
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Get age of the edge in seconds
    pub fn age(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - self.created_at
    }

    /// Check if edge was modified since creation
    pub fn is_modified(&self) -> bool {
        self.updated_at > self.created_at
    }

    /// Convert edge to a pretty string representation
    pub fn to_string_pretty(&self) -> String {
        format!(
            "Edge(id: {}, {} --[{}]--> {}, properties: {:?})",
            self.id, self.source, self.label, self.target, self.properties
        )
    }
}

impl std::fmt::Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Edge[{}:{}->{}]", self.label, self.source, self.target)
    }
}
