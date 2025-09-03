use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Node data structure with properties and metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// Unique node identifier
    pub id: String,
    /// Node label/type
    pub label: String,
    /// Node properties
    pub properties: HashMap<String, Value>,
    /// Creation timestamp (Unix timestamp)
    pub created_at: u64,
    /// Last update timestamp (Unix timestamp)
    pub updated_at: u64,
}

impl Node {
    /// Create a new node with a label
    pub fn new(label: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id: Uuid::new_v4().to_string(),
            label: label.to_string(),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a node with a specific ID
    pub fn with_id(label: &str, id: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            id,
            label: label.to_string(),
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a property to the node
    pub fn with_property(mut self, key: &str, value: Value) -> Self {
        self.properties.insert(key.to_string(), value);
        self.update_timestamp();
        self
    }

    /// Add multiple properties to the node
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

    /// Check if node has a property
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

    /// Check if node matches a label
    pub fn has_label(&self, label: &str) -> bool {
        self.label == label
    }

    /// Update the label
    pub fn set_label(&mut self, label: &str) {
        self.label = label.to_string();
        self.update_timestamp();
    }

    /// Update the timestamp
    fn update_timestamp(&mut self) {
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Get age of the node in seconds
    pub fn age(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - self.created_at
    }

    /// Check if node was modified since creation
    pub fn is_modified(&self) -> bool {
        self.updated_at > self.created_at
    }

    /// Convert node to a JSON-like string representation
    pub fn to_string_pretty(&self) -> String {
        format!(
            "Node(id: {}, label: {}, properties: {:?}, created: {}, updated: {})",
            self.id, self.label, self.properties, self.created_at, self.updated_at
        )
    }

    /// Clone node with new ID
    pub fn clone_with_new_id(&self) -> Self {
        Self::new(&self.label).with_properties(self.properties.clone())
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node[{}:{}]", self.label, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node = Node::new("person")
            .with_property("name", Value::String("Alice".to_string()))
            .with_property("age", Value::Integer(30));

        assert_eq!(node.label, "person");
        assert_eq!(node.property_count(), 2);
        assert!(node.has_property("name"));
        assert_eq!(
            node.get_property("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }
}
