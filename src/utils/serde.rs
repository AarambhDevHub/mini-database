use crate::utils::error::{DatabaseError, Result};
use serde::{Deserialize, Serialize};

/// Serialization/deserialization for data persistence
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    bincode::serialize(value).map_err(DatabaseError::from)
}

/// Deserialize binary data to a type
pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    bincode::deserialize(data).map_err(DatabaseError::from)
}

/// Serialize to JSON string
pub fn serialize_json<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(DatabaseError::from)
}

/// Serialize to pretty JSON string
pub fn serialize_json_pretty<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string_pretty(value).map_err(DatabaseError::from)
}

/// Deserialize JSON string to a type
pub fn deserialize_json<T: for<'de> Deserialize<'de>>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(DatabaseError::from)
}

/// Serialize with compression
pub fn serialize_compressed<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let serialized = serialize(value)?;
    Ok(lz4_flex::compress_prepend_size(&serialized))
}

/// Deserialize with decompression
pub fn deserialize_compressed<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    let decompressed = lz4_flex::decompress_size_prepended(data)
        .map_err(|e| DatabaseError::Serialization(format!("Decompression failed: {}", e)))?;
    deserialize(&decompressed)
}

/// Calculate serialized size of a value
pub fn serialized_size<T: Serialize>(value: &T) -> Result<usize> {
    let serialized = serialize(value)?;
    Ok(serialized.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Node, Value};

    #[test]
    fn test_serialization() -> Result<()> {
        let node = Node::new("test").with_property("name", Value::String("Alice".to_string()));

        let serialized = serialize(&node)?;
        let deserialized: Node = deserialize(&serialized)?;

        assert_eq!(node, deserialized);
        Ok(())
    }

    #[test]
    fn test_json_serialization() -> Result<()> {
        let node = Node::new("test").with_property("age", Value::Integer(30));

        let json = serialize_json(&node)?;
        let deserialized: Node = deserialize_json(&json)?;

        assert_eq!(node.label, deserialized.label);
        Ok(())
    }

    #[test]
    fn test_compressed_serialization() -> Result<()> {
        let node = Node::new("test").with_property("description", Value::String("A".repeat(1000)));

        let compressed = serialize_compressed(&node)?;
        let regular = serialize(&node)?;

        // Compression should reduce size for repetitive data
        assert!(compressed.len() < regular.len());

        let deserialized: Node = deserialize_compressed(&compressed)?;
        assert_eq!(node, deserialized);

        Ok(())
    }
}
