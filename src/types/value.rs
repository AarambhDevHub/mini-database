use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Generic value type supporting multiple data formats
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)] // Remove PartialOrd from derive
pub enum Value {
    /// String value
    String(String),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Boolean(bool),
    /// Array of values
    Array(Vec<Value>),
    /// Object/map of key-value pairs
    Object(HashMap<String, Value>),
    /// Binary data
    Bytes(Vec<u8>),
    /// Null value
    Null,
}

// Implement PartialOrd manually with custom type ordering
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;

        match (self, other) {
            // Same types comparison
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Array(a), Value::Array(b)) => a.partial_cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
            (Value::Null, Value::Null) => Some(Ordering::Equal),

            // Cross-type numeric comparisons
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),

            // HashMap comparisons - compare by size first, then by key count
            (Value::Object(a), Value::Object(b)) => {
                match a.len().partial_cmp(&b.len()) {
                    Some(Ordering::Equal) => {
                        // If same size, compare by number of keys lexicographically
                        let mut a_keys: Vec<_> = a.keys().collect();
                        let mut b_keys: Vec<_> = b.keys().collect();
                        a_keys.sort();
                        b_keys.sort();
                        a_keys.partial_cmp(&b_keys)
                    }
                    other => other,
                }
            }

            // Different types comparison - use explicit type ordering
            // Define a custom hierarchy: Null < Boolean < Integer < Float < String < Bytes < Array < Object
            _ => {
                let type_order = |v: &Value| match v {
                    Value::Null => 0,
                    Value::Boolean(_) => 1,
                    Value::Integer(_) => 2,
                    Value::Float(_) => 3,
                    Value::String(_) => 4,
                    Value::Bytes(_) => 5,
                    Value::Array(_) => 6,
                    Value::Object(_) => 7,
                };

                let a_order = type_order(self);
                let b_order = type_order(other);
                a_order.partial_cmp(&b_order)
            }
        }
    }
}

impl Value {
    /// Create a string value
    pub fn string<S: Into<String>>(s: S) -> Self {
        Self::String(s.into())
    }

    /// Create an integer value
    pub fn int(i: i64) -> Self {
        Self::Integer(i)
    }

    /// Create a float value
    pub fn float(f: f64) -> Self {
        Self::Float(f)
    }

    /// Create a boolean value
    pub fn bool(b: bool) -> Self {
        Self::Boolean(b)
    }

    /// Create an array value
    pub fn array(arr: Vec<Value>) -> Self {
        Self::Array(arr)
    }

    /// Create an object value
    pub fn object(obj: HashMap<String, Value>) -> Self {
        Self::Object(obj)
    }

    /// Create a bytes value
    pub fn bytes(data: Vec<u8>) -> Self {
        Self::Bytes(data)
    }

    /// Create a null value
    pub fn null() -> Self {
        Self::Null
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get value type as string
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::Integer(_) => "integer",
            Self::Float(_) => "float",
            Self::Boolean(_) => "boolean",
            Self::Array(_) => "array",
            Self::Object(_) => "object",
            Self::Bytes(_) => "bytes",
            Self::Null => "null",
        }
    }

    /// Try to convert to string
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to convert to integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(i) => Some(*i),
            Self::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Try to convert to float
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert to boolean
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to convert to array
    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Self::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Try to convert to object
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Self::Object(obj) => Some(obj),
            _ => None,
        }
    }

    /// Try to convert to bytes
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Self::Bytes(data) => Some(data),
            _ => None,
        }
    }

    /// Convert to string representation
    pub fn to_string_value(&self) -> String {
        match self {
            Self::String(s) => s.clone(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Boolean(b) => b.to_string(),
            Self::Array(arr) => format!(
                "[{}]",
                arr.iter()
                    .map(|v| v.to_string_value())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Object(obj) => format!(
                "{{{}}}",
                obj.iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_string_value()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Bytes(data) => format!("bytes[{}]", data.len()),
            Self::Null => "null".to_string(),
        }
    }

    /// Get size in bytes (approximate)
    pub fn size_bytes(&self) -> usize {
        match self {
            Self::String(s) => s.len(),
            Self::Integer(_) => 8,
            Self::Float(_) => 8,
            Self::Boolean(_) => 1,
            Self::Array(arr) => arr.iter().map(|v| v.size_bytes()).sum::<usize>() + 24,
            Self::Object(obj) => {
                obj.iter()
                    .map(|(k, v)| k.len() + v.size_bytes())
                    .sum::<usize>()
                    + 24
            }
            Self::Bytes(data) => data.len(),
            Self::Null => 0,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string_value())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self::Integer(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Self::Integer(i as i64)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self::Float(f)
    }
}

impl From<f32> for Value {
    fn from(f: f32) -> Self {
        Self::Float(f as f64)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self::Boolean(b)
    }
}

impl From<Vec<Value>> for Value {
    fn from(arr: Vec<Value>) -> Self {
        Self::Array(arr)
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(obj: HashMap<String, Value>) -> Self {
        Self::Object(obj)
    }
}

impl From<Vec<u8>> for Value {
    fn from(data: Vec<u8>) -> Self {
        Self::Bytes(data)
    }
}
