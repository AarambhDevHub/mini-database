//! # Mini Database
//!
//! A high-performance mini graph database with node/edge storage,
//! ultra-fast file reading, and comprehensive query capabilities.

pub mod client;
pub mod core;
pub mod query;
pub mod server;
pub mod storage;
pub mod types;
pub mod utils;

// Re-export main API
pub use client::DatabaseClient;
pub use core::{Database, DatabaseConfig};
pub use types::{Edge, Node, Value};
pub use utils::error::{DatabaseError, Result};

// Re-export query builder
pub use query::QueryBuilder;

// Re-export server components
pub use client::NetworkDatabaseClient;
pub use client::{AggregateFunction, JoinBuilder, JoinCondition, JoinResult, JoinType};
pub use server::{ConnectionPool, DatabaseServer, ServerConfig};

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_basic_operations() -> Result<()> {
        let config = DatabaseConfig::default();
        let db = Database::new(config).await?;
        let client = DatabaseClient::new(db);

        // Create a node
        let node = Node::new("person")
            .with_property("name", Value::String("Alice".to_string()))
            .with_property("age", Value::Integer(30));

        let node_id = client.create_node(node).await?;

        // Retrieve the node
        let retrieved = client.get_node(&node_id).await?;
        assert!(retrieved.is_some());

        Ok(())
    }
}
