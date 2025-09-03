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
