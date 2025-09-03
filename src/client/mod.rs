//! Client interface for database operations

pub mod database_client;
pub mod network_client;
pub mod result;

pub use database_client::DatabaseClient;
pub use network_client::NetworkDatabaseClient;
pub use result::{QueryCursor, QueryResult, ResultIterator};
