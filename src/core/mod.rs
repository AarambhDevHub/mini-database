//! Core database functionality

pub mod config;
pub mod database;

pub use config::DatabaseConfig;
pub use database::{Database, DatabaseStats};
