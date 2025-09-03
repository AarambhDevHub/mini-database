use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};
use uuid::Uuid;

/// Connection information
#[derive(Debug, Clone)]
pub struct Connection {
    pub id: String,
    pub client_addr: String,
    pub connected_at: Instant,
    pub last_activity: Instant,
    pub query_count: u64,
}

/// Database connection pool
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<String, Connection>>>,
    max_connections: usize,
    connection_semaphore: Arc<Semaphore>,
    idle_timeout: Duration,
}

impl ConnectionPool {
    /// Create new connection pool
    pub fn new(max_connections: usize) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections,
            connection_semaphore: Arc::new(Semaphore::new(max_connections)),
            idle_timeout: Duration::from_secs(300), // 5 minutes
        }
    }

    /// Acquire a connection permit
    pub async fn acquire_connection(&self) -> Option<tokio::sync::SemaphorePermit> {
        self.connection_semaphore.try_acquire().ok()
    }

    /// Register a new connection
    pub async fn register_connection(
        &self,
        stream: &TcpStream,
        client_id: Option<String>,
    ) -> crate::utils::error::Result<String> {
        let connection_id = client_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let peer_addr = stream
            .peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        let connection = Connection {
            id: connection_id.clone(),
            client_addr: peer_addr,
            connected_at: Instant::now(),
            last_activity: Instant::now(),
            query_count: 0,
        };

        let mut connections = self.connections.write().await;
        connections.insert(connection_id.clone(), connection);

        info!("New connection registered: {}", connection_id);
        Ok(connection_id)
    }

    /// Update connection activity
    pub async fn update_activity(&self, connection_id: &str) {
        let mut connections = self.connections.write().await;
        if let Some(connection) = connections.get_mut(connection_id) {
            connection.last_activity = Instant::now();
            connection.query_count += 1;
        }
    }

    /// Remove connection
    pub async fn remove_connection(&self, connection_id: &str) -> bool {
        let mut connections = self.connections.write().await;
        if connections.remove(connection_id).is_some() {
            info!("Connection removed: {}", connection_id);
            true
        } else {
            false
        }
    }

    /// Get connection info
    pub async fn get_connection(&self, connection_id: &str) -> Option<Connection> {
        let connections = self.connections.read().await;
        connections.get(connection_id).cloned()
    }

    /// Get active connection count
    pub async fn active_connections(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }

    /// Cleanup idle connections
    pub async fn cleanup_idle_connections(&self) {
        let mut connections = self.connections.write().await;
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (id, connection) in connections.iter() {
            if now.duration_since(connection.last_activity) > self.idle_timeout {
                to_remove.push(id.clone());
            }
        }

        for id in to_remove {
            connections.remove(&id);
            warn!("Removed idle connection: {}", id);
        }
    }

    /// Get connection statistics
    pub async fn get_stats(&self) -> ConnectionPoolStats {
        let connections = self.connections.read().await;
        let total_queries: u64 = connections.values().map(|c| c.query_count).sum();

        ConnectionPoolStats {
            active_connections: connections.len(),
            max_connections: self.max_connections,
            total_queries,
            available_slots: self.connection_semaphore.available_permits(),
        }
    }
}

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct ConnectionPoolStats {
    pub active_connections: usize,
    pub max_connections: usize,
    pub total_queries: u64,
    pub available_slots: usize,
}
