use crate::client::DatabaseClient;
use crate::core::Database;
use crate::server::handler::RequestHandler;
use crate::server::{ConnectionPool, DatabaseProtocol, Request, Response};
use crate::utils::error::{DatabaseError, Result};

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

/// Database server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
    pub buffer_size: usize,
    pub cleanup_interval: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 5432, // PostgreSQL default port
            max_connections: 100,
            buffer_size: 8192,
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

/// Database server
pub struct DatabaseServer {
    database: Arc<Database>,
    client: Arc<DatabaseClient>,
    connection_pool: Arc<ConnectionPool>,
    config: ServerConfig,
}

impl DatabaseServer {
    /// Create new database server
    pub fn new(database: Database, config: ServerConfig) -> Self {
        let database = Arc::new(database);
        let client = Arc::new(DatabaseClient::new(database.as_ref().clone()));
        let connection_pool = Arc::new(ConnectionPool::new(config.max_connections));

        Self {
            database,
            client,
            connection_pool,
            config,
        }
    }

    /// Start the database server
    pub async fn start(&self) -> Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| DatabaseError::Connection(format!("Failed to bind to {}: {}", addr, e)))?;

        info!("🚀 Database server started on {}", addr);
        info!("📊 Max connections: {}", self.config.max_connections);

        // Start cleanup task
        let cleanup_pool = self.connection_pool.clone();
        let cleanup_interval = self.config.cleanup_interval;
        tokio::spawn(async move {
            let mut interval = interval(cleanup_interval);
            loop {
                interval.tick().await;
                cleanup_pool.cleanup_idle_connections().await;
            }
        });

        // Accept connections
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("📡 New connection from: {}", addr);

                    // Check connection limit
                    if let Some(_permit) = self.connection_pool.acquire_connection().await {
                        let server = Arc::new(self.clone());
                        tokio::spawn(async move {
                            if let Err(e) = server.handle_connection(stream).await {
                                error!("❌ Connection error: {}", e);
                            }
                        });
                    } else {
                        warn!("🚫 Connection limit reached, rejecting: {}", addr);
                        // Send rejection response and close
                        let mut stream = stream;
                        let response = Response::Error {
                            message: "Server connection limit reached".to_string(),
                            code: 503,
                        };
                        if let Ok(data) = DatabaseProtocol::serialize_response(&response) {
                            let _ = stream.write_all(&data).await;
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Handle a client connection
    async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let connection_id = self
            .connection_pool
            .register_connection(&stream, None)
            .await?;

        let mut buffer = vec![0; self.config.buffer_size];

        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => {
                    debug!("🔌 Connection closed by client: {}", connection_id);
                    break;
                }
                Ok(n) => {
                    // Update connection activity
                    self.connection_pool.update_activity(&connection_id).await;

                    // Process request
                    let response = match DatabaseProtocol::deserialize_request(&buffer[..n]) {
                        Ok(request) => {
                            debug!("📥 Request from {}: {:?}", connection_id, request);
                            self.process_request(request, &connection_id).await
                        }
                        Err(e) => {
                            warn!("📛 Invalid request from {}: {}", connection_id, e);
                            Response::InvalidRequest {
                                message: format!("Failed to parse request: {}", e),
                            }
                        }
                    };

                    // Send response
                    match DatabaseProtocol::serialize_response(&response) {
                        Ok(response_data) => {
                            if let Err(e) = stream.write_all(&response_data).await {
                                error!("📤 Failed to send response to {}: {}", connection_id, e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("📦 Failed to serialize response: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("📡 Connection error with {}: {}", connection_id, e);
                    break;
                }
            }
        }

        self.connection_pool.remove_connection(&connection_id).await;
        Ok(())
    }

    /// Process a client request
    async fn process_request(&self, request: Request, connection_id: &str) -> Response {
        let handler = RequestHandler::new(self.database.clone(), self.connection_pool.clone());
        handler.handle_request(request, connection_id).await
    }
}

// Make server cloneable for async tasks
impl Clone for DatabaseServer {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            client: self.client.clone(),
            connection_pool: self.connection_pool.clone(),
            config: self.config.clone(),
        }
    }
}
