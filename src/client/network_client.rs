use crate::server::{DatabaseProtocol, Request, Response};
use crate::types::Node;
use crate::utils::error::{DatabaseError, Result};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

/// Network database client
pub struct NetworkDatabaseClient {
    stream: TcpStream,
    server_addr: String,
    client_id: String,
    buffer_size: usize,
}

impl NetworkDatabaseClient {
    /// Connect to database server
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let server_addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&server_addr).await.map_err(|e| {
            DatabaseError::Connection(format!("Failed to connect to {}: {}", server_addr, e))
        })?;

        let client_id = uuid::Uuid::new_v4().to_string();

        let mut client = Self {
            stream,
            server_addr: server_addr.clone(),
            client_id: client_id.clone(),
            buffer_size: 8192,
        };

        // Send connect request
        let connect_request = Request::Connect { client_id };
        match client.send_request(connect_request).await? {
            Response::Connected { session_id } => {
                info!(
                    "✅ Connected to database server: {} (session: {})",
                    server_addr, session_id
                );
                Ok(client)
            }
            Response::Error { message, .. } => Err(DatabaseError::Connection(format!(
                "Connection rejected: {}",
                message
            ))),
            _ => Err(DatabaseError::Connection(
                "Unexpected connection response".to_string(),
            )),
        }
    }

    /// Send request and receive response
    async fn send_request(&mut self, request: Request) -> Result<Response> {
        // Serialize and send request
        let request_data = DatabaseProtocol::serialize_request(&request)?;
        self.stream
            .write_all(&request_data)
            .await
            .map_err(|e| DatabaseError::Connection(format!("Failed to send request: {}", e)))?;

        // Read response
        let mut buffer = vec![0; self.buffer_size];
        let n =
            self.stream.read(&mut buffer).await.map_err(|e| {
                DatabaseError::Connection(format!("Failed to read response: {}", e))
            })?;

        // Deserialize response
        let response = DatabaseProtocol::deserialize_response(&buffer[..n])?;
        debug!("📥 Received response: {:?}", response);

        Ok(response)
    }

    /// Create a node
    pub async fn create_node(&mut self, node: Node) -> Result<String> {
        let request = Request::CreateNode { node };
        match self.send_request(request).await? {
            Response::NodeCreated { node_id } => Ok(node_id),
            Response::Error { message, .. } => Err(DatabaseError::Query(message)),
            _ => Err(DatabaseError::Query("Unexpected response".to_string())),
        }
    }

    /// Get a node
    pub async fn get_node(&mut self, node_id: &str) -> Result<Option<Node>> {
        let request = Request::GetNode {
            node_id: node_id.to_string(),
        };
        match self.send_request(request).await? {
            Response::NodeData { node } => Ok(node),
            Response::Error { message, .. } => Err(DatabaseError::Query(message)),
            _ => Err(DatabaseError::Query("Unexpected response".to_string())),
        }
    }

    /// Breadth-first search
    pub async fn bfs(&mut self, start_node: &str, max_depth: usize) -> Result<Vec<Node>> {
        let request = Request::BreadthFirstSearch {
            start_node: start_node.to_string(),
            max_depth,
        };
        match self.send_request(request).await? {
            Response::TraversalResult { nodes } => Ok(nodes),
            Response::Error { message, .. } => Err(DatabaseError::Query(message)),
            _ => Err(DatabaseError::Query("Unexpected response".to_string())),
        }
    }

    /// Ping server
    pub async fn ping(&mut self) -> Result<()> {
        let request = Request::Ping;
        match self.send_request(request).await? {
            Response::Pong => Ok(()),
            Response::Error { message, .. } => Err(DatabaseError::Connection(message)),
            _ => Err(DatabaseError::Connection(
                "Unexpected ping response".to_string(),
            )),
        }
    }
}
