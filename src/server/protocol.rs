use crate::client::QueryResult;
use crate::types::{Edge, Node};
use serde::{Deserialize, Serialize};

/// Database network protocol messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    // Connection management
    Connect {
        client_id: String,
    },
    Disconnect {
        client_id: String,
    },
    Ping,

    // Node operations
    CreateNode {
        node: Node,
    },
    GetNode {
        node_id: String,
    },
    UpdateNode {
        node: Node,
    },
    DeleteNode {
        node_id: String,
    },
    FindNodesByLabel {
        label: String,
    },

    // Edge operations
    CreateEdge {
        edge: Edge,
    },
    GetEdge {
        edge_id: String,
    },
    DeleteEdge {
        edge_id: String,
    },
    GetNodeEdges {
        node_id: String,
    },

    // Graph operations
    BreadthFirstSearch {
        start_node: String,
        max_depth: usize,
    },
    DepthFirstSearch {
        start_node: String,
        max_depth: usize,
    },
    ShortestPath {
        start_node: String,
        end_node: String,
    },

    // Query operations
    ExecuteQuery {
        query: String,
    }, // JSON serialized query

    // Management operations
    GetStats,
    Flush,
    Export,

    // Transaction operations (future)
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    // Success responses
    Ok,
    Connected { session_id: String },
    Disconnected,
    Pong,

    // Data responses
    NodeCreated { node_id: String },
    NodeData { node: Option<Node> },
    NodesData { nodes: Vec<Node> },
    EdgeCreated { edge_id: String },
    EdgeData { edge: Option<Edge> },
    EdgesData { edges: Vec<Edge> },
    QueryResult { result: QueryResult },
    Stats { stats: crate::core::DatabaseStats },
    ExportData { data: String },

    // Graph responses
    PathFound { nodes: Vec<Node> },
    NoPathFound,
    TraversalResult { nodes: Vec<Node> },

    // Error responses
    Error { message: String, code: u32 },
    InvalidRequest { message: String },
    NotFound { resource: String },
    Unauthorized,
    ServerError { message: String },
}

/// Database protocol handler
pub struct DatabaseProtocol;

impl DatabaseProtocol {
    /// Serialize request to bytes
    pub fn serialize_request(request: &Request) -> crate::utils::error::Result<Vec<u8>> {
        crate::utils::serde::serialize(request)
    }

    /// Deserialize request from bytes
    pub fn deserialize_request(data: &[u8]) -> crate::utils::error::Result<Request> {
        crate::utils::serde::deserialize(data)
    }

    /// Serialize response to bytes
    pub fn serialize_response(response: &Response) -> crate::utils::error::Result<Vec<u8>> {
        crate::utils::serde::serialize(response)
    }

    /// Deserialize response from bytes
    pub fn deserialize_response(data: &[u8]) -> crate::utils::error::Result<Response> {
        crate::utils::serde::deserialize(data)
    }
}
