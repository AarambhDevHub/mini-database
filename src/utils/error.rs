use thiserror::Error;

/// Custom error types and error handling mechanisms
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("I/O error: {0}")]
    Io(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Deadlock detected: {0}")]
    Deadlock(String),

    #[error("Lock timeout: {0}")]
    LockTimeout(String),

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Authentication error: {0}")]
    Authentication(String),

    #[error("Authorization error: {0}")]
    Authorization(String),

    #[error("Generic error: {0}")]
    Generic(String),
}

impl DatabaseError {
    /// Create an I/O error
    pub fn io<S: Into<String>>(msg: S) -> Self {
        Self::Io(msg.into())
    }

    /// Create a serialization error
    pub fn serialization<S: Into<String>>(msg: S) -> Self {
        Self::Serialization(msg.into())
    }

    /// Create a not found error
    pub fn not_found<S: Into<String>>(msg: S) -> Self {
        Self::NotFound(msg.into())
    }

    /// Create an invalid operation error
    pub fn invalid_operation<S: Into<String>>(msg: S) -> Self {
        Self::InvalidOperation(msg.into())
    }

    /// Create a query error
    pub fn query<S: Into<String>>(msg: S) -> Self {
        Self::Query(msg.into())
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Io(_)
                | Self::Timeout(_)
                | Self::Connection(_)
                | Self::ResourceExhausted(_)
                | Self::Internal(_)
        )
    }

    /// Check if error is retriable
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Io(_) | Self::Timeout(_) | Self::Connection(_) | Self::ResourceExhausted(_)
        )
    }

    /// Get error category
    pub fn category(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Serialization(_) => "serialization",
            Self::NotFound(_) => "not_found",
            Self::InvalidOperation(_) => "invalid_operation",
            Self::Configuration(_) => "configuration",
            Self::Index(_) => "index",
            Self::Query(_) => "query",
            Self::Transaction(_) => "transaction",
            Self::Connection(_) => "connection",
            Self::Timeout(_) => "timeout",
            Self::PermissionDenied(_) => "permission_denied",
            Self::ResourceExhausted(_) => "resource_exhausted",
            Self::Internal(_) => "internal",
            Self::Network(_) => "network",
            Self::Protocol(_) => "protocol",
            Self::Deadlock(_) => "deadlock",
            Self::Authentication(_) => "authentication",
            Self::Authorization(_) => "authorization",
            Self::LockTimeout(_) => "lock_timeout",
            Self::ConstraintViolation(_) => "constraint_violation",
            Self::Config(_) => "config",
            Self::Generic(_) => "generic",
        }
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<bincode::Error> for DatabaseError {
    fn from(error: bincode::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}

impl From<serde_json::Error> for DatabaseError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}

/// Result type alias for database operations
pub type Result<T> = std::result::Result<T, DatabaseError>;

/// Error context helper
pub trait ErrorContext<T> {
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: std::fmt::Display,
{
    fn with_context<F>(self, f: F) -> Result<T>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| DatabaseError::Internal(format!("{}: {}", f(), e)))
    }
}
