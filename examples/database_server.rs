//! Database server example - Run this to start the database server

use mini_database::{Database, DatabaseConfig, DatabaseServer, ServerConfig};
use tokio::signal;
use tracing::{Level, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("🚀 Starting Mini Database Server");

    // Create database
    let db_config = DatabaseConfig::new("./server_data")
        .with_cache_size(128) // 128MB cache
        .with_compression(true);

    let database = Database::new(db_config).await?;

    // Create server config
    let server_config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 5431,
        max_connections: 100,
        buffer_size: 8192,
        cleanup_interval: tokio::time::Duration::from_secs(60),
    };

    info!("📊 Server Configuration:");
    info!(
        "  📍 Address: {}:{}",
        server_config.host, server_config.port
    );
    info!("  🔗 Max Connections: {}", server_config.max_connections);
    info!("  📦 Buffer Size: {} bytes", server_config.buffer_size);

    // Create and start server
    let server = DatabaseServer::new(database, server_config);

    // Handle graceful shutdown
    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("❌ Server error: {}", e);
        }
    });

    // Wait for Ctrl+C
    signal::ctrl_c().await?;
    info!("🛑 Shutting down server...");

    server_handle.abort();
    info!("✅ Server stopped");

    Ok(())
}
