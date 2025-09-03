//! Database client example - Connect to the database server and perform operations

use mini_database::{NetworkDatabaseClient, Node, Value};
use tracing::{Level, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("🔌 Connecting to Mini Database Server");

    // Connect to server
    let mut client = NetworkDatabaseClient::connect("127.0.0.1", 5431).await?;
    info!("✅ Connected to database server");

    // Test ping
    client.ping().await?;
    info!("🏓 Ping successful");

    // Create some nodes
    info!("📝 Creating test data...");

    let alice = Node::new("person")
        .with_property("name", Value::String("Alice".to_string()))
        .with_property("age", Value::Integer(30))
        .with_property("city", Value::String("New York".to_string()));

    let bob = Node::new("person")
        .with_property("name", Value::String("Bob".to_string()))
        .with_property("age", Value::Integer(25))
        .with_property("city", Value::String("San Francisco".to_string()));

    // Store nodes
    let alice_id = client.create_node(alice).await?;
    let bob_id = client.create_node(bob).await?;

    info!("👤 Created Alice: {}", alice_id);
    info!("👤 Created Bob: {}", bob_id);

    // Retrieve nodes
    if let Some(alice_node) = client.get_node(&alice_id).await? {
        let name = alice_node.get_property("name").unwrap();
        let age = alice_node.get_property("age").unwrap();
        info!("📖 Retrieved Alice: {} (age: {})", name, age);
    }

    // Perform graph traversal
    info!("🔍 Performing BFS from Alice...");
    let bfs_result = client.bfs(&alice_id, 2).await?;
    info!("🔍 BFS found {} nodes", bfs_result.len());
    for node in &bfs_result {
        if let Some(name) = node.get_property("name") {
            info!("  🎯 Found: {}", name);
        }
    }

    info!("✅ Client operations completed successfully");

    Ok(())
}
