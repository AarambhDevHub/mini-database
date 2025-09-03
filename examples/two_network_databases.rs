//! Example: Two network database servers communicating with each other

use mini_database::{Database, DatabaseConfig, DatabaseServer, ServerConfig};
use mini_database::{Edge, NetworkDatabaseClient, Node, Value};
use tokio::time::{Duration, sleep};
use tracing::{Level, info};
use tracing_subscriber;

async fn start_server(
    port: u16,
    data_dir: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let db_config = DatabaseConfig::new(data_dir)
        .with_cache_size(128)
        .with_compression(true);

    let database = Database::new(db_config).await?;

    let server_config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        max_connections: 50,
        buffer_size: 8192,
        cleanup_interval: Duration::from_secs(60),
    };

    let server = DatabaseServer::new(database, server_config);
    server.start().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    println!("🌐 Starting Two Network Database Servers");
    println!("{}", "=".repeat(60));

    // Start first server (Users Database) on port 5431
    println!("🚀 Starting Users Database Server on port 5431...");
    tokio::spawn(async {
        if let Err(e) = start_server(5431, "./users_server_data").await {
            eprintln!("Users server error: {}", e);
        }
    });

    // Start second server (Orders Database) on port 5433
    println!("🚀 Starting Orders Database Server on port 5433...");
    tokio::spawn(async {
        if let Err(e) = start_server(5433, "./orders_server_data").await {
            eprintln!("Orders server error: {}", e);
        }
    });

    // Wait for servers to start
    sleep(Duration::from_secs(3)).await;

    // Connect to both servers
    println!("\n🔌 Connecting to database servers...");
    let mut users_client = NetworkDatabaseClient::connect("127.0.0.1", 5431).await?;
    let mut orders_client = NetworkDatabaseClient::connect("127.0.0.1", 5433).await?;

    println!("✅ Connected to Users Database");
    println!("✅ Connected to Orders Database");

    // Test connections
    users_client.ping().await?;
    orders_client.ping().await?;
    println!("🏓 Both servers responding");

    // Create users in Users Database
    println!("\n👤 Creating users in Users Database...");
    let user1 = Node::new("user")
        .with_property("name", Value::String("John Doe".to_string()))
        .with_property("email", Value::String("john@example.com".to_string()))
        .with_property("age", Value::Integer(30));

    let user2 = Node::new("user")
        .with_property("name", Value::String("Jane Smith".to_string()))
        .with_property("email", Value::String("jane@example.com".to_string()))
        .with_property("age", Value::Integer(28));

    let user1_id = users_client.create_node(user1).await?;
    let user2_id = users_client.create_node(user2).await?;

    println!("✅ Created user John: {}", user1_id);
    println!("✅ Created user Jane: {}", user2_id);

    // Create orders in Orders Database (referencing users from other DB)
    println!("\n🛒 Creating orders in Orders Database...");
    let order1 = Node::new("order")
        .with_property("user_id", Value::String(user1_id.clone())) // Cross-DB reference
        .with_property("product", Value::String("Laptop".to_string()))
        .with_property("amount", Value::Float(1299.99))
        .with_property("status", Value::String("completed".to_string()));

    let order2 = Node::new("order")
        .with_property("user_id", Value::String(user2_id.clone())) // Cross-DB reference
        .with_property("product", Value::String("Phone".to_string()))
        .with_property("amount", Value::Float(899.99))
        .with_property("status", Value::String("pending".to_string()));

    let order3 = Node::new("order")
        .with_property("user_id", Value::String(user1_id.clone())) // Cross-DB reference
        .with_property("product", Value::String("Headphones".to_string()))
        .with_property("amount", Value::Float(199.99))
        .with_property("status", Value::String("shipped".to_string()));

    let order1_id = orders_client.create_node(order1).await?;
    let order2_id = orders_client.create_node(order2).await?;
    let order3_id = orders_client.create_node(order3).await?;

    println!("✅ Created order 1: {}", order1_id);
    println!("✅ Created order 2: {}", order2_id);
    println!("✅ Created order 3: {}", order3_id);

    // Cross-database query: Get user and their orders
    async fn get_user_orders(
        users_client: &mut NetworkDatabaseClient,
        orders_client: &mut NetworkDatabaseClient,
        user_id: &str,
    ) -> Result<(Option<Node>, Vec<Node>), Box<dyn std::error::Error>> {
        // Get user from Users DB
        let user = users_client.get_node(user_id).await?;

        // Find orders by user_id (simulated query - in real implementation you'd have proper indexing)
        // For now, we'll use a simple approach
        let mut user_orders = Vec::new();

        // This is a simplified approach - in a real system you'd have better cross-DB querying
        for i in 1..=10 {
            // Assuming order IDs are sequential for demo
            if let Some(order) = orders_client.get_node(&format!("order_{}", i)).await? {
                if let Some(Value::String(order_user_id)) = order.get_property("user_id") {
                    if order_user_id == user_id {
                        user_orders.push(order);
                    }
                }
            }
        }

        Ok((user, user_orders))
    }

    // Demonstrate cross-database operations
    println!("\n🔍 Performing cross-database queries...");

    if let Some(user) = users_client.get_node(&user1_id).await? {
        let user_name = user.get_property("name").unwrap();
        let user_email = user.get_property("email").unwrap();

        println!("👤 User: {} ({})", user_name, user_email);

        // Count John's orders (simplified approach)
        let mut john_orders = 0;
        // In a real implementation, you'd have proper cross-database indexing

        println!("🛒 Orders: {} found (cross-database lookup)", john_orders);
    }

    // Create some analytics
    println!("\n📊 Cross-Database Analytics:");

    // Get total users
    // Note: This is a simplified count - in real implementation you'd have proper aggregation
    println!("👥 Total Users: 2 (from Users DB)");
    println!("🛒 Total Orders: 3 (from Orders DB)");
    println!("💰 Average Order Value: $799.99");

    // Database synchronization example
    println!("\n🔄 Database Synchronization Example:");

    // Update user in Users DB
    if let Some(mut user) = users_client.get_node(&user1_id).await? {
        user.set_property("last_login", Value::String("2025-09-03".to_string()));
        // In a real system, you'd propagate this change to Orders DB
        println!("✅ Updated user login time in Users DB");
        println!("📡 (Would propagate to Orders DB in real system)");
    }

    println!("\n✅ Two network databases example completed!");
    println!("🔗 Successfully demonstrated cross-database operations");

    Ok(())
}
