//! Example: Database federation - A proxy that coordinates multiple databases

use mini_database::{Database, DatabaseClient, DatabaseConfig, Edge, Node, Value};
use std::collections::HashMap;
use tracing::{Level, info};
use tracing_subscriber;

/// A federation manager that coordinates multiple databases
pub struct DatabaseFederation {
    databases: HashMap<String, DatabaseClient>,
}

impl DatabaseFederation {
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    pub async fn add_database(
        &mut self,
        name: &str,
        data_dir: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config = DatabaseConfig::new(data_dir)
            .with_cache_size(64)
            .with_compression(true);

        let database = Database::new(config).await?;
        let client = DatabaseClient::new(database);

        self.databases.insert(name.to_string(), client);
        info!("Added database: {}", name);
        Ok(())
    }

    pub fn get_database(&self, name: &str) -> Option<&DatabaseClient> {
        self.databases.get(name)
    }

    /// Cross-database query: Find related nodes across all databases
    pub async fn find_related_nodes(
        &self,
        node_id: &str,
        relation: &str,
    ) -> Result<Vec<(String, Node)>, Box<dyn std::error::Error>> {
        let mut related_nodes = Vec::new();

        for (db_name, client) in &self.databases {
            // Look for nodes that reference this node_id
            let nodes = client
                .find_nodes_by_property(relation, &Value::String(node_id.to_string()))
                .await?;

            for node in nodes {
                related_nodes.push((db_name.clone(), node));
            }
        }

        Ok(related_nodes)
    }

    /// Get comprehensive statistics across all databases
    pub async fn get_federation_stats(
        &self,
    ) -> Result<FederationStats, Box<dyn std::error::Error>> {
        let mut total_nodes = 0;
        let mut total_edges = 0;
        let mut db_stats = HashMap::new();

        for (db_name, client) in &self.databases {
            let stats = client.get_stats().await?;
            total_nodes += stats.node_count;
            total_edges += stats.edge_count;
            db_stats.insert(db_name.clone(), stats);
        }

        Ok(FederationStats {
            total_nodes,
            total_edges,
            database_count: self.databases.len(),
            individual_stats: db_stats,
        })
    }
}

#[derive(Debug)]
pub struct FederationStats {
    pub total_nodes: u64,
    pub total_edges: u64,
    pub database_count: usize,
    pub individual_stats: HashMap<String, mini_database::core::DatabaseStats>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    println!("🔗 Database Federation Example");
    println!("{}", "=".repeat(50));

    // Create federation manager
    let mut federation = DatabaseFederation::new();

    // Add multiple databases to federation
    println!("📊 Setting up database federation...");
    federation
        .add_database("users", "./federation_users")
        .await?;
    federation
        .add_database("products", "./federation_products")
        .await?;
    federation
        .add_database("orders", "./federation_orders")
        .await?;
    federation
        .add_database("analytics", "./federation_analytics")
        .await?;

    println!("✅ Federation created with 4 databases");

    // Create data in different databases
    println!("\n👤 Creating users...");
    let users_db = federation.get_database("users").unwrap();
    let user1 = Node::new("user")
        .with_property("name", Value::String("Alice Cooper".to_string()))
        .with_property("email", Value::String("alice@example.com".to_string()));
    let user1_id = users_db.create_node(user1).await?;

    println!("📦 Creating products...");
    let products_db = federation.get_database("products").unwrap();
    let product1 = Node::new("product")
        .with_property("name", Value::String("Gaming Laptop".to_string()))
        .with_property("price", Value::Float(1599.99))
        .with_property("category", Value::String("Electronics".to_string()));
    let product1_id = products_db.create_node(product1).await?;

    println!("🛒 Creating orders...");
    let orders_db = federation.get_database("orders").unwrap();
    let order1 = Node::new("order")
        .with_property("user_id", Value::String(user1_id.clone()))
        .with_property("product_id", Value::String(product1_id.clone()))
        .with_property("quantity", Value::Integer(1))
        .with_property("total", Value::Float(1599.99));
    let order1_id = orders_db.create_node(order1).await?;

    println!("📈 Creating analytics data...");
    let analytics_db = federation.get_database("analytics").unwrap();
    let analytics1 = Node::new("purchase_event")
        .with_property("user_id", Value::String(user1_id.clone()))
        .with_property("order_id", Value::String(order1_id.clone()))
        .with_property(
            "timestamp",
            Value::String("2025-09-03T10:30:00Z".to_string()),
        )
        .with_property("revenue", Value::Float(1599.99));
    analytics_db.create_node(analytics1).await?;

    // Demonstrate cross-database queries
    println!("\n🔍 Cross-database relationship queries...");

    // Find all nodes related to user1
    let user_related = federation.find_related_nodes(&user1_id, "user_id").await?;
    println!(
        "Found {} nodes related to user {} across databases:",
        user_related.len(),
        user1_id
    );

    for (db_name, node) in user_related {
        println!("  📍 {}: {} node", db_name, node.label);
    }

    // Get federation statistics
    println!("\n📊 Federation Statistics:");
    let fed_stats = federation.get_federation_stats().await?;

    println!("🏢 Total Databases: {}", fed_stats.database_count);
    println!("📦 Total Nodes: {}", fed_stats.total_nodes);
    println!("🔗 Total Edges: {}", fed_stats.total_edges);

    println!("\n📋 Individual Database Stats:");
    for (db_name, stats) in &fed_stats.individual_stats {
        println!(
            "  • {}: {} nodes, {} edges",
            db_name, stats.node_count, stats.edge_count
        );
    }

    // Flush all databases
    println!("\n💾 Flushing all databases...");
    for (name, client) in &federation.databases {
        client.flush().await?;
        println!("✅ Flushed {}", name);
    }

    println!("\n🎉 Database Federation example completed!");
    println!("🔗 Successfully demonstrated multi-database coordination");

    Ok(())
}
