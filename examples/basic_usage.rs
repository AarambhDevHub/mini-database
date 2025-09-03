//! Basic database usage examples

use mini_database::{Database, DatabaseClient, DatabaseConfig, Edge, Node, Value};
use tracing::{Level, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Mini Database basic usage example");

    // Create database configuration
    let config = DatabaseConfig::new("./examples/data")
        .with_cache_size(64) // 64MB cache
        .with_compression(true);

    // Initialize database
    let database = Database::new(config).await?;
    let client = DatabaseClient::new(database);

    // Create some nodes
    info!("Creating nodes...");
    let alice = Node::new("person")
        .with_property("name", Value::String("Alice".to_string()))
        .with_property("age", Value::Integer(30))
        .with_property("city", Value::String("New York".to_string()));

    let bob = Node::new("person")
        .with_property("name", Value::String("Bob".to_string()))
        .with_property("age", Value::Integer(25))
        .with_property("city", Value::String("San Francisco".to_string()));

    let company = Node::new("company")
        .with_property("name", Value::String("TechCorp".to_string()))
        .with_property("founded", Value::Integer(2010))
        .with_property("employees", Value::Integer(500));

    // Store nodes
    let alice_id = client.create_node(alice).await?;
    let bob_id = client.create_node(bob).await?;
    let company_id = client.create_node(company).await?;

    info!(
        "Created nodes: Alice={}, Bob={}, Company={}",
        alice_id, bob_id, company_id
    );

    // Create edges/relationships
    info!("Creating relationships...");
    let friendship = Edge::new(&alice_id, &bob_id, "friends")
        .with_property("since", Value::String("2020".to_string()))
        .with_property("strength", Value::Float(0.8));

    let alice_works = Edge::new(&alice_id, &company_id, "works_at")
        .with_property("position", Value::String("Senior Engineer".to_string()))
        .with_property("salary", Value::Integer(120000));

    let bob_works = Edge::new(&bob_id, &company_id, "works_at")
        .with_property("position", Value::String("Product Manager".to_string()))
        .with_property("salary", Value::Integer(110000));

    let friendship_id = client.create_edge(friendship).await?;
    let alice_work_id = client.create_edge(alice_works).await?;
    let bob_work_id = client.create_edge(bob_works).await?;

    info!(
        "Created edges: friendship={}, alice_work={}, bob_work={}",
        friendship_id, alice_work_id, bob_work_id
    );

    // Query examples
    info!("Running queries...");

    // Find all people
    let people = client.find_nodes_by_label("person").await?;
    info!("Found {} people", people.len());
    for person in &people {
        let name = person.get_property("name").unwrap();
        let age = person.get_property("age").unwrap();
        info!("  Person: {} (age: {})", name, age);
    }

    // Find people in New York
    let ny_people = client
        .find_nodes_by_property("city", &Value::String("New York".to_string()))
        .await?;
    info!("Found {} people in New York", ny_people.len());

    // Get Alice's connections
    let alice_edges = client.get_node_edges(&alice_id).await?;
    info!("Alice has {} connections", alice_edges.len());
    for edge in &alice_edges {
        info!(
            "  Connection: {} -> {} ({})",
            edge.source, edge.target, edge.label
        );
    }

    // Query using query builder
    info!("Using query builder...");
    let query_result = client
        .execute_query(
            client
                .query_nodes()
                .with_label("person")
                .where_gt("age", Value::Integer(25))
                .order_by_asc("name")
                .limit(10),
        )
        .await?;

    if let Some(nodes) = query_result.nodes() {
        info!("Query found {} people older than 25", nodes.len());
        for node in nodes {
            let name = node.get_property("name").unwrap();
            let age = node.get_property("age").unwrap();
            info!("  Result: {} (age: {})", name, age);
        }
    }

    // Graph traversal
    info!("Performing graph traversal...");
    let connected_nodes = client.bfs(&alice_id, 2).await?;
    info!(
        "BFS from Alice found {} connected nodes",
        connected_nodes.len()
    );

    // Update a node
    info!("Updating Alice's age...");
    if let Some(mut alice_node) = client.get_node(&alice_id).await? {
        alice_node.set_property("age", Value::Integer(31));
        client.update_node(&alice_node).await?;
        info!("Updated Alice's age to 31");
    }

    // Batch operations
    info!("Demonstrating batch operations...");
    let batch_nodes = vec![
        Node::new("product")
            .with_property("name", Value::String("Laptop".to_string()))
            .with_property("price", Value::Float(999.99)),
        Node::new("product")
            .with_property("name", Value::String("Phone".to_string()))
            .with_property("price", Value::Float(699.99)),
    ];

    let product_ids = client.batch_create_nodes(batch_nodes).await?;
    info!("Batch created {} products", product_ids.len());

    // Database statistics
    let stats = client.get_stats().await?;
    info!("Database statistics:");
    info!("  Nodes: {}", stats.node_count);
    info!("  Edges: {}", stats.edge_count);
    info!("  Cache hit rate: {:.2}%", stats.cache_hit_rate * 100.0);
    info!("  Cache size: {} bytes", stats.cache_size);

    // Flush to disk
    client.flush().await?;
    info!("Flushed database to disk");

    info!("Basic usage example completed successfully!");
    Ok(())
}
