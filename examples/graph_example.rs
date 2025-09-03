//! Graph traversal and relationship query examples

use mini_database::{Database, DatabaseClient, DatabaseConfig, Edge, Node, Value};
use tracing::{Level, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Mini Database graph example");

    // Create database
    let config = DatabaseConfig::new("./examples/graph_data")
        .with_cache_size(32)
        .with_compression(true);

    let database = Database::new(config).await?;
    let client = DatabaseClient::new(database);

    // Create a social network graph
    info!("Building social network graph...");

    // Create people
    let people = vec![
        ("Alice", 30, "Engineer"),
        ("Bob", 25, "Designer"),
        ("Charlie", 35, "Manager"),
        ("Diana", 28, "Analyst"),
        ("Eve", 32, "Developer"),
        ("Frank", 29, "Tester"),
    ];

    let mut person_ids = Vec::new();
    for (name, age, role) in people {
        let person = Node::new("person")
            .with_property("name", Value::String(name.to_string()))
            .with_property("age", Value::Integer(age))
            .with_property("role", Value::String(role.to_string()));

        let id = client.create_node(person).await?;
        person_ids.push((name.to_string(), id.clone()));
        info!("Created person: {} ({})", name, id.clone());
    }

    // Create relationships
    info!("Creating relationships...");
    let relationships = vec![
        (0, 1, "friends", 0.9),      // Alice -> Bob
        (0, 2, "reports_to", 1.0),   // Alice -> Charlie
        (1, 3, "collaborates", 0.7), // Bob -> Diana
        (1, 4, "friends", 0.8),      // Bob -> Eve
        (2, 3, "manages", 1.0),      // Charlie -> Diana
        (2, 4, "manages", 1.0),      // Charlie -> Eve
        (3, 4, "friends", 0.6),      // Diana -> Eve
        (4, 5, "mentors", 0.9),      // Eve -> Frank
        (5, 0, "learns_from", 0.8),  // Frank -> Alice
    ];

    for (source_idx, target_idx, relation, strength) in relationships {
        let source_id = &person_ids[source_idx].1;
        let target_id = &person_ids[target_idx].1;

        let edge = Edge::new(source_id, target_id, relation)
            .with_property("strength", Value::Float(strength))
            .with_property("created", Value::String("2023".to_string()));

        let _edge_id = client.create_edge(edge).await?;
        info!(
            "Created relationship: {} -> {} ({})",
            person_ids[source_idx].0, person_ids[target_idx].0, relation
        );
    }

    // Graph analysis
    info!("Performing graph analysis...");

    // 1. Find Alice's direct connections
    let alice_id = &person_ids[0].1;
    let alice_connections = client.get_node_edges(alice_id).await?;
    info!("Alice has {} direct connections", alice_connections.len());

    // 2. Breadth-first search from Alice
    let bfs_result = client.bfs(alice_id, 3).await?;
    info!(
        "BFS from Alice (depth 3) reached {} nodes",
        bfs_result.len()
    );
    for node in &bfs_result {
        let name = node.get_property("name").unwrap();
        info!("  Reached: {}", name);
    }

    // 3. Depth-first search from Alice
    let dfs_result = client.dfs(alice_id, 3).await?;
    info!(
        "DFS from Alice (depth 3) reached {} nodes",
        dfs_result.len()
    );

    // 4. Find shortest path between Alice and Frank
    let frank_id = &person_ids[5].1;
    let shortest_path = client.shortest_path(alice_id, frank_id).await?;
    if let Some(path) = shortest_path {
        info!(
            "Shortest path from Alice to Frank ({} steps):",
            path.len() - 1
        );
        for (i, node) in path.iter().enumerate() {
            let name = node.get_property("name").unwrap();
            if i == 0 {
                info!("  Start: {}", name);
            } else if i == path.len() - 1 {
                info!("  End: {}", name);
            } else {
                info!("  Via: {}", name);
            }
        }
    } else {
        info!("No path found from Alice to Frank");
    }

    // 5. Find all managers
    let managers = client
        .find_nodes_by_property("role", &Value::String("Manager".to_string()))
        .await?;
    info!("Found {} managers", managers.len());
    for manager in &managers {
        let name = manager.get_property("name").unwrap();
        info!("  Manager: {}", name);

        // Find who reports to this manager
        let manages_edges = client.get_incoming_edges(&manager.id).await?;
        let reports = manages_edges
            .into_iter()
            .filter(|e| e.label == "reports_to")
            .count();
        info!("    Has {} direct reports", reports);
    }

    // 6. Find strong friendships (strength > 0.8)
    let all_edges = client.query_edges().with_label("friends");

    let friendship_result = client.execute_query(all_edges).await?;
    if let Some(edges) = friendship_result.edges() {
        let strong_friendships: Vec<_> = edges
            .iter()
            .filter(|e| {
                if let Some(Value::Float(strength)) = e.get_property("strength") {
                    *strength > 0.8
                } else {
                    false
                }
            })
            .collect();

        info!(
            "Found {} strong friendships (strength > 0.8)",
            strong_friendships.len()
        );
        for edge in strong_friendships {
            let source_node = client.get_node(&edge.source).await?.unwrap();
            let target_node = client.get_node(&edge.target).await?.unwrap();
            let source_name = source_node.get_property("name").unwrap();
            let target_name = target_node.get_property("name").unwrap();
            let strength = edge.get_property("strength").unwrap();
            info!(
                "  {} <-> {} (strength: {})",
                source_name, target_name, strength
            );
        }
    }

    // 7. Find people with most connections
    info!("Finding people with most connections...");
    let mut connection_counts = Vec::new();
    for (name, id) in &person_ids {
        let connections = client.get_node_edges(id).await?;
        connection_counts.push((name.clone(), connections.len()));
    }

    connection_counts.sort_by(|a, b| b.1.cmp(&a.1));
    info!("Top connected people:");
    for (i, (name, count)) in connection_counts.iter().take(3).enumerate() {
        info!("  {}. {} ({} connections)", i + 1, name, count);
    }

    // 8. Calculate graph metrics
    let stats = client.get_stats().await?;
    info!("Graph metrics:");
    info!("  Total nodes: {}", stats.node_count);
    info!("  Total edges: {}", stats.edge_count);
    info!(
        "  Average connections per person: {:.2}",
        stats.edge_count as f64 / stats.node_count as f64 * 2.0
    ); // Each edge connects 2 nodes

    // 9. Find isolated nodes (no connections)
    info!("Checking for isolated nodes...");
    let mut isolated_count = 0;
    for (name, id) in &person_ids {
        let connections = client.get_node_edges(id).await?;
        if connections.is_empty() {
            info!("  Isolated node: {}", name);
            isolated_count += 1;
        }
    }
    if isolated_count == 0 {
        info!("  No isolated nodes found - graph is well connected!");
    }

    // 10. Complex query using query builder
    info!("Running complex queries...");
    let young_engineers = client
        .execute_query(
            client
                .query_nodes()
                .with_label("person")
                .where_eq("role", Value::String("Engineer".to_string()))
                .where_lt("age", Value::Integer(35))
                .order_by_desc("age"),
        )
        .await?;

    if let Some(nodes) = young_engineers.nodes() {
        info!("Found {} young engineers:", nodes.len());
        for node in nodes {
            let name = node.get_property("name").unwrap();
            let age = node.get_property("age").unwrap();
            info!("  {} (age: {})", name, age);
        }
    }

    client.flush().await?;
    info!("Graph example completed successfully!");

    Ok(())
}
