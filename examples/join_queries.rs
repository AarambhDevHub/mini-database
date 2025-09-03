//! Comprehensive join examples using the new join functionality

use mini_database::{AggregateFunction, JoinType};
use mini_database::{Database, DatabaseClient, DatabaseConfig, Edge, Node, Value};
use std::collections::HashMap;
use tracing::{Level, info};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    println!("🔗 Comprehensive Join Examples - Mini Database");
    println!("{}", "=".repeat(60));

    // Create database
    let config = DatabaseConfig::new("./comprehensive_joins_data")
        .with_cache_size(128)
        .with_compression(true);

    let database = Database::new(config).await?;
    let client = DatabaseClient::new(database);

    // Setup data
    setup_sample_data(&client).await?;

    // 1. INNER JOIN with Fluent API
    println!("\n1️⃣ INNER JOIN - Users with Orders");
    println!("{}", "-".repeat(40));

    let inner_result = client
        .join("user", "order")
        .join_type(JoinType::Inner)
        .on_edge("places_order".to_string())
        .select(vec![
            ("user".to_string(), "name".to_string()),
            ("order".to_string(), "date".to_string()),
            ("order".to_string(), "total".to_string()),
        ])
        .order_by("order.total".to_string(), false) // DESC
        .execute()
        .await?;

    inner_result.print_with_limit(Some(5));

    // 2. LEFT JOIN - All Users (with/without orders)
    println!("\n2️⃣ LEFT JOIN - All Users (with/without Orders)");
    println!("{}", "-".repeat(40));

    let left_result = client
        .join("user", "order")
        .join_type(JoinType::Left)
        .on_property("id".to_string(), "user_id".to_string())
        .select(vec![
            ("user".to_string(), "name".to_string()),
            ("user".to_string(), "city".to_string()),
            ("order".to_string(), "total".to_string()),
        ])
        .execute()
        .await?;

    left_result.print();

    // 3. CROSS JOIN - Limited for demonstration
    println!("\n3️⃣ CROSS JOIN - Users × Products (limited)");
    println!("{}", "-".repeat(40));

    let cross_result = client
        .join("user", "product")
        .join_type(JoinType::Cross)
        .select(vec![
            ("user".to_string(), "name".to_string()),
            ("product".to_string(), "name".to_string()),
            ("product".to_string(), "price".to_string()),
        ])
        .limit(8) // Limit to avoid too much output
        .execute()
        .await?;

    cross_result.print();

    // 4. Aggregate Join
    println!("\n4️⃣ AGGREGATE JOIN - Order Totals by User");
    println!("{}", "-".repeat(40));

    let aggregates = client
        .aggregate_join(
            "user",
            "order",
            "places_order",
            "name",  // group by user name
            "total", // aggregate order total
            AggregateFunction::Sum,
        )
        .await?;

    println!("┌{:─<20}┬{:─<15}┐", "", "");
    println!("│{:^20}│{:^15}│", "User", "Total Spent");
    println!("├{:─<20}┼{:─<15}┤", "", "");
    for (user, total) in &aggregates {
        println!("│{:^20}│{:^15.2}│", user, total);
    }
    println!("└{:─<20}┴{:─<15}┘", "", "");

    // 5. Complex Join with WHERE conditions
    println!("\n5️⃣ COMPLEX JOIN - High-Value Orders with Filtering");
    println!("{}", "-".repeat(40));

    let complex_result = client
        .join("user", "order")
        .join_type(JoinType::Inner)
        .on_edge("places_order".to_string())
        .select(vec![
            ("user".to_string(), "name".to_string()),
            ("user".to_string(), "city".to_string()),
            ("order".to_string(), "total".to_string()),
            ("order".to_string(), "date".to_string()),
        ])
        .where_condition(|row: &HashMap<String, Value>| {
            // Filter orders > $100
            if let Some(Value::Float(total)) = row.get("order.total") {
                *total > 100.0
            } else {
                false
            }
        })
        .order_by("order.total".to_string(), false)
        .limit(10)
        .execute()
        .await?;

    complex_result.print();

    // 6. Self Join - Employee Hierarchy (if we had manager relationships)
    println!("\n6️⃣ DEMONSTRATION - Multiple Aggregation Functions");
    println!("{}", "-".repeat(40));

    // Show different aggregate functions
    let functions = vec![
        ("SUM", AggregateFunction::Sum),
        ("AVG", AggregateFunction::Avg),
        ("COUNT", AggregateFunction::Count),
        ("MAX", AggregateFunction::Max),
        ("MIN", AggregateFunction::Min),
    ];

    println!("┌{:─<12}┬{:─<15}┐", "", "");
    println!("│{:^12}│{:^15}│", "Function", "Alice's Orders");
    println!("├{:─<12}┼{:─<15}┤", "", "");

    for (name, func) in functions {
        let result = client
            .aggregate_join("user", "order", "places_order", "name", "total", func)
            .await?;

        let alice_result = result.get("Alice").unwrap_or(&0.0);
        println!("│{:^12}│{:^15.2}│", name, alice_result);
    }
    println!("└{:─<12}┴{:─<15}┘", "", "");

    // Cleanup
    client.flush().await?;
    println!("\n✅ Comprehensive join examples completed!");

    Ok(())
}

async fn setup_sample_data(client: &DatabaseClient) -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Setting up sample data...");

    // Create users
    let users = vec![
        ("Alice", "New York", 30),
        ("Bob", "San Francisco", 25),
        ("Charlie", "Chicago", 35),
        ("Diana", "Boston", 28),
    ];

    let mut user_ids = Vec::new();
    for (name, city, age) in users {
        let user = Node::new("user")
            .with_property("name", Value::String(name.to_string()))
            .with_property("city", Value::String(city.to_string()))
            .with_property("age", Value::Integer(age));

        let user_id = client.create_node(user).await?;
        user_ids.push((name.to_string(), user_id));
    }

    // Create products
    let products = vec![
        ("Laptop", 999.99),
        ("Phone", 699.99),
        ("Headphones", 199.99),
        ("Tablet", 399.99),
    ];

    let mut product_ids = Vec::new();
    for (name, price) in products {
        let product = Node::new("product")
            .with_property("name", Value::String(name.to_string()))
            .with_property("price", Value::Float(price));

        let product_id = client.create_node(product).await?;
        product_ids.push((name.to_string(), product_id));
    }

    // Create orders
    let orders = vec![
        (0, 1299.99, "2025-01-15"), // Alice
        (1, 699.99, "2025-01-16"),  // Bob
        (0, 199.99, "2025-01-17"),  // Alice again
        (2, 399.99, "2025-01-18"),  // Charlie
                                    // Diana has no orders for LEFT JOIN demo
    ];

    for (user_idx, total, date) in orders.clone() {
        let order = Node::new("order")
            .with_property("user_id", Value::String(user_ids[user_idx].1.clone()))
            .with_property("total", Value::Float(total))
            .with_property("date", Value::String(date.to_string()))
            .with_property("status", Value::String("completed".to_string()));

        let order_id = client.create_node(order).await?;

        // Create user->order relationship
        let places_order = Edge::new(&user_ids[user_idx].1, &order_id, "places_order");
        client.create_edge(places_order).await?;
    }

    println!(
        "✅ Sample data created: {} users, {} products, {} orders",
        user_ids.len(),
        product_ids.len(),
        orders.len()
    );

    Ok(())
}
