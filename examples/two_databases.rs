
//! Example: Create two separate databases and connect them

use mini_database::{Database, DatabaseClient, DatabaseConfig, Node, Edge, Value};
use std::collections::HashMap;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();

    info!("🔗 Creating Two Connected Databases Example");
    println!("{}", "=".repeat(60));

    // 🏢 Create first database - "Company Database"
    println!("🏢 Creating Company Database...");
    let company_config = DatabaseConfig::new("./company_db")
        .with_cache_size(64)
        .with_compression(true);

    let company_db = Database::new(company_config).await?;
    let company_client = DatabaseClient::new(company_db);

    // 👥 Create second database - "Employee Database"
    println!("👥 Creating Employee Database...");
    let employee_config = DatabaseConfig::new("./employee_db")
        .with_cache_size(64)
        .with_compression(true);

    let employee_db = Database::new(employee_config).await?;
    let employee_client = DatabaseClient::new(employee_db);

    // Create companies in Company DB
    println!("\n📊 Creating companies in Company Database...");
    let tech_corp = Node::new("company")
        .with_property("name", Value::String("TechCorp Inc".to_string()))
        .with_property("industry", Value::String("Technology".to_string()))
        .with_property("founded", Value::Integer(2010))
        .with_property("employees", Value::Integer(500));

    let design_studio = Node::new("company")
        .with_property("name", Value::String("Design Studio".to_string()))
        .with_property("industry", Value::String("Creative".to_string()))
        .with_property("founded", Value::Integer(2015))
        .with_property("employees", Value::Integer(50));

    let techcorp_id = company_client.create_node(tech_corp).await?;
    let design_id = company_client.create_node(design_studio).await?;

    println!("✅ Created TechCorp: {}", techcorp_id);
    println!("✅ Created Design Studio: {}", design_id);

    // Create employees in Employee DB
    println!("\n👤 Creating employees in Employee Database...");
    let alice = Node::new("employee")
        .with_property("name", Value::String("Alice Johnson".to_string()))
        .with_property("role", Value::String("Senior Engineer".to_string()))
        .with_property("salary", Value::Integer(120000))
        .with_property("company_id", Value::String(techcorp_id.clone())); // Reference to Company DB

    let bob = Node::new("employee")
        .with_property("name", Value::String("Bob Wilson".to_string()))
        .with_property("role", Value::String("Designer".to_string()))
        .with_property("salary", Value::Integer(95000))
        .with_property("company_id", Value::String(design_id.clone())); // Reference to Company DB

    let charlie = Node::new("employee")
        .with_property("name", Value::String("Charlie Brown".to_string()))
        .with_property("role", Value::String("Product Manager".to_string()))
        .with_property("salary", Value::Integer(110000))
        .with_property("company_id", Value::String(techcorp_id.clone()));

    let alice_id = employee_client.create_node(alice).await?;
    let bob_id = employee_client.create_node(bob).await?;
    let charlie_id = employee_client.create_node(charlie).await?;

    println!("✅ Created Alice: {}", alice_id);
    println!("✅ Created Bob: {}", bob_id);
    println!("✅ Created Charlie: {}", charlie_id);

    // Create cross-database relationships in Employee DB
    println!("\n🔗 Creating cross-database relationships...");
    let alice_bob_collab = Edge::new(&alice_id, &bob_id, "collaborates_with")
        .with_property("project", Value::String("Mobile App UI".to_string()))
        .with_property("frequency", Value::String("weekly".to_string()));

    let alice_charlie_reports = Edge::new(&alice_id, &charlie_id, "reports_to")
        .with_property("since", Value::String("2023-01-01".to_string()));

    employee_client.create_edge(alice_bob_collab).await?;
    employee_client.create_edge(alice_charlie_reports).await?;

    // Cross-database lookup function
    async fn get_employee_with_company(
        employee_client: &DatabaseClient,
        company_client: &DatabaseClient,
        employee_id: &str,
    ) -> Result<(Node, Option<Node>), Box<dyn std::error::Error>> {
        // Get employee from Employee DB
        let employee = employee_client.get_node(employee_id).await?
            .ok_or("Employee not found")?;

        // Get company ID from employee
        let company_id = employee.get_property("company_id")
            .and_then(|v| v.as_string());

        // Get company from Company DB
        let company = if let Some(company_id) = company_id {
            company_client.get_node(company_id).await?
        } else {
            None
        };

        Ok((employee, company))
    }

    // Demonstrate cross-database queries
    println!("\n🔍 Performing cross-database queries...");

    let (alice_employee, alice_company) = get_employee_with_company(
        &employee_client,
        &company_client,
        &alice_id
    ).await?;

    let alice_name = alice_employee.get_property("name").unwrap();
    let alice_role = alice_employee.get_property("role").unwrap();

    println!("👤 Employee: {} - {}", alice_name, alice_role);

    if let Some(company) = alice_company {
        let company_name = company.get_property("name").unwrap();
        let industry = company.get_property("industry").unwrap();
        println!("🏢 Works at: {} ({})", company_name, industry);
    }

    // Find all employees for a specific company
    println!("\n📋 Finding all TechCorp employees...");
    let techcorp_employees = employee_client.find_nodes_by_property(
        "company_id",
        &Value::String(techcorp_id.clone())
    ).await?;

    println!("Found {} employees at TechCorp:", techcorp_employees.len());
    for emp in &techcorp_employees {
        let name = emp.get_property("name").unwrap();
        let role = emp.get_property("role").unwrap();
        println!("  • {} - {}", name, role);
    }

    // Database statistics
    println!("\n📊 Database Statistics:");
    let company_stats = company_client.get_stats().await?;
    let employee_stats = employee_client.get_stats().await?;

    println!("🏢 Company DB: {} nodes, {} edges",
             company_stats.node_count, company_stats.edge_count);
    println!("👥 Employee DB: {} nodes, {} edges",
             employee_stats.node_count, employee_stats.edge_count);

    // Flush both databases
    company_client.flush().await?;
    employee_client.flush().await?;

    println!("\n✅ Two-database example completed successfully!");

    Ok(())
}
