use mini_database::{Database, DatabaseClient, TransactionIsolationLevel, TransactionManager};
use mini_database::{Edge, Node, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Run all transaction examples
    basic_acid_example().await?;
    money_transfer_example().await?;
    savepoint_example().await?;
    isolation_levels_example().await?;
    complex_business_transaction().await?;
    transaction_monitoring_example().await?;

    println!("\n🎉 All transaction examples completed successfully!");
    Ok(())
}

async fn basic_acid_example() -> Result<(), Box<dyn std::error::Error>> {
    let tx_manager = setup_transaction_manager().await?;

    println!("=== Basic ACID Transaction Example ===");

    // Begin transaction with READ_COMMITTED isolation
    let tx_id = tx_manager
        .begin_transaction(TransactionIsolationLevel::ReadCommitted)
        .await?;
    println!("Started transaction: {}", tx_id);

    // Create user node
    let mut user = Node::new("user");
    user.set_property("name", Value::String("Alice Smith".to_string()));
    user.set_property("email", Value::String("alice@example.com".to_string()));
    user.set_property("balance", Value::Float(1000.0));

    let user_id = tx_manager.create_node_tx(&tx_id, user).await?;
    println!("Created user node: {}", user_id);

    // Create account node
    let mut account = Node::new("account");
    account.set_property("account_number", Value::String("ACC123456".to_string()));
    account.set_property("balance", Value::Float(1000.0));
    account.set_property("type", Value::String("checking".to_string()));

    let account_id = tx_manager.create_node_tx(&tx_id, account).await?;
    println!("Created account node: {}", account_id);

    // Create relationship between user and account
    let mut owns_edge = Edge::new(&user_id, &account_id, "owns");
    owns_edge.set_property("since", Value::String("2025-01-01".to_string()));

    let edge_id = tx_manager.create_edge_tx(&tx_id, owns_edge).await?;
    println!("Created ownership edge: {}", edge_id);

    // Commit transaction - demonstrates ATOMICITY and DURABILITY
    tx_manager.commit_transaction(&tx_id).await?;
    println!("✅ Transaction committed successfully - All changes are permanent");

    Ok(())
}

async fn setup_transaction_manager() -> Result<TransactionManager, Box<dyn std::error::Error>> {
    let config = mini_database::DatabaseConfig::default();
    let database = Database::new(config).await?;
    let client = DatabaseClient::new(database);

    let tx_manager =
        TransactionManager::new(client, std::path::PathBuf::from("./examples/wal.log"), true)
            .await?;

    Ok(tx_manager)
}

async fn money_transfer_example() -> Result<(), Box<dyn std::error::Error>> {
    let tx_manager = setup_transaction_manager().await?;

    println!("\n=== Money Transfer Transaction (Atomicity Demo) ===");

    // Setup: Create two accounts
    let setup_tx = tx_manager
        .begin_transaction(TransactionIsolationLevel::ReadCommitted)
        .await?;

    let mut account1 = Node::new("account");
    account1.set_property("name", Value::String("Alice's Account".to_string()));
    account1.set_property("balance", Value::Float(1000.0));
    let acc1_id = tx_manager.create_node_tx(&setup_tx, account1).await?;

    let mut account2 = Node::new("account");
    account2.set_property("name", Value::String("Bob's Account".to_string()));
    account2.set_property("balance", Value::Float(500.0));
    let acc2_id = tx_manager.create_node_tx(&setup_tx, account2).await?;

    tx_manager.commit_transaction(&setup_tx).await?;
    println!("Setup complete - Alice: $1000, Bob: $500");

    // Money transfer transaction
    let transfer_tx = tx_manager
        .begin_transaction_with_timeout(
            TransactionIsolationLevel::Serializable, // Highest isolation
            30,                                      // 30 second timeout
        )
        .await?;

    let transfer_amount = 200.0;
    println!("Transferring ${} from Alice to Bob", transfer_amount);

    // Read current balances
    let mut alice_account = tx_manager
        .get_node_tx(&transfer_tx, &acc1_id)
        .await?
        .ok_or("Alice's account not found")?;
    let mut bob_account = tx_manager
        .get_node_tx(&transfer_tx, &acc2_id)
        .await?
        .ok_or("Bob's account not found")?;

    let alice_balance = alice_account
        .get_property("balance")
        .and_then(|v| {
            if let Value::Float(f) = v {
                Some(*f)
            } else {
                None
            }
        })
        .unwrap_or(0.0);
    let bob_balance = bob_account
        .get_property("balance")
        .and_then(|v| {
            if let Value::Float(f) = v {
                Some(*f)
            } else {
                None
            }
        })
        .unwrap_or(0.0);

    println!(
        "Current balances - Alice: ${}, Bob: ${}",
        alice_balance, bob_balance
    );

    // Check if Alice has sufficient funds
    if alice_balance < transfer_amount {
        println!("❌ Insufficient funds! Rolling back transaction...");
        tx_manager.abort_transaction(&transfer_tx).await?;
        println!("Transaction rolled back - no changes made");
        return Ok(());
    }

    // Perform transfer (both operations must succeed)
    alice_account.set_property("balance", Value::Float(alice_balance - transfer_amount));
    bob_account.set_property("balance", Value::Float(bob_balance + transfer_amount));

    tx_manager
        .update_node_tx(&transfer_tx, alice_account)
        .await?;
    tx_manager.update_node_tx(&transfer_tx, bob_account).await?;

    // Commit transaction
    tx_manager.commit_transaction(&transfer_tx).await?;
    println!("✅ Transfer completed successfully!");
    println!(
        "New balances - Alice: ${}, Bob: ${}",
        alice_balance - transfer_amount,
        bob_balance + transfer_amount
    );

    Ok(())
}

async fn savepoint_example() -> Result<(), Box<dyn std::error::Error>> {
    let tx_manager = setup_transaction_manager().await?;

    println!("\n=== Savepoint and Nested Transaction Example ===");

    let tx_id = tx_manager
        .begin_transaction(TransactionIsolationLevel::RepeatableRead)
        .await?;

    // Create initial user
    let mut user = Node::new("user");
    user.set_property("name", Value::String("Charlie".to_string()));
    user.set_property("score", Value::Integer(100));
    let user_id = tx_manager.create_node_tx(&tx_id, user).await?;
    println!("Created user Charlie with score: 100");

    // Create first savepoint
    tx_manager
        .create_savepoint(&tx_id, "after_user_creation".to_string())
        .await?;
    println!("📍 Savepoint 'after_user_creation' created");

    // Update user score
    let mut updated_user = tx_manager.get_node_tx(&tx_id, &user_id).await?.unwrap();
    updated_user.set_property("score", Value::Integer(150));
    tx_manager.update_node_tx(&tx_id, updated_user).await?;
    println!("Updated Charlie's score to: 150");

    // Create second savepoint
    tx_manager
        .create_savepoint(&tx_id, "after_score_update".to_string())
        .await?;
    println!("📍 Savepoint 'after_score_update' created");

    // Add email (this operation might fail)
    let mut user_with_email = tx_manager.get_node_tx(&tx_id, &user_id).await?.unwrap();
    user_with_email.set_property("email", Value::String("invalid-email".to_string()));
    tx_manager.update_node_tx(&tx_id, user_with_email).await?;
    println!("Added invalid email to Charlie");

    // Simulate validation failure - rollback to savepoint
    println!("❌ Email validation failed! Rolling back to 'after_score_update'");
    tx_manager
        .rollback_to_savepoint(&tx_id, "after_score_update")
        .await?;

    // Verify user still has score 150 but no email
    let final_user = tx_manager.get_node_tx(&tx_id, &user_id).await?.unwrap();
    let score = final_user.get_property("score");
    let email = final_user.get_property("email");

    println!("After rollback - Score: {:?}, Email: {:?}", score, email);

    // Add valid email
    let mut user_valid_email = tx_manager.get_node_tx(&tx_id, &user_id).await?.unwrap();
    user_valid_email.set_property("email", Value::String("charlie@example.com".to_string()));
    tx_manager.update_node_tx(&tx_id, user_valid_email).await?;
    println!("Added valid email: charlie@example.com");

    // Commit entire transaction
    tx_manager.commit_transaction(&tx_id).await?;
    println!("✅ Transaction committed with savepoint recovery");

    Ok(())
}

async fn isolation_levels_example() -> Result<(), Box<dyn std::error::Error>> {
    let tx_manager = setup_transaction_manager().await?;

    println!("\n=== Transaction Isolation Levels Example ===");

    // Setup shared data
    let setup_tx = tx_manager
        .begin_transaction(TransactionIsolationLevel::ReadCommitted)
        .await?;
    let mut shared_node = Node::new("counter");
    shared_node.set_property("value", Value::Integer(0));
    let node_id = tx_manager.create_node_tx(&setup_tx, shared_node).await?;
    tx_manager.commit_transaction(&setup_tx).await?;

    // Demonstrate READ UNCOMMITTED (allows dirty reads)
    println!("\n--- READ UNCOMMITTED Demo ---");
    let tx1 = tx_manager
        .begin_transaction(TransactionIsolationLevel::ReadUncommitted)
        .await?;
    let tx2 = tx_manager
        .begin_transaction(TransactionIsolationLevel::ReadUncommitted)
        .await?;

    // TX1: Update but don't commit
    if let Some(mut node_tx1) = tx_manager.get_node_tx(&tx1, &node_id).await? {
        node_tx1.set_property("value", Value::Integer(100));
        tx_manager.update_node_tx(&tx1, node_tx1).await?;
        println!("TX1: Updated value to 100 (not committed yet)");
    } else {
        println!("TX1: Node not found");
        return Ok(());
    }

    // TX2: Read uncommitted data (dirty read)
    if let Some(node_tx2) = tx_manager.get_node_tx(&tx2, &node_id).await? {
        let dirty_value = node_tx2.get_property("value");
        println!("TX2: Read value (dirty read): {:?}", dirty_value);
    } else {
        println!("TX2: Node not found");
    }

    // TX1: Rollback
    tx_manager.abort_transaction(&tx1).await?;
    tx_manager.abort_transaction(&tx2).await?;
    println!("Both transactions aborted");

    // Demonstrate SERIALIZABLE (strictest isolation)
    println!("\n--- SERIALIZABLE Demo ---");
    let tx3 = tx_manager
        .begin_transaction(TransactionIsolationLevel::Serializable)
        .await?;
    let tx4 = tx_manager
        .begin_transaction(TransactionIsolationLevel::Serializable)
        .await?;

    // Both transactions try to update the same node
    println!("TX3 and TX4 both trying to update the same node...");

    // TX3: Read and update
    if let Some(mut node_tx3) = tx_manager.get_node_tx(&tx3, &node_id).await? {
        node_tx3.set_property("value", Value::Integer(200));
        tx_manager.update_node_tx(&tx3, node_tx3).await?;
        println!("TX3: Updated value to 200");
    }

    // TX4: This should block or fail due to conflict
    match tx_manager.get_node_tx(&tx4, &node_id).await {
        Ok(Some(mut node_tx4)) => {
            node_tx4.set_property("value", Value::Integer(300));
            match tx_manager.update_node_tx(&tx4, node_tx4).await {
                Ok(_) => println!("TX4: Update succeeded"),
                Err(e) => println!("TX4: Update failed due to conflict: {}", e),
            }
        }
        Ok(None) => println!("TX4: Node not found"),
        Err(e) => println!("TX4: Read failed due to lock: {}", e),
    }

    // Commit TX3
    tx_manager.commit_transaction(&tx3).await?;
    println!("TX3: Committed successfully");

    // TX4 should be aborted due to conflict
    let _ = tx_manager.abort_transaction(&tx4).await;
    println!("TX4: Aborted due to serialization conflict");

    Ok(())
}

async fn complex_business_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let tx_manager = setup_transaction_manager().await?;

    println!("\n=== Complex Business Transaction Example ===");
    println!("Scenario: E-commerce order processing");

    let tx_id = tx_manager
        .begin_transaction(TransactionIsolationLevel::ReadCommitted)
        .await?;

    // Create customer
    let mut customer = Node::new("customer");
    customer.set_property("name", Value::String("Jane Doe".to_string()));
    customer.set_property("email", Value::String("jane@example.com".to_string()));
    customer.set_property("credit_limit", Value::Float(5000.0));
    let customer_id = tx_manager.create_node_tx(&tx_id, customer).await?;
    println!("✓ Created customer: Jane Doe");

    // Create savepoint after customer creation
    tx_manager
        .create_savepoint(&tx_id, "customer_created".to_string())
        .await?;

    // Create product
    let mut product = Node::new("product");
    product.set_property("name", Value::String("Laptop".to_string()));
    product.set_property("price", Value::Float(1200.0));
    product.set_property("stock", Value::Integer(10));
    let product_id = tx_manager.create_node_tx(&tx_id, product).await?;
    println!("✓ Created product: Laptop ($1200, 10 in stock)");

    // Create order
    let mut order = Node::new("order");
    order.set_property("order_id", Value::String("ORD001".to_string()));
    order.set_property("quantity", Value::Integer(2));
    order.set_property("total", Value::Float(2400.0));
    order.set_property("status", Value::String("processing".to_string()));
    let order_id = tx_manager.create_node_tx(&tx_id, order).await?;
    println!("✓ Created order: ORD001 (2 laptops, $2400 total)");

    // Create savepoint after order creation
    tx_manager
        .create_savepoint(&tx_id, "order_created".to_string())
        .await?;

    // Create relationships
    let customer_order_edge = Edge::new(&customer_id, &order_id, "placed");
    tx_manager
        .create_edge_tx(&tx_id, customer_order_edge)
        .await?;

    let mut order_product_edge = Edge::new(&order_id, &product_id, "contains");
    order_product_edge.set_property("quantity", Value::Integer(2));
    tx_manager
        .create_edge_tx(&tx_id, order_product_edge)
        .await?;
    println!("✓ Created order relationships");

    // Check inventory and credit limit
    let current_product = tx_manager.get_node_tx(&tx_id, &product_id).await?.unwrap();
    let stock = current_product
        .get_property("stock")
        .and_then(|v| {
            if let Value::Integer(i) = v {
                Some(*i)
            } else {
                None
            }
        })
        .unwrap_or(0);

    let current_customer = tx_manager.get_node_tx(&tx_id, &customer_id).await?.unwrap();
    let credit_limit = current_customer
        .get_property("credit_limit")
        .and_then(|v| {
            if let Value::Float(f) = v {
                Some(*f)
            } else {
                None
            }
        })
        .unwrap_or(0.0);

    let order_total = 2400.0;
    let order_quantity = 2;

    // Business rule validation
    if stock < order_quantity {
        println!("❌ Insufficient inventory! Rolling back to order creation...");
        tx_manager
            .rollback_to_savepoint(&tx_id, "order_created")
            .await?;

        // Update order status to failed
        let mut failed_order = tx_manager.get_node_tx(&tx_id, &order_id).await?.unwrap();
        failed_order.set_property(
            "status",
            Value::String("failed - insufficient inventory".to_string()),
        );
        tx_manager.update_node_tx(&tx_id, failed_order).await?;

        tx_manager.commit_transaction(&tx_id).await?;
        return Ok(());
    }

    if order_total > credit_limit {
        println!("❌ Credit limit exceeded! Rolling back entire transaction...");
        tx_manager
            .rollback_to_savepoint(&tx_id, "customer_created")
            .await?;
        tx_manager.abort_transaction(&tx_id).await?;
        return Ok(());
    }

    // Process order - update inventory
    let mut updated_product = tx_manager.get_node_tx(&tx_id, &product_id).await?.unwrap();
    updated_product.set_property("stock", Value::Integer(stock - order_quantity));
    tx_manager.update_node_tx(&tx_id, updated_product).await?;
    println!(
        "✓ Updated inventory: {} units remaining",
        stock - order_quantity
    );

    // Update order status
    let mut completed_order = tx_manager.get_node_tx(&tx_id, &order_id).await?.unwrap();
    completed_order.set_property("status", Value::String("completed".to_string()));
    completed_order.set_property(
        "processed_at",
        Value::String(chrono::Utc::now().to_rfc3339()),
    );
    tx_manager.update_node_tx(&tx_id, completed_order).await?;

    // Create payment record
    let mut payment = Node::new("payment");
    payment.set_property("amount", Value::Float(order_total));
    payment.set_property("method", Value::String("credit_card".to_string()));
    payment.set_property("status", Value::String("completed".to_string()));
    let payment_id = tx_manager.create_node_tx(&tx_id, payment).await?;

    let payment_edge = Edge::new(&order_id, &payment_id, "paid_by");
    tx_manager.create_edge_tx(&tx_id, payment_edge).await?;
    println!("✓ Payment processed: $2400");

    // Final commit
    tx_manager.commit_transaction(&tx_id).await?;
    println!("🎉 Order completed successfully!");
    println!("   - Customer: Jane Doe");
    println!("   - Product: 2x Laptop");
    println!("   - Total: $2400");
    println!("   - Inventory updated: 8 units remaining");

    Ok(())
}

async fn transaction_monitoring_example() -> Result<(), Box<dyn std::error::Error>> {
    let tx_manager = setup_transaction_manager().await?;

    println!("\n=== Transaction Monitoring Example ===");

    let tx_id = tx_manager
        .begin_transaction(TransactionIsolationLevel::RepeatableRead)
        .await?;

    // Perform some operations
    let mut node1 = Node::new("test");
    node1.set_property("data", Value::String("test data".to_string()));
    tx_manager.create_node_tx(&tx_id, node1).await?;

    let mut node2 = Node::new("test2");
    node2.set_property("data", Value::String("more test data".to_string()));
    tx_manager.create_node_tx(&tx_id, node2).await?;

    // Get transaction statistics
    let stats = tx_manager.get_transaction_stats(&tx_id).await?;

    println!("Transaction Statistics:");
    println!("  ID: {}", stats.id);
    println!("  State: {:?}", stats.state);
    println!("  Isolation Level: {:?}", stats.isolation_level);
    println!("  Duration: {:?}", stats.duration);
    println!("  Reads: {}", stats.reads_count);
    println!("  Writes: {}", stats.writes_count);
    println!("  Savepoints: {}", stats.savepoints_count);

    // Monitor active transactions
    let active_transactions = tx_manager.get_active_transaction_ids();
    println!("Active Transactions: {:?}", active_transactions);
    println!("Total Active: {}", tx_manager.active_transaction_count());

    tx_manager.commit_transaction(&tx_id).await?;
    println!("✅ Monitoring transaction completed");

    Ok(())
}
