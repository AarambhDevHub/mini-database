//! Network client-server performance benchmark

use mini_database::{Database, DatabaseConfig, DatabaseServer, ServerConfig};
use mini_database::{Edge, NetworkDatabaseClient, Node, Value};
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{Level, info};
use tracing_subscriber;

struct BenchmarkResult {
    operation: String,
    total_ops: usize,
    duration: Duration,
    ops_per_sec: f64,
    avg_latency_us: f64,
}

impl BenchmarkResult {
    fn new(operation: &str, total_ops: usize, duration: Duration) -> Self {
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
        let avg_latency_us = duration.as_micros() as f64 / total_ops as f64;

        Self {
            operation: operation.to_string(),
            total_ops,
            duration,
            ops_per_sec,
            avg_latency_us,
        }
    }

    fn print(&self) {
        println!(
            "📈 {:<20} | {:>8} ops | {:>8.2}ms | {:>10.0} ops/sec | {:>8.2}μs/op",
            self.operation,
            self.total_ops,
            self.duration.as_millis(),
            self.ops_per_sec,
            self.avg_latency_us
        );
    }
}

async fn start_test_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Create database for server
    let db_config = DatabaseConfig::new("./benchmark_server_data")
        .with_cache_size(256) // 256MB cache
        .with_compression(true);

    let database = Database::new(db_config).await?;

    // Create server config
    let server_config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 5433, // Different port to avoid conflicts
        max_connections: 50,
        buffer_size: 8192,
        cleanup_interval: Duration::from_secs(60),
    };

    // Start server
    let server = DatabaseServer::new(database, server_config);
    server.start().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging with minimal output
    tracing_subscriber::fmt().with_max_level(Level::WARN).init();

    println!("🌐 Starting Mini Database Network Benchmark");
    println!("{}", "=".repeat(80));

    // Start server in background
    println!("🚀 Starting database server...");
    tokio::spawn(async {
        if let Err(e) = start_test_server().await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;

    // Connect to server
    println!("🔌 Connecting to server...");
    let mut client = NetworkDatabaseClient::connect("127.0.0.1", 5433).await?;
    println!("✅ Connected to server");

    // Test connection
    client.ping().await?;
    println!("🏓 Connection test successful");
    println!();

    // Benchmark parameters
    const WARM_UP_OPS: usize = 500; // Fewer for network
    const BENCHMARK_OPS: usize = 5_000; // Fewer for network
    const BATCH_SIZE: usize = 50;

    println!("🔧 Configuration:");
    println!("   Warm-up operations: {}", WARM_UP_OPS);
    println!("   Benchmark operations: {}", BENCHMARK_OPS);
    println!("   Batch size: {}", BATCH_SIZE);
    println!();

    // Warm-up phase
    println!("🔥 Warming up...");
    for i in 0..WARM_UP_OPS {
        let node = Node::new("warmup").with_property("id", Value::Integer(i as i64));
        client.create_node(node).await?;
    }
    println!("✅ Warm-up completed");
    println!();

    let mut results = Vec::new();

    // 1. Node Creation Benchmark
    println!("📝 Running Node Creation Benchmark...");
    let start = Instant::now();
    let mut node_ids = Vec::with_capacity(BENCHMARK_OPS);

    for i in 0..BENCHMARK_OPS {
        let node = Node::new("person")
            .with_property("name", Value::String(format!("NetworkUser_{}", i)))
            .with_property("age", Value::Integer((20 + (i % 50)) as i64))
            .with_property("score", Value::Float(i as f64 * 0.1));

        let node_id = client.create_node(node).await?;
        node_ids.push(node_id);
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "Node Creation",
        BENCHMARK_OPS,
        duration,
    ));

    // 2. Node Retrieval Benchmark
    println!("🔍 Running Node Retrieval Benchmark...");
    let start = Instant::now();
    let mut retrieved_count = 0;

    for node_id in &node_ids {
        if client.get_node(node_id).await?.is_some() {
            retrieved_count += 1;
        }
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "Node Retrieval",
        retrieved_count,
        duration,
    ));

    // 3. Graph Traversal Benchmark (BFS)
    println!("🌐 Running BFS Traversal Benchmark...");
    let start = Instant::now();
    let mut total_traversed = 0;
    let traversal_count = 5; // Fewer traversals for network

    for node_id in node_ids.iter().take(traversal_count) {
        let traversed = client.bfs(node_id, 2).await?; // Smaller depth
        total_traversed += traversed.len();
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "BFS Traversals",
        traversal_count,
        duration,
    ));

    // 4. Connection Latency Test
    println!("⚡ Running Latency Test...");
    let ping_count = 100;
    let start = Instant::now();

    for _ in 0..ping_count {
        client.ping().await?;
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new("Ping Latency", ping_count, duration));

    // 5. Mixed Workload Benchmark
    println!("🎯 Running Mixed Network Workload...");
    let start = Instant::now();
    let mixed_ops = 500; // Fewer operations for network

    for i in 0..mixed_ops {
        match i % 3 {
            0 => {
                // Create node
                let node = Node::new("mixed").with_property("id", Value::Integer(i as i64));
                client.create_node(node).await?;
            }
            1 => {
                // Query node
                if !node_ids.is_empty() {
                    let idx = i % node_ids.len();
                    client.get_node(&node_ids[idx]).await?;
                }
            }
            2 => {
                // BFS
                if !node_ids.is_empty() {
                    let idx = i % node_ids.len();
                    client.bfs(&node_ids[idx], 1).await?;
                }
            }
            _ => unreachable!(),
        }
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new("Mixed Workload", mixed_ops, duration));

    // Print results
    println!();
    println!("📊 BENCHMARK RESULTS - NETWORK MODE");
    println!("{}", "=".repeat(80));
    println!(
        "{:<20} | {:>8} | {:>8} | {:>10} | {:>8}",
        "Operation", "Count", "Duration", "Ops/Sec", "Latency"
    );
    println!("{}", "-".repeat(80));

    for result in &results {
        result.print();
    }

    // Calculate total operations and time
    let total_ops: usize = results.iter().map(|r| r.total_ops).sum();
    let total_time: Duration = results.iter().map(|r| r.duration).sum();
    let overall_ops_per_sec = total_ops as f64 / total_time.as_secs_f64();

    println!();
    println!("🎯 NETWORK PERFORMANCE:");
    println!("   Total Operations: {}", total_ops);
    println!("   Total Time: {:.2}s", total_time.as_secs_f64());
    println!("   Overall Throughput: {:.0} ops/sec", overall_ops_per_sec);

    // Network-specific metrics
    let ping_result = results
        .iter()
        .find(|r| r.operation.contains("Ping"))
        .unwrap();
    println!(
        "   Average Network Latency: {:.2}μs",
        ping_result.avg_latency_us
    );

    println!();
    println!("✅ Network benchmark completed!");

    Ok(())
}
