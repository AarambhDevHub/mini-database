//! Local database performance benchmark

use mini_database::{Database, DatabaseClient, DatabaseConfig, Edge, Node, Value};
use std::time::{Duration, Instant};
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging with minimal output
    tracing_subscriber::fmt().with_max_level(Level::WARN).init();

    println!("🚀 Starting Mini Database Local Benchmark");
    println!("{}", "=".repeat(80));

    // Create database
    let config = DatabaseConfig::new("./benchmark_data")
        .with_cache_size(256) // 256MB cache
        .with_compression(true);

    let database = Database::new(config).await?;
    let client = DatabaseClient::new(database);

    // Benchmark parameters
    const WARM_UP_OPS: usize = 1_000;
    const BENCHMARK_OPS: usize = 10_000;
    const BATCH_SIZE: usize = 100;

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
            .with_property("name", Value::String(format!("User_{}", i)))
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

    // 3. Node Update Benchmark
    println!("✏️ Running Node Update Benchmark...");
    let start = Instant::now();
    let mut updated_count = 0;

    for (i, node_id) in node_ids.iter().take(BENCHMARK_OPS / 2).enumerate() {
        if let Some(mut node) = client.get_node(node_id).await? {
            node.set_property("updated", Value::Boolean(true));
            node.set_property("update_time", Value::Integer(i as i64));
            if client.update_node(&node).await? {
                updated_count += 1;
            }
        }
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        "Node Updates",
        updated_count,
        duration,
    ));

    // 4. Edge Creation Benchmark
    println!("🔗 Running Edge Creation Benchmark...");
    let start = Instant::now();
    let mut edge_count = 0;

    for i in 0..(BENCHMARK_OPS / 10) {
        let source_idx = i % node_ids.len();
        let target_idx = (i + 1) % node_ids.len();

        let edge = Edge::new(&node_ids[source_idx], &node_ids[target_idx], "knows")
            .with_property("strength", Value::Float((i % 100) as f64 / 100.0))
            .with_property("created_at", Value::Integer(i as i64));

        client.create_edge(edge).await?;
        edge_count += 1;
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new("Edge Creation", edge_count, duration));

    // 5. Query by Label Benchmark
    println!("🔎 Running Query by Label Benchmark...");
    let start = Instant::now();

    let found_nodes = client.find_nodes_by_label("person").await?;

    let duration = start.elapsed();
    results.push(BenchmarkResult::new(
        &format!("Label Query ({})", found_nodes.len()),
        1,
        duration,
    ));

    // 6. Property Query Benchmark
    println!("🔍 Running Property Query Benchmark...");
    let start = Instant::now();
    let mut total_found = 0;

    for age in 20..30 {
        let found = client
            .find_nodes_by_property("age", &Value::Integer(age))
            .await?;
        total_found += found.len();
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new("Property Queries", 10, duration));

    // 7. Graph Traversal Benchmark (BFS)
    println!("🌐 Running BFS Traversal Benchmark...");
    let start = Instant::now();
    let mut total_traversed = 0;

    for node_id in node_ids.iter().take(10) {
        let traversed = client.bfs(node_id, 3).await?;
        total_traversed += traversed.len();
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new("BFS Traversals", 10, duration));

    // 8. Batch Operations Benchmark
    println!("📦 Running Batch Operations Benchmark...");
    let batch_nodes: Vec<_> = (0..BATCH_SIZE)
        .map(|i| {
            Node::new("batch")
                .with_property("batch_id", Value::Integer(i as i64))
                .with_property("data", Value::String(format!("batch_data_{}", i)))
        })
        .collect();

    let start = Instant::now();
    client.batch_create_nodes(batch_nodes).await?;
    let duration = start.elapsed();
    results.push(BenchmarkResult::new("Batch Creation", BATCH_SIZE, duration));

    // 9. Mixed Workload Benchmark
    println!("🎯 Running Mixed Workload Benchmark...");
    let start = Instant::now();
    let mixed_ops = 1000;

    for i in 0..mixed_ops {
        match i % 4 {
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
                // Find by label
                client.find_nodes_by_label("person").await?;
            }
            3 => {
                // BFS
                if !node_ids.is_empty() {
                    let idx = i % node_ids.len();
                    client.bfs(&node_ids[idx], 2).await?;
                }
            }
            _ => unreachable!(),
        }
    }

    let duration = start.elapsed();
    results.push(BenchmarkResult::new("Mixed Workload", mixed_ops, duration));

    // Get final stats
    let stats = client.get_stats().await?;

    // Print results
    println!();
    println!("📊 BENCHMARK RESULTS - LOCAL MODE");
    println!("{}", "=".repeat(80));
    println!(
        "{:<20} | {:>8} | {:>8} | {:>10} | {:>8}",
        "Operation", "Count", "Duration", "Ops/Sec", "Latency"
    );
    println!("{}", "-".repeat(80));

    for result in &results {
        result.print();
    }

    println!();
    println!("💾 Database Statistics:");
    println!("   Nodes: {}", stats.node_count);
    println!("   Edges: {}", stats.edge_count);
    println!("   Cache Hit Rate: {:.2}%", stats.cache_hit_rate * 100.0);
    println!(
        "   Cache Size: {:.2} MB",
        stats.cache_size as f64 / (1024.0 * 1024.0)
    );

    // Calculate total operations and time
    let total_ops: usize = results.iter().map(|r| r.total_ops).sum();
    let total_time: Duration = results.iter().map(|r| r.duration).sum();
    let overall_ops_per_sec = total_ops as f64 / total_time.as_secs_f64();

    println!();
    println!("🎯 OVERALL PERFORMANCE:");
    println!("   Total Operations: {}", total_ops);
    println!("   Total Time: {:.2}s", total_time.as_secs_f64());
    println!("   Overall Throughput: {:.0} ops/sec", overall_ops_per_sec);

    // Cleanup
    client.flush().await?;
    println!();
    println!("✅ Local benchmark completed!");

    Ok(())
}
