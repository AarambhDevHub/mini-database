//! Complete join functionality for Mini Database
//! Supports INNER, LEFT, RIGHT, FULL OUTER, CROSS joins with aggregation

use crate::client::DatabaseClient;
use crate::types::{Edge, Node, Value};
use crate::utils::error::Result;
use std::collections::HashMap;
use tracing::{debug, trace};

/// Aggregate function types
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Sum,
    Avg,
    Count,
    Max,
    Min,
}

/// Join types
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

/// Join condition types
#[derive(Clone)]
pub enum JoinCondition {
    /// Join via edge relationship
    Edge { edge_label: String },
    /// Join via property equality
    Property {
        left_prop: String,
        right_prop: String,
    },
}

// Manual Debug implementation for JoinCondition
impl std::fmt::Debug for JoinCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinCondition::Edge { edge_label } => f
                .debug_struct("Edge")
                .field("edge_label", edge_label)
                .finish(),
            JoinCondition::Property {
                left_prop,
                right_prop,
            } => f
                .debug_struct("Property")
                .field("left_prop", left_prop)
                .field("right_prop", right_prop)
                .finish(),
        }
    }
}

/// Structure for holding join query results
#[derive(Debug, Clone)]
pub struct JoinResult {
    pub columns: Vec<String>,
    pub rows: Vec<HashMap<String, Value>>,
    pub total_rows: usize,
}

impl JoinResult {
    /// Create new join result
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            rows: Vec::new(),
            total_rows: 0,
        }
    }

    /// Add a row to the result
    pub fn add_row(&mut self, row: HashMap<String, Value>) {
        self.rows.push(row);
        self.total_rows += 1;
    }

    /// Print results in table format
    pub fn print(&self) {
        self.print_with_limit(None);
    }

    /// Print results with row limit
    pub fn print_with_limit(&self, limit: Option<usize>) {
        let width = 18;
        let col_count = self.columns.len();

        // Print header
        println!("┌{}┐", "─".repeat(width * col_count + col_count - 1));
        print!("│");
        for col in &self.columns {
            print!("{:^width$}│", col, width = width);
        }
        println!();
        println!("├{}┤", "─".repeat(width * col_count + col_count - 1));

        // Print rows
        let display_rows = if let Some(limit) = limit {
            self.rows.iter().take(limit).collect::<Vec<_>>()
        } else {
            self.rows.iter().collect::<Vec<_>>()
        };

        for row in display_rows {
            print!("│");
            for col in &self.columns {
                let val = row
                    .get(col)
                    .map(|v| v.to_string_value())
                    .unwrap_or_else(|| "NULL".to_string());
                let truncated = if val.len() > width - 2 {
                    format!("{}...", &val[..width - 5])
                } else {
                    val
                };
                print!("{:^width$}│", truncated, width = width);
            }
            println!();
        }

        println!("└{}┘", "─".repeat(width * col_count + col_count - 1));

        if let Some(limit) = limit {
            if self.total_rows > limit {
                println!("Showing {} of {} rows", limit, self.total_rows);
            } else {
                println!("Total rows: {}", self.total_rows);
            }
        } else {
            println!("Total rows: {}", self.total_rows);
        }
    }

    /// Get iterator over rows
    pub fn iter(&self) -> impl Iterator<Item = &HashMap<String, Value>> {
        self.rows.iter()
    }

    /// Convert to Vec of rows
    pub fn into_rows(self) -> Vec<HashMap<String, Value>> {
        self.rows
    }

    /// Get specific column values
    pub fn get_column(&self, column: &str) -> Vec<&Value> {
        self.rows.iter().filter_map(|row| row.get(column)).collect()
    }
}

/// Join builder for fluent API
pub struct JoinBuilder<'a> {
    client: &'a DatabaseClient,
    left_label: String,
    right_label: String,
    join_type: JoinType,
    condition: Option<JoinCondition>,
    select_columns: Vec<(String, String)>, // (table, column)
    where_conditions: Vec<Box<dyn Fn(&HashMap<String, Value>) -> bool + Send + Sync>>, // Add back where conditions
    order_by: Option<(String, bool)>, // (column, ascending)
    limit: Option<usize>,
}

// Manual Debug implementation for JoinBuilder (avoiding function pointer issues)
impl<'a> std::fmt::Debug for JoinBuilder<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinBuilder")
            .field("left_label", &self.left_label)
            .field("right_label", &self.right_label)
            .field("join_type", &self.join_type)
            .field("condition", &self.condition)
            .field("select_columns", &self.select_columns)
            .field("where_conditions_count", &self.where_conditions.len()) // Show count instead of actual functions
            .field("order_by", &self.order_by)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl<'a> JoinBuilder<'a> {
    pub fn new(client: &'a DatabaseClient, left_label: String, right_label: String) -> Self {
        Self {
            client,
            left_label,
            right_label,
            join_type: JoinType::Inner,
            condition: None,
            select_columns: Vec::new(),
            where_conditions: Vec::new(), // Initialize where conditions
            order_by: None,
            limit: None,
        }
    }

    /// Set join type
    pub fn join_type(mut self, join_type: JoinType) -> Self {
        self.join_type = join_type;
        self
    }

    /// Set edge-based join condition
    pub fn on_edge(mut self, edge_label: String) -> Self {
        self.condition = Some(JoinCondition::Edge { edge_label });
        self
    }

    /// Set property-based join condition
    pub fn on_property(mut self, left_prop: String, right_prop: String) -> Self {
        self.condition = Some(JoinCondition::Property {
            left_prop,
            right_prop,
        });
        self
    }

    /// Add selected columns
    pub fn select(mut self, columns: Vec<(String, String)>) -> Self {
        self.select_columns = columns;
        self
    }

    /// Add where condition - THE MISSING METHOD
    pub fn where_condition<F>(mut self, condition: F) -> Self
    where
        F: Fn(&HashMap<String, Value>) -> bool + Send + Sync + 'static,
    {
        self.where_conditions.push(Box::new(condition));
        self
    }

    /// Add order by
    pub fn order_by(mut self, column: String, ascending: bool) -> Self {
        self.order_by = Some((column, ascending));
        self
    }

    /// Add limit
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the join
    pub async fn execute(self) -> Result<JoinResult> {
        self.client.execute_join_builder(self).await
    }
}

/// Join functionality implementation for DatabaseClient
impl DatabaseClient {
    /// Create a join builder
    pub fn join(&self, left_label: &str, right_label: &str) -> JoinBuilder {
        JoinBuilder::new(self, left_label.to_string(), right_label.to_string())
    }

    /// Execute join builder
    pub async fn execute_join_builder(&self, builder: JoinBuilder<'_>) -> Result<JoinResult> {
        match builder.join_type {
            JoinType::Inner => self.inner_join_impl(builder).await,
            JoinType::Left => self.left_join_impl(builder).await,
            JoinType::Right => self.right_join_impl(builder).await,
            JoinType::FullOuter => self.full_outer_join_impl(builder).await,
            JoinType::Cross => self.cross_join_impl(builder).await,
        }
    }

    /// INNER JOIN implementation
    pub async fn inner_join(
        &self,
        left_label: &str,
        right_label: &str,
        edge_label: &str,
        select_columns: Vec<(String, String)>,
    ) -> Result<JoinResult> {
        self.join(left_label, right_label)
            .join_type(JoinType::Inner)
            .on_edge(edge_label.to_string())
            .select(select_columns)
            .execute()
            .await
    }

    /// LEFT JOIN implementation
    pub async fn left_join(
        &self,
        left_label: &str,
        right_label: &str,
        left_prop: &str,
        right_prop: &str,
        select_columns: Vec<(String, String)>,
    ) -> Result<JoinResult> {
        self.join(left_label, right_label)
            .join_type(JoinType::Left)
            .on_property(left_prop.to_string(), right_prop.to_string())
            .select(select_columns)
            .execute()
            .await
    }

    /// CROSS JOIN implementation
    pub async fn cross_join(
        &self,
        left_label: &str,
        right_label: &str,
        select_columns: Vec<(String, String)>,
    ) -> Result<JoinResult> {
        self.join(left_label, right_label)
            .join_type(JoinType::Cross)
            .select(select_columns)
            .execute()
            .await
    }

    /// Aggregate join with grouping
    pub async fn aggregate_join(
        &self,
        left_label: &str,
        right_label: &str,
        edge_label: &str,
        group_by_column: &str,
        aggregate_column: &str,
        function: AggregateFunction,
    ) -> Result<HashMap<String, f64>> {
        let left_nodes = self.find_nodes_by_label(left_label).await?;
        let mut groups: HashMap<String, Vec<f64>> = HashMap::new();

        for left_node in left_nodes {
            let group_key = left_node
                .get_property(group_by_column)
                .map(|v| v.to_string_value())
                .unwrap_or_else(|| "NULL".to_string());

            let edges = self.get_outgoing_edges(&left_node.id).await?;

            for edge in edges {
                if edge.label == edge_label {
                    if let Some(right_node) = self.get_node(&edge.target).await? {
                        if right_node.label == right_label {
                            if let Some(value) = right_node.get_property(aggregate_column) {
                                if let Some(num_val) = value.as_float() {
                                    groups
                                        .entry(group_key.clone())
                                        .or_insert_with(Vec::new)
                                        .push(num_val);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Apply aggregate function
        let mut result = HashMap::new();
        for (group, values) in groups {
            if values.is_empty() {
                continue;
            }

            let aggregated = match function {
                AggregateFunction::Sum => values.iter().sum(),
                AggregateFunction::Avg => values.iter().sum::<f64>() / values.len() as f64,
                AggregateFunction::Count => values.len() as f64,
                AggregateFunction::Max => values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)),
                AggregateFunction::Min => values.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
            };
            result.insert(group, aggregated);
        }

        debug!("Aggregate join completed: {} groups", result.len());
        Ok(result)
    }

    // Private implementation methods
    async fn inner_join_impl(&self, builder: JoinBuilder<'_>) -> Result<JoinResult> {
        let mut result = JoinResult::new(
            builder
                .select_columns
                .iter()
                .map(|(table, col)| format!("{}.{}", table, col))
                .collect(),
        );

        let left_nodes = self.find_nodes_by_label(&builder.left_label).await?;

        for left_node in left_nodes {
            let matching_rights = self.find_matching_nodes(&left_node, &builder).await?;

            for right_node in matching_rights {
                let row = self.build_result_row(&left_node, &right_node, &builder);
                if self.passes_where_conditions(&row, &builder.where_conditions) {
                    // Apply where conditions
                    result.add_row(row);
                }
            }
        }

        self.apply_post_processing(&mut result, &builder);
        debug!("Inner join completed: {} rows", result.total_rows);
        Ok(result)
    }

    async fn left_join_impl(&self, builder: JoinBuilder<'_>) -> Result<JoinResult> {
        let mut result = JoinResult::new(
            builder
                .select_columns
                .iter()
                .map(|(table, col)| format!("{}.{}", table, col))
                .collect(),
        );

        let left_nodes = self.find_nodes_by_label(&builder.left_label).await?;

        for left_node in left_nodes {
            let matching_rights = self.find_matching_nodes(&left_node, &builder).await?;

            if matching_rights.is_empty() {
                // LEFT JOIN: include left node with NULLs for right
                let row = self.build_left_null_row(&left_node, &builder);
                if self.passes_where_conditions(&row, &builder.where_conditions) {
                    // Apply where conditions
                    result.add_row(row);
                }
            } else {
                for right_node in matching_rights {
                    let row = self.build_result_row(&left_node, &right_node, &builder);
                    if self.passes_where_conditions(&row, &builder.where_conditions) {
                        // Apply where conditions
                        result.add_row(row);
                    }
                }
            }
        }

        self.apply_post_processing(&mut result, &builder);
        debug!("Left join completed: {} rows", result.total_rows);
        Ok(result)
    }

    async fn right_join_impl(&self, builder: JoinBuilder<'_>) -> Result<JoinResult> {
        // RIGHT JOIN = LEFT JOIN with tables swapped
        let swapped_builder = JoinBuilder {
            client: builder.client,
            left_label: builder.right_label.clone(),
            right_label: builder.left_label.clone(),
            join_type: JoinType::Left,
            condition: builder.condition,
            select_columns: builder
                .select_columns
                .into_iter()
                .map(|(table, col)| {
                    let new_table = if table == builder.left_label {
                        builder.right_label.clone()
                    } else {
                        builder.left_label.clone()
                    };
                    (new_table, col)
                })
                .collect(),
            where_conditions: builder.where_conditions, // Pass where conditions
            order_by: builder.order_by,
            limit: builder.limit,
        };

        self.left_join_impl(swapped_builder).await
    }

    async fn full_outer_join_impl(&self, builder: JoinBuilder<'_>) -> Result<JoinResult> {
        // FULL OUTER JOIN = UNION of LEFT JOIN and RIGHT JOIN
        let left_result = self
            .left_join_impl(JoinBuilder {
                client: builder.client,
                left_label: builder.left_label.clone(),
                right_label: builder.right_label.clone(),
                join_type: JoinType::Left,
                condition: builder.condition.clone(),
                select_columns: builder.select_columns.clone(),
                where_conditions: Vec::new(), // Apply conditions later
                order_by: None,
                limit: None,
            })
            .await?;

        let right_result = self
            .right_join_impl(JoinBuilder {
                client: builder.client,
                left_label: builder.left_label.clone(),
                right_label: builder.right_label.clone(),
                join_type: JoinType::Right,
                condition: builder.condition.clone(),
                select_columns: builder.select_columns.clone(),
                where_conditions: Vec::new(), // Apply conditions later
                order_by: None,
                limit: None,
            })
            .await?;

        // Union results (remove duplicates)
        let mut result = JoinResult::new(
            builder
                .select_columns
                .iter()
                .map(|(table, col)| format!("{}.{}", table, col))
                .collect(),
        );

        let mut seen_rows = std::collections::HashSet::new();

        for row in left_result
            .rows
            .into_iter()
            .chain(right_result.rows.into_iter())
        {
            let row_key = format!("{:?}", row); // Simple deduplication
            if !seen_rows.contains(&row_key)
                && self.passes_where_conditions(&row, &builder.where_conditions)
            {
                // Apply where conditions
                seen_rows.insert(row_key);
                result.add_row(row);
            }
        }

        self.apply_post_processing(&mut result, &builder);
        debug!("Full outer join completed: {} rows", result.total_rows);
        Ok(result)
    }

    async fn cross_join_impl(&self, builder: JoinBuilder<'_>) -> Result<JoinResult> {
        let mut result = JoinResult::new(
            builder
                .select_columns
                .iter()
                .map(|(table, col)| format!("{}.{}", table, col))
                .collect(),
        );

        let left_nodes = self.find_nodes_by_label(&builder.left_label).await?;
        let right_nodes = self.find_nodes_by_label(&builder.right_label).await?;

        for left_node in &left_nodes {
            for right_node in &right_nodes {
                let row = self.build_result_row(left_node, right_node, &builder);
                if self.passes_where_conditions(&row, &builder.where_conditions) {
                    // Apply where conditions
                    result.add_row(row);
                }
            }
        }

        self.apply_post_processing(&mut result, &builder);
        debug!("Cross join completed: {} rows", result.total_rows);
        Ok(result)
    }

    // Helper methods
    async fn find_matching_nodes(
        &self,
        left_node: &Node,
        builder: &JoinBuilder<'_>,
    ) -> Result<Vec<Node>> {
        match &builder.condition {
            Some(JoinCondition::Edge { edge_label }) => {
                let edges = self.get_outgoing_edges(&left_node.id).await?;
                let mut matching = Vec::new();

                for edge in edges {
                    if edge.label == *edge_label {
                        if let Some(right_node) = self.get_node(&edge.target).await? {
                            if right_node.label == builder.right_label {
                                matching.push(right_node);
                            }
                        }
                    }
                }
                Ok(matching)
            }
            Some(JoinCondition::Property {
                left_prop,
                right_prop,
            }) => {
                if let Some(left_value) = left_node.get_property(left_prop) {
                    let right_nodes = self.find_nodes_by_property(right_prop, left_value).await?;
                    Ok(right_nodes
                        .into_iter()
                        .filter(|n| n.label == builder.right_label)
                        .collect())
                } else {
                    Ok(Vec::new())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    fn build_result_row(
        &self,
        left_node: &Node,
        right_node: &Node,
        builder: &JoinBuilder<'_>,
    ) -> HashMap<String, Value> {
        let mut row = HashMap::new();

        for (table, col) in &builder.select_columns {
            let node = if *table == builder.left_label {
                left_node
            } else {
                right_node
            };

            let value = if col == "id" {
                Value::String(node.id.clone())
            } else if col == "label" {
                Value::String(node.label.clone())
            } else {
                node.get_property(col).cloned().unwrap_or(Value::Null)
            };

            row.insert(format!("{}.{}", table, col), value);
        }

        row
    }

    fn build_left_null_row(
        &self,
        left_node: &Node,
        builder: &JoinBuilder<'_>,
    ) -> HashMap<String, Value> {
        let mut row = HashMap::new();

        for (table, col) in &builder.select_columns {
            let value = if *table == builder.left_label {
                if col == "id" {
                    Value::String(left_node.id.clone())
                } else if col == "label" {
                    Value::String(left_node.label.clone())
                } else {
                    left_node.get_property(col).cloned().unwrap_or(Value::Null)
                }
            } else {
                Value::Null // Right side is NULL in LEFT JOIN
            };

            row.insert(format!("{}.{}", table, col), value);
        }

        row
    }

    // Add the missing helper function
    fn passes_where_conditions(
        &self,
        row: &HashMap<String, Value>,
        conditions: &[Box<dyn Fn(&HashMap<String, Value>) -> bool + Send + Sync>],
    ) -> bool {
        conditions.iter().all(|condition| condition(row))
    }

    fn apply_post_processing(&self, result: &mut JoinResult, builder: &JoinBuilder<'_>) {
        // Apply ORDER BY
        if let Some((order_column, ascending)) = &builder.order_by {
            result.rows.sort_by(|a, b| {
                let a_val = a.get(order_column).unwrap_or(&Value::Null);
                let b_val = b.get(order_column).unwrap_or(&Value::Null);

                let cmp = a_val
                    .partial_cmp(b_val)
                    .unwrap_or(std::cmp::Ordering::Equal);
                if *ascending { cmp } else { cmp.reverse() }
            });
        }

        // Apply LIMIT
        if let Some(limit) = builder.limit {
            result.rows.truncate(limit);
        }

        result.total_rows = result.rows.len();
    }
}

// Utility implementations
impl JoinCondition {
    pub fn edge(edge_label: &str) -> Self {
        Self::Edge {
            edge_label: edge_label.to_string(),
        }
    }

    pub fn property(left_prop: &str, right_prop: &str) -> Self {
        Self::Property {
            left_prop: left_prop.to_string(),
            right_prop: right_prop.to_string(),
        }
    }
}
