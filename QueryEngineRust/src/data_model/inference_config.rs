use anyhow::Result;
use serde_yaml::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;

use crate::data_model::{PromQLSchema, QueryConfig, QueryLanguage};
use promql_utilities::data_model::KeyByLabelNames;
use sql_utilities::sqlhelper::{SQLSchema, Table};

/// Schema configuration that can be either PromQL or SQL format
#[derive(Debug, Clone)]
pub enum SchemaConfig {
    PromQL(PromQLSchema),
    SQL(SQLSchema),
}

#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub schema: SchemaConfig,
    pub query_configs: Vec<QueryConfig>,
}

impl InferenceConfig {
    pub fn new(query_language: QueryLanguage) -> Self {
        let schema = match query_language {
            QueryLanguage::promql => SchemaConfig::PromQL(PromQLSchema::new()),
            QueryLanguage::sql => SchemaConfig::SQL(SQLSchema::new(Vec::new())),
        };
        Self {
            schema,
            query_configs: Vec::new(),
        }
    }

    pub fn from_yaml_file(yaml_file: &str, query_language: QueryLanguage) -> Result<Self> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);
        let data: Value = serde_yaml::from_reader(reader)?;

        Self::from_yaml_data(&data, query_language)
    }

    pub fn from_yaml_data(data: &Value, query_language: QueryLanguage) -> Result<Self> {
        let schema = match query_language {
            QueryLanguage::promql => {
                let promql_schema = Self::parse_promql_schema(data)?;
                SchemaConfig::PromQL(promql_schema)
            }
            QueryLanguage::sql => {
                let sql_schema = Self::parse_sql_schema(data)?;
                SchemaConfig::SQL(sql_schema)
            }
        };

        let query_configs = Self::parse_query_configs(data)?;

        Ok(Self {
            schema,
            query_configs,
        })
    }

    /// Parse PromQL schema from YAML data (metrics: key)
    fn parse_promql_schema(data: &Value) -> Result<PromQLSchema> {
        let mut promql_schema = PromQLSchema::new();
        if let Some(metrics) = data.get("metrics") {
            if let Some(metrics_map) = metrics.as_mapping() {
                for (metric_name_val, labels_val) in metrics_map {
                    if let (Some(metric_name), Some(labels_seq)) =
                        (metric_name_val.as_str(), labels_val.as_sequence())
                    {
                        let labels: Vec<String> = labels_seq
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        let key_by_label_names = KeyByLabelNames::new(labels);
                        promql_schema =
                            promql_schema.add_metric(metric_name.to_string(), key_by_label_names);
                    }
                }
            }
        }
        Ok(promql_schema)
    }

    /// Parse SQL schema from YAML data (sql_schema: key)
    fn parse_sql_schema(data: &Value) -> Result<SQLSchema> {
        let sql_schema_data = data
            .get("sql_schema")
            .ok_or_else(|| anyhow::anyhow!("Missing sql_schema field for SQL query language"))?;

        let tables_data = sql_schema_data
            .get("tables")
            .and_then(|v| v.as_sequence())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid tables field in sql_schema"))?;

        let mut tables = Vec::new();
        for table_data in tables_data {
            let name = table_data
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing name field in table"))?
                .to_string();

            let time_column = table_data
                .get("time_column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing time_column field in table {}", name))?
                .to_string();

            let value_columns: HashSet<String> = table_data
                .get("value_columns")
                .and_then(|v| v.as_sequence())
                .ok_or_else(|| anyhow::anyhow!("Missing value_columns field in table {}", name))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();

            let metadata_columns: HashSet<String> = table_data
                .get("metadata_columns")
                .and_then(|v| v.as_sequence())
                .ok_or_else(|| anyhow::anyhow!("Missing metadata_columns field in table {}", name))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();

            tables.push(Table::new(
                name,
                time_column,
                value_columns,
                metadata_columns,
            ));
        }

        Ok(SQLSchema::new(tables))
    }

    fn parse_query_configs(data: &Value) -> Result<Vec<QueryConfig>> {
        // Handle queries field -> query_configs
        let query_configs = if let Some(queries) = data.get("queries").and_then(|v| v.as_sequence())
        {
            let mut configs = Vec::new();
            for query_data in queries {
                let query = query_data
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing query field"))?
                    .to_string();

                // Parse aggregations if present
                let aggregations = if let Some(aggregations_data) =
                    query_data.get("aggregations").and_then(|v| v.as_sequence())
                {
                    let mut agg_refs = Vec::new();
                    for agg_data in aggregations_data {
                        let aggregation_id = agg_data
                            .get("aggregation_id")
                            .and_then(|v| v.as_u64())
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing aggregation_id in aggregation")
                            })?;

                        let num_aggregates_to_retain = agg_data
                            .get("num_aggregates_to_retain")
                            .and_then(|v| v.as_u64());

                        agg_refs.push(crate::data_model::AggregationReference::new(
                            aggregation_id,
                            num_aggregates_to_retain,
                        ));
                    }
                    agg_refs
                } else {
                    Vec::new()
                };

                let config = QueryConfig::new(query).with_aggregations(aggregations);
                configs.push(config);
            }
            configs
        } else {
            Vec::new()
        };
        Ok(query_configs)
    }
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self::new(QueryLanguage::promql)
    }
}
