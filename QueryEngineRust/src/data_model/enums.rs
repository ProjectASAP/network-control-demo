#[derive(clap::ValueEnum, Clone, Debug)]
pub enum InputFormat {
    Json,
    Byte,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum StreamingEngine {
    Flink,
    Arroyo,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
#[allow(non_camel_case_types)]
pub enum QueryLanguage {
    #[value(alias = "SQL")]
    sql,
    #[value(alias = "PROMQL")]
    promql,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum QueryProtocol {
    #[value(alias = "PROMETHEUS_HTTP")]
    PrometheusHttp,
    #[value(alias = "CLICKHOUSE_HTTP")]
    ClickHouseHttp,
    // Future: ElasticHttp, DuckDbHttp, etc.
}

#[derive(clap::ValueEnum, Clone, Debug, Copy, PartialEq)]
pub enum LockStrategy {
    #[value(name = "global")]
    Global,
    #[value(name = "per-key")]
    PerKey,
}
