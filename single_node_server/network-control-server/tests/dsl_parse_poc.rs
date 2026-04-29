//! PoC: parse the request shapes the sketch server currently supports
//! using the elasticsearch-dsl-ast crate, and measure parse RTT.
//!
//! Currently supported by sketch server (per src/server/planner.rs +
//! src/server/query.rs):
//!   - query: `{ bool: { filter: [ { term: {<key>: ...} }, { term: { epoch: N } } ] } }`
//!   - aggs:  `<name>: { percentiles: { field, percents } }`
//!   - aggs:  `<name>: { sum: { field } }`  (standard ES; was previously
//!     exposed under the non-standard name `cumulative`)
//!
//! This test verifies that those shapes parse cleanly with the typed AST,
//! prints parse latency, and confirms that the legacy `cumulative` name
//! still rejects (so we don't accidentally regress to it).
use elasticsearch_dsl_ast::Search;
use serde_json::json;
use std::time::Instant;

fn time_parse(label: &str, body: &serde_json::Value, iters: u32) -> Result<(), String> {
    // Warmup + correctness check.
    let _: Search = serde_json::from_value(body.clone())
        .map_err(|e| format!("{label}: parse failed: {e}"))?;

    let mut total_ns: u128 = 0;
    for _ in 0..iters {
        let cloned = body.clone();
        let t0 = Instant::now();
        let _: Search = serde_json::from_value(cloned).expect("parse");
        total_ns += t0.elapsed().as_nanos();
    }
    let avg_us = (total_ns as f64) / (iters as f64) / 1000.0;
    println!("{label:<55} avg parse RTT: {avg_us:8.2} µs   (n={iters})");
    Ok(())
}

#[test]
fn poc_parse_supported_request_shapes() {
    let iters: u32 = 1000;

    // ---------- 1. bool.filter.term + percentiles ----------
    let percentiles_body = json!({
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    { "term": { "node": "N001" } },
                    { "term": { "epoch": 7 } }
                ]
            }
        },
        "aggs": {
            "cpu_pct": { "percentiles": { "field": "cpu",  "percents": [0, 50, 90, 100] } },
            "mem_pct": { "percentiles": { "field": "mem",  "percents": [0, 50, 90, 100] } },
            "net_pct": { "percentiles": { "field": "net",  "percents": [0, 50, 90, 100] } }
        }
    });

    // ---------- 2. bool.filter.term + sum (standard ES) ----------
    let sum_body = json!({
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    { "term": { "node": "N001" } },
                    { "term": { "epoch": 7 } }
                ]
            }
        },
        "aggs": {
            "cpu_sum": { "sum": { "field": "cpu" } },
            "mem_sum": { "sum": { "field": "mem" } },
            "net_sum": { "sum": { "field": "net" } }
        }
    });

    // ---------- 3. percentiles + sum combined (what ES side actually queries) ----------
    let combined_body = json!({
        "size": 0,
        "query": {
            "bool": {
                "filter": [
                    { "term": { "node": "N001" } },
                    { "term": { "epoch": 7 } }
                ]
            }
        },
        "aggs": {
            "cpu_pct": { "percentiles": { "field": "cpu", "percents": [0, 50, 90, 100] } },
            "mem_pct": { "percentiles": { "field": "mem", "percents": [0, 50, 90, 100] } },
            "net_pct": { "percentiles": { "field": "net", "percents": [0, 50, 90, 100] } },
            "cpu_sum": { "sum": { "field": "cpu" } },
            "mem_sum": { "sum": { "field": "mem" } },
            "net_sum": { "sum": { "field": "net" } }
        }
    });

    // ---------- 4. custom 'cumulative' agg (sketch server's current name) ----------
    let cumulative_body = json!({
        "size": 0,
        "query": {
            "bool": {
                "filter": [ { "term": { "node": "N001" } } ]
            }
        },
        "aggs": {
            "cpu_cum": { "cumulative": { "field": "cpu" } }
        }
    });

    // ---------- run ----------
    println!();
    println!("--- from_value (Value -> Search) ---");
    time_parse("[1] bool.filter.term + percentiles", &percentiles_body, iters).unwrap();
    time_parse("[2] bool.filter.term + sum",         &sum_body,         iters).unwrap();
    time_parse("[3] bool.filter.term + percentiles+sum", &combined_body, iters).unwrap();

    println!("\n--- from_slice (raw JSON bytes -> Search; closer to actual HTTP path) ---");
    let bench_from_slice = |label: &str, body: &serde_json::Value| {
        let bytes = serde_json::to_vec(body).unwrap();
        // warmup
        let _: Search = serde_json::from_slice(&bytes).unwrap();
        let mut total_ns: u128 = 0;
        for _ in 0..iters {
            let t0 = Instant::now();
            let _: Search = serde_json::from_slice(&bytes).unwrap();
            total_ns += t0.elapsed().as_nanos();
        }
        let avg_us = (total_ns as f64) / (iters as f64) / 1000.0;
        println!("{label:<55} avg parse RTT: {avg_us:8.2} µs   (n={iters}, body={} B)", bytes.len());
    };
    bench_from_slice("[1] bool.filter.term + percentiles",    &percentiles_body);
    bench_from_slice("[2] bool.filter.term + sum",            &sum_body);
    bench_from_slice("[3] bool.filter.term + percentiles+sum", &combined_body);

    // Expected to fail — demonstrates that our custom 'cumulative' is not standard DSL.
    let res: Result<Search, _> = serde_json::from_value(cumulative_body);
    match res {
        Ok(_) => println!("[4] custom 'cumulative' agg: parsed UNEXPECTEDLY ✓"),
        Err(e) => println!("[4] custom 'cumulative' agg: parse failed (expected): {e}"),
    }

    // Verify the parsed structure of [3] matches what we'd plan against.
    let s: Search = serde_json::from_value(combined_body.clone()).unwrap();
    assert_eq!(s.size, Some(0));
    assert!(s.query.is_some(), "query parsed");
    assert_eq!(s.aggs.len(), 6, "all 6 aggs parsed");
    println!("\nparsed Search has {} aggs, query={}", s.aggs.len(), s.query.is_some());
}
