use std::{
    collections::HashMap,
    sync::RwLock,
    time::{Duration, Instant},
};

use crate::metrics::MetricField;

struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
}

type PercentileCacheKey = (MetricField, Option<String>, Vec<i32>);

pub struct QueryCache {
    percentiles: RwLock<HashMap<PercentileCacheKey, CacheEntry<Vec<f64>>>>,
    ttl: Duration,
}

impl QueryCache {
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            percentiles: RwLock::new(HashMap::new()),
            ttl: Duration::from_millis(ttl_ms),
        }
    }

    fn cache_key(field: MetricField, key: Option<&str>, percents: &[f64]) -> PercentileCacheKey {
        (
            field,
            key.map(String::from),
            percents.iter().map(|p| *p as i32).collect(),
        )
    }

    pub fn get_percentiles(
        &self,
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
    ) -> Option<Vec<f64>> {
        if self.ttl.is_zero() {
            return None;
        }
        let cache_key = Self::cache_key(field, key, percents);
        let cache = self.percentiles.read().ok()?;
        let entry = cache.get(&cache_key)?;
        if entry.expires_at > Instant::now() {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    pub fn set_percentiles(
        &self,
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
        value: Vec<f64>,
    ) {
        if self.ttl.is_zero() {
            return;
        }
        let cache_key = Self::cache_key(field, key, percents);
        if let Ok(mut cache) = self.percentiles.write() {
            cache.insert(
                cache_key,
                CacheEntry {
                    value,
                    expires_at: Instant::now() + self.ttl,
                },
            );
        }
    }
}
