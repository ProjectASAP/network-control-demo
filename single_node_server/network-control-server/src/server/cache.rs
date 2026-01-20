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

pub(crate) type PercentileCacheKey = (MetricField, Option<String>, Vec<i32>);

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

    pub(crate) fn is_enabled(&self) -> bool {
        !self.ttl.is_zero()
    }

    pub(crate) fn build_percentiles_cache_key(
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
    ) -> PercentileCacheKey {
        (
            field,
            key.map(String::from),
            percents.iter().map(|p| *p as i32).collect(),
        )
    }

    #[allow(dead_code)]
    pub fn get_percentiles(
        &self,
        field: MetricField,
        key: Option<&str>,
        percents: &[f64],
    ) -> Option<Vec<f64>> {
        if self.ttl.is_zero() {
            return None;
        }
        let cache_key = Self::build_percentiles_cache_key(field, key, percents);
        self.get_percentiles_with_key(&cache_key)
    }

    pub(crate) fn get_percentiles_with_key(
        &self,
        cache_key: &PercentileCacheKey,
    ) -> Option<Vec<f64>> {
        if self.ttl.is_zero() {
            return None;
        }
        let cache = self.percentiles.read().ok()?;
        let entry = cache.get(cache_key)?;
        if entry.expires_at > Instant::now() {
            Some(entry.value.clone())
        } else {
            None
        }
    }

    #[allow(dead_code)]
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
        let cache_key = Self::build_percentiles_cache_key(field, key, percents);
        self.set_percentiles_with_key(cache_key, value);
    }

    pub(crate) fn set_percentiles_with_key(
        &self,
        cache_key: PercentileCacheKey,
        value: Vec<f64>,
    ) {
        if self.ttl.is_zero() {
            return;
        }
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
