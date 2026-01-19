mod countmin;
mod frequency;
mod key;
mod quantiles;
mod store;

pub use store::{EntityEstimate, MetricField, MetricPreAggregation, MetricStore};
pub type InsertTiming = store::InsertTiming;
