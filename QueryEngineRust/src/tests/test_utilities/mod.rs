//! Test utilities for query equivalence testing
//!
//! This module provides utilities for testing that semantically equivalent
//! PromQL and SQL queries produce equivalent internal logic in the QueryEngine.

pub mod comparison;
pub mod config_builders;

// Re-export commonly used items
pub use comparison::*;
pub use config_builders::*;
