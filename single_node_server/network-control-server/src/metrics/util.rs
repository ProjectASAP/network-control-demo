/// A metric field identifier, driven by configuration rather than hardcoded variants.
///
/// `idx` is the metric's position within its index schema's metric list, used as a
/// direct array index into `PerKeyData.metrics` to skip per-call HashMap lookups.
/// `name` is retained for error messages and for callers that still need the
/// storage_field string (e.g. building forwarded ES bodies).
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MetricField {
    idx: usize,
    name: String,
}

impl MetricField {
    pub fn new(idx: usize, name: impl Into<String>) -> Self {
        Self {
            idx,
            name: name.into(),
        }
    }

    pub fn idx(&self) -> usize {
        self.idx
    }

    pub fn as_storage_field(&self) -> &str {
        &self.name
    }
}
