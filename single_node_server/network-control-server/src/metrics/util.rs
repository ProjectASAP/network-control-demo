/// A metric field identifier, driven by configuration rather than hardcoded variants.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MetricField(pub String);

impl MetricField {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_storage_field(&self) -> &str {
        &self.0
    }
}
