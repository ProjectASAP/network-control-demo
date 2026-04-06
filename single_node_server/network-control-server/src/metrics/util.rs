#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum MetricField {
    CpuCores,
    MemoryGb,
    NetworkMbps,
}

impl MetricField {
    pub fn from_spec(spec: &str) -> Option<Self> {
        let normalized = spec
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .replace(' ', "_");
        Self::from_storage_field(&normalized).or_else(|| match normalized.as_str() {
            "cpucores" => Some(Self::CpuCores),
            "memorygb" => Some(Self::MemoryGb),
            "networkmbps" => Some(Self::NetworkMbps),
            _ => None,
        })
    }

    pub fn from_storage_field(spec: &str) -> Option<Self> {
        match spec.trim().to_ascii_lowercase().as_str() {
            "cpu_cores" => Some(Self::CpuCores),
            "memory_gb" => Some(Self::MemoryGb),
            "network_mbps" => Some(Self::NetworkMbps),
            _ => None,
        }
    }

    pub fn as_storage_field(&self) -> &'static str {
        match self {
            Self::CpuCores => "cpu_cores",
            Self::MemoryGb => "memory_gb",
            Self::NetworkMbps => "network_mbps",
        }
    }
}
