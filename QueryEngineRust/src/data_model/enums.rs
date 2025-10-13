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
