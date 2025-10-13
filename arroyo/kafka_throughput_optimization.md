# Arroyo Kafka Throughput Optimization Guide

## Current Performance Issues

Kafka source in Arroyo is 20x slower than impulse source due to:

1. **Small Batch Size**: 512 records vs impulse's 8192 max
2. **Missing rdkafka Optimizations**: No fetch size/timeout configs  
3. **Individual Message Processing**: No Kafka-level batching
4. **Fixed Flush Interval**: 50ms regardless of throughput

## (A) Code Changes for Higher Throughput

### 1. Increase Batch Size
**File**: `crates/arroyo-rpc/default.toml:5`
```toml
# Change from 512 to match impulse source
source-batch-size = 8192
```

### 2. Add rdkafka Consumer Configs (Code Changes Required)
**File**: `crates/arroyo-connectors/src/kafka/source/mod.rs:82-87`

Add these settings to the consumer configuration:
```rust
let consumer: StreamConsumer = client_config
    .set("bootstrap.servers", &self.bootstrap_servers)
    .set("enable.partition.eof", "false")
    .set("enable.auto.commit", "false")
    .set("group.id", group_id)
    // Add these for higher throughput:
    .set("fetch.min.bytes", "1048576")        // 1MB minimum fetch
    .set("fetch.max.wait.ms", "10")           // Max 10ms wait
    .set("max.partition.fetch.bytes", "4194304")  // 4MB per partition
    .set("queued.max.messages.kbytes", "65536")   // 64MB queue
    .set("receive.message.max.bytes", "104857600") // 100MB max message
    .create_with_context(self.context.clone())?;
```

### 3. Optimize Flush Logic
**File**: `crates/arroyo-connectors/src/kafka/source/mod.rs:191-291`

Change flush ticker from fixed 50ms to adaptive:
```rust
// Replace fixed 50ms with adaptive interval based on throughput
let base_interval = Duration::from_millis(10);
let mut flush_ticker = tokio::time::interval(base_interval);
```

## (B) Kafka Configuration Without Code Changes

### User-Configurable Consumer Settings

You can configure many Kafka consumer settings through SQL or Web UI using:

**SQL Example:**
```sql
CREATE TABLE high_throughput_table (
    id TEXT,
    data TEXT
) WITH (
    connector = 'kafka',
    topic = 'benchmark_topic',
    format = 'json',
    bootstrap_servers = 'localhost:9092',
    type = 'source',
    'source.offset' = 'latest',
    'source.read_mode' = 'read_uncommitted',
    'client_configs' = 'session.timeout.ms=30000,heartbeat.interval.ms=3000,auto.offset.reset=latest,isolation.level=read_uncommitted'
);
```

**Connection Profile (connectionProperties):**
```json
{
  "connectionProperties": {
    "session.timeout.ms": "30000",
    "heartbeat.interval.ms": "3000",
    "fetch.min.bytes": "1048576",
    "fetch.max.wait.ms": "10",
    "max.partition.fetch.bytes": "4194304"
  }
}
```

## (C) Optimal Kafka Producer/Broker/Consumer Configuration

### Kafka Broker Settings (server.properties)
```properties
# Network & I/O
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Log & Storage
log.segment.bytes=1073741824
log.retention.hours=1
log.cleanup.policy=delete
log.flush.interval.messages=10000
log.flush.interval.ms=1000

# Replication (adjust based on availability needs)
default.replication.factor=1
min.insync.replicas=1

# Performance
compression.type=lz4
batch.size=1048576
```

### Kafka Producer Settings (for benchmark data generation)
```properties
bootstrap.servers=localhost:9092
compression.type=lz4
batch.size=1048576
linger.ms=10
buffer.memory=67108864
max.request.size=104857600
acks=1
retries=0
```

### Additional Arroyo Consumer Settings
Configure these through `client_configs` or `connectionProperties`:

```
session.timeout.ms=30000
heartbeat.interval.ms=3000
auto.offset.reset=latest
isolation.level=read_uncommitted
fetch.min.bytes=1048576
fetch.max.wait.ms=10
max.partition.fetch.bytes=4194304
queued.max.messages.kbytes=65536
receive.message.max.bytes=104857600
```

## Expected Performance Impact

- **Batch Size Increase**: 16x improvement (512→8192)
- **rdkafka Fetch Optimizations**: Reduced consumer latency
- **Broker/Producer Configs**: Optimized end-to-end message flow
- **Combined Effect**: Should achieve throughput much closer to impulse source

## Implementation Priority

1. **High Impact, No Code**: Increase `source-batch-size` in `default.toml`
2. **Medium Impact, No Code**: Configure consumer settings via `client_configs`
3. **High Impact, Code Required**: Add rdkafka optimizations to source implementation
4. **Infrastructure**: Optimize Kafka broker and producer configurations

## Configuration Locations

- **Code Changes**: `crates/arroyo-connectors/src/kafka/source/mod.rs`
- **Batch Size**: `crates/arroyo-rpc/default.toml`
- **User Config**: SQL DDL `client_configs` field or Web UI connection properties
- **Schemas**: `crates/arroyo-connectors/src/kafka/profile.json` and `table.json`

## (D) Applying Impulse Source Optimizations to Kafka

### Analysis: Why Impulse is Faster

**Impulse Source Advantages:**
1. **Direct Arrow RecordBatch Creation**: Bypasses deserializer framework entirely
2. **Custom Batch Logic**: Uses 1-8192 dynamic batching vs fixed 512
3. **No Format Parsing Overhead**: Generates data directly into Arrow builders
4. **Direct Collection**: Calls `collector.collect(RecordBatch)` instead of `deserialize_slice()` + `flush_buffer()`

**Architecture Comparison:**
```
Impulse:  Data Generation → Arrow Builders → RecordBatch (1-8192) → collector.collect()
Kafka:    Kafka Messages → Deserializer → Buffer (512) → flush_buffer() → RecordBatch
```

### Feasible Optimizations for Kafka Source

#### ✅ 1. Kafka-Level Message Batching (Hybrid Approach)

**Current Flow:**
```
Kafka consumer → Individual messages → Deserializer (512 limit) → Arrow batch
```

**Optimized Flow:**
```
Kafka consumer → Multiple messages in one poll → Batch deserialize → Larger Arrow batch
```

**Implementation Strategy:**
**File**: `crates/arroyo-connectors/src/kafka/source/mod.rs:191-291`

Replace single message processing with batched collection:
```rust
// Replace: message = consumer.recv() =>
// With batched approach:
let mut message_batch = Vec::with_capacity(8192);
let batch_timeout = Duration::from_millis(10);
let batch_start = Instant::now();

// Collect messages until batch full or timeout
while message_batch.len() < 8192 && batch_start.elapsed() < batch_timeout {
    match consumer.poll(Duration::from_millis(1)) {
        Some(Ok(msg)) => message_batch.push(msg),
        Some(Err(_)) => break,
        None => break,
    }
}

// Process batch efficiently
for message in message_batch {
    // Process message (existing logic)
    collector.deserialize_slice(payload, timestamp, metadata).await?;
}

// Flush larger batch
if collector.should_flush() {
    collector.flush_buffer().await?;
}
```

**Expected Impact**: 5-10x improvement by reducing per-message overhead

#### ✅ 2. Format-Specific Direct Building (Limited Scope)

**Constraints**: Only works for simple, known schemas (JSON with fixed fields)
**Benefits**: Bypass deserializer for performance-critical scenarios

**Implementation Example for JSON:**
```rust
// For known JSON schemas, skip deserializer
if format_is_simple_json && schema_is_known {
    let mut builders = create_arrow_builders_from_schema(output_schema, 8192);
    
    for message in kafka_message_batch {
        parse_json_directly_into_builders(message.payload(), &mut builders)?;
    }
    
    let record_batch = RecordBatch::try_new(
        output_schema.clone(), 
        finish_arrow_builders(builders)
    )?;
    
    collector.collect(record_batch).await;
} else {
    // Fall back to deserializer for complex formats
    // (Avro, Protobuf, Schema Registry, etc.)
}
```

#### ✅ 3. Custom Batch Buffer Management

**Strategy**: Accumulate more records before creating Arrow batches
```rust
struct KafkaCustomBuffer {
    records: Vec<ProcessedRecord>,
    target_batch_size: usize, // 8192 instead of 512
}

impl KafkaCustomBuffer {
    fn should_flush(&self) -> bool {
        self.records.len() >= self.target_batch_size
    }
    
    fn flush_to_arrow(&mut self) -> Result<RecordBatch> {
        // Build Arrow batch from accumulated records
        // Bypass 512-record deserializer limit
    }
}
```

### What CANNOT Be Applied

#### ❌ Complete Deserializer Bypass
- Kafka messages require format parsing (JSON/Avro/Protobuf)
- Schema inference and validation still needed
- Dynamic schema support requires deserializer framework

#### ❌ Direct Schema Access
- Unlike impulse (fixed schema), Kafka supports multiple formats
- Schema Registry integration requires runtime schema resolution
- Format-dependent field mapping cannot be bypassed

#### ❌ Deterministic Data Generation
- Kafka delivers external data, cannot generate like impulse
- Message ordering and partitioning constraints apply
- Consumer group and offset management required

### Recommended Implementation Approach

**Phase 1: Kafka Message Batching**
1. Replace single `consumer.recv()` with batch collection
2. Process multiple messages before hitting deserializer
3. Maintain existing format compatibility

**Phase 2: Selective Direct Building**
1. Identify common JSON schemas in benchmarks
2. Implement fast-path for known simple schemas
3. Fall back to deserializer for complex cases

**Phase 3: Custom Buffer Management**
1. Increase effective batch sizes beyond 512
2. Optimize Arrow batch construction timing
3. Reduce memory allocation overhead

**Expected Combined Impact**: 8-15x improvement, getting much closer to impulse performance while maintaining Kafka's flexibility and format support.

### Trade-offs

**Pros:**
- Significant throughput improvement
- Maintains format compatibility
- Preserves Kafka consumer semantics
- Incremental implementation possible

**Cons:**
- Increased code complexity
- Format-specific optimizations needed
- Higher memory usage during batching
- Potential latency increase for low-volume streams