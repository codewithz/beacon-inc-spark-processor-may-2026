# Spark Structured Streaming

## What is Structured Streaming?

Structured Streaming is Spark's engine for processing **continuous, unbounded data** — data that never stops arriving. Instead of loading a fixed dataset and running a query once, Structured Streaming treats the incoming data stream as a table that keeps growing, and runs your query continuously as new rows arrive.

The key insight: **you write the same DataFrame/SQL code you already know. Spark handles the streaming mechanics underneath.**

---

## The Mental Model — Unbounded Table

Think of your stream as an infinite table being appended to over time.

```
Time →

t=1  [ row1 ]
t=2  [ row1 | row2 ]
t=3  [ row1 | row2 | row3 ]
t=4  [ row1 | row2 | row3 | row4 ]
       ...keeps growing forever...
```

Every time Spark wakes up (on a trigger), it sees the new rows that arrived since the last run and processes them — this is called a **micro-batch**.

---

## Core Concepts

### Source
Where data comes in. Common sources:

| Source | Description |
|---|---|
| **Kafka** | Most common in production — topics as streams |
| **File** | Watches a directory for new files (CSV, JSON, Parquet) |
| **Socket** | TCP socket — for local testing only |
| **Rate** | Generates rows at a fixed rate — for benchmarking |

### Trigger
How often Spark wakes up to process new data.

| Trigger | Behaviour |
|---|---|
| `ProcessingTime("10 seconds")` | Micro-batch every 10 seconds |
| `Once()` | Process everything available, then stop |
| `Continuous("1 second")` | True millisecond-latency streaming (experimental) |
| *(none set)* | Run as fast as possible |

### Output Mode
What Spark writes to the sink in each batch.

| Mode | Writes | Use when |
|---|---|---|
| `append` | Only new rows | No aggregations — most common |
| `update` | Only rows that changed | Aggregations with running totals |
| `complete` | Entire result table every batch | Small aggregations you want fully refreshed |

### Sink
Where processed data goes.

| Sink | Description |
|---|---|
| **Console** | Prints to terminal — development only |
| **Kafka** | Write back to another Kafka topic |
| **Parquet / Delta / Hudi** | Write to a data lake |
| **JDBC** | Write to a relational database |
| **Memory** | In-memory table — testing only |

---

## How a Streaming Query is Built

Every Structured Streaming job follows the same three-step pattern:

```
readStream()  →  transform()  →  writeStream()
   (source)      (your logic)      (sink)
```

### Step 1 — Read from the source

```java
Dataset<Row> raw = spark.readStream()
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "beacon.property.transactions")
    .option("startingOffsets", "earliest")
    .load();
```

### Step 2 — Transform (same API as batch)

```java
Dataset<Row> parsed = raw
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .filter(col("transactionAmount").gt(1_000_000));
```

### Step 3 — Write to the sink

```java
StreamingQuery query = parsed.writeStream()
    .format("console")
    .outputMode("append")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start();

query.awaitTermination();  // keeps the job alive
```

---

## What Happens Inside Each Micro-Batch

```
Trigger fires (every 10s)
        │
        ▼
Spark checks source for new data
        │
        ▼
New rows found → process them through your transform logic
        │
        ▼
Write results to sink
        │
        ▼
Commit offset (mark "how far we've read")
        │
        ▼
Sleep until next trigger
```

The **offset** is the bookmark. If your job crashes and restarts, Spark reads the last committed offset and resumes from exactly where it left off — no data loss, no duplicates.

---

## Fault Tolerance — Checkpointing

Checkpointing is what makes Structured Streaming reliable. It saves the job's progress to disk so it can recover after a failure.

```java
Dataset<Row> parsed.writeStream()
    .format("parquet")
    .option("checkpointLocation", "/tmp/checkpoint/transactions")  // ← required for production
    .outputMode("append")
    .start();
```

**Without a checkpoint location**, if your Spark job crashes, it starts reading from the beginning of the topic again — duplicate processing. Always set it in production.

---

## Kafka-Specific Behaviour

When reading from Kafka, Spark gives you these columns automatically:

| Column | Type | Description |
|---|---|---|
| `key` | binary | Kafka message key |
| `value` | binary | The actual payload — cast to String and parse |
| `topic` | string | Topic name |
| `partition` | int | Kafka partition number |
| `offset` | long | Position within the partition |
| `timestamp` | timestamp | When the message was produced |

The `value` column is always raw bytes. The first thing you always do:

```java
col("value").cast("string")           // bytes → JSON string
from_json(col("value").cast("string"), schema)  // JSON string → typed columns
```

---

## startingOffsets

Controls where Spark starts reading when the job runs for the **first time**:

```java
.option("startingOffsets", "earliest")  // read everything in the topic from the beginning
.option("startingOffsets", "latest")    // only read messages that arrive after the job starts
```

After the first run, the checkpoint takes over and Spark always resumes from where it stopped — `startingOffsets` is ignored on restarts.

---

## Stateful vs Stateless Operations

### Stateless — no memory between batches
Each batch is processed independently. Examples: `filter`, `select`, `map`, `from_json`. This is what our beacon pipeline does — parse and forward each transaction as it arrives.

### Stateful — memory across batches
Spark keeps running state across micro-batches. Examples:

- **Window aggregations** — count transactions per area in the last 5 minutes
- **Watermarking** — handle late-arriving data gracefully
- **Stream-stream joins** — join two streams together

```java
// Example: count transactions per area in a 5-minute sliding window
parsed
    .withWatermark("transactionDate", "10 minutes")   // tolerate 10 min late data
    .groupBy(
        window(col("transactionDate"), "5 minutes"),
        col("area")
    )
    .count()
```

---

## Structured Streaming vs Spark Batch — at a Glance

| | Batch | Structured Streaming |
|---|---|---|
| Data | Bounded (has an end) | Unbounded (never ends) |
| Trigger | Run once manually | Runs continuously |
| Read API | `spark.read()` | `spark.readStream()` |
| Write API | `df.write()` | `df.writeStream()` |
| Output | File / table once | Continuous sink updates |
| Fault tolerance | Re-run the job | Checkpoint + offset tracking |
| Same transform code? | ✅ Yes | ✅ Yes |

---

## In Our Beacon Pipeline

```
Corelate Emulator
      │  POST /api/transactions (every 60s)
      ▼
beacon-ingestion (Spring Boot)
      │  KafkaTemplate.send()
      ▼
Kafka Topic: beacon.property.transactions
      │  spark.readStream().format("kafka")
      ▼
beacon-spark-consumer (Spark Structured Streaming)
      │  parse JSON → typed DataFrame
      ▼
Console (Phase 1) → Hudi (Phase 2)
```

Spark wakes up every 10 seconds, reads whatever transactions arrived in Kafka since the last batch, parses the JSON, and writes them out. The emulator fires every 60 seconds, so roughly every other Spark batch will have data and the rest will be empty — that is expected behaviour.