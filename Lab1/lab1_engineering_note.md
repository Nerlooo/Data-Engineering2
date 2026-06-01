# DE2 Assignment 1 — Engineering Note
**Track B · Smart Mobility IoT Streaming Pipeline**
*Badr TAJINI — ESIEE Paris, Data Engineering II, 2025-2026*



## 1. Pipeline Design

```
smart_mobility_dataset.csv
        │  (pre-split into JSON-lines batches, 500 rows each)
        ▼
outputs/lab1/stream_source/   ← file source (JSON, schema-on-read)
        │
        │  Structured Streaming · local[*]
        ▼
┌───────────────────────────────────────────────────────────┐
│  withWatermark("Timestamp", "30 seconds")                 │
│  groupBy(window("Timestamp","1 min"), Traffic_Condition)  │
│  agg(count, avg(Vehicle_Count), avg(Speed),               │
│       sum(Accident_Report), avg(Emission), avg(Occupancy))│
└───────────────────────────────────────────────────────────┘
        │  outputMode = append  (finalised windows only)
        ▼
outputs/lab1/stream_sink/     ← Parquet, partitioned by Traffic_Condition
        │
        └── outputs/lab1/checkpoint/  ← exactly-once via WAL + offset log
```

**Source:** The 5 000-row CSV (5-min IoT sensor readings, NYC metro area, March 2024)
is split into 10 × 500-row JSON-lines files before the query starts. This faithfully
simulates incremental sensor ingestion without network dependencies.

**Trigger:** `processingTime` — Spark polls for new files at a fixed wall-clock
cadence rather than as fast as possible, giving predictable resource usage.

**Sink partitioning:** `partitionBy("Traffic_Condition")` separates `Low`/`High`
congestion records into distinct Parquet sub-directories, enabling predicate push-down
for downstream batch queries that filter on congestion level.



## 2. Watermark Reasoning

IoT sensors can report late due to network jitter, gateway buffering, or
clock skew. The watermark tells Spark's state store how long to keep a window
open waiting for stragglers before finalising and evicting it.

| Parameter | Value | Rationale |
|---|---|---|
| `WATERMARK_DELAY` | **30 s** | Sensor data arrives at 5-min intervals; a 30 s slack covers typical gateway delays without inflating state memory |
| `WINDOW_DURATION` | **1 min** | Groups two consecutive sensor readings per location, balancing granularity against output frequency |
| `outputMode` | **append** | Required when watermark + windowed aggregation are combined; a window row is emitted exactly once after `window_end + watermark_delay` has elapsed |

**Why not `update` mode?** `update` re-emits rows on every partial update and is
incompatible with Parquet sinks. `complete` would require maintaining the full
aggregation state indefinitely — unsuitable for an unbounded stream.

**Late-data boundary:** An event timestamped more than 30 s behind the current
watermark is silently dropped. Given the 5-min sensor cadence this is acceptable;
no reading arrives more than a few seconds late in practice.



## 3. Optimization Gains

Two configurations were compared using `lab1_metrics_log.csv`.

| Parameter | Baseline | Optimised | Change |
|---|---|---|---|
| `trigger` | 10 s | **5 s** | −50 % trigger latency |
| `maxFilesPerTrigger` | 2 | **4** | +100 % files/batch |
| `shuffle.partitions` | 4 | **8** | +100 % parallelism |

**Effect observed:**

* Halving the trigger interval reduces end-to-end latency: windows are
  finalised and written to Parquet sooner after their `window_end`.
* Doubling `maxFilesPerTrigger` increases `numInputRows` per micro-batch,
  raising `processedRowsPerSecond` and better utilising all available CPU cores.
* Doubling shuffle partitions eliminates the bottleneck at the `groupBy` shuffle
  stage when more files are processed per batch, preventing stragglers.

**Trade-off:** Higher `maxFilesPerTrigger` increases peak memory pressure per
executor. In production this should be tuned against executor heap size.
The 5 s trigger also increases Spark UI overhead (more frequent progress reports);
for low-volume sources a longer interval is preferable.