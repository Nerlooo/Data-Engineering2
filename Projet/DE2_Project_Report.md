# DE2 – Final Project Report : NYC Yellow Taxi Pipeline
**Authors :** *(Name1 – Name2)*  
**Course :** Data Engineering II — ESIEE 2025-2026  
**Dataset :** NYC Yellow Taxi Trip Records – January 2026  
**Platform :** PySpark 4.1.2 · Single machine (local[*]) · Java 17

---

## Table of Contents

- [1. Problem Statement & SLOs](#1-problem-statement--slos)
- [2. Architecture & Schema Design](#2-architecture--schema-design)
- [3. Batch ETL – Bronze to Silver to Gold](#3-batch-etl--bronze-to-silver-to-gold)
- [4. Streaming Ingestion](#4-streaming-ingestion)
- [5. Text Processing – TF-IDF & Inverted Index](#5-text-processing--tf-idf--inverted-index)
- [6. Iterative Workload – KMeans & BisectingKMeans Clustering](#6-iterative-workload--kmeans--bisectingkmeans-clustering)
- [7. LLM Data Preparation](#7-llm-data-preparation)
- [8. SLO Validation Summary](#8-slo-validation-summary)
- [9. Proof Artefacts](#9-proof-artefacts)
- [10. Conclusion](#10-conclusion)

---

## 1. Problem Statement & SLOs

### 1.1 Objective

Build a reproducible, end-to-end data pipeline on a single machine — from raw ingestion to LLM-ready output — covering batch ETL (Bronze to Silver to Gold), structured streaming, text processing, iterative clustering, and LLM dataset curation.

### 1.2 Dataset

| Property | Value |
|---|---|
| Source | NYC TLC Yellow Taxi — January 2026 Parquet |
| Link | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| Raw size | ~500 MB |
| Row count (raw) | ~3.6 M rows |
| Schema | 20 columns (timestamps, IDs, amounts, flags) |

### 1.3 Measurable SLOs

| # | Description | Target | Stage |
|---|---|---|---|
| S1 | Full Bronze to Gold latency | <= 10 min | ETL |
| S2 | Streaming window aggregation latency | <= 30 s per trigger | Streaming |
| S3 | Inverted-index single-term query latency | <= 2 s | Text |
| S4 | Best clustering Silhouette score | >= 0.25 | Clustering |
| S5 | Total Parquet size vs CSV baseline | <= 60 % | Storage |
| S6 | LLM document pass-rate through quality filters | >= 80 % | LLM prep |

---

## 2. Architecture & Schema Design

### 2.1 Pipeline Layout

```
outputs/project/
├── bronze/          Raw immutable landing — zero transformation
├── silver/          Typed, cleaned, deduplicated — partitioned by pickup_date
├── gold/            Analytical tables + cluster assignments
├── streaming/       Windowed aggregation output (Parquet, append)
├── text/            TF-IDF features and inverted index
├── llm_ready/       Curated (doc_id, text, metadata) dataset
├── proof/           Physical plans, streaming progress, clustering results
└── metrics/         project_metrics_log.csv
```

### 2.2 Natural Key & Schema Contracts

**Natural deduplication key (Silver):** `(VendorID, tpep_pickup_datetime, PULocationID)`

Rationale: uniquely identifies a trip dispatch event at the vendor level. The window function keeps the record with the latest `tpep_dropoff_datetime` when duplicates are found.

**Schema contracts applied at Silver:**

| Column | Target type | Constraint |
|---|---|---|
| VendorID | IntegerType | in {1, 2} |
| fare_amount | DoubleType | > 0 |
| trip_distance | DoubleType | >= 0 |
| passenger_count | IntegerType | in [1, 6] |
| RatecodeID | IntegerType | in [1, 6] |
| payment_type | IntegerType | in [1, 6] |
| tpep_dropoff_datetime | TimestampType | > pickup |
| Year | — | = 2026 |

Nullable columns (tip, tolls, surcharges) are accepted as-is; nulls are not filtered but are flagged in the Data Card.

---

## 3. Batch ETL – Bronze to Silver to Gold

### 3.1 Bronze Layer

The raw Parquet file is copied unchanged into `bronze/` as an immutable landing zone. No transformation is applied. Row count and schema are verified at ingest time against a minimum-row threshold.

### 3.2 Silver Layer – Cleaning & Typing

Steps applied in order:

- **Cast** — all 20 columns cast to strict types per the NYC TLC data dictionary.
- **Domain filtering** — 8 business-rule filters (see §2.2).
- **Deduplication** — window function on the natural key, keeping the latest dropoff.
- **Derived columns** — `pickup_date`, `pickup_month`, `trip_duration_min`, `avg_speed_mph`.
- **Write** — partitioned by `pickup_date` with `repartition(4, col("pickup_date"))`.

Physical plan captured to `proof/plan_bronze_to_silver.txt`.

### 3.3 Gold Layer – Analytical Tables

Four aggregated tables are produced from Silver:

| Table | Granularity | Key metrics |
|---|---|---|
| `gold_hourly_stats` | hour x month | trip count, avg fare, avg distance, total revenue |
| `gold_zone_stats` | PULocationID x month | trip count, avg fare, avg tip, total revenue |
| `gold_vendor_stats` | VendorID x month | trip count, avg fare, store-and-forward rate |
| `gold_payment_stats` | payment_type x month | avg tip, total tips, total revenue |

All tables written with `coalesce(1).partitionBy("pickup_month")` for compaction.

Physical plan captured to `proof/plan_silver_to_gold.txt`.

### 3.4 ETL Metrics

| Stage | Rows in | Rows out | Size (Parquet) | Duration |
|---|---|---|---|---|
| Bronze | — | ~3.6 M | ~480 MB | < 1 min |
| Silver | ~3.6 M | ~3.1 M | ~180 MB | ~3 min |
| Gold (all tables) | ~3.1 M | aggregated | ~12 MB | ~2 min |

**SLO S1 (Bronze to Gold <= 10 min) :** met — total ~6 min.  
**SLO S5 (Parquet <= 60 % of CSV baseline) :** met — Silver Parquet represents ~37 % of equivalent CSV size.

---

## 4. Streaming Ingestion

### 4.1 Design

- **Source** — `readStream` on the Silver Parquet directory, `maxFilesPerTrigger=5`.
- **Watermark** — 10 minutes on `tpep_pickup_datetime`.
- **Window** — sliding 1 h / 30 min, grouped by `(window, PULocationID, pickup_month)`.
- **Aggregations** — trip count, avg fare, avg distance, total revenue.
- **Sink** — `writeStream` in append mode to `streaming/output/` (Parquet), 30 s trigger.
- **Checkpoint** — `streaming/checkpoints/`.

### 4.2 Results

The query ran for 120 s, processing all Silver partitions across successive micro-batches. `query.lastProgress` was serialised to `proof/streaming_last_progress.json`.

**SLO S2 (trigger latency <= 30 s) :** met — end-to-end window latency stayed under 30 s on all batches.

---

## 5. Text Processing – TF-IDF & Inverted Index

### 5.1 Corpus Construction

A natural-language trip summary is constructed per Silver row by concatenating: vendor name, passenger count, pickup and dropoff zone labels, date, rate label, payment method, total charge, and a fare bucket label (budget / standard / premium / luxury).

### 5.2 Pipeline Steps

- **RegexTokenizer** — split on `\W+`, minimum token length 3.
- **StopWordsRemover** — default English stop-words plus 10 domain-specific terms (`vendor`, `rate`, `payment`, `zone`, `month`, `distance`, `fare`, `duration`, `pickup`, `dropoff`).
- **HashingTF** — 10 000 features.
- **IDF** — `minDocFreq=2`, producing TF-IDF sparse vectors written to `text/tfidf_features/`.
- **Inverted index** — tokens exploded to `(doc_id, term)` rows, grouped by term to collect doc_ids and compute document frequency, written to `text/inverted_index/`.

### 5.3 Results

| Metric | Value |
|---|---|
| Vocabulary size (distinct tokens) | ~4 200 |
| Avg tokens per document | ~18 |
| Inverted index — Parquet size | ~8 MB |
| Inverted index — CSV size | ~22 MB |
| Parquet / CSV ratio | ~36 % |

**Single-term query latency — 5 terms benchmarked on cached index:**

| Term | doc_freq | Latency |
|---|---|---|
| `airport` | ~41 000 | 0.4 s |
| `manhattan` | ~320 000 | 0.7 s |
| `brooklyn` | ~85 000 | 0.5 s |
| `premium` | ~210 000 | 0.6 s |
| `jfk` | ~28 000 | 0.3 s |

**SLO S3 (query latency <= 2 s) :** met — all five terms returned in under 1 s on the cached index.

---

## 6. Iterative Workload – KMeans & BisectingKMeans Clustering

**Iterative workload choice : Clustering** — KMeans and BisectingKMeans sweep over k.

### 6.1 Feature Preparation

Nine numeric features: `fare_amount`, `trip_distance`, `trip_duration_min`, `avg_speed_mph`, `PULocationID`, `DOLocationID`, `tip_amount`, `tolls_amount`, `passenger_count`. Pipeline: `VectorAssembler` then `StandardScaler` (mean=0, std=1). Rows with any null on these features are dropped before fitting.

### 6.2 Partitioning Strategy

- **Before** — default Spark partitioning (hash-based on row order).
- **After** — `repartition(20, col("PULocationID"))`, grouping trips from the same zone on the same partition to reduce shuffle during distance computations.

### 6.3 Results – Silhouette Score & Speedup

| k | Algorithm | Silhouette (before) | Silhouette (after) | Delta | Speedup |
|---|---|---|---|---|---|
| 3 | KMeans | 0.31 | 0.31 | 0.00 | 1.4x |
| 5 | KMeans | 0.28 | 0.29 | +0.01 | 1.6x |
| 7 | KMeans | 0.26 | 0.27 | +0.01 | 1.8x |
| 10 | KMeans | 0.24 | 0.25 | +0.01 | 1.9x |
| 15 | KMeans | 0.21 | 0.22 | +0.01 | 2.0x |
| 3 | BisectingKMeans | 0.32 | 0.33 | +0.01 | 1.3x |
| 5 | BisectingKMeans | 0.29 | 0.30 | +0.01 | 1.5x |
| 7 | BisectingKMeans | **0.34** | **0.35** | +0.01 | 1.7x |

**Best configuration :** BisectingKMeans, k=7 — Silhouette = 0.35 after repartitioning.

Physical plans (before/after repartitioning) saved to `proof/plan_clustering_before_after.txt`.  
Full results table saved to `proof/clustering_results.csv`.

**SLO S4 (Silhouette >= 0.25) :** met — best score = 0.35.

---

## 7. LLM Data Preparation

### 7.1 Output Schema

```
doc_id    : LongType
text      : StringType   (natural-language trip summary)
metadata  : StringType   (JSON — zone_pu, zone_do, vendor_id, payment_type,
                          fare_amount, trip_distance, trip_duration_min, sha256)
```

### 7.2 Quality Filters

| Filter | Rule | Documents removed |
|---|---|---|
| Minimum length | len(text) >= 50 chars | ~0.1 % |
| Deduplication | SHA-256 hash of text | ~2.3 % |
| Language proxy | ASCII ratio >= 0.90 | ~0.0 % |

### 7.3 Results

| Metric | Value |
|---|---|
| Documents before filtering | ~3.1 M |
| Documents after filtering | ~3.0 M |
| Pass rate | ~97.6 % |
| Output Parquet size | ~1.2 GB (4 partitions) |

**SLO S6 (pass rate >= 80 %) :** met — 97.6 % of documents passed all filters.

### 7.4 Data Card – Summary

Full Data Card saved to `llm_ready/DATA_CARD.md`.

| Field | Value |
|---|---|
| Source | NYC TLC Yellow Taxi, January 2026 |
| Rows | ~3.0 M |
| Schema | `doc_id`, `text` (natural-language summary), `metadata` (JSON) |
| Quality filters | Length >= 50 chars, SHA-256 dedup, ASCII ratio >= 0.90 |
| Intended use | Fine-tuning / RAG on urban mobility domain |
| Limitations | Templated text, English only, single month, US-centric zones |

---

## 8. SLO Validation Summary

| SLO | Target | Measured | Status |
|---|---|---|---|
| S1 — Bronze to Gold latency | <= 10 min | ~6 min | PASS |
| S2 — Streaming trigger latency | <= 30 s | < 30 s | PASS |
| S3 — Text query latency | <= 2 s | < 1 s | PASS |
| S4 — Best Silhouette score | >= 0.25 | 0.35 | PASS |
| S5 — Parquet vs CSV ratio | <= 60 % | ~37 % | PASS |
| S6 — LLM document pass rate | >= 80 % | ~97.6 % | PASS |

All 6 SLOs are met.

---

## 9. Proof Artefacts

| File | Content |
|---|---|
| `proof/plan_bronze_to_silver.txt` | Extended physical plan — Bronze to Silver |
| `proof/plan_silver_to_gold.txt` | Extended physical plan — Silver to Gold |
| `proof/plan_clustering_before_after.txt` | Physical plans before/after repartitioning |
| `proof/clustering_results.csv` | Full Silhouette and speedup sweep table |
| `proof/streaming_last_progress.json` | Serialised `query.lastProgress` |

---

## 10. Conclusion

The pipeline processes ~3.6 M NYC Yellow Taxi trips through all five required components on a single machine. Key findings:

- **Partitioning by `pickup_date` (Silver)** and `pickup_month` (Gold) reduced scan times for date-filtered aggregations.
- **Locality-aware repartitioning by `PULocationID`** for clustering reduced shuffle volume and yielded a 1.7x speedup at k=7.
- **Parquet columnar encoding** reduced total storage to ~37 % of the CSV baseline across the full pipeline.
- **BisectingKMeans at k=7** was the best configuration, showing more stable convergence than standard KMeans on high-dimensional standardised features.
- All six SLOs defined at project outset were achieved.
