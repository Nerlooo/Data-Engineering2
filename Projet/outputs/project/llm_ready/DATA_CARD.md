
# Data Card - NYC Yellow Taxi LLM-Ready Dataset

## Source
- **Original dataset:** NYC TLC Yellow Taxi Trip Records - January 2026
- **Raw file:** yellow_tripdata_2026-01.parquet (~64 MB Parquet)
- **Pipeline:** Bronze -> Silver -> LLM Prep (DE2_Project_Notebook_EN.ipynb)

## Size
- **Documents:** 2,497,173
- **Parquet size:** 350.46 MB
- **Partitions:** 4

## Schema
| Field    | Type   | Description |
|----------|--------|-------------|
| doc_id   | long   | Unique document identifier (monotonically increasing) |
| text     | string | Natural-language trip summary (~120-160 chars per doc) |
| metadata | string | JSON blob: pickup_month, zones, vendor_id, payment_type, fare, distance, duration, text_hash |

## Quality Filters Applied
| Filter | Threshold | Removed |
|--------|-----------|---------|
| Minimum text length | ≥ 50 characters | 0 docs |
| Language detection  | English marker words | 0 docs |
| Deduplication       | SHA-256 hash on text | 1,731 docs |
| **Total removed**   | | **1,731** (0.07%) |
| **Pass rate**       | SLO ≥ 80% | **99.93%** |

## Intended Use
- **LLM fine-tuning:** instruction-tuning on NYC taxi domain (fare estimation, trip Q&A)
- **RAG (Retrieval-Augmented Generation):** index trip summaries for semantic search
- **NOT intended for:** PII-sensitive applications (no passenger names/IDs present)

## Limitations
- Single month (January 2026) - seasonal bias possible
- Synthetic-style text (template-generated, not free-form human writing)
- No demographic or personal data included

## Licence
NYC TLC open data - public domain.
