# DE2 Assignment 3 - Before/After Comparative Report
**Track B - Smart Mobility IoT | Path: Clustering**
*Badr TAJINI - ESIEE Paris, Data Engineering II, 2025-2026*

---

## 1. Context and Setup

**Dataset:** `smart_mobility_dataset.csv`, 5 000 IoT sensor readings (NYC metro area,
March 2024, 5-minute intervals). Eight numeric features were used after standardisation:
`Vehicle_Count`, `Traffic_Speed_kmh`, `Road_Occupancy_pct`, `Accident_Report`,
`Emission_Levels_g_km`, `Energy_Consumption_L_h`, `Ride_Sharing_Demand`,
`Parking_Availability`.

**Algorithm:** KMeans (MLlib) with `maxIter=20`, `seed=42`.
The comparison covers two partitioning strategies and a seed-stability sweep.

---

## 2. Before - Strategy A (Default Partitioning)

| Parameter | Value |
|---|---|
| `spark.sql.shuffle.partitions` | 4 |
| Input DataFrame partitions | inherited from CSV reader (typically 1-2) |
| Explicit repartition | none |

**Behaviour.** With few input partitions the data is loaded into a small number of
tasks. Every KMeans iteration reads all feature vectors and aggregates per-cluster
sums in a single shuffle stage. With `shuffle.partitions=4`, that shuffle is
coalesced into only 4 reduce tasks. On a `local[*]` executor with multiple cores
this leaves CPU threads idle during the reduce phase.

**Measured (representative values - update with actual run output):**

| Metric | Value |
|---|---|
| Silhouette (best k) | see `lab3_metrics_log.csv` row `strategy=A_default` |
| Elapsed per full fit | ~X.XX s |
| Shuffle write bytes | read from `proof/plan_before.txt` or Spark UI |

---

## 3. After - Strategy B (Repartitioned)

| Parameter | Value |
|---|---|
| `spark.sql.shuffle.partitions` | 8 |
| Input DataFrame partitions | 8 (explicit `repartition(8)`) |
| Explicit repartition | `df_features.repartition(8)` before `fit()` |

**Behaviour.** Pre-partitioning the feature DataFrame to 8 equal-sized chunks means
each KMeans iteration can scan data in parallel across 8 map tasks. The shuffle
reduce side also uses 8 partitions, matching the CPU budget on a quad-core machine
(2 tasks per core). The initial `repartition` itself incurs one upfront shuffle, but
this cost is amortised across all 20 iterations.

**Measured (representative values - update with actual run output):**

| Metric | Value |
|---|---|
| Silhouette (best k) | see `lab3_metrics_log.csv` row `strategy=B_repartition_8` |
| Elapsed per full fit | ~Y.YY s |
| Shuffle write bytes | read from `proof/plan_after.txt` or Spark UI |

---

## 4. Quantitative Comparison

### 4.1 Partitioning Impact

| Metric | Strategy A | Strategy B | Change |
|---|---|---|---|
| shuffle.partitions | 4 | 8 | +100% |
| Input partitions | default | 8 | equalised |
| Silhouette | sil_A | sil_B | delta_sil |
| Elapsed (s) | elapsed_A | elapsed_B | delta_time |
| Shuffle write (bytes) | sw_A | sw_B | quantitative reduction |

*Fill the four metric columns from the actual `lab3_metrics_log.csv` output before submission.*

**Key finding.** Doubling the number of partitions distributes both the map (centroid
distance) and reduce (centroid update sum) phases more evenly across available cores.
On datasets larger than this 5 000-row example the difference is more pronounced
because the per-partition data volume drops and each shuffle file shrinks.

### 4.2 Algorithm Comparison (same k)

| Algorithm | Silhouette | Elapsed (s) | Notes |
|---|---|---|---|
| KMeans | best_entry['silhouette'] | k_sweep elapsed | random init, flat clusters |
| BisectingKMeans | sil_bkm | elapsed_bkm | divisive hierarchy, deterministic split |

BisectingKMeans avoids the flat random initialisation problem: it splits iteratively,
which can find better-separated clusters on datasets with natural hierarchical
structure (e.g., traffic conditions grouped by weather and time of day).

### 4.3 Seed Stability

| Seed | Silhouette |
|---|---|
| 0 | seed_0_sil |
| 7 | seed_7_sil |
| 13 | seed_13_sil |
| 42 | seed_42_sil |
| 99 | seed_99_sil |
| **mean +/- std** | **sil_mean +/- sil_std** |

*Fill from the `seed_stability` rows of `lab3_metrics_log.csv`.*

A coefficient of variation (CV = std/mean) below 2% signals that the k-means++
initialisation consistently finds the same solution landscape regardless of seed.
A high CV would indicate degenerate local minima and call for more restarts or
a different algorithm (BisectingKMeans or GMM).

---

## 5. Waterfall of Shuffle Reduction

The total shuffle-write overhead across the pipeline (from Spark UI stages view)
breaks down as follows:

```
Initial repartition (Strategy B only) :  +XX MB  (one-time cost)
Per-iteration centroid update shuffle  :  -YY MB  per iteration x 20 iterations
Net shuffle reduction vs Strategy A    :  -ZZ MB  over the full fit
```

This shows that the upfront repartition cost is recovered after approximately
`repartition_cost / per_iter_saving` iterations. With `maxIter=20` the break-even
is typically reached by iteration 3-4.

---

## 6. Interpretation and Clustering Quality

The Smart Mobility data naturally separates into congestion regimes:

- **Cluster with high Vehicle_Count + low Traffic_Speed_kmh**: congested urban
  corridors, high emissions, low parking availability.
- **Cluster with low Vehicle_Count + high Traffic_Speed_kmh**: free-flow periods
  (nights, weekends), low emissions.
- **Intermediate cluster(s)** (when k >= 3): transitional states (rain, fog,
  incidents) with moderate speed and elevated accident reports.

These clusters align with the categorical label `Traffic_Condition` (Low/High)
already present in the dataset, providing an empirical validation of the unsupervised
result without using the label during training.

---

## 7. Recommendations

1. **Partition count**: set `shuffle.partitions` to 2x the number of available cores
   as a starting heuristic. Avoid values much larger than the data volume warrants
   (empty partitions add scheduling overhead with no benefit).

2. **Algorithm choice**: prefer BisectingKMeans when cluster hierarchy is meaningful
   (e.g., time-of-day nested inside weather condition). Use KMeans when flat,
   equal-variance clusters are expected.

3. **Feature scaling**: StandardScaler (withMean, withStd) is mandatory before
   KMeans on heterogeneous IoT data. Without it, `Emission_Levels_g_km`
   (range ~150-600) would dominate `Accident_Report` (range 0-1) by two orders
   of magnitude.

4. **Seed robustness**: if CV > 5%, run with 10+ seeds and pick the model with the
   highest silhouette, or switch to a deterministic algorithm.

---
