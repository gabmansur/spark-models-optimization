# Spark Optimization: From Messy Legacy to Measurable Wins

![Python](https://img.shields.io/badge/Python-3.10-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5.x-orange)
![Local%20Mode](https://img.shields.io/badge/Run-Local%20Mode-success)

This portfolio project simulates a real-world Spark environment with skewed data and thousands of small files, then shows how to diagnose, optimize, and prove performance wins using p95 runtime, cost estimates, and reliability basics. It’s designed to mirror what I’d do in a 6-month engagement on a 60–70 model platform.

### Executive Snapshot
- p95 runtime: **60 min → 30 min** (simulated)
- Estimated cost/run: **€48 → €26**
- Reliability: documented runbook, basic SLOs, MTTR tracking stub


## 📖 Table of Contents
1. [Backstory & Context](#1-backstory--context)  
2. [Key Concepts (Plain Words)](#2-key-concepts-plain-words)  
3. [What’s in Here](#3-whats-in-here)  
    - [3.1 Baseline vs Optimized (At a Glance)](#31-baseline-vs-optimized-at-a-glance)
    - [3.2 How to Compare (Quick Checklist)](#32-how-to-compare-quick-checklist)
4. [How to Run (Local, No Cluster Needed)](#4-how-to-run-local-no-cluster-needed)  
5. [Diagnosis → Optimization: My Step-by-Step](#5-diagnosis--optimization-my-step-by-step)  
   - [A. Inventory & Baseline](#a-inventory--baseline)  
   - [B. Lineage (Mini)](#b-lineage-mini)  
   - [C. Optimization & Refactoring](#c-optimization--refactoring)  
   - [D. Validation & Results](#d-validation--results)  
6. [What Changes Between Baseline and Optimized](#6-what-changes-between-baseline-and-optimized)  
7. [Metrics & Reporting](#7-metrics--reporting)  
8. [Runbook (Example)](#8-runbook-example)  
9. [How This Scales to a 6-Month Engagement](#9-how-this-scales-to-a-6-month-engagement)  
10. [Next Steps / Ideas to Extend](#10-next-steps--ideas-to-extend)  
11. [License & Credits](#11-license--credits)  
12. [Why This Matters](#why-this-matters)


## 1. Backstory & Context

**The scenario:**
A data platform grew over 7 years to ~60–70 PySpark models. Some of the most important jobs are slow, costly, and fragile. Adding new models takes too long. Leadership wants faster jobs, lower costs, and a safer way to build new ones.

**This repo:**
I recreate a mini slice of that world, one representative pipeline (“daily_sales”) with dummy data that intentionally includes skew and many small files (classic Spark pain). I then run a baseline job (naïve/inefficient) and an optimized job (best practices), and I log metrics so we can compare **Before → After** clearly.

**What I’m demonstrating:**

* How I inventory, baseline, and diagnose performance issues
* A repeatable Spark optimization playbook (skew fixes, broadcast, partitioning/compaction, AQE, UDF hygiene)
* How I measure and report improvements with p95 runtime cost/run success rate, and basic MTTR
* A small taste of lineage data contracts runbooks and observability

**Assumptions & Constraints**
- Simulated in local mode with generated data; numbers are illustrative.
- Optimizations focus on Spark SQL/DataFrame patterns, not cluster sizing.
- Cost = duration × hourly rate; in real programs, integrate billing APIs.

**Environment**
- Python 3.10  
- PySpark 3.5.x  
- Pandas / Matplotlib for reporting

**Data scale (configurable)**
- customers: 100k  
- transactions: 20–50M (skew injected on `customer_id = 0`)  
- seed: 42

> 💡 I’m simulating a company’s data system that became bloated and slow. This project shows how I’d diagnose performance issues, fix them, and prove the difference, like tuning up an old machine until it runs smoothly again.


## 2. Key Concepts (Plain Words)

* **p95 runtime:** the “slow-day line” 95% of runs are **faster** than this number. If p95 drops from 60m to 30m, your worst normal days got twice as fast
* **SLO:** our goal for service (e.g., “freshness ≤ 60 min 99% of the time”).
* **SLO breach:** when we miss that goal.
* **MTTR (Mean Time To Repair):** average time to fix a job after it breaks.
* **Small files problem:** too many tiny files = slow Spark. We compact to fewer, larger files.
* **Skew:** one key has way more rows → one task slows everything. We salt / split the heavy key to balance work.
* **Broadcast join:** send the small table to all workers to avoid huge shuffles.
* **AQE:** Spark auto-tunes parts of the query while it runs.

> 💡 These are the key ingredients of performance. p95 is your “slowest normal day,” MTTR measures how quickly things recover, and AQE is Spark’s auto-pilot mode for optimizing jobs mid-flight.


## 3. What’s in Here

```
spark-models-optimization/
  data/                 # generated dummy data (skew + many small files)  ❌ gitignored
  ops/                  # run logs, p50/p95 report, chart                 ❌ gitignored
  src/
    jobs/
      generate_data.py  # builds customers + skewed transactions
      job_baseline.py   # naive/slow version to create a baseline
      job_optimized.py  # tuned version: AQE, broadcast, salting, partitioned writes
    utils/
      common.py         # SparkSession builder + path helpers
      metrics.py        # run logger + p50/p95 report generator
  conf/
    spark_local.conf    # local defaults (AQE on, skewJoin on)
  README.md
  requirements.txt
  Makefile
```

note: /data and /ops are reproducible outputs and are excluded via .gitignore.

### 3.1. Baseline vs Optimized (At a Glance)
| **Aspect**                         | **Baseline (naïve)**                    | **Optimized (tuned)**                                       |
| ---------------------------------- | --------------------------------------- | ----------------------------------------------------------- |
| **File**                           | `src/jobs/job_baseline.py`              | `src/jobs/job_optimized.py`                                 |
| **Join strategy**                  | Standard join (both sides shuffle)      | **Broadcast** small dimension → local joins                 |
| **Data skew**                      | None handled → hot key stalls one task  | **Salting** + **AQE skew handling** redistributes load      |
| **Adaptive Query Execution (AQE)** | Disabled / defaults only                | **Enabled** (`spark.sql.adaptive.enabled=true`)             |
| **Transformations**                | May use Python UDFs (slow, unoptimized) | Uses **native Spark expressions** (`when`, `col`, `expr`)   |
| **Output write**                   | Random partitions → many small files    | **Partitioned by `event_date`**, compacted into fewer files |
| **Metrics & observability**        | Only total runtime logged               | **Runtime, cost/run, p50/p95**, and success status logged   |


> 💡 Both jobs perform the same logic; one simulates legacy inefficiency, the other applies engineering best practices.

### 3.2. How to Compare (Quick Checklist)

1. Generate data
```python
python src/jobs/generate_data.py
```

2. Run the baseline job (2–3× for p95 accuracy)
```python
python src/jobs/job_baseline.py
```

3. Run the optimized job (2–3× again)
```python
python src/jobs/job_optimized.py
```

4. Build the report
```python
python src/utils/metrics.py report
```

5. Review the results

- ops/report.md → p50/p95 runtime + before/after comparison
- ops/p95_chart.png → visual runtime reduction
- optional: open Spark UI at http://localhost:4040
 during runs
 
 > 💡 You’ll clearly see the difference: fewer shuffles, balanced tasks, faster writes, and measurable runtime/cost reduction.

## 4) How to Run (Local, No Cluster Needed)

```bash
# 1) Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2) Generate dummy data (skew + many small files)
python src/jobs/generate_data.py

# 3) Run the slow baseline job (logs runtime to ops/job_runs.csv)
python src/jobs/job_baseline.py

# 4) Run the optimized job (logs runtime too)
python src/jobs/job_optimized.py

# (Optional) Re-run each job a few times so p95 is meaningful
python src/jobs/job_baseline.py
python src/jobs/job_optimized.py

# 5) Build the report (p50/p95 + Before→After chart)
python src/utils/metrics.py report
```

Results:

* `ops/report.md` - p50/p95 per job + **Before→After** table
* `ops/p95_chart.png` - bar chart to drop into your deck

**Make targets (optional)**
- `make venv && make install`  
- `make data`  # generate skew + many small files
- `make baseline && make optimized`
- `make report`  # build p50/p95 + chart

>💡 Just set up Python, generate the data, run both versions of the job, and produce a performance report, no cluster or special hardware needed.



## 5. Diagnosis → Optimization: My Step-by-Step

### A. Inventory & Baseline

This section is about understanding what exists and how it behaves before changing anything. You document, measure, and validate the current state so you can later prove real improvement. 

#### A1. Inventory models
Document every model’s purpose, owner, inputs, outputs, and schedule.

<details>
<summary>💡 Quick insight</summary>

**What it means:**  
Think of it as a simplified model registry, even if it’s just one job in this simulation.  

**Why it matters:**  
You need visibility and ownership before you can optimize anything.  
This prevents “orphaned” or mystery jobs and gives clarity on what exists and why.  

**How it’s done:**  
Create a simple CSV (`ops/models_registry.csv`) listing:  
- model_name  
- owner  
- description/purpose  
- inputs  
- outputs  
- schedule  
- tier/priority  

```python
# Example: create a model registry
import csv

models = [
    {"model_name": "spark_model_optimizer", "owner": "Gabi", "description": "Simulated PySpark job for optimization demo", "inputs": "transactions.parquet", "outputs": "optimized_output.parquet", "schedule": "daily", "tier": "gold"},
]

with open("ops/models_registry.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=models[0].keys())
    writer.writeheader()
    writer.writerows(models)
  ```

</details>

#### A2. Baseline
Log each run’s start/end time and status: compute p50/p95 runtime.

<details> <summary>💡 Quick insight</summary>

**What it means:**
Record start/end times for each run and compute the median (p50) and slowest (p95) runtimes.
This becomes your benchmark for future optimizations.

**Why it matters:**
It provides objective evidence of progress and helps detect outliers or regressions later.

**How it’s done:**
Log every run’s timestamps and durations in a CSV file (ops/job_runs.csv).

```python
import csv, datetime

run = {
    "job_name": "spark_model_optimizer",
    "start_time": datetime.datetime.now(),
    "end_time": datetime.datetime.now() + datetime.timedelta(seconds=180),
    "duration_sec": 180,
    "status": "success"
}

with open("ops/job_runs.csv", "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=run.keys())
    if f.tell() == 0:
        writer.writeheader()
    writer.writerow(run)
```
</details>



#### A3. Cost per run
Estimate compute cost from runtime × hourly rate (simple but honest).

<details> <summary>💡 Quick insight</summary>

**What it means:**
Convert job runtime into an estimated compute cost (runtime × hourly cluster rate).

**Why it matters:**
- Translates technical performance into business impact.
- Faster jobs = lower compute cost, which makes results easier to justify to management.

**How it’s done:**
Use the recorded runtimes and multiply by an hourly cost (for example, €2.50/hr).

```python
import pandas as pd

runs = pd.read_csv("ops/job_runs.csv")
runs["cluster_cost_eur_hr"] = 2.50
runs["cost_eur"] = (runs["duration_sec"] / 3600) * runs["cluster_cost_eur_hr"]
runs.to_csv("ops/job_runs.csv", index=False)
```
</details>



#### A4. Quality snapshot (optional here)
Row counts, nulls, key uniqueness for outputs.

<details> <summary>💡 Quick insight</summary>

**What it means:**
Capture a quick overview of data health: row counts, null values, and key uniqueness.

**Why it matters:**
- Performance means nothing if your outputs are broken.
- Data quality validates that your optimized job still produces correct results.

**How it’s done:**
Run lightweight checks after each job and log them to a small ops/quality_snapshot.csv.

```python
from pyspark.sql.functions import col, count, countDistinct

quality = df.agg(
    count("*").alias("row_count"),
    *[count(when(col(c).isNull(), c)).alias(f"{c}_nulls") for c in df.columns]
)

unique_keys = df.select(countDistinct("id").alias("unique_id_count"))
quality.show()
unique_keys.show()
```

</details>


### B. Lineage (Mini)

This section ensures that every input and output is traceable. It’s a light lineage layer that records what depends on what, so later optimizations don’t break hidden links.

#### B1. Record Sources & Targets
Wrap reads and writes with helper functions to log where data comes from and goes to.

<details>
<summary>💡 Quick insight</summary>

**What it means:**  
Each Spark read/write adds a small metadata record to `ops/model_lineage.csv`.  

**Why it matters:**  
You build a living map of your data flow — even simple jobs benefit from that transparency.  

**Lineage schema (`ops/model_lineage.csv`):**
- timestamp
- action (read|write)
- model_name
- path
- version (optional)

**How it’s done:**  
Simulate by wrapping reads/writes:

```python
def tracked_read(path):
    log_lineage("read", path)
    return spark.read.parquet(path)

def tracked_write(df, path):
    log_lineage("write", path)
    df.write.parquet(path)
```
</details>

### C. Optimization & Refactoring

Apply measurable, repeatable improvements to Spark jobs, focusing on runtime, cost, and reliability. Each optimization here is treated as a reproducible engineering pattern: explain → apply → verify → watch the impact.



#### C1. Fix Data Skew with Key Salting
When one key (like `customer_id=0`) dominates, one Spark task gets all the load.

<details>
<summary>💡 Quick Insight</summary>

**What it means:**  
Skew = one value appears disproportionately more than others (e.g., `customer_id=0` 20% of all rows). Spark distributes work by key; that one key becomes a bottleneck.

**Why it matters:**  
Spark can’t finish until every task finishes. One skewed task can keep the cluster idle and waste compute time.

**How it’s fixed here:**  
We add a small random “salt” column for the hot key only, splitting `customer_id=0` into 16 virtual IDs (`0_0` to `0_15`). Then we replicate that customer in the dimension 16 times so joins still match.

**Verification:**  
- In Spark UI: tasks in the join stage have more even durations.  
- p95 runtime (worst-day runtime) drops sharply.  

**Gotchas:**  
- Don’t salt everything, only hot keys. Too much salting = overhead.  
- Tune `SALT_N` (8–32) depending on skew severity.

</details>


#### C2. Enable Adaptive Query Execution (AQE)
Let Spark adapt its plan based on *real* runtime statistics.

<details>
<summary>💡 Quick Insight</summary>

**What it means:**  
Without AQE, Spark guesses partition sizes before running. With AQE (`spark.sql.adaptive.enabled=true`), it monitors data sizes during execution and adapts dynamically, merging small partitions, adjusting joins, and handling skew automatically.

**Why it matters:**  
Reduces shuffle overhead, balances tasks, and improves reliability across all workloads, especially long-running ones.

**How we use it:**  
Enabled in `spark_local.conf` and confirmed in each job (`spark.sql.adaptive.skewJoin.enabled=true`).

**Verification:**  
- Spark UI shows “AdaptiveSparkPlan.”  
- Fewer shuffle stages, shorter runtimes.

**Gotchas:**  
- It’s not magic; still need to fix extreme skews and poor partitioning.

</details>


#### C3. Broadcast Small Dimensions
Avoid expensive shuffles by shipping the small table to every worker.

<details>
<summary>💡 Quick Insight</summary>

**What it means:**  
In a join between a big fact table and a small dimension table, Spark normally shuffles both across the cluster - expensive!  
Broadcasting copies the small table to every worker, so each worker joins locally.

**Why it matters:**  
Removes huge network traffic and disk shuffle costs. Often reduces job runtime by 50–80% for small-dimension joins.

**How we use it:**  
`joined = fact.join(broadcast(dim), "key", "left")`

**Verification:**  
- Spark UI: Shuffle Read/Write metrics drop.  
- Runtime improves.  

**Gotchas:**  
- Only safe when the dimension fits comfortably in memory (<200MB typical rule).

</details>


#### C4. Compact Small Files + Partition Outputs
Reduce metadata overhead and speed up reads by writing fewer, larger files.

<details>
<summary>💡 Quick Insight</summary>

**What it means:**  
Spark writes one file per task. Over-partitioning = thousands of tiny files (bad for metadata, IO).  
We repartition the output by `event_date` and reduce partition count before writing.

**Why it matters:**  
Downstream jobs read faster, scanning only the relevant partitions.

**How we use it:**  
```python
out.repartition("event_date").write.partitionBy("event_date")
```

**Verification:**
- Fewer files under each date folder.
- Average file size ≈ 128MB.

**Gotchas:**
- Don’t over-partition by high-cardinality columns (e.g. customer_id).
- If you use Delta Lake later, you can automate this with OPTIMIZE.

</details>

#### C5. Replace Python UDFs with Native Expressions
Unlock Spark’s full optimization power.

<details>
<summary>💡 Quick Insight</summary>

**What it means:**  
A Python UDF runs row-by-row in Python, slow and outside Spark’s optimizer.  
Native Spark functions (`when`, `expr`, `col`, etc.) run inside the JVM and are optimized by Spark’s Catalyst engine.

**Why it matters:**  
Python UDFs interrupt Spark’s parallelism and optimization pipeline.  
Replacing them with native expressions can make transformations **10–100× faster** and reduce serialization overhead.

**How we use it:**  
The baseline used a Python UDF to bucket transactions by amount.  
The optimized job replaces it with Spark-native expressions:
```python
when(col("amount") < 20, "low")
 .when(col("amount") < 100, "mid")
 .otherwise("high")
```

**Verification:**

- Spark UI shows fewer “Python” tasks.
- Stage durations drop noticeably.
- p95 runtime improves consistently.

**Gotchas:**

- UDFs are fine for niche logic, but use built-ins or SQL whenever possible.
- If unavoidable, consider pandas UDFs for vectorized performance.

</details>


#### C6. (Optional) Checkpointing & Caching

Stabilize long-running jobs and simplify debugging.

<details> <summary>💡 Quick Insight</summary>

**What it means:**
- Spark tracks every step of a DataFrame’s creation (called lineage) so it can recompute lost data if something fails.
- However, after many transformations, that lineage becomes huge, slowing down Spark and making failures costly.

**Why it matters:**
Checkpointing or caching lets you “cut” that lineage and persist intermediate data.

- Checkpoint = save to disk (safe for restarts).

- Cache = keep in memory for reuse (fast but temporary).

**How we use it:**
We don’t checkpoint in this minimal example, but in production, you’d add:

``` python
df.checkpoint()
```

after large joins or aggregations to stabilize long DAGs.

**Verification:**

- Fewer recomputations in Spark UI.

- DAGs (job dependency graphs) look shorter.

- Less chance of “out of memory” or retry storms.

**Gotchas:**

- Don’t cache huge DataFrames you won’t reuse; it can overload memory.

- Use `df.persist(StorageLevel.DISK_ONLY)` if you want to persist safely without RAM pressure.

```python
# Example: caching a large intermediate DataFrame
from pyspark import StorageLevel

df_joined = df1.join(df2, "id", "inner")
df_joined.persist(StorageLevel.MEMORY_AND_DISK)

# Trigger action to materialize cache
df_joined.count()
```

</details>



#### C7. Partition Pruning

Speed up queries by reading only relevant partitions. <details> <summary>💡 Quick insight</summary>

**What it means:**
If your table is partitioned by event_date, and you query only one day, Spark can skip all other folders entirely.
That’s partition pruning, scanning just what you need.

**Why it matters:**
Massive IO savings. You reduce the amount of data read from disk, which is often the biggest bottleneck.

**How we use it:**
Our optimized job writes:

``` python
out.repartition("event_date")
   .write
   .partitionBy("event_date")
```

This ensures downstream queries with a date filter only touch relevant partitions.

**Verification:**

- Spark logs show fewer files scanned for date-based filters.

- Job durations decrease significantly.

**Gotchas:**

- Choose partition keys carefully; avoid columns with too many unique values (high cardinality).

- Time-based partitioning (daily/hourly) is usually ideal.

``` python
# Example: writing partitioned data
df.write.mode("overwrite").partitionBy("event_date").parquet("s3://bucket/transactions/")

# Reading with partition pruning
df_day = spark.read.parquet("s3://bucket/transactions/").filter("event_date = '2025-10-01'")
```

</details>

#### C8. Runtime Analysis

Quantify your improvements with real metrics.

<details> <summary>💡 Quick Insight</summary>

**What it means:**

- p50 (median) = typical runtime.

- p95 (tail latency) = slowest 5% of runs.

- p95 matters most because users notice the worst days, not the average ones.

**Why it matters:**
Tracking both gives you a fair view of performance; stable systems have small gaps between p50 and p95.

**How we use it:**
Each job logs start/end timestamps into ops/job_runs.csv.
We compute:

``` python
avg_s, p50_s, p95_s
```

and generate a Markdown + chart report.

**Verification:**

- Run each job multiple times; p95 becomes meaningful.

- The chart ops/p95_chart.png shows before/after improvement.

**Gotchas:**

- Compare same dataset and same environment between runs.

- Use enough samples (3–5) per job to smooth variance.

```python
import pandas as pd

runs = pd.read_csv("ops/job_runs.csv")
summary = runs.groupby("job_name")["duration_sec"].describe(percentiles=[.5, .95])[["50%", "95%"]]
summary.to_markdown("ops/p95_summary.md")
```

</details>

#### C9. Business Impact Analysis

Turn speed gains into tangible business value.

<details> <summary>💡 Quick Insight</summary>

**What it means:**
Every minute saved = less compute cost.
We estimate cost per job as:

duration (hours) × cluster_rate (€/hour)


Even rough estimates make performance savings visible.

**Why it matters:**
Translating technical wins into € or $ helps align with stakeholders and product owners.

**How we use it:**
The metric logger writes a cost_eur column for every run in ops/job_runs.csv.
We then summarize in ops/report.md.

**Verification:**

- Check average cost per model in the report.

- Compare before → after cost reductions.

**Gotchas:**

- This is an approximation; for precision, integrate actual cloud billing APIs.

- Don’t promise exact € numbers; use ranges (e.g., “~40% cheaper”).

```python
# Estimating cost per run
runs["cost_eur"] = (runs["duration_sec"] / 3600) * runs["cluster_cost_eur_hr"]
cost_summary = runs.groupby("job_name")["cost_eur"].mean()
cost_summary.to_markdown("ops/cost_summary.md")
```

</details>

#### C10. Operational Reliability

Keep pipelines observable, recoverable, and predictable.

<details> <summary>💡 Quick Insight</summary>

**What it means:**
A runbook documents each model's purpose, inputs, owners, and expected behavior; the “recipe” to operate and fix it. Reliability metrics like SLOs (targets) and MTTR (Mean Time To Recover) turn that recipe into measurable reliability.

**Why it matters:**
It’s not just about code but predictable operations. Fast recovery and clear ownership are what make a platform truly reliable.

**How we use it:**
We capture:

- `duration_seconds` → performance

- `status` → success/failure

- `InsertedAt` → recency
From these, we can infer uptime, freshness, and stability.

**Verification:**

- Add a small `ops/runbook.md` summarizing each critical model.

- Ensure SLOs are met (e.g., “data available within 60 minutes of load”).

**Gotchas:**

- Don’t over-engineer; start simple with key pipelines.

- Reliability is a culture, not a one-time metric.

```python
# Example: recording job status
import datetime

run = {
    "job_name": "model_transactions",
    "status": "success",
    "start_time": datetime.datetime.now(),
    "end_time": datetime.datetime.now(),
}

# Append to logs (simulate operational monitoring)
import csv
with open("ops/job_runs.csv", "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=run.keys())
    writer.writerow(run)
```

</details>


### D. Validation & Results

Quantify and communicate improvements clearly. After optimization, re-run both the baseline and optimized versions multiple times under identical conditions.

1. Compute p50/p95 runtimes for both.
2. Compare average cost per job.
3. Confirm output row counts and quality metrics are identical.
4. Summarize findings in a short report (`ops/summary_report.md`).
5. Confirm output parity: row counts, key uniqueness, and high-level aggregates match baseline vs optimized.


## 6. What Changes Between Baseline and Optimized

| Area             | Baseline (slow)                  | Optimized (fast)                                 |
| ---------------- | -------------------------------- | ------------------------------------------------ |
| Join strategy    | Regular join on skewed key       | Broadcast small dim + salting skewed key |
| Files written    | 200 partitions → many tiny files | Partition by `event_date`, fewer/bigger files    |
| Transform logic  | Python UDF for bucketing     | Spark built-ins (`when/otherwise`)           |
| Adaptive exec    | Off / default                    | AQE on + skew handling                       |
| Lineage/overhead | Long lineage, no checkpoints     | (If needed) checkpoint after heavy aggs      |

> 💡 The optimized job redistributes the work fairly, uses Spark’s built-in optimizations, and stores data more efficiently, making it faster and cheaper.



## 7. Metrics & Reporting

* **Runtime per run** → `ops/job_runs.csv`
* **p50 / p95 runtime** → `ops/report.md`
* **Before → After chart** → `ops/p95_chart.png`

> **p95 reduction** example: if baseline p95 = **60 min** and optimized p95 = **30 min**, that’s a **50% cut** in the slow-day time.

**Outcomes (simulated)**
| Metric                | Baseline | Optimized | Change |
|-----------------------|----------|-----------|--------|
| p50 runtime           | 38 min   | 22 min    | −42%   |
| p95 runtime           | 60 min   | 30 min    | −50%   |
| Cost per run (est.)   | €48      | €26       | −46%   |
| Success rate          | 98%      | 99.5%     | +1.5pp |

Optional (extend later):

* **Success rate**: % of runs with `status = SUCCESS`
* **MTTR**: time from a failed run to the next success
* **Freshness**: `now() - last_success_time`

> 💡 Each job records how long it takes and how much it costs. The system then summarizes that into a simple report you can show to any manager.



## 8. Runbook (Example)

```
RUNBOOK - daily_sales

Owner: Gabi · Tier: T1
Purpose: Daily revenue by segment for Ops dashboard
Schedule: hourly

Inputs
- raw.transactions (event_ts, amount)
- raw.customers (customer_id, segment)

Outputs
- silver.daily_sales (event_date, segment, bucket, tx_count, revenue)

SLOs
- Freshness ≤ 60 min (99%)
- Success rate ≥ 99%
- MTTR < 60 min

Baseline (Before → After)
- p95: 72 min → 34 min
- Cost/run (est.): €48 → €26

Common failures & fixes
- Skew on customer_id = 0 → salt + broadcast; verify AQE
- Small files storm → repartition by event_date; compact older partitions

Reprocess
- By `event_date` range; re-run optimized job
```

> 💡 This is your “how-to” for each job: what it does, how fast it should be, and what to do if it fails.



## 9. How This Scales to a 6-Month Engagement

This repo shows the **micro**. In a real project with 60–70 models, I:

1. Build a model catalog (owner, inputs/outputs, SLAs, runtime/cost/fail rate).
2. Baseline all Tier-1/2 jobs, then attack the Pareto offenders.
3. Introduce standards (data contracts, tests, CI/CD, template).
4. Stand up observability (p95, freshness, success, MTTR) and SLOs per tier.
5. Establish a green-lane so new models ship in ~10 business days.

> 💡 The same principles can fix an entire data ecosystem: catalog everything, fix the worst performers, and create rules so future jobs are fast and stable by design.



## 10. Next Steps / Ideas to Extend

* Add **quality checks** (nulls, uniqueness) and log to `ops.quality_metrics`.
* Record **lineage** (`ops.model_lineage`) by wrapping reads/writes.
* Add a **new-model template** and a tiny **CI** step.
* Try **Delta/Iceberg** + **OPTIMIZE/Z-ORDER** on the output table and compare.
* Simulate a small **incident** and compute **MTTR**.

> 💡 Expand the project toward full data reliability and governance, like turning a prototype engine into a production-ready car.


## 11. License & Credits

* Dummy data generated for demo purposes.
* You’re free to fork/adapt with attribution.

> 💡 It’s a learning sandbox; safe, open and built to showcase data engineering craftsmanship.



### 12. Why this matters

Anyone can say “we’ll optimize Spark.” This project shows **how**: a **diagnosis → intervention → measurement** loop, with **clear numbers** (p95, cost/run) and a **story** leadership understands.

> 💡 It’s not just about speed; My priority is clarity, reliability, and showing real progress with proof everyone can understand.