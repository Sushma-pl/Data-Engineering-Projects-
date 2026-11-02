# 🛒 ShopStream — Real-Time CDC Data Pipeline

A production-style **Change Data Capture (CDC)** pipeline that streams every database change from PostgreSQL through Apache Kafka into a Delta Lake medallion architecture (Bronze → Silver → Gold), processed by Apache Spark Structured Streaming.

> **Status:** Phases A–D complete (Infrastructure, Generator, Bronze, Silver). Gold, Tests, and MinIO in progress.

---

## 📖 Table of Contents

1. [What This Project Does](#1-what-this-project-does)
2. [The Big Picture — How Data Flows](#2-the-big-picture--how-data-flows)
3. [Architecture Diagram](#3-architecture-diagram)
4. [The Medallion Architecture Explained](#4-the-medallion-architecture-explained)
5. [Tech Stack](#5-tech-stack)
6. [Project Structure](#6-project-structure)
7. [Component Deep Dive](#7-component-deep-dive)
8. [Debezium Message Format](#8-debezium-message-format)
9. [Prerequisites](#9-prerequisites)
10. [How to Run — Step by Step](#10-how-to-run--step-by-step)
11. [Key Design Decisions](#11-key-design-decisions)
12. [What's Coming Next](#12-whats-coming-next)

---

## 1. What This Project Does

Imagine you run an e-commerce shop. Your app writes orders and customer data into a PostgreSQL database. Your data team wants:

- A **real-time copy** of this data for analytics
- **Historical snapshots** — what did the data look like last Tuesday?
- **Aggregated reports** — daily revenue by status

The traditional approach — running a batch job every night — is slow, misses deletes, and puts load on your production database.

**ShopStream solves this with CDC (Change Data Capture):**

Every time a row is inserted, updated, or deleted in PostgreSQL, that change is automatically captured and streamed through Kafka into Delta Lake tables — in near real-time, without touching the source database with heavy queries.

**The result:**
| What happens in Postgres | What happens in the pipeline |
|---|---|
| New order inserted | Appears in Silver within seconds |
| Order status updated | Silver row updated to match |
| Order deleted | Row removed from Silver |
| App goes down | Pipeline catches up automatically when it restarts |

---

## 2. The Big Picture — How Data Flows

This is the most important thing to understand. **Data only flows in one direction — left to right. Spark never writes back to Postgres.**

```
YOUR APPLICATION
      │
      │  writes orders, customers
      ▼
┌─────────────────┐
│   PostgreSQL    │  ← the SOURCE. Your app writes here.
│   (source DB)   │    Spark never touches this directly.
│                 │
│   wal_level     │
│   = logical     │  ← this setting is the key that unlocks CDC
└────────┬────────┘
         │
         │  Debezium reads the Write-Ahead Log (WAL)
         │  It's like reading a diary of every change
         │  without disturbing the database
         ▼
┌─────────────────┐
│    Debezium     │  ← CDC engine. Converts WAL entries
│  Kafka Connect  │    into JSON events.
└────────┬────────┘
         │
         │  publishes JSON events to Kafka topics
         │  shop.public.orders
         │  shop.public.customers
         ▼
┌─────────────────┐
│  Apache Kafka   │  ← message broker. Stores events
│                 │    durably. Multiple consumers can
│                 │    read independently.
└────────┬────────┘
         │
         ├──────────────────────┐
         │                      │
         ▼                      ▼
  Bronze Stream           (future: other consumers)
  (Spark reads Kafka,
   writes raw Delta)
         │
         ▼
┌─────────────────┐
│  Bronze Delta   │  ← raw events, exactly as received.
│  (local files)  │    Your permanent replay buffer.
└────────┬────────┘
         │
         │  Silver Stream reads Bronze
         ▼
┌─────────────────┐
│  Silver Delta   │  ← cleaned, current state.
│  (local files)  │    One row per order = mirrors Postgres.
└────────┬────────┘
         │
         │  Gold batch reads Silver
         ▼
┌─────────────────┐
│   Gold Delta    │  ← aggregated business metrics.
│  (local files)  │    Daily revenue, order counts.
└─────────────────┘
```

---

## 3. Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Docker Network                                  │
│                                                                           │
│  ┌─────────────────┐   WAL logical    ┌──────────────────────────────┐  │
│  │    POSTGRES      │◄── replication ──│      DEBEZIUM CONNECT        │  │
│  │  shopstream_     │    (pgoutput)    │    shopstream_connect         │  │
│  │  postgres        │                  │    port: 8083                 │  │
│  │  port: 5433      │                  │                               │  │
│  │  DB: shop        │                  │  Registered via:              │  │
│  │                  │                  │  POST /connectors             │  │
│  │  tables:         │                  │  connector/connector.json     │  │
│  │  - customers     │                  └──────────────┬────────────────┘  │
│  │  - orders        │                                 │                   │
│  │                  │                                 │ publishes to      │
│  │  wal_level=      │                                 │ Kafka topics      │
│  │  logical ✓       │                                 ▼                   │
│  └─────────────────┘                  ┌──────────────────────────────┐   │
│                                        │         KAFKA                │   │
│                                        │    shopstream_kafka           │   │
│                                        │    port: 9092                │   │
│                                        │                              │   │
│                                        │  Topics:                     │   │
│                                        │  shop.public.orders          │   │
│                                        │  shop.public.customers       │   │
│                                        └──────────────┬───────────────┘   │
│                                                       │                   │
│                                        ┌──────────────▼───────────────┐   │
│                                        │          SPARK               │   │
│                                        │    shopstream_spark           │   │
│                                        │    port: 4040 (UI)           │   │
│                                        │                              │   │
│                                        │  bronze_stream.py            │   │
│                                        │  silver_stream.py            │   │
│                                        │  gold_batch.py (coming soon) │   │
│                                        └──────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘

Outside Docker (your laptop):
  generator/generate_changes.py  → writes to Postgres via port 5433
  spark/check_bronze.py          → reads Delta files via volume mount
  spark/check_silver.py          → reads Delta files via volume mount
```

---

## 4. The Medallion Architecture Explained

The data passes through three layers — each one cleaner and more refined than the last.

### 🥉 Bronze — Raw Events (append-only)

**What it is:** Every Kafka message landed exactly as received. No parsing, no cleaning.

**Why it exists:** It's your **permanent replay buffer**. Kafka deletes events after 7 days. Bronze never deletes anything. If Silver has a bug, you fix the code and replay from Bronze to rebuild Silver — no need to touch Postgres or Kafka.

**What's stored:**
```
key          | value (raw JSON string)    | topic                | partition | offset | kafka_ts  | ingested_at
-------------|---------------------------|----------------------|-----------|--------|-----------|------------
{"id":1,...} | {"payload":{"op":"c",...}} | shop.public.orders   | 0         | 42     | 15:30:00  | 15:30:05
```

**Rule:** Bronze is append-only. You never update or delete from Bronze.

---

### 🥈 Silver — Current State (one row per order)

**What it is:** A clean, deduplicated table that mirrors the `orders` table in Postgres. One row per order ID reflecting the latest known state.

**Why it exists:** Bronze has 343+ events for the same set of 154 orders (inserts, updates, deletes). Silver collapses all of that into the current state — like SELECT * FROM orders in Postgres.

**What's stored:**
```
id  | customer_id | amount  | status    | created_at          | updated_at          | lsn
----|-------------|---------|-----------|---------------------|---------------------|----------
1   | 1           | 1500.00 | cancelled | 2026-11-02 04:12:45 | 2026-11-02 04:54:12 | 26787272
6   | 3           | 2215.63 | created   | 2026-11-02 04:52:08 | 2026-11-02 04:55:09 | 26812960
```

**How it stays in sync:** Every 10 seconds, Spark reads new Bronze events, parses the Debezium envelope, and runs a MERGE:
- `op = r, c, u` → UPSERT the row
- `op = d` → DELETE the row
- Guard: only update if `incoming lsn > stored lsn` (prevents replays from going backwards)

---

### 🥇 Gold — Business Aggregates (coming soon)

**What it is:** Pre-aggregated metrics built from Silver. Rebuilt by a batch job (not streaming).

**Why it exists:** Analysts and dashboards don't need raw rows — they need answers. "How much revenue did we make today?" "Which city has the most orders?"

**What will be stored:**
```
day        | status    | order_count | total_revenue
-----------|-----------|-------------|---------------
2026-11-02 | delivered | 13          | 18750.50
2026-11-02 | shipped   | 15          | 22100.25
```

---

## 5. Tech Stack

| Technology | Version | Role |
|---|---|---|
| **PostgreSQL** | 16 | Source database. `wal_level=logical` enables CDC. |
| **Debezium** | 2.7.3 | CDC engine. Reads Postgres WAL, publishes JSON to Kafka. |
| **Apache Kafka** | 7.6.0 (Confluent) | Message broker. Decouples source from consumers. KRaft mode (no ZooKeeper). |
| **Apache Spark** | 3.5.1 | Structured Streaming engine. Reads Kafka, writes Delta. |
| **Delta Lake** | 3.2.0 | ACID storage layer on top of Parquet files. Enables MERGE, time travel, schema evolution. |
| **Python** | 3.12 | Generator script, Spark jobs |
| **psycopg2** | latest | Python PostgreSQL client (used by generator) |
| **Docker / Compose** | 28 / v2 | Container orchestration. One command to start everything. |

---

## 6. Project Structure

```
05-ShopStream/
│
├── docker-compose.yml          # 🏗️  All 4 services: Postgres, Kafka, Debezium, Spark
│
├── connector/
│   └── connector.json          # ⚙️  Tells Debezium which tables to watch
│
├── postgres/
│   └── init/
│       └── 01_schema.sql       # 🗄️  Auto-runs on first Postgres start.
│                               #     Creates customers + orders tables + seed data.
│
├── generator/
│   └── generate_changes.py     # 🔄  Continuously inserts/updates/deletes orders.
│                               #     Runs on your laptop (outside Docker).
│
├── spark/
│   ├── bronze_stream.py        # 🥉  Kafka → Bronze Delta (raw, append-only)
│   ├── silver_stream.py        # 🥈  Bronze Delta → Silver Delta (MERGE, current state)
│   ├── gold_batch.py           # 🥇  Silver Delta → Gold Delta (aggregates) [coming soon]
│   ├── check_bronze.py         # 🔍  Verify Bronze data
│   └── check_silver.py         # 🔍  Verify Silver data
│
├── tests/
│   └── test_pipeline.py        # ✅  Unit + reconciliation tests [coming soon]
│
├── docs/
│   └── build-log.md            # 📓  Your learning journal
│
└── data/                       # 📁  Delta Lake files (git-ignored)
    ├── lake/
    │   ├── bronze/orders/      # Raw Kafka events as Delta
    │   ├── silver/orders/      # Current state as Delta
    │   └── gold/daily_revenue/ # Aggregates as Delta [coming soon]
    └── checkpoints/            # Spark streaming checkpoints
```

---

## 7. Component Deep Dive

### 7.1 PostgreSQL — The Source

The source database is configured with one critical startup flag:

```yaml
command:
  - postgres
  - -c
  - wal_level=logical    # ← enables CDC
```

**What is `wal_level=logical`?**

PostgreSQL's Write-Ahead Log (WAL) records every change before applying it — it's how Postgres recovers from crashes. By default it stores minimal information. `logical` level adds full row data (before AND after every change), which Debezium needs to understand what changed.

```
wal_level=minimal:  "page 47 was modified"           ← useless for CDC
wal_level=logical:  "row {id:1} changed to {id:1, status:'shipped'}"  ← perfect
```

**Why `REPLICA IDENTITY FULL`?**

```sql
ALTER TABLE orders REPLICA IDENTITY FULL;
```

By default, Postgres only includes the primary key in the "before" image of an UPDATE. With `FULL`, it includes the entire old row. This means Debezium can tell you what the row looked like before and after every change.

**Init SQL auto-runs on first boot:**

The `postgres/init/` folder is mounted to `/docker-entrypoint-initdb.d/` inside the container. Postgres runs every `.sql` file there automatically on first start. The `01_` prefix ensures correct execution order if you have multiple files.

---

### 7.2 Kafka — The Message Broker

Kafka stores CDC events as messages in topics. Think of a topic as an endless, durable log file.

**Topic naming:** `{prefix}.{schema}.{table}`
```
shop.public.orders     ← all changes to the orders table
shop.public.customers  ← all changes to the customers table
```

**Why Kafka between Debezium and Spark?**

Without Kafka, Debezium would write directly to Spark/Delta. Problems:
- If Spark is slow, Debezium slows down → WAL backs up → Postgres disk fills
- Only one consumer can receive changes

With Kafka:
- Debezium writes at full speed regardless of consumer speed
- Multiple consumers (Bronze, future analytics) read independently
- Events retained for 7 days — you can replay if needed

**KRaft mode (no ZooKeeper):**

Traditional Kafka needed a separate ZooKeeper cluster. KRaft mode (used here) eliminates that dependency — Kafka manages its own metadata internally.

**Two listeners:**
```
INTERNAL://kafka:29092   ← used by Debezium (inside Docker)
EXTERNAL://localhost:9092 ← used by your laptop tools
```

This is the #1 source of confusion. Spark runs inside Docker, so it uses `kafka:29092`. Your terminal tools use `localhost:9092`.

---

### 7.3 Debezium — The CDC Engine

Debezium runs inside Kafka Connect and reads Postgres's WAL via a **replication slot**.

**What is a replication slot?**

A replication slot is a bookmark in the WAL. Postgres guarantees it will NOT delete WAL segments until the slot confirms it has read them. This means:
- Debezium can restart and resume from exactly where it left off
- No changes are ever missed

⚠️ **Production risk:** If Debezium stops for days with heavy writes, WAL accumulates and can fill your disk. Monitor slot lag in production.

**Connector configuration (`connector/connector.json`):**

```json
{
  "table.include.list": "public.customers,public.orders",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "string",
  "plugin.name": "pgoutput"
}
```

| Setting | Why |
|---|---|
| `table.include.list` | Only capture changes from these tables — reduces WAL overhead |
| `snapshot.mode: initial` | On first run, capture all existing rows as `op: "r"` events, then switch to streaming |
| `decimal.handling.mode: string` | Postgres `NUMERIC` becomes base64 gibberish by default — `string` gives `"1500.00"` |
| `plugin.name: pgoutput` | Built into Postgres 10+ — no extra installation needed |

---

### 7.4 Bronze Stream (`spark/bronze_stream.py`)

Bronze is intentionally simple. Its only job is to move data from Kafka to Delta as fast as possible, without losing anything.

```python
# Read from Kafka — bytes
raw = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "shop.public.orders")
    .option("startingOffsets", "earliest")   # first run: start from beginning
    .option("failOnDataLoss", "true")        # crash if Kafka deleted unseen events
    .load()

# Cast bytes to strings — that's the ONLY transformation
bronze = raw.select(
    col("key").cast("string"),
    col("value").cast("string"),   # null for tombstone messages
    "topic", "partition", "offset",
    col("timestamp").alias("kafka_ts"),
    current_timestamp().alias("ingested_at")
)

# Append to Delta — never update, never delete
bronze.writeStream.format("delta").outputMode("append").start(BRONZE_PATH)
```

**Why `startingOffsets: earliest`?**

This only applies on the very first run (no checkpoint yet). It tells Spark to start from the beginning of the topic — picking up the initial snapshot events. After the first run, Spark ignores this and resumes from the checkpoint.

**Why `failOnDataLoss: true`?**

If Kafka's retention expires while Spark was stopped, some offsets may be gone. With `true`, Spark crashes and tells you. With `false`, it silently skips those events. Crashing loudly is better than silently losing data.

---

### 7.5 Silver Stream (`spark/silver_stream.py`)

Silver reads Bronze and applies all transformations. Its output mirrors the `orders` table in Postgres exactly.

**Step 1 — Parse the JSON envelope**

The raw `value` string in Bronze looks like:
```json
{
  "payload": {
    "before": null,
    "after": {"id": 1, "amount": "1500.00", "status": "created", ...},
    "op": "c",
    "source": {"lsn": 26787272}
  }
}
```

We define a schema that matches this structure exactly and use `from_json()` to parse it. If the schema doesn't match, Spark returns nulls silently — so getting this right is critical.

**Step 2 — Fix data types**

| Raw value | Problem | Fix |
|---|---|---|
| `"1500.00"` (string) | Can't do math on strings | `.cast("decimal(10,2)")` |
| `1793592765124509` (long) | Unreadable microseconds | `timestamp_micros()` |

**Step 3 — Deduplicate within each batch**

If the generator ran 5 updates on order #1 in 10 seconds, one batch might contain 5 events for the same order ID. MERGE fails if multiple source rows match one target row. Solution:

```python
w = Window.partitionBy("id").orderBy(col("lsn").desc())
latest = changes.withColumn("rn", row_number().over(w)).filter("rn = 1")
```

Keep only the event with the highest `lsn` (Log Sequence Number) per order ID. That's the most recent change.

**Step 4 — MERGE into Silver**

```
source op = "r" or "c" or "u"  →  UPSERT  (insert if new, update if exists)
source op = "d"                 →  DELETE  (remove from Silver)
```

The LSN guard on updates:
```python
.whenMatchedUpdate(
    condition="s.op != 'd' AND s.lsn > t.lsn",  # ← only update if newer
    ...
)
```

This makes the MERGE **idempotent** — replaying the same batch twice produces the same result. If Spark crashes mid-batch and replays it, no data goes backwards.

**Why read from Bronze, not Kafka?**

Bronze is permanent. Kafka has a 7-day retention. If Silver has a bug, you fix the code, delete Silver's checkpoint and table, and replay from Bronze. You can do this 6 months later. If Silver read from Kafka, you'd only have 7 days to catch bugs.

---

### 7.6 Generator (`generator/generate_changes.py`)

Runs on your laptop (outside Docker). Continuously mutates the source database so the pipeline always has data to process.

```
60% INSERT a new order
30% UPDATE a random order's status
10% DELETE a random order
```

The weights are skewed toward inserts because the table starts empty and deletes on an empty table fail.

Connects to Postgres via `localhost:5433` (the mapped host port) using `psycopg2`.

---

## 8. Debezium Message Format

Every change in Postgres becomes a JSON message in Kafka. Here's what each type looks like:

### INSERT (`op: "c"` — create)
```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 3,
      "customer_id": 2,
      "amount": "2500.00",
      "status": "created",
      "created_at": 1793592765124509,
      "updated_at": 1793592765124509
    },
    "op": "c",
    "source": { "lsn": 26730968 }
  }
}
```

### UPDATE (`op: "u"`)
```json
{
  "payload": {
    "before": { "id": 3, "status": "created", ... },
    "after":  { "id": 3, "status": "shipped", ... },
    "op": "u",
    "source": { "lsn": 26753824 }
  }
}
```
Both `before` and `after` are populated because of `REPLICA IDENTITY FULL`.

### DELETE (`op: "d"`)
```json
{
  "payload": {
    "before": { "id": 3, "status": "shipped", ... },
    "after": null,
    "op": "d",
    "source": { "lsn": 26779728 }
  }
}
```

### Snapshot Read (`op: "r"`) — only on first run
```json
{
  "payload": {
    "before": null,
    "after": { "id": 1, "status": "created", ... },
    "op": "r",
    "source": { "lsn": 26723216 }
  }
}
```

### Tombstone — null value message after DELETE
After every delete event, Debezium sends a second message with the same key but a **null value**. This supports Kafka log compaction. Silver filters these out with:
```python
parsed.filter(col("value").isNotNull())
```

---

## 9. Prerequisites

| Tool | Check | Install |
|---|---|---|
| Docker Desktop | `docker --version` | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Docker Compose | `docker compose version` | Included with Docker Desktop |
| Python 3.10+ | `python --version` | [python.org](https://www.python.org/downloads/) |
| 8GB+ free RAM | Docker Desktop → Resources | Increase in Docker settings |

---

## 10. How to Run — Step by Step

### Step 1 — Start all services

```powershell
cd 05-ShopStream
docker compose up -d
```

Wait 30 seconds. Verify everything is running:
```powershell
docker compose ps
```
All 4 services (`shopstream_postgres`, `shopstream_kafka`, `shopstream_connect`, `shopstream_spark`) should show `running`.

---

### Step 2 — Register the Debezium connector

```powershell
curl.exe -X POST http://localhost:8083/connectors `
  -H "Content-Type: application/json" `
  --data-binary "@connector/connector.json"
```

Verify it's running:
```powershell
curl.exe http://localhost:8083/connectors/shopstream-connector/status
```

Look for `"state": "RUNNING"` in the response.

---

### Step 3 — Start the data generator (optional — for live data)

```powershell
pip install psycopg2-binary
python generator/generate_changes.py
```

This continuously inserts/updates/deletes orders. Leave it running in a dedicated terminal.

---

### Step 4 — Start the Bronze stream

Open a shell inside the Spark container:
```powershell
docker exec -it -u root shopstream_spark bash
```

Run Bronze:
```bash
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 /opt/spark-apps/bronze_stream.py
```

First run downloads packages (~2 minutes). You'll see batch progress every 10 seconds.

---

### Step 5 — Start the Silver stream

In a new terminal, exec into Spark again:
```powershell
docker exec -it -u root shopstream_spark bash
```

Run Silver:
```bash
/opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 /opt/spark-apps/silver_stream.py
```

You'll see:
```
--- Batch 0 ---
Changes in this batch: 194
Batch 0 complete.
```

---

### Step 6 — Verify data

Check Bronze:
```bash
/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark-apps/check_bronze.py
```

Check Silver:
```bash
/opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0 /opt/spark-apps/check_silver.py
```

Reconcile with Postgres:
```powershell
docker exec -it shopstream_postgres psql -U postgres -d shop -c "SELECT count(*) FROM orders;"
```

Silver count should equal Postgres count.

---

### Stop everything

```powershell
docker compose down
```

To also wipe all data and start fresh:
```powershell
docker compose down -v
```

---

## 11. Key Design Decisions

### Why Bronze → Silver instead of Kafka → Silver directly?

Kafka has a 7-day retention. Bronze is permanent. If Silver's parsing logic has a bug, you fix the code and rebuild Silver from Bronze any time — even months later. If Silver read from Kafka directly, you'd have 7 days to catch bugs before losing the ability to replay.

### Why `lsn` as the ordering field instead of `updated_at`?

Application code can set `updated_at` to any value (including wrong ones). `lsn` is the Log Sequence Number assigned by Postgres — it's strictly monotonically increasing and reflects the true commit order. Using `s.lsn > t.lsn` as the MERGE guard means the pipeline is immune to application bugs in timestamp handling.

### Why UPSERT instead of separate INSERT/UPDATE logic?

`INSERT ... ON CONFLICT DO UPDATE` (or Delta's MERGE) is **idempotent** — running the same operation twice produces the same result. This is essential because Spark uses at-least-once delivery: after a crash, the last batch replays. Idempotent writes mean replays are harmless.

### Why `decimal.handling.mode: string`?

Postgres `NUMERIC` is encoded as base64 binary by Debezium by default. Without this setting, `amount` arrives as `"AxHG"` instead of `"1500.00"`. Setting it to `string` gives a readable representation that Spark can cast to `DECIMAL`.

### Why KRaft Kafka (no ZooKeeper)?

Traditional Kafka required a separate ZooKeeper cluster (3+ nodes) for coordination. KRaft mode (Kafka 3.x) handles this internally. For a local dev setup this reduces containers from 5+ to 4 and eliminates one more service to manage.

### Why `shuffle.partitions = 4` in Silver?

Spark's default is 200 partitions for shuffle operations. On a laptop with small data, this creates 200 tiny files. Setting it to 4 keeps file counts manageable and avoids the small-file problem that makes Delta reads slow over time.

---

## 12. What's Coming Next

| Phase | What | Status |
|---|---|---|
| **E** | Gold batch job — daily revenue aggregates | 🔜 Next |
| **F** | Tests — unit tests for `parse_changes`, reconciliation test vs Postgres | 🔜 Planned |
| **G** | MinIO — swap local file paths for `s3a://` object storage | 🔜 Planned |
| **H** | Failure experiments — kill Spark mid-batch, stop Kafka, rebuild Silver from Bronze | 🔜 Planned |

---

*Built step by step while learning — every design decision was made with understanding, not copy-pasting.*
