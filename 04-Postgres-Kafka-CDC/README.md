# 🔄 Postgres → Kafka CDC Pipeline

> **Change Data Capture (CDC)** in action — automatically stream every `INSERT`, `UPDATE`, and `DELETE` from a source PostgreSQL database into a target PostgreSQL database in near real-time, using **Debezium** and **Apache Kafka**.

---

## 📖 Table of Contents

1. [What is CDC and Why Does It Matter?](#1-what-is-cdc-and-why-does-it-matter)
2. [Project Overview](#2-project-overview)
3. [Architecture Diagram](#3-architecture-diagram)
4. [Data Flow — Step by Step](#4-data-flow--step-by-step)
5. [Tech Stack](#5-tech-stack)
6. [Project Structure](#6-project-structure)
7. [Component Deep Dive](#7-component-deep-dive)
   - [Source PostgreSQL](#71-source-postgresql)
   - [Apache Kafka (KRaft Mode)](#72-apache-kafka-kraft-mode)
   - [Debezium Kafka Connect](#73-debezium-kafka-connect)
   - [Target PostgreSQL](#74-target-postgresql)
   - [consumer.py — Debug Consumer](#75-consumerpy--debug-consumer)
   - [sink.py — Production Sink](#76-sinkpy--production-sink)
8. [Debezium Message Format Explained](#8-debezium-message-format-explained)
9. [Prerequisites](#9-prerequisites)
10. [Setup & Run Guide (Step by Step)](#10-setup--run-guide-step-by-step)
11. [Testing the Pipeline](#11-testing-the-pipeline)
12. [Key Design Decisions](#12-key-design-decisions)
13. [Common Errors & Troubleshooting](#13-common-errors--troubleshooting)

---

## 1. What is CDC and Why Does It Matter?

### The Problem

Imagine you have a production database powering your e-commerce website. Your data team also needs a copy of this data for analytics, reporting, and dashboards — but they can't query the production database directly (it would slow it down and be a security risk).

**Traditional Solution:** Run a batch job every hour/night that copies data from source to destination.

**Problems with batch jobs:**
- Data is stale (up to 24 hours old)
- Full table scans are expensive and slow
- Hard to know *what changed*, not just *what exists*

### The CDC Solution

**Change Data Capture (CDC)** solves this by watching the database's internal log (called the **Write-Ahead Log** or WAL in PostgreSQL) and capturing every single change the moment it happens.

```
Traditional Batch:   Source DB ──(every night)──► Destination DB
                     Data is always hours/days stale

CDC Streaming:       Source DB ──(every millisecond)──► Destination DB
                     Data is always in sync, in real-time
```

CDC tells you:
- ✅ A new row was **inserted** — here's what it looks like
- ✅ A row was **updated** — here's what it looked like *before* and *after*
- ✅ A row was **deleted** — here's what was removed

This project implements a **CDC pipeline** using industry-standard tools to keep two PostgreSQL databases in sync automatically.

---

## 2. Project Overview

This project builds a **real-time data replication pipeline** with the following behavior:

| What happens on Source DB | What happens on Target DB |
|---|---|
| `INSERT` a new customer | Customer appears in target within seconds |
| `UPDATE` a customer's city | Target is updated to match |
| `DELETE` a customer | Customer is removed from target |

**The pipeline runs without any manual intervention** — once set up, changes flow automatically from source to target through Kafka.

---

## 3. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Docker Network                                  │
│                                                                              │
│   ┌──────────────────────┐    Reads WAL    ┌──────────────────────────────┐ │
│   │   SOURCE POSTGRES    │◄───(pgoutput)───│     DEBEZIUM CONNECT         │ │
│   │   container:         │   replication   │     container:               │ │
│   │   cdc_postgres       │      slot       │     cdc_connect              │ │
│   │   port: 5434         │                 │     port: 8083               │ │
│   │   DB: shop           │                 │                              │ │
│   │   table: customers   │                 │  Registered via REST API:    │ │
│   │                      │                 │  POST /connectors            │ │
│   │   wal_level=logical  │                 │  (config/connector.json)     │ │
│   └──────────────────────┘                 └──────────────┬───────────────┘ │
│                                                           │                 │
│                                                           │ Publishes JSON  │
│                                                           │ CDC events      │
│                                                           ▼                 │
│                                             ┌─────────────────────────────┐ │
│                                             │      APACHE KAFKA           │ │
│                                             │      container: cdc_kafka   │ │
│                                             │      port: 9092             │ │
│                                             │                             │ │
│                                             │  Topic: shop.public.customers│ │
│                                             │  (KRaft mode, no ZooKeeper) │ │
│                                             └──────────────┬──────────────┘ │
└────────────────────────────────────────────────────────────┼────────────────┘
                                                             │
                              ┌──────────────────────────────┤
                              │                              │
                              ▼                              ▼
                  ┌───────────────────────┐    ┌────────────────────────────┐
                  │   consumer.py         │    │   sink.py                  │
                  │   (Debug / Monitor)   │    │   (Production Sink)        │
                  │                       │    │                            │
                  │   Reads CDC events    │    │   Reads CDC events         │
                  │   Prints to terminal  │    │   Writes to Target DB      │
                  │                       │    │   Manual offset commit     │
                  │   group:              │    │   group:                   │
                  │   customers-consumer  │    │   customers-sink-group     │
                  └───────────────────────┘    └────────────────┬───────────┘
                                                                │
                                                                ▼
                                               ┌──────────────────────────────┐
                                               │   TARGET POSTGRES            │
                                               │   container:                 │
                                               │   cdc_target_postgres        │
                                               │   port: 5435                 │
                                               │   DB: shop_target            │
                                               │   table: customers           │
                                               └──────────────────────────────┘
```

### What Each Arrow Means

| Arrow | What's Happening |
|---|---|
| Debezium ← Source Postgres | Debezium reads the WAL (transaction log) via a replication slot |
| Debezium → Kafka | Debezium converts WAL entries into JSON messages and publishes them |
| sink.py ← Kafka | The sink consumes messages from the topic |
| sink.py → Target Postgres | The sink runs SQL (INSERT/UPDATE/DELETE) on the target |

---

## 4. Data Flow — Step by Step

Here's exactly what happens from the moment you insert a row until it appears in the target database:

```
Step 1: You run this SQL on Source Postgres
        INSERT INTO customers (name, email, city)
        VALUES ('Charlie', 'charlie@example.com', 'Bangalore');

Step 2: Postgres writes the change to its WAL (Write-Ahead Log)
        [WAL]: INSERT on public.customers, new row = {id:3, name:"Charlie", ...}

Step 3: Debezium reads the WAL via the replication slot
        Debezium sees: "A new row was inserted on public.customers"

Step 4: Debezium converts it to a JSON CDC event and publishes to Kafka
        Topic: shop.public.customers
        Message:
        {
          "payload": {
            "before": null,
            "after": { "id": 3, "name": "Charlie", "email": "charlie@example.com", "city": "Bangalore" },
            "op": "c"
          }
        }

Step 5: sink.py reads the message from Kafka

Step 6: sink.py sees op="c" (create/insert), runs UPSERT on Target Postgres
        INSERT INTO customers (id, name, email, city)
        VALUES (3, 'Charlie', 'charlie@example.com', 'Bangalore')
        ON CONFLICT (id) DO UPDATE SET name=..., email=..., city=...;

Step 7: sink.py commits the Kafka offset (marks message as processed)

Step 8: Target Postgres now has Charlie's row — pipeline complete! ✅
```

The same flow applies for UPDATE and DELETE operations with different `op` values.

---

## 5. Tech Stack

| Technology | Version | Role |
|---|---|---|
| **PostgreSQL** | 16 | Source database (wal_level=logical enables CDC) |
| **Apache Kafka** | 7.6.0 (Confluent) | Message broker — decouples source from target |
| **Debezium** | 2.7.3.Final | CDC engine — reads Postgres WAL, publishes to Kafka |
| **Kafka Connect** | Built into Debezium image | Framework that runs Debezium as a connector plugin |
| **Python** | 3.x | Sink consumer logic (`sink.py`, `consumer.py`) |
| **kafka-python** | Latest | Python Kafka client library |
| **psycopg2** | Latest | Python PostgreSQL client |
| **Docker / Docker Compose** | Latest | Container orchestration for all services |

---

## 6. Project Structure

```
04-Postgres-Kafka-CDC/
│
├── docker-compose.yml          # 🏗️  Spins up all 4 services (Postgres x2, Kafka, Debezium)
│
├── config/
│   └── connector.json          # ⚙️  Debezium connector config — tells Debezium WHICH table to watch
│
├── postgres/
│   └── table_ddl/
│       └── customers.sql       # 🗄️  DDL for both source & target tables + seed data (Alice, Bob)
│
├── consumer/
│   ├── consumer.py             # 🔍  Debug consumer — reads Kafka events and prints to terminal
│   └── requirements.txt        # 📦  Python dependencies (kafka-python)
│
├── sink/
│   └── sink.py                 # 🚰  Production sink — reads Kafka, writes to Target Postgres
│
├── connect/                    # 📁  Empty placeholder (future connector plugins)
├── kafka/                      # 📁  Empty placeholder (future Kafka config)
└── producer/                   # 📁  Empty placeholder (future custom data producer)
```

---

## 7. Component Deep Dive

### 7.1 Source PostgreSQL

**Container:** `cdc_postgres` | **Port:** `5434` | **Database:** `shop`

The source database is where your application data lives. The critical thing that makes CDC possible is the special startup configuration:

```yaml
# docker-compose.yml
command:
  - postgres
  - -c
  - wal_level=logical    # ← THIS is the magic setting
  - -c
  - max_replication_slots=10
  - -c
  - max_wal_senders=10
```

**What is `wal_level=logical`?**

PostgreSQL's Write-Ahead Log (WAL) is a file that records every change before it's applied to the database. It's PostgreSQL's durability mechanism — if the server crashes, it replays the WAL to recover.

By default, the WAL only contains enough information to recover the database. The `logical` level adds extra information (like the full row data before and after a change) that allows external tools like Debezium to *decode* the log and understand what changed.

```
WAL level=minimal:  "Page 47 was modified"  ← useless for CDC
WAL level=replica:  "Row on page 47 was updated" ← not detailed enough
WAL level=logical:  "Row {id:1, name:'Alice'} was updated to {id:1, name:'Alicia'}" ← perfect for CDC!
```

**Replication Slot:**  
When Debezium registers its connector, it creates a **replication slot** named `shop_cdc_slot` in Postgres. This slot acts like a bookmark — Postgres guarantees it will NOT delete WAL segments until Debezium has read them. This ensures no changes are ever missed, even if Debezium restarts.

**Publication:**  
Debezium also creates a **publication** named `shop_publication` covering only the `public.customers` table. A publication is PostgreSQL's way of saying "these are the tables whose changes are available for logical replication."

---

### 7.2 Apache Kafka (KRaft Mode)

**Container:** `cdc_kafka` | **Port:** `9092` | **Topic:** `shop.public.customers`

Kafka is the **message bus** between Debezium and your consumers. Think of it as a very durable, high-performance queue.

**Why Kafka in the middle?**

Without Kafka, Debezium would write directly to the target database. This creates tight coupling:
- If the target DB is slow, Debezium slows down, which backs up the replication slot, which causes the source DB to run out of disk (WAL accumulates!)
- Only one consumer can receive changes

With Kafka in the middle:
- Debezium writes at full speed regardless of consumer speed
- Multiple consumers can independently read from the same topic
- Messages are retained for a configurable period (default 7 days) — you can replay history

**KRaft Mode (No ZooKeeper):**

Traditionally, Kafka required a separate ZooKeeper cluster for coordination. In newer versions, Kafka can manage its own metadata using a built-in consensus protocol called **KRaft** (Kafka Raft Metadata mode). This simplifies the setup significantly.

```yaml
# docker-compose.yml
KAFKA_PROCESS_ROLES: broker, controller  # This node is both a broker AND a controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk     # Static ID required for KRaft
```

**Three Listeners Explained:**

```
INTERNAL://kafka:29092  → Used by Debezium Connect (inside Docker network)
EXTERNAL://localhost:9092 → Used by your Python scripts (outside Docker, on your machine)
CONTROLLER://kafka:9093  → Used internally for KRaft leader election
```

This is a common source of confusion! The `bootstrap_servers` in your Python code is `localhost:9092` (external), but Debezium uses `kafka:29092` (internal Docker DNS).

**Topic Naming Convention:**

Debezium creates topics automatically using the pattern:
```
{topic.prefix}.{schema}.{table}
→ shop.public.customers
```

---

### 7.3 Debezium Kafka Connect

**Container:** `cdc_connect` | **Port:** `8083`

Debezium runs *inside* Kafka Connect, which is a framework for scalable, reliable data streaming between Kafka and other systems. You interact with it via a **REST API**.

**How to Register the Connector:**

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/connector.json
```

The `connector.json` file:

```json
{
  "name": "shop-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",     ← Docker container name (internal DNS)
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "shop",
    "topic.prefix": "shop",             ← Topics will be named shop.{schema}.{table}
    "plugin.name": "pgoutput",          ← Built-in Postgres logical decoding plugin
    "slot.name": "shop_cdc_slot",       ← Replication slot name (created automatically)
    "publication.name": "shop_publication",
    "publication.autocreate.mode": "filtered",  ← Only create pub for listed tables
    "table.include.list": "public.customers",   ← ONLY watch this table
    "snapshot.mode": "initial"          ← First run: capture existing rows too
  }
}
```

**What `snapshot.mode: initial` means:**

On the very first run, Debezium takes a **full snapshot** of the `customers` table and emits every existing row as a `"op": "r"` (read) event. This bootstraps the target database with all pre-existing data. After the snapshot, it switches to streaming real-time WAL changes.

```
First run timeline:
  T=0:   Snapshot begins
  T=1:   Alice's row emitted as op="r"
  T=2:   Bob's row emitted as op="r"
  T=3:   Snapshot ends, streaming begins
  T=4+:  Real-time INSERT/UPDATE/DELETE events streamed
```

**Kafka Connect Internal Topics:**

Debezium stores its own state in three Kafka topics (auto-created):
- `connect_configs` — connector configuration
- `connect_offsets` — last processed WAL position (so it resumes correctly after restart)
- `connect_status` — connector health status

---

### 7.4 Target PostgreSQL

**Container:** `cdc_target_postgres` | **Port:** `5435` | **Database:** `shop_target`

The target database is the destination — a replica of the source, kept in sync by the sink.

**Important: Source vs Target Schema Difference**

```sql
-- Source (shop DB) - uses SERIAL for auto-incrementing IDs
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,   ← Postgres auto-generates IDs
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100)
);

-- Target (shop_target DB) - uses plain INTEGER
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,  ← IDs come from CDC events, NOT auto-generated
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100)
);
```

This is a critical detail. If you used `SERIAL` on the target, Postgres would generate its own IDs and they might clash with the IDs coming from the source. The target must accept whatever ID the source assigned.

---

### 7.5 `consumer.py` — Debug Consumer

**Location:** `consumer/consumer.py`

This script is your **window into the pipeline** — it reads CDC events from Kafka and prints them to the terminal so you can see exactly what's flowing through. It does NOT write to any database.

```python
consumer = KafkaConsumer(
    "shop.public.customers",
    bootstrap_servers=["localhost:9092"],
    group_id="customers-consumer-group",   # ← different group than sink.py
    auto_offset_reset="earliest",          # ← start from the beginning of topic
    enable_auto_commit=True,               # ← Kafka handles offset tracking
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)
```

**Consumer Groups:**

Kafka tracks your reading position (offset) per **consumer group**. Because `consumer.py` uses a different group (`customers-consumer-group`) than `sink.py` (`customers-sink-group`), they independently track their own positions. Running `consumer.py` does not affect the sink's progress.

**Sample output when you run consumer.py:**
```
Waiting for customer events...

Topic: shop.public.customers
Partition: 0
Offset: 0

---------- CDC EVENT ----------
SNAPSHOT: {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'city': 'Delhi'}

---------- CDC EVENT ----------
SNAPSHOT: {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'city': 'Mumbai'}

---------- CDC EVENT ----------
INSERT: {'id': 3, 'name': 'Charlie', 'email': 'charlie@example.com', 'city': 'Bangalore'}
```

---

### 7.6 `sink.py` — Production Sink

**Location:** `sink/sink.py`

This is the **heart of the pipeline** — it bridges Kafka and the target database. Every CDC event is translated into a SQL operation on the target.

**Key difference from `consumer.py`: Manual Offset Commits**

```python
consumer = KafkaConsumer(
    ...
    enable_auto_commit=False,   # ← CRITICAL: we control when offset is committed
    ...
)
```

Why manual commits? **Delivery guarantees.**

```
Auto-commit (consumer.py approach):
  1. Read message from Kafka
  2. Kafka auto-commits offset  ← committed here, BEFORE we do anything
  3. Try to write to database
  4. If database write fails → message is LOST (offset already committed)

Manual commit (sink.py approach):
  1. Read message from Kafka
  2. Write to database          ← do the real work first
  3. If write succeeds → commit offset  ← only now mark as done
  4. If write fails → offset NOT committed → message replayed on restart
```

This gives us **at-least-once delivery**: every change will be applied, even if the process crashes mid-flight.

**Handling Tombstone Messages:**

When Debezium emits a DELETE, it actually sends TWO Kafka messages:
1. The DELETE event with `op: "d"` and the deleted row in `before`
2. A **tombstone message** — a message with a `null` value (not just null payload, the entire message value is null)

The tombstone exists to support Kafka log compaction (Kafka can compact duplicate keys, and a null value signals "this key is gone"). The sink handles it gracefully:

```python
if event is None:           # tombstone message
    consumer.commit(...)    # just commit and skip
    continue
```

**The UPSERT Logic:**

For insert, update, and snapshot-read operations, the sink uses an UPSERT (insert-or-update):

```sql
INSERT INTO customers (id, name, email, city)
VALUES (%s, %s, %s, %s)
ON CONFLICT (id)            ← if a row with this id already exists...
DO UPDATE SET               ← ...update it instead of failing
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    city = EXCLUDED.city;
```

This is **idempotent** — running the same INSERT twice produces the same result. This is important because with at-least-once delivery, you might process the same event more than once (e.g., after a crash). The UPSERT ensures this is safe.

**Operation Mapping:**

```
Kafka message op="r" (snapshot read) → UPSERT  (bootstrapping from snapshot)
Kafka message op="c" (create/insert) → UPSERT  (new row arrives)
Kafka message op="u" (update)        → UPSERT  (row is updated)
Kafka message op="d" (delete)        → DELETE  (row is removed)
```

**Full safe write sequence:**

```python
with db_connection:            # auto-commit on success, rollback on exception
    with db_connection.cursor() as cursor:
        cursor.execute(sql, params)
# Only reach here if DB write succeeded
consumer.commit({offset})      # now safe to commit Kafka offset
```

---

## 8. Debezium Message Format Explained

Understanding the message format is key to working with any CDC pipeline. Here's what each event type looks like (with `KEY_CONVERTER_SCHEMAS_ENABLE: "false"` — plain JSON, no Avro schema):

### INSERT (`op: "c"` for create)
```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 3,
      "name": "Charlie",
      "email": "charlie@example.com",
      "city": "Bangalore"
    },
    "op": "c",
    "ts_ms": 1718000000000
  }
}
```
- `before` is `null` — nothing existed before an insert
- `after` contains the new row

### UPDATE (`op: "u"`)
```json
{
  "payload": {
    "before": {
      "id": 1,
      "name": "Alice",
      "email": "alice@example.com",
      "city": "Delhi"
    },
    "after": {
      "id": 1,
      "name": "Alice",
      "email": "alice@example.com",
      "city": "Hyderabad"
    },
    "op": "u",
    "ts_ms": 1718000001000
  }
}
```
- Both `before` and `after` are populated — you can see exactly what changed
- Here Alice's city changed from `Delhi` to `Hyderabad`

### DELETE (`op: "d"`)
```json
{
  "payload": {
    "before": {
      "id": 2,
      "name": "Bob",
      "email": "bob@example.com",
      "city": "Mumbai"
    },
    "after": null,
    "op": "d",
    "ts_ms": 1718000002000
  }
}
```
- `before` contains the deleted row — so you always know what was removed
- `after` is `null`

### Snapshot Read (`op: "r"`)
```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "name": "Alice",
      "email": "alice@example.com",
      "city": "Delhi"
    },
    "op": "r",
    "ts_ms": 1718000003000
  }
}
```
- Same structure as INSERT but `op` is `"r"` (read)
- Only appears during the initial snapshot

---

## 9. Prerequisites

Make sure you have these installed before starting:

| Tool | How to Check | Install Link |
|---|---|---|
| Docker Desktop | `docker --version` | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Docker Compose | `docker compose version` | Included with Docker Desktop |
| Python 3.8+ | `python --version` | [python.org](https://www.python.org/downloads/) |
| pip | `pip --version` | Included with Python |
| curl (optional) | `curl --version` | For registering connectors from terminal |

---

## 10. Setup & Run Guide (Step by Step)

### Step 1: Start All Services

Open a terminal, navigate to the project folder, and run:

```bash
docker compose up -d
```

This starts 4 containers:
- `cdc_postgres` (source database)
- `cdc_kafka` (message broker)
- `cdc_connect` (Debezium)
- `cdc_target_postgres` (target database)

Check they're all running:
```bash
docker ps
```

You should see all 4 containers with status `Up`.

Wait about **30 seconds** for Kafka and Debezium to fully start.

---

### Step 2: Create the Source Table and Seed Data

Connect to the source database and create the `customers` table:

```bash
docker exec -it cdc_postgres psql -U postgres -d shop
```

Inside the psql shell, paste:

```sql
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100)
);

INSERT INTO customers (name, email, city)
VALUES
    ('Alice', 'alice@example.com', 'Delhi'),
    ('Bob', 'bob@example.com', 'Mumbai');
```

Verify:
```sql
SELECT * FROM customers;
```

Exit psql:
```sql
\q
```

---

### Step 3: Create the Target Table

Connect to the target database:

```bash
docker exec -it cdc_target_postgres psql -U postgres -d shop_target
```

Inside psql, paste:

```sql
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100)
);
```

Exit psql:
```sql
\q
```

---

### Step 4: Verify Debezium Connect is Ready

```bash
curl http://localhost:8083/
```

You should get a JSON response with Debezium version info. If you get a connection error, wait a bit longer and retry.

---

### Step 5: Register the Debezium Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/connector.json
```

Check the connector status:

```bash
curl http://localhost:8083/connectors/shop-postgres-connector/status
```

Look for `"state": "RUNNING"` in the output.

```json
{
  "name": "shop-postgres-connector",
  "connector": { "state": "RUNNING", ... },
  "tasks": [{ "state": "RUNNING", ... }]
}
```

---

### Step 6: Install Python Dependencies

```bash
cd sink
pip install kafka-python psycopg2-binary
```

Or for the consumer:
```bash
cd consumer
pip install -r requirements.txt
```

---

### Step 7: Run the Sink

In one terminal:

```bash
python sink/sink.py
```

You should see:
```
Connected to target PostgreSQL!
Waiting for CDC events...
```

Because the snapshot mode is `initial`, the sink will immediately process Alice and Bob's rows:

```
---------- CDC EVENT ----------
Operation: r
UPSERT: {'id': 1, 'name': 'Alice', 'email': 'alice@example.com', 'city': 'Delhi'}
Kafka offset committed: 1

---------- CDC EVENT ----------
Operation: r
UPSERT: {'id': 2, 'name': 'Bob', 'email': 'bob@example.com', 'city': 'Mumbai'}
Kafka offset committed: 2
```

---

### Step 8 (Optional): Run the Debug Consumer

In another terminal, run the debug consumer to see raw events:

```bash
python consumer/consumer.py
```

---

### Step 9: Test the Pipeline!

Now go back to the source database and make some changes:

```bash
docker exec -it cdc_postgres psql -U postgres -d shop
```

Try these operations and watch your `sink.py` terminal:

```sql
-- Insert a new customer
INSERT INTO customers (name, email, city) 
VALUES ('Charlie', 'charlie@example.com', 'Bangalore');

-- Update a customer's city
UPDATE customers SET city = 'Hyderabad' WHERE name = 'Alice';

-- Delete a customer
DELETE FROM customers WHERE name = 'Bob';
```

After each operation, check the target database:

```bash
docker exec -it cdc_target_postgres psql -U postgres -d shop_target -c "SELECT * FROM customers;"
```

The target should reflect every change within a second or two!

---

### Step 10: Stop Everything

```bash
docker compose down
```

To also delete all stored data (volumes):
```bash
docker compose down -v
```

---

## 11. Testing the Pipeline

Here's a quick reference for all test operations and what to expect:

### INSERT Test
```sql
-- On source
INSERT INTO customers (name, email, city) VALUES ('Diana', 'diana@example.com', 'Chennai');

-- Expected on target (within ~1 second)
-- Row with id=4, name=Diana appears in shop_target.customers
```

### UPDATE Test
```sql
-- On source
UPDATE customers SET email = 'new_alice@example.com' WHERE id = 1;

-- Expected on target
-- Alice's email updated to new_alice@example.com
```

### DELETE Test
```sql
-- On source
DELETE FROM customers WHERE id = 2;

-- Expected on target
-- Bob's row removed from shop_target.customers
```

### Verify Target State
```bash
docker exec -it cdc_target_postgres psql -U postgres -d shop_target -c "SELECT * FROM customers ORDER BY id;"
```

### Check Kafka Topic Messages (Optional)
```bash
docker exec -it cdc_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic shop.public.customers \
  --from-beginning
```

---

## 12. Key Design Decisions

### Why KRaft Mode (No ZooKeeper)?

Traditional Kafka setups require a separate ZooKeeper cluster (usually 3+ nodes) just for coordination. KRaft mode (introduced in Kafka 2.8, production-ready in 3.x) eliminates this dependency. For a dev/learning setup, this reduces the number of containers from 5+ to 4.

### Why `pgoutput` Plugin Instead of `wal2json` or `decoderbufs`?

`pgoutput` is built into PostgreSQL 10+ — no extra installation needed. The Debezium image supports it natively. Other plugins like `decoderbufs` require separate installation in Postgres.

### Why Manual Kafka Offset Commits in the Sink?

Auto-commit marks a message as "done" as soon as it's read, regardless of whether your processing succeeded. If `sink.py` crashes between reading a message and writing to the database, that change would be **silently lost**.

Manual commits ensure the offset is only advanced after the database write succeeds, giving us **at-least-once delivery** (you might process a message twice after a crash, but you'll never skip one).

### Why UPSERT Instead of Separate INSERT/UPDATE Logic?

Using `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) makes the sink **idempotent** — running the same event twice produces the same result. This is essential for at-least-once delivery, where duplicate processing is possible after failures.

### Why Two Separate Tables (Source SERIAL vs Target INTEGER)?

The source uses `SERIAL` because the application inserts rows without specifying IDs. The target uses `INTEGER` because IDs arrive via CDC events and must not be auto-generated — they must match the source exactly.

---

## 13. Common Errors & Troubleshooting

### ❌ `Connection refused` when registering connector

**Symptom:** `curl: (7) Failed to connect to localhost port 8083`

**Cause:** Debezium hasn't finished starting up.

**Fix:** Wait 30-60 seconds after `docker compose up` and try again. Check logs:
```bash
docker logs cdc_connect
```

---

### ❌ Connector state is `FAILED`

**Symptom:** `curl http://localhost:8083/connectors/shop-postgres-connector/status` shows `"state": "FAILED"`

**Cause:** Usually the `customers` table doesn't exist yet, or the replication slot couldn't be created.

**Fix:**
1. Make sure you created the table in Step 2
2. Delete the failed connector and re-register:
```bash
curl -X DELETE http://localhost:8083/connectors/shop-postgres-connector
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @config/connector.json
```

---

### ❌ `sink.py` shows `UPSERT` but target table is empty

**Symptom:** Sink.py prints successful UPSERT operations but `SELECT * FROM customers` on target returns nothing.

**Cause:** The `customers` table doesn't exist in `shop_target` (Step 3 was skipped).

**Fix:** Create the target table as described in Step 3.

---

### ❌ `kafka-python` not found / Import error

**Symptom:** `ModuleNotFoundError: No module named 'kafka'`

**Fix:**
```bash
pip install kafka-python psycopg2-binary
```

---

### ❌ `psycopg2.OperationalError: connection refused` in sink.py

**Symptom:** Sink can't connect to target Postgres.

**Cause:** Either the container isn't running or the port is wrong.

**Fix:** Verify the target container is up:
```bash
docker ps | grep target
```
The sink connects to `localhost:5435` (not 5432 — that's the source!).

---

### ❌ No messages arriving in sink after changes

**Cause 1:** Connector isn't running. Check: `curl http://localhost:8083/connectors/shop-postgres-connector/status`

**Cause 2:** Consumer group already consumed all messages. The `earliest` reset only applies when the group has no committed offset. If you restart `sink.py` with the same group, it resumes from where it left off.

**Fix for replaying from beginning:** Delete the consumer group offsets (advanced) or change the `group_id` in `sink.py` to a new name.

---

*Built with ❤️ using PostgreSQL, Apache Kafka, and Debezium*
