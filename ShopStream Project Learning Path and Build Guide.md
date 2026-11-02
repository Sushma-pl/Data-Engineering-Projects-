# ShopStream Project: Learning Path and Build Guide

Oct 21, 2026 · @potus

This guide takes you from zero Kafka experience to a working change-data-capture pipeline in about 4 weeks, learning each piece in the order you need it.

## How to use this guide

Learn each tool just enough to build the next step, then build it. Do not finish all the theory first. Reading about Kafka for a week without running anything is the fastest way to forget it.

**What you are building (ShopStream):** a small shop database whose every change is captured and turned into clean tables.

```text
Postgres  ->  Debezium  ->  Kafka  ->  Spark Structured Streaming  ->  Delta tables on MinIO
(orders,      (reads the    (stores      (bronze: raw events,
 customers)    change log)   the events)  silver: current state, gold: daily revenue)
```

**The learning loop for every phase (use it every time)**

1. **Read** the concept notes in this guide (30-60 minutes).
2. **Run** the small exercise in the phase, on its own, outside the project.
3. **Break** it on purpose (stop a container, send bad data) and see what happens.
4. **Explain** it aloud in 2 minutes, as if to an interviewer. If you cannot, repeat step 2.
5. **Write** 5 lines in your build log: what you did, what broke, how you fixed it.

Only move to the next phase when you pass the "Done when" checks at the end of the current one.

**Time budget:** about 2 hours a day, 5 days a week, for 4 weeks. Keep your daily DSA and SQL practice separate and unchanged. If a phase takes longer than planned, extend the schedule. Do not skip the break and explain steps.

**Order you will follow**

| Phase | Topic | Days |
| --- | --- | --- |
| 0 | Docker and Docker Compose | 1-2 |
| 1 | Postgres write-ahead log and logical replication | 1 |
| 2 | Kafka from zero | 3-4 |
| 3 | Kafka Connect and Debezium | 2 |
| 4 | Delta Lake and MinIO | 1-2 |
| 5 | Spark Structured Streaming | 3-4 |
| 6 | Build the full pipeline, test it, write the README | 6-8 |

**Before you start, check these:**

- A laptop with at least 8 GB of free RAM (16 GB is better) and about 25 GB of free disk.
- Docker Desktop installed and running (`docker --version` works).
- Python 3.10 or newer, and a virtual environment tool (`python -m venv`).
- Git and a GitHub account. Commit from day one.
- A text editor such as VS Code.

**Honest caveat:** tool versions change often (Kafka, Debezium, Spark and Delta Lake). Commands and image names in this guide are the stable ideas. When an image tag or version number does not work, check the official documentation for the current one rather than copying from old blog posts. Matching Spark and Delta Lake versions is the most common source of errors, and it is covered in Phase 5.

## Phase 0: Docker and Docker Compose (1-2 days)

The whole project runs as 5-6 programs that must talk to each other. Docker lets you start all of them with one command, and throw them away and start again when something breaks.

### Concepts to learn

- **Image:** a packaged program with everything it needs (for example, the Postgres image).
- **Container:** a running copy of an image. You can stop it, delete it and start a fresh one.
- **Port mapping** (`-p 5433:5432`): connects a port on your laptop to a port inside the container. Left side is your laptop, right side is the container.
- **Volume:** storage that survives when a container is deleted. Without one, your data disappears.
- **Network:** containers in the same Docker Compose project can reach each other by **service name**, for example `postgres:5432`. From your laptop you use `localhost` and the mapped port. Mixing these up is the number one beginner error.
- **Environment variables:** how you pass settings (passwords, database names) to a container.
- **Docker Compose:** one YAML file that describes all your containers, networks and volumes. Commands: `docker compose up -d`, `docker compose ps`, `docker compose logs -f <service>`, `docker compose down`, `docker compose down -v` (also deletes volumes).

### Exercises

1. Run Postgres alone:

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16
docker exec -it pg psql -U postgres
```

Create a table and insert two rows. Then `docker rm -f pg` and run it again. Is your data still there? (No. Now repeat with `-v pgdata:/var/lib/postgresql/data` and compare.)

2. Write a `docker-compose.yml` with Postgres and `pgAdmin` or `adminer`. Open the admin page in your browser and connect using the service name as the host.
3. Stop one container with `docker stop` and watch the other react. Read its logs with `docker compose logs`.
4. Look at resource use with `docker stats`. Note how much memory each container takes. You will need this later to see how much room your laptop has.

### Break it on purpose

- Map the same host port twice and read the error.
- Give a wrong password and find the message in the logs.
- Delete a volume and see the data go.

### Done when

- You can explain image vs container vs volume in under a minute.
- You can explain why `localhost` inside a container is not your laptop.
- You can bring a stack up, read the logs of one service, and tear it down, without looking anything up.

### Resources

- Docker's official "Get started" guide (docs.docker.com), the sections on containers, volumes and Compose.
- The Compose file reference, only when you need to look up a key.

## Phase 1: Postgres basics for CDC (1 day)

You already know SQL. This phase covers only the Postgres internals that change data capture depends on.

### Concepts to learn

- **CDC (change data capture):** capturing every insert, update and delete from a database as an event, instead of repeatedly querying the table. Compare it with polling: polling misses deletes and intermediate updates, and puts load on the database. CDC reads the database's own change log.
- **WAL (write-ahead log):** Postgres writes every change to a log before applying it. Crash recovery and replication both use it. Debezium reads this log.
- **Logical decoding:** a feature that turns the WAL into readable row-level changes. It needs the setting `wal_level = logical`.
- **Replication slot:** a bookmark in the WAL that remembers how far a reader (Debezium) has read. Postgres keeps WAL files until the slot confirms them. **Warning:** if Debezium stops for a long time, the WAL grows and can fill your disk. This is a real production risk and a good interview point.
- **Publication:** the list of tables whose changes are exposed (the `pgoutput` plugin uses it).
- **Primary key and REPLICA IDENTITY:** to produce the "before" row for updates and deletes, Postgres needs a replica identity. With the default setting you get the key only. `ALTER TABLE orders REPLICA IDENTITY FULL;` includes the full old row, at a storage cost.

### Exercises

1. Start Postgres in Docker with logical replication on:

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16 -c wal_level=logical
```

2. Check it: `SHOW wal_level;` should return `logical`.
3. Create the tables you will use:

```sql
CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  city TEXT,
  updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  customer_id INT REFERENCES customers(id),
  amount NUMERIC(10,2),
  status TEXT,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);
```

4. Try logical decoding by hand:

```sql
SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
INSERT INTO customers (name, email, city) VALUES ('Asha', 'asha@x.com', 'Pune');
UPDATE customers SET city = 'Mumbai' WHERE name = 'Asha';
SELECT * FROM pg_logical_slot_get_changes('test_slot', NULL, NULL);
SELECT pg_drop_replication_slot('test_slot');
```

You will see each insert and update as a text event. This is exactly the information Debezium will turn into JSON.

5. Look at your slots and how much WAL they hold: `SELECT slot_name, active, restart_lsn FROM pg_replication_slots;`

### Break it on purpose

- Create a slot, make 10,000 inserts, and do not read from it. Check `pg_replication_slots` and the size of the WAL folder. This shows why unused slots are dangerous.
- Update a row, then compare the output with `REPLICA IDENTITY DEFAULT` and `FULL`.

### Done when

- You can explain WAL, replication slot and publication in your own words.
- You can explain why CDC beats polling a table, and what a replication slot can do to a disk.
- You saw an insert, update and delete as decoded events.

### Resources

- PostgreSQL documentation: "Logical Decoding" and "Write-Ahead Logging".
- Debezium's Postgres connector documentation, the section on setting up Postgres.

## Phase 2: Kafka from zero (3-4 days)

This is the biggest new topic, so it gets the most time. Learn the ideas first (day 1), then run every idea on your laptop (days 2-4).

### Day 1: Concepts

**What Kafka is:** a distributed, durable, append-only log. Producers write events to it, consumers read events from it, and events stay for a set time even after being read. It decouples the systems that produce data from those that consume it.

| Concept | Plain meaning | Why it matters for your project |
| --- | --- | --- |
| Event (record) | A key, a value, a timestamp, and optional headers | One database change = one event |
| Topic | A named stream of events, like a table of an endless log | One topic per Postgres table |
| Partition | A topic is split into partitions, each an ordered log | Gives parallelism. **Order is guaranteed only inside one partition** |
| Offset | The position of an event within a partition | How a consumer remembers where it stopped |
| Broker | A Kafka server | You will run one |
| Producer | Writes events to a topic | Debezium (through Kafka Connect) is your producer |
| Consumer | Reads events from a topic | Spark is your consumer |
| Consumer group | Consumers sharing the work of a topic, each partition read by one member of the group | Scaling reads, and tracking offsets per group |
| Key | Decides the partition (hash of the key) | Same key means same partition, so one row's changes stay in order |
| Retention | How long events are kept (time or size) | After it passes, old events are deleted |
| Log compaction | Keeps only the latest event per key | Useful to keep the current state of each row |
| Replication factor | Copies of each partition on different brokers | Fault tolerance. You use 1 locally |
| Leader and ISR | One replica leads each partition, and in-sync replicas can take over | How Kafka survives a broker loss |
| Lag | How far a consumer group is behind the latest offset | The main health metric for a pipeline |

**Producer settings that decide reliability**

- `acks=0`: do not wait. Fast, can lose data.
- `acks=1`: wait for the leader. Can lose data if the leader dies before copying.
- `acks=all`: wait for all in-sync replicas. Safest.
- **Idempotent producer** (`enable.idempotence=true`): retries do not create duplicates.

**Delivery semantics**

- **At most once:** may lose events, never duplicates.
- **At least once:** never loses events, may duplicate. Default in most pipelines, and the reason you will deduplicate in silver.
- **Exactly once:** each event processed once. Kafka transactions plus idempotent writes can achieve it inside Kafka. For an external sink like Delta, the practical way is **at-least-once delivery plus idempotent writes** (MERGE on a key). Be ready to say this in interviews.

**Consumers and offsets**

- A consumer commits the offset it has processed. If it crashes and restarts, it resumes from the last committed offset, so some events may be read again.
- `auto.offset.reset` = `earliest` or `latest`: where a new group starts.
- A **rebalance** happens when a consumer joins or leaves a group, and partitions are reassigned.

**KRaft vs ZooKeeper:** older Kafka needed a separate ZooKeeper cluster. Newer versions use **KRaft** and do not. Use a KRaft setup. If a tutorial asks for ZooKeeper, it is old.

### Days 2-4: Hands-on exercises

Run a single Kafka broker (check the Apache Kafka Docker documentation for the current image tag):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:latest
```

The command-line tools are inside the container at `/opt/kafka/bin/`.

**Exercise 1: topics**

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-orders --partitions 3 --replication-factor 1
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic test-orders
```

Read the output. Which broker leads each partition? What is the replication factor?

**Exercise 2: produce and consume from the console**

```bash
# Terminal A: producer with keys (type key:value lines)
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-orders --property parse.key=true --property key.separator=:

# Terminal B: consumer printing partition and offset
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

Send `1:a`, `2:b`, `1:c`, `3:d`, `1:e`. Which partition does key 1 always land on? Is the order for key 1 preserved?

**Exercise 3: consumer groups and lag**

Start a consumer with `--group g1`. Stop it, send 5 more events, then check:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group g1
```

Find the LAG column. Restart the consumer and watch the lag go to 0. Then start two consumers in the same group and see how the 3 partitions are shared. Start a third and a fourth. What happens to the extra ones?

**Exercise 4: Python producer and consumer**

```bash
pip install confluent-kafka
```

```python
# producer.py
import json, time
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092", "enable.idempotence": True, "acks": "all"})

def done(err, msg):
    if err: print("failed:", err)
    else: print(f"{msg.topic()}[{msg.partition()}]@{msg.offset()}")

for i in range(20):
    event = {"order_id": i % 5, "amount": 100 + i}
    p.produce("test-orders", key=str(event["order_id"]), value=json.dumps(event), callback=done)
    p.poll(0)
    time.sleep(0.2)
p.flush()
```

```python
# consumer.py
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "py-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
c.subscribe(["test-orders"])
try:
    while True:
        msg = c.poll(1.0)
        if msg is None: continue
        if msg.error(): print(msg.error()); continue
        print(msg.partition(), msg.offset(), msg.key(), msg.value())
        c.commit(msg)      # commit after processing = at-least-once
finally:
    c.close()
```

Run the producer, then the consumer. Change `enable.auto.commit` and when you call `commit`, and observe what happens when you kill the consumer after processing but before committing (duplicates on restart).

**Exercise 5: retention and compaction**

Create a topic with `--config retention.ms=60000` and watch old events disappear. Create another with `--config cleanup.policy=compact`, send several events with the same key, and consume from the beginning to see only the latest per key (compaction is not instant, so it may take a while and settings may need lowering).

**Exercise 6: ordering across partitions**

Send events for 3 different keys and consume them. Then create a topic with 1 partition and repeat. In which case is the global order preserved? Why do you not get a global order with more partitions?

### Break it on purpose

- Stop the Kafka container while the Python producer runs. What does the producer do? (It retries up to its timeout.)
- Delete a topic that a consumer is reading.
- Send an event bigger than the default limit (about 1 MB) and read the error.

### Done when

- You can explain topic, partition, offset, consumer group and lag with an example from your exercises.
- You can explain why order is only guaranteed per partition, and how a key preserves the order for one row.
- You can explain at-least-once delivery and why you deduplicate downstream.
- You can explain what `acks=all` and idempotence do.
- You have run a producer and consumer in Python, and checked lag from the command line.

### Resources

- **Confluent Developer: "Kafka 101"** course (developer.confluent.io), free short videos that match days 1-2.
- **Apache Kafka documentation:** the "Introduction" and "Design" sections.
- **Designing Data-Intensive Applications**, chapter 11 (stream processing), the parts on logs, CDC and event sourcing. You already planned to read this.

## Phase 3: Kafka Connect and Debezium (2 days)

Debezium is the program that reads the Postgres change log and writes events to Kafka. It runs inside **Kafka Connect**, so you learn the two together.

### Concepts to learn

- **Kafka Connect:** a framework that moves data between Kafka and other systems without you writing producer or consumer code. You configure it with JSON through a REST API (port 8083 by default).
- **Source connector** reads from an external system into Kafka (Debezium is a source connector). A **sink connector** does the reverse.
- **Worker, connector, task:** a worker is the Connect process, a connector is one configured job, and tasks do the actual work. The Postgres connector uses one task.
- **Converters:** decide how keys and values are serialized. `JsonConverter` gives readable JSON. With `schemas.enable=true` every message carries its schema, which makes events very large and wraps your data in `schema` and `payload` fields. For learning, turn it off.
- **Connect's internal topics:** it stores its own configs, offsets and status in Kafka topics. Offsets are how it remembers where it stopped in the WAL.

**Debezium event structure**

Every change becomes an event with an envelope:

```json
{
  "before": null,
  "after": { "id": 1, "name": "Asha", "city": "Pune" },
  "source": { "connector": "postgresql", "db": "shop", "schema": "public", "table": "customers", "ts_ms": 1710000000000, "lsn": 12345678 },
  "op": "c",
  "ts_ms": 1710000000100
}
```

| `op` | Meaning | `before` | `after` |
| --- | --- | --- | --- |
| `r` | Read, from the initial snapshot | null | Row |
| `c` | Create (insert) | null | Row |
| `u` | Update | Old row (depends on replica identity) | New row |
| `d` | Delete | Old row | null |

- **Snapshot then stream:** on first start, Debezium reads the current contents of the tables (`op = r`), then continues from the WAL. Know this, because your first events will be snapshot reads.
- **Message key:** the primary key of the row. This keeps all changes to one row in one partition, in order.
- **Tombstone events:** after a delete event, Debezium by default sends a second message with the same key and a null value. It lets log compaction remove the key. Your Spark code must cope with null values.
- **Topic names:** `<topic.prefix>.<schema>.<table>`, for example `shop.public.orders`.
- **Gotchas with types:** Postgres `NUMERIC` becomes a base64-encoded binary by default. Set `decimal.handling.mode` to `string` or `double` to get readable numbers. Timestamps arrive as numbers (microseconds since 1970 by default), so convert them in Spark.

### Setting up the stack

Use the official **Debezium tutorial** (debezium.io, documentation, tutorial) as your starting point for the Docker Compose file, because image names and versions change. It gives you Kafka, Kafka Connect with the Debezium plugins, and Postgres. Then extend it with your own tables from Phase 1. Make sure Postgres starts with `wal_level=logical`.

Once Connect is running, check it:

```bash
curl -s localhost:8083/ | head
curl -s localhost:8083/connector-plugins | grep -i postgres
```

### Register the connector

Save this as `connector.json` (values are examples; match them to your Compose file):

```json
{
  "name": "shop-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "shop",
    "topic.prefix": "shop",
    "plugin.name": "pgoutput",
    "slot.name": "shop_slot",
    "table.include.list": "public.customers,public.orders",
    "decimal.handling.mode": "string",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

Then:

```bash
curl -s -X POST -H "Content-Type: application/json" --data @connector.json localhost:8083/connectors
curl -s localhost:8083/connectors/shop-connector/status
```

The status should show `RUNNING` for the connector and its task. If it shows `FAILED`, read the `trace` field and `docker compose logs connect`.

### Exercises

1. List the topics (`kafka-topics.sh --list`) and find `shop.public.customers` and `shop.public.orders`.
2. Consume `shop.public.customers` from the beginning. Identify the snapshot events (`op = r`).
3. In Postgres, insert a customer, update their city, and delete them. Find the three events and one tombstone. Compare `before` and `after` for each.
4. Run `ALTER TABLE orders REPLICA IDENTITY FULL;`, update an order, and compare the `before` field with what you had earlier.
5. Pause Connect (`docker compose stop connect`), make 20 changes in Postgres, then start Connect again. Do you get all 20 events? Why? (The slot kept the WAL, and Connect resumes from its stored offset.)
6. Check `SELECT slot_name, active, restart_lsn FROM pg_replication_slots;` while Connect is stopped and after it restarts.
7. Delete the connector (`curl -X DELETE localhost:8083/connectors/shop-connector`) and see that the slot is dropped (or not, depending on settings). Why does this matter for disk space?

### Break it on purpose

- Register the connector with a wrong password, and find the error.
- Add a column to `orders` while the connector runs, and look at the next event.
- Leave Connect stopped for a while with heavy writes, and watch the WAL grow.

### Done when

- You can explain the flow Postgres WAL, replication slot, Debezium, Connect, Kafka topic.
- You can read a Debezium event and say what happened to the row.
- You can explain snapshot vs streaming, tombstones, and what happens if Connect is down for a day.
- You know the three type gotchas (decimals, timestamps, schema wrapper).

### Resources

- **Debezium documentation:** the "Tutorial" and the "PostgreSQL connector" pages (especially the sections on events, data types and deployment).
- **Kafka Connect documentation** (kafka.apache.org, "Kafka Connect"), the overview only.
- Confluent Developer's short Kafka Connect course, if you want video.

## Phase 4: Delta Lake and MinIO (1-2 days)

You have used Delta at work, so this phase is about running it yourself, outside Databricks, and understanding what it does under the hood.

### Concepts to learn

- **Object storage and the S3 API:** cloud storage that holds files in buckets, accessed over HTTP. **MinIO** is a free S3-compatible server you run in Docker, so your laptop acts like a small cloud.
- **Delta Lake** = Parquet data files + a **transaction log** (`_delta_log/` folder of JSON files). Every write adds a new numbered log file that lists which data files were added or removed. That log gives you:
  - **ACID transactions** on files (readers never see half-written data)
  - **Time travel** (read the table as of an earlier version)
  - **Schema enforcement** (a write with the wrong schema fails) and **schema evolution** (`mergeSchema`)
  - **MERGE, UPDATE, DELETE** on files that are normally immutable (it rewrites the affected files)
- **Checkpoints:** every 10 commits Delta writes a checkpoint file so readers do not replay the whole log.
- **Maintenance:** `OPTIMIZE` compacts small files and `VACUUM` deletes old unreferenced files. Streaming writes create many small files, so this matters for your project.
- **Bronze, silver, gold (medallion):** bronze holds raw events as received, silver holds cleaned and de-duplicated current state, gold holds business aggregates.

### Setup

Start with Delta on your **local disk**. This avoids the hardest setup problem and lets you make progress. Add MinIO afterward, and treat it as optional if it blocks you for more than half a day.

```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark delta-spark
```

**Version rule:** each `delta-spark` release works with specific PySpark versions. Check the compatibility table in the Delta Lake documentation (docs.delta.io) and install matching versions, for example by pinning `pyspark==X.Y.Z` and the `delta-spark` version listed for it. A mismatch gives confusing Java errors.

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (SparkSession.builder.appName("delta-lab")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

### Exercises (local path first)

1. Write a small DataFrame as Delta, then read it back:

```python
df = spark.createDataFrame([(1, "Asha", "Pune"), (2, "Ravi", "Delhi")], ["id", "name", "city"])
df.write.format("delta").mode("overwrite").save("/tmp/lab/customers")
spark.read.format("delta").load("/tmp/lab/customers").show()
```

2. Open the folder `/tmp/lab/customers/_delta_log/` and read the first JSON file. Find the `add` entries and the Parquet file names.
3. Update and delete rows with `DeltaTable`, then look at the log again. What new entries appear?

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/tmp/lab/customers")
dt.update("id = 1", {"city": "'Mumbai'"})
dt.delete("id = 2")
dt.history().select("version", "operation", "operationParameters").show(truncate=False)
```

4. Time travel: `spark.read.format("delta").option("versionAsOf", 0).load(path)`.
5. MERGE (you will use this in silver):

```python
updates = spark.createDataFrame([(1, "Asha", "Chennai"), (3, "Meera", "Goa")], ["id", "name", "city"])
(dt.alias("t").merge(updates.alias("s"), "t.id = s.id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())
```

6. Schema enforcement: try to append a DataFrame with an extra column and read the error. Repeat with `.option("mergeSchema", "true")`.
7. Make 20 tiny appends, count the files in the folder, then run `dt.optimize().executeCompaction()` and count again. This shows the small-file problem.

### Exercises (MinIO, optional second step)

Start MinIO:

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

Open `http://localhost:9001`, log in, and create a bucket named `lake`. To let Spark write to it you need the `hadoop-aws` jar (matching your Spark's Hadoop version) and these settings:

```python
.config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
.config("spark.hadoop.fs.s3a.access.key", "minioadmin")
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

Then write to `s3a://lake/customers`. This setup is the most version-sensitive part of the project. If the jar versions do not match, you get `ClassNotFoundException` or `NoSuchMethodError`. Look up the Hadoop version bundled with your Spark and use the same `hadoop-aws` version. If it takes too long, continue with a local folder mounted as a Docker volume and come back to MinIO later.

### Break it on purpose

- Delete one Parquet file from the table folder by hand, and read the table.
- Run two writers at once on the same table and see what the conflict message says.
- Run `VACUUM` with a short retention and then try to time travel to an old version.

### Done when

- You can explain how the transaction log gives ACID on plain files.
- You have done an update, delete, MERGE and time travel, and looked at the log after each.
- You can explain schema enforcement vs evolution, and why streaming creates small files.
- You can explain bronze vs silver vs gold in one sentence each.

### Resources

- **Delta Lake documentation** (docs.delta.io): quickstart, "Table batch reads and writes", "Table deletes, updates, and merges".
- The blog post and paper "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores", for the design behind the log.
- The MinIO quickstart in its documentation.

## Phase 5: Spark Structured Streaming (3-4 days)

You know Spark batch. Streaming uses the same DataFrame API on data that never stops arriving. Learn the new ideas, then run each one on small examples before touching Debezium data.

### Concepts to learn

- **Streaming DataFrame:** an unbounded table. Spark treats new data as new rows appended to that table and runs your query repeatedly.
- **Micro-batch:** by default, Spark processes data in small batches. Each batch has an id. This is why you can use batch tools such as MERGE inside `foreachBatch`.
- **Triggers:** when a batch runs. `processingTime="10 seconds"` runs every 10 seconds. `availableNow=True` processes everything available and stops, which is excellent for tests.
- **Output modes:** `append` (only new rows), `update` (rows that changed), `complete` (whole result table, for small aggregates).
- **Checkpoint location:** a folder where Spark stores which offsets it has processed and any state. **Every query that writes needs its own checkpoint folder.** If you delete it, the query starts again from scratch.
- **Exactly-once in Structured Streaming:** a replayable source (Kafka offsets) plus a checkpoint plus an idempotent sink gives effectively exactly-once results. A crash replays the last batch, so the write must be safe to repeat (MERGE on a key, or Delta's own batch-id tracking).
- **Event time vs processing time:** event time is when the thing happened (in the data), processing time is when Spark sees it. Windowed counts should use event time.
- **Watermark:** `withWatermark("ts", "10 minutes")` tells Spark how late data may arrive. Events older than the watermark are dropped, which lets Spark discard old state and not grow memory forever. It is the trade-off between completeness and resources.
- **Stateful operations:** aggregations, windows, `dropDuplicates` and stream-stream joins keep state. State is stored with the checkpoint, and without a watermark it can grow forever.
- **Kafka source columns:** `key`, `value` (both binary), `topic`, `partition`, `offset`, `timestamp`. You cast and parse `value` yourself.
- **Useful options:** `startingOffsets` (`earliest` or `latest`, used only the first time), `maxOffsetsPerTrigger` (limits batch size), `failOnDataLoss` (what to do if Kafka deleted data Spark still needed).

### Setup for the Kafka source

Spark needs the Kafka connector package, and its version must match your Spark and Scala versions.

```python
spark = (SparkSession.builder.appName("stream-lab")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:<your Spark version>")
    .getOrCreate())
```

Check your versions with `pyspark --version`. The suffix `_2.12` or `_2.13` must match the Scala version printed there. When you also need Delta, include both packages (Delta's documentation shows how).

### Exercises

**1. The rate source (no Kafka needed)**

```python
df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
q = (df.writeStream.format("console").outputMode("append")
       .trigger(processingTime="5 seconds").start())
```

Watch the batches. Stop with `q.stop()`. Try `availableNow=True` on a file source.

**2. A file source**

Stream JSON files dropped into a folder:

```python
stream = spark.readStream.schema("id INT, amount INT").json("/tmp/lab/incoming")
q = stream.writeStream.format("console").option("checkpointLocation", "/tmp/lab/ckpt1").start()
```

Copy new JSON files into the folder and watch only the new ones get processed. Restart the query and check that old files are not processed again.

**3. Kafka source, parsed**

Use the `test-orders` topic from Phase 2.

```python
from pyspark.sql import functions as F, types as T

schema = T.StructType([T.StructField("order_id", T.IntegerType()), T.StructField("amount", T.IntegerType())])
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test-orders")
    .option("startingOffsets", "earliest").load())

parsed = (raw.select(F.col("key").cast("string").alias("key"),
                     F.from_json(F.col("value").cast("string"), schema).alias("v"),
                     "partition", "offset", "timestamp")
             .select("key", "v.*", "partition", "offset", "timestamp"))

q = (parsed.writeStream.format("console")
       .option("checkpointLocation", "/tmp/lab/ckpt-kafka").start())
```

Run your Python producer from Phase 2 and watch the rows appear.

**4. Checkpoint and restart**

Stop the query, send 10 more events, restart it with the same checkpoint folder. Does it process only the 10 new events? Now delete the checkpoint folder and restart. What changes? Look inside the checkpoint folder (`offsets`, `commits`, `state`).

**5. Windows and watermarks**

```python
agg = (parsed.withWatermark("timestamp", "2 minutes")
             .groupBy(F.window("timestamp", "1 minute"), "order_id")
             .agg(F.sum("amount").alias("total")))
q = agg.writeStream.outputMode("update").format("console").option("checkpointLocation", "/tmp/lab/ckpt-win").start()
```

With `append` mode a window only appears after the watermark passes it. With `update` you see results early. Send an event with an old timestamp and see whether it is dropped.

**6. Deduplication with a watermark**

```python
deduped = parsed.withWatermark("timestamp", "10 minutes").dropDuplicates(["event_id"])
```

Add a unique \`event\_id\` field to the JSON your producer sends and to the schema, then send the same event twice and watch the second one disappear. Duplicates in Kafka have different offsets, so you must deduplicate on a business or event id, never on the offset. Think about why this needs state.

**7. foreachBatch and MERGE into a local Delta table**

This is the pattern for your silver layer.

```python
from delta.tables import DeltaTable

def upsert(batch_df, batch_id):
    latest = (batch_df
        .withColumn("rn", F.row_number().over(Window.partitionBy("order_id").orderBy(F.col("offset").desc())))
        .filter("rn = 1").drop("rn"))
    target = DeltaTable.forPath(spark, "/tmp/lab/orders_silver")
    (target.alias("t").merge(latest.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

(parsed.writeStream.foreachBatch(upsert)
    .option("checkpointLocation", "/tmp/lab/ckpt-merge").start())
```

Create the Delta target first (an empty table with the right schema). Import `Window` from `pyspark.sql`. Note why you keep only the latest row per key inside each batch: MERGE fails if several source rows match one target row.

**8. Rate limiting**

Add `.option("maxOffsetsPerTrigger", 5)` and watch batch sizes. Why would you use this when catching up on a large backlog?

### Break it on purpose

- Kill the Spark process in the middle of a batch (`kill -9`) and restart. Check for duplicates and gaps.
- Change the query (add a column) and restart with the old checkpoint. Read the error. Some changes need a new checkpoint, and you must know which.
- Stop Kafka, restart it, and see how the query reacts.
- Remove old topic data (short retention) while Spark is stopped, and read the `failOnDataLoss` error.

### Done when

- You can explain micro-batch, trigger, output mode, checkpoint, watermark and state in your own words.
- You can explain why a crash does not lose or double-count data (replayable source, checkpoint, idempotent sink).
- You have a working Kafka to Delta MERGE stream and have restarted it safely.
- You can explain what happens to events that arrive after the watermark.

### Resources

- **Spark Structured Streaming Programming Guide** (spark.apache.org), especially the sections on the programming model, output modes, window operations on event time, watermarking and fault tolerance.
- **Structured Streaming + Kafka Integration Guide** (same site).
- Databricks' free Structured Streaming material on Databricks Academy, if you want videos.
- *Spark: The Definitive Guide*, the streaming chapters.

## Phase 6: Build the pipeline, step by step (6-8 days)

By now you have run every piece on its own. Now you join them. Work in a Git repository and commit after every step. Use this folder layout:

```text
shopstream/
  docker-compose.yml
  connector/connector.json
  generator/generate_changes.py
  spark/
    bronze_stream.py
    silver_stream.py
    gold_batch.py
  tests/test_reconciliation.py
  docs/build-log.md
  README.md
```

### Step 1: The stack (Compose, Postgres, Kafka, Connect, Spark)

1. Start from the Debezium tutorial's Compose file (Kafka, Kafka Connect with Debezium, Postgres). Check its current image tags in the documentation.
2. Make sure Postgres starts with `wal_level=logical`, and create a database named `shop` with the `customers` and `orders` tables from Phase 1 (the connector in Phase 3 points at `shop`).
3. **Decide where Spark runs.** This decision causes most beginner network errors.
   - **Option A (simplest):** run Spark in a container on the same Docker network as Kafka, so it reaches Kafka at `kafka:<internal port>`.
   - **Option B:** run Spark on your laptop. Then Kafka needs a second listener that advertises `localhost:9092` for outside clients, while Connect keeps using the internal address. Look up "Kafka advertised listeners" in the Kafka docs. The symptom of a wrong setup is Spark connecting, then hanging or failing on a hostname it cannot resolve.
4. Optionally add MinIO. Until it works, write Delta tables to a local folder or Docker volume (for example `/data/lake/bronze`).

**Done when:** `docker compose up -d` brings everything up, `docker compose ps` shows all services running, you can open a SQL shell in Postgres, list Kafka topics, and `curl localhost:8083/connectors` answers.

### Step 2: The change generator

Write a Python script that keeps changing the data, so you always have events to look at.

```python
import random, time
import psycopg2

conn = psycopg2.connect(host="localhost", port=5433, dbname="shop", user="postgres", password="postgres")
conn.autocommit = True
cur = conn.cursor()

cities = ["Pune", "Mumbai", "Delhi", "Chennai", "Hyderabad"]
statuses = ["created", "paid", "shipped", "delivered", "cancelled"]

for i in range(20):
    cur.execute("INSERT INTO customers (name, email, city) VALUES (%s, %s, %s)",
                (f"Customer {i}", f"c{i}@example.com", random.choice(cities)))

while True:
    action = random.choices(["insert", "update", "delete"], weights=[6, 3, 1])[0]
    if action == "insert":
        cur.execute("INSERT INTO orders (customer_id, amount, status) VALUES (%s, %s, 'created')",
                    (random.randint(1, 20), round(random.uniform(100, 5000), 2)))
    elif action == "update":
        cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)",
                    (random.choice(statuses),))
    else:
        cur.execute("DELETE FROM orders WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)")
    time.sleep(random.uniform(0.1, 1.0))
```

Install with `pip install psycopg2-binary`. Add command-line options later (rate of changes, number of changes), so you can run a burst of 10,000 changes for the performance test.

**Done when:** you can run it, stop it, and see rows changing in Postgres.

### Step 3: Register the Debezium connector

Use the `connector.json` and the `curl` commands from Phase 3, with the table list `public.customers,public.orders`.

**Done when:** the connector status is `RUNNING`, the topics `shop.public.customers` and `shop.public.orders` exist, and the console consumer shows `r` events first and then `c`, `u` and `d` events as the generator runs. Save three sample events in `docs/` for your README.

### Step 4: Bronze, raw events into Delta

Bronze stores events exactly as received, plus Kafka metadata. **Do not parse or clean anything here.** If your parsing logic has a bug later, you can replay from bronze without losing data.

```python
from pyspark.sql import SparkSession, functions as F

BRONZE = "/data/lake/bronze/orders"
CKPT = "/data/checkpoints/bronze_orders"

raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")        # adjust to your listener setup
    .option("subscribe", "shop.public.orders")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load())

bronze = raw.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),          # null for tombstones
    "topic", "partition", "offset",
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("ingested_at"))

q = (bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CKPT)
    .trigger(processingTime="10 seconds")
    .start(BRONZE))
```

Run the generator, wait a minute, then read the table in a notebook or batch job and count rows. Check that `(partition, offset)` pairs are unique.

**Done when:** bronze keeps growing while the generator runs, tombstone rows have a null `value`, and you can explain why you keep `partition` and `offset`.

**Write in your build log:** what the Kafka listener problem looked like if you hit it, and how you fixed it.

### Step 5: Silver, the current state of each order

Silver reads bronze, understands the Debezium envelope, and keeps **one row per order** that matches Postgres. Create the target table first:

```python
SILVER = "/data/lake/silver/orders"
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS delta.`{SILVER}` (
    id INT, customer_id INT, amount DECIMAL(10,2), status STRING,
    updated_at TIMESTAMP, lsn BIGINT
  ) USING DELTA
""")
```

Define the shape of the events (matching the connector settings from Phase 3, where `NUMERIC` arrives as a string and timestamps as microseconds):

```python
from pyspark.sql import functions as F, types as T, Window
from delta.tables import DeltaTable

order = T.StructType([
    T.StructField("id", T.IntegerType()),
    T.StructField("customer_id", T.IntegerType()),
    T.StructField("amount", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("created_at", T.LongType()),
    T.StructField("updated_at", T.LongType()),
])
envelope = T.StructType([
    T.StructField("before", order),
    T.StructField("after", order),
    T.StructField("op", T.StringType()),
    T.StructField("source", T.StructType([T.StructField("lsn", T.LongType())])),
])
```

A pure function that turns raw bronze rows into clean change rows. Keep it separate so you can unit test it:

```python
def parse_changes(df):
    e = F.from_json(F.col("value"), envelope)
    return (df.where(F.col("value").isNotNull())            # tombstones carry no value
        .select(e.alias("e"))
        .select(
            F.col("e.op").alias("op"),
            F.col("e.source.lsn").alias("lsn"),
            F.coalesce(F.col("e.after.id"), F.col("e.before.id")).alias("id"),
            F.col("e.after.customer_id").alias("customer_id"),
            F.col("e.after.amount").cast("decimal(10,2)").alias("amount"),
            F.col("e.after.status").alias("status"),
            F.expr("timestamp_micros(e.after.updated_at)").alias("updated_at")))
```

The upsert applied to each micro-batch:

```python
def upsert_orders(batch_df, batch_id):
    changes = parse_changes(batch_df)
    w = Window.partitionBy("id").orderBy(F.col("lsn").desc())
    latest = changes.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    target = DeltaTable.forPath(spark, SILVER)
    (target.alias("t").merge(latest.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.op = 'd'")
        .whenMatchedUpdate(
            condition="s.op != 'd' AND s.lsn > t.lsn",
            set={"customer_id": "s.customer_id", "amount": "s.amount", "status": "s.status",
                 "updated_at": "s.updated_at", "lsn": "s.lsn"})
        .whenNotMatchedInsert(
            condition="s.op != 'd'",
            values={"id": "s.id", "customer_id": "s.customer_id", "amount": "s.amount",
                    "status": "s.status", "updated_at": "s.updated_at", "lsn": "s.lsn"})
        .execute())

q = (spark.readStream.format("delta").load(BRONZE)
    .writeStream.foreachBatch(upsert_orders)
    .option("checkpointLocation", "/data/checkpoints/silver_orders")
    .trigger(processingTime="10 seconds").start())
```

**Why each design choice exists (say these in interviews)**

- Reading from bronze, not Kafka, lets you rebuild silver any time by deleting its checkpoint and table.
- Keeping only the latest row per key per batch avoids the MERGE error "multiple source rows matched".
- `s.lsn > t.lsn` makes the update **idempotent**: replaying a batch after a crash cannot move a row backwards.
- Delete events use `before.id`, because `after` is null.

**Done when:** `SELECT count(*), sum(amount) FROM orders` in Postgres equals the same query on silver after the generator has been paused and the stream has caught up. Then repeat the same check with the generator running, and understand why numbers can differ by a few rows while data is still in flight.

### Step 6: Failure experiments (the most valuable step)

Run each experiment with the generator running, and compare Postgres to silver afterward. Write the result of each in your build log.

1. **Kill the silver query** (`kill -9`) in the middle of a batch. Restart it with the same checkpoint. Expect no missing rows and no duplicates.
2. **Stop Kafka Connect** for 5 minutes while the generator runs, then start it. Expect the connector to catch up from its stored offset.
3. **Stop Kafka** briefly and restart it.
4. **Rebuild silver from bronze:** delete the silver checkpoint and the silver table, recreate the table, and start the stream. Expect the same final result.
5. **Pause Spark for a long time** while the generator runs, then restart. Note the batch sizes as it catches up (try `maxOffsetsPerTrigger`).
6. **Watch the replication slot** in Postgres during experiment 2.

**Done when:** for every experiment you can say what broke, what the system did on its own, what you had to do by hand, and why the data stayed correct.

### Step 7: Ordering, late and duplicate events

- Kafka keeps order **per key within a partition**. Debezium keys events by primary key, so changes to one order arrive in order. Ordering across different orders is not guaranteed, and you do not need it.
- Debezium events arrive in WAL commit order, and `lsn` is your tie-breaker. Do not use `updated_at` to decide which version is newer, because application code can set it to anything.
- Test duplicates by replaying an old bronze batch into silver by hand. Expect no change in the result (thanks to `s.lsn > t.lsn`).
- Late data matters for time-based gold aggregates. If you build a streaming windowed aggregate, use a watermark and write down the lateness you chose and why.

### Step 8: Schema evolution

1. While everything runs, add a column in Postgres: `ALTER TABLE orders ADD COLUMN coupon TEXT;` and update a few orders with a coupon.
2. Look at new Debezium events. The `after` object has a new field.
3. Check what silver does. With an explicit schema in `from_json`, the new field is ignored and the pipeline keeps running. Bronze still holds the full raw event, so nothing is lost.
4. Add `coupon` to the `order` schema, add the column to silver (`ALTER TABLE ... ADD COLUMNS (coupon STRING)`) and to the MERGE, restart, and **backfill** by replaying bronze.
5. Try harder changes: rename a column, change a type, drop a column. Note what breaks and how you would roll these out safely.

**Done when:** you can describe a safe rollout for an added column and for a type change, and explain why keeping raw data in bronze makes both recoverable.

### Step 9: Gold, tests and data quality

**Gold** is a batch job over silver. Start with daily revenue:

```sql
SELECT date(updated_at) AS day,
       count(*)          AS orders,
       sum(amount)       AS revenue
FROM delta.`/data/lake/silver/orders`
WHERE status <> 'cancelled'
GROUP BY date(updated_at)
```

Write it to a Delta table with overwrite. Repeat the silver pattern for `customers`, then add a second gold table joining orders and customers by city.

**Tests with pytest** (`tests/`):

1. **Unit test for `parse_changes`:** build a small DataFrame with sample events (create, update, delete, tombstone) and assert on the output rows.
2. **Reconciliation test:** pause the generator, wait for the stream to catch up, then assert Postgres and silver agree.

```python
def test_silver_matches_postgres(pg_cursor, spark):
    pg_cursor.execute("SELECT count(*), coalesce(sum(amount), 0) FROM orders")
    pg_count, pg_sum = pg_cursor.fetchone()
    s = spark.read.format("delta").load(SILVER).agg(F.count("*"), F.sum("amount")).first()
    assert pg_count == s[0]
    assert pg_sum == s[1]
```

3. **Quality checks:** no null or duplicate `id` in silver, `amount` greater than 0, status in the allowed list. Send bad rows to a **quarantine** table instead of dropping them (you already did this in your NYC Taxi project).
4. **Monitoring:** log `q.lastProgress` for each query (`numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, `batchId`), and track Debezium lag by comparing the latest Kafka offsets with what bronze has consumed.

### Step 10: Measure, then write up

1. Run a **burst test**: let the generator write 50,000 changes as fast as it can. Record how long the pipeline takes to catch up, and rows per second at each stage.
2. Measure **end-to-end latency** at a steady rate: the time from the Postgres commit (`source.ts_ms`) to the row appearing in silver.
3. Run `OPTIMIZE` on bronze and silver, and compare file counts and read time before and after.
4. Write the README using the template in the last section, with these numbers in it.

**Done when:** you have a repository someone else can start with one command, a README with an architecture diagram and measured results, and a build log with every failure you hit.

## 4-week schedule (20 working days, about 2 hours each)

This assumes you also keep your daily DSA practice. If a day takes longer, shift the rest and use the weekend as a buffer. Never skip the "teach-back" (explain aloud in 2 minutes) and the build log entry at the end of a day.

| Day | Focus | What to do | Done when |
| --- | --- | --- | --- |
| 1 | Docker | Concepts, exercise 1 (run Postgres, test volumes) | You can explain image, container, volume, port mapping |
| 2 | Docker | Exercises 2-4, a Compose file with Postgres and an admin UI, break-it list | You can bring a stack up, read logs, tear it down |
| 3 | Postgres for CDC | WAL, replication slot, `wal_level`, logical decoding exercises, replica identity | You saw insert, update, delete as decoded events |
| 4 | Kafka concepts | Read the concept table, Kafka 101 videos, run a broker, topic exercises 1-2 | You can explain topic, partition, offset, key |
| 5 | Kafka practice | Consumer groups and lag (exercise 3), Python producer and consumer (exercise 4) | You produced, consumed and checked lag |
| 6 | Kafka finish | Retention, compaction, ordering (exercises 5-6), break-it list, teach-back | You can explain at-least-once and why order is per partition |
| 7 | Debezium setup | Bring up the Debezium tutorial stack, create the `shop` database, register the connector | Connector `RUNNING`, topics exist |
| 8 | Debezium practice | Exercises 1-7, read events for insert, update, delete, tombstone, restart Connect | You can read an event and explain snapshot vs streaming |
| 9 | Delta local | Exercises 1-5 (write, log, update, delete, time travel, MERGE) | You looked inside `_delta_log` after each change |
| 10 | Delta finish | Exercises 6-7, small files and OPTIMIZE, MinIO if it works, teach-back | You can explain how the log gives ACID |
| 11 | Streaming basics | Rate source, file source, Kafka source parsed (exercises 1-3) | A Kafka stream prints parsed rows |
| 12 | Streaming state | Checkpoint and restart, windows and watermarks, dedup (exercises 4-6) | You can explain checkpoint and watermark |
| 13 | Streaming to Delta | foreachBatch MERGE, rate limiting, break-it list (exercises 7-8) | A Kafka to Delta MERGE stream restarts safely |
| 14 | Build 1-2 | Repository, Compose stack, Postgres tables, change generator | Everything starts, rows change continuously |
| 15 | Build 3-4 | Register the connector, bronze stream | Bronze grows while the generator runs |
| 16 | Build 5 | Silver with parse, dedupe, MERGE, deletes | Silver matches Postgres when paused |
| 17 | Build 6 | Failure experiments 1-6, write results in the build log | Each experiment explained |
| 18 | Build 7-8 | Ordering and duplicate tests, schema evolution and backfill | You can describe safe schema rollout |
| 19 | Build 9 | Gold tables, pytest tests, quarantine, monitoring | Tests pass, a mismatch is caught |
| 20 | Build 10 | Burst and latency tests, OPTIMIZE, README, diagram, publish to GitHub | Repo public, README with numbers |

**Weekly check, every Friday (15 minutes)**

- Can you explain, without notes, each topic from this week in 2 minutes?
- Does your build log have an entry for each day?
- Is everything committed to Git?
- What took longer than planned, and why?

**If you fall behind:** drop MinIO and use local volumes, drop the optional exercises (Kafka compaction, rate limiting), and keep the failure experiments. The failure experiments are what you will talk about in interviews.

**After the project:** post the repository on LinkedIn with a 5-line summary, add 3 bullets to your resume with the measured numbers, and use the repository link in your referral messages.

## Debugging checklist

**Method for any problem: follow the data one hop at a time.** Is the change in Postgres? Is the event in Kafka? Is the row in bronze? Is it in silver? The first hop where data is missing tells you which component is at fault. Read that component's logs before changing anything.

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| Spark connects to Kafka, then hangs or fails on a hostname | Kafka advertised listeners do not match where Spark runs | Run Spark in the same Docker network, or add an external listener (Step 1) |
| `Failed to find data source: kafka` | Kafka connector package missing | Add `spark-sql-kafka-0-10` with your Spark and Scala versions |
| `ClassNotFoundException` or `NoSuchMethodError` | Version mismatch between Spark, Delta, Kafka package or hadoop-aws | Check each compatibility table, pin exact versions |
| Connector status `FAILED` | Wrong password, `wal_level` not `logical`, slot or publication problem | `curl .../status` and read `trace`, then `docker compose logs connect` |
| Change in Postgres but no event in Kafka | Table not in `table.include.list`, connector stopped, or reading the wrong topic or offset | Connector status, topic list, use `--from-beginning` |
| Amounts look like random characters | `NUMERIC` encoded as bytes | Set `decimal.handling.mode` to `string` |
| Timestamps are huge numbers | Microseconds since 1970 | Convert with `timestamp_micros` |
| MERGE fails with "multiple source rows matched" | Several changes for one key in the same batch | Keep only the latest per key before MERGE |
| Stream reprocesses everything after restart | Checkpoint folder missing or a different path | Use a fixed checkpoint path per query |
| Stream fails after you changed the code | Checkpoint is incompatible with the new query | Use a new checkpoint and replay from bronze |
| Postgres disk fills up | Replication slot not being read | `pg_replication_slots`, restart Connect or drop the unused slot |
| Silver has more rows than Postgres | Deletes not applied | Check delete handling and tombstones |
| Silver has fewer rows than bronze events imply | Parse returns nulls, so rows are skipped | Compare `parse_changes` output on a sample, check the schema |
| Docker or Spark runs out of memory | Too little memory for all services | Raise Docker memory, set `spark.sql.shuffle.partitions` to about 8 locally |

## README template

A good README makes the project easy to judge in 5 minutes. Use these headings:

1. **Title and one-line summary:** what the pipeline does and why it matters.
2. **Architecture diagram:** draw it with draw.io or Excalidraw and export it as an image.
3. **Tech stack:** Postgres, Debezium, Kafka, Spark Structured Streaming, Delta Lake, MinIO, Docker, pytest.
4. **How to run:** three commands or fewer, plus a note on needed RAM.
5. **Data model:** what bronze, silver and gold hold, with a sample record from each.
6. **Design decisions and trade-offs:** CDC vs polling, raw bronze for replay, idempotent MERGE using `lsn`, at-least-once delivery plus idempotent writes, explicit schema in parsing.
7. **Failure experiments:** a table with scenario, what happened, how the system recovered, and what you verified.
8. **Performance results:** throughput, end-to-end latency, catch-up time after an outage, and the effect of `OPTIMIZE`.
9. **Schema evolution:** how an added column is rolled out, and how you backfill.
10. **Testing and data quality:** the tests, the quarantine table, the monitoring.
11. **Limitations and what I would change at scale:** more partitions and a Kafka cluster with replication, a schema registry with Avro, Iceberg or Delta on cloud storage, alerting on lag and on replication slot size, exactly-once end to end.

## Talking about it in interviews

**Two-minute pitch (fill in your numbers):**

"I built a change-data-capture pipeline. A Postgres shop database is the source. Debezium reads its write-ahead log and publishes each insert, update and delete to Kafka. Spark Structured Streaming lands the raw events in a bronze Delta table, then a silver stream applies them with an idempotent MERGE so silver always matches the source, including deletes. A batch job builds gold revenue tables. I tested six failure scenarios, such as killing the stream mid-batch, stopping Connect and rebuilding silver from bronze, and verified no lost or duplicated rows. At \[X\] changes per second the end-to-end latency was about \[Y\] seconds."

**Questions this project prepares you for, with the core of the answer**

- **Why CDC and not polling?** Polling misses deletes and intermediate updates and loads the source. CDC reads the log, so it is complete and low-impact.
- **How do you get exactly-once?** A replayable source (Kafka offsets), checkpointing, and an idempotent sink (MERGE guarded by `lsn`). Delivery is at-least-once, and the write makes repeats harmless.
- **How do you handle deletes?** Debezium sends a delete event (and a tombstone). Silver deletes the row on `op = d`, using the `before` key.
- **What if Connect or Spark is down for a day?** The replication slot and Kafka retention hold the data. The risk is WAL growth in Postgres and Kafka retention expiring, so you monitor slot size and lag.
- **How do you handle schema changes?** Bronze stores raw events, silver uses an explicit schema, and a new column is added to silver and backfilled from bronze.
- **Why a bronze layer?** Replay and recovery. Bugs in parsing can be fixed and the data rebuilt without going back to the source.
- **What breaks at 100 times the data?** Single-partition topics, one connector task, small files from streaming, MERGE cost, and skewed keys. You would add partitions, compact files, partition tables sensibly, and consider Iceberg or Delta on cloud storage.
- **How did you test it?** Unit tests on the parsing function, a reconciliation test against Postgres, data quality checks, and failure experiments.
- **What would you do differently?** Schema registry with Avro, alerting, and a managed Kafka service.

**Resume bullets (replace the brackets with your measured numbers):**

- Built an end-to-end CDC pipeline (Postgres, Debezium, Kafka, Spark Structured Streaming, Delta Lake) handling \[X\] changes/sec with \[Y\] s end-to-end latency.
- Implemented idempotent upserts and delete handling with MERGE ordered by log sequence number, and verified zero data loss across \[N\] failure scenarios, including rebuild from the bronze layer.
- Added reconciliation tests, quarantine of bad records and lag monitoring, and documented the design trade-offs and a schema-evolution runbook.

## Resources, in one place

- **Docker:** docs.docker.com, "Get started".
- **Postgres:** documentation pages on Logical Decoding and Write-Ahead Logging.
- **Kafka:** Confluent Developer "Kafka 101" and Kafka Connect courses (developer.confluent.io), and the Apache Kafka documentation ("Introduction", "Design").
- **Debezium:** debezium.io documentation, the Tutorial and the PostgreSQL connector page.
- **Delta Lake:** docs.delta.io, the quickstart and the pages on updates, deletes and merges.
- **Spark:** the Structured Streaming Programming Guide and the Kafka Integration Guide on spark.apache.org.
- **Concepts:** *Designing Data-Intensive Applications*, chapter 11 on stream processing.
- **Reference lab:** the Data Engineering Lab you shared, for dbt, data modeling and Iceberg, after this project.
