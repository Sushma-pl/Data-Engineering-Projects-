# ShopStream — Build Log

> A day-by-day journal of what was built, what broke, and how it was fixed.
> This is the most honest record of the project.

---

## Day 1 — Infrastructure (Phase A)

### What I built
- `docker-compose.yml` with 4 services: Postgres, Kafka (KRaft), Debezium Connect, Spark
- `postgres/init/01_schema.sql` — auto-creates `customers` and `orders` tables on first boot
- `connector/connector.json` — Debezium connector config watching both tables
- Registered the connector via REST API and verified `"state": "RUNNING"`

### Key decisions made
- **KRaft mode Kafka** — no ZooKeeper needed, simpler single-node setup
- **`01_` prefix on SQL file** — ensures correct execution order if more init files are added later
- **`condition: service_healthy` on Debezium's `depends_on`** — ensures Connect only starts after Kafka and Postgres are actually ready, not just running
- **`decimal.handling.mode: string`** — prevents Postgres NUMERIC from arriving as base64 gibberish
- **`REPLICA IDENTITY FULL`** — captures full "before" row on updates and deletes, not just the PK

### What broke
- **PowerShell curl quoting issue** — `Get-Content connector.json | curl` mangled the JSON.
  Fix: use `curl.exe --data-binary "@connector/connector.json"` — the `@` makes curl read the file directly without PowerShell touching it.

- **Connector registered but returned 500 error** — same quoting issue as above.
  Fix: same — `--data-binary "@file"` pattern.

### Verified
```
curl http://localhost:8083/connectors/shopstream-connector/status
→ "state": "RUNNING" for both connector and task
```

Consumed `shop.public.orders` topic from beginning and saw snapshot events with `"op": "r"`.

---

## Day 2 — Data Generator (Phase B)

### What I built
- `generator/generate_changes.py` — continuously inserts/updates/deletes orders
- Weighted random actions: 60% INSERT, 30% UPDATE, 10% DELETE
- Uses `psycopg2` to connect to Postgres on `localhost:5433`

### Key decisions made
- **Weights skewed toward inserts** — deletes on a nearly empty table cause errors
- **`conn.autocommit = True`** — each statement commits immediately, no transaction needed
- **`ORDER BY random() LIMIT 1`** for updates/deletes — picks a random existing row each time

### Verified
Ran generator and consumed Kafka topic simultaneously. Saw all three op types flowing:
- `"op": "c"` on INSERT
- `"op": "u"` on UPDATE
- `"op": "d"` on DELETE

---

## Day 2 — Bronze Stream (Phase C)

### What I built
- `spark/bronze_stream.py` — reads `shop.public.orders` topic, writes raw events to Delta

### Key decisions made
- **`startingOffsets: earliest`** — first run picks up all existing Kafka events including the snapshot
- **`kafka:29092` not `localhost:9092`** — Spark runs inside Docker, so it uses the internal listener
- **`failOnDataLoss: true`** — crash loudly rather than silently skip missing offsets
- **No transformation** — Bronze stores everything exactly as received. Parsing belongs in Silver.
- **Append-only Delta** — Bronze never updates or deletes

### What broke
- **`spark-submit: command not found`** — `apache/spark` image doesn't add `/opt/spark/bin` to PATH.
  Fix: use full path `/opt/spark/bin/spark-submit`

- **Ivy FileNotFoundException** — `spark` user couldn't write to `/home/spark/.ivy2/cache`.
  Fix: exec into container as root (`docker exec -it -u root`), fixed permissions, added `user: root` to docker-compose.yml spark service.

- **Backslash line continuation didn't work** — the shell inside the container interpreted `\` differently.
  Fix: always run spark-submit as a single line inside Docker.

### Verified
```python
df = spark.read.format("delta").load("/opt/spark-data/lake/bronze/orders")
df.count()  # → 343
df.printSchema()
# key: string, value: string, topic: string, partition: int, offset: long,
# kafka_ts: timestamp, ingested_at: timestamp
```

---

## Day 3 — Silver Stream (Phase D)

### What I built
- `spark/silver_stream.py` — reads Bronze Delta, parses Debezium envelope, applies MERGE to Silver

### Key decisions made

**Schema definition:**
- Defined `ORDER_SCHEMA`, `SOURCE_SCHEMA`, `PAYLOAD_SCHEMA`, `ENVELOPE_SCHEMA` explicitly
- `amount` kept as `StringType` in schema (cast to decimal later) — always parse first, cast after
- `created_at`/`updated_at` kept as `LongType` (microseconds) — convert with `timestamp_micros()` later

**`parse_changes()` function:**
- Kept as a pure function (no side effects) so it can be unit tested independently
- Filters tombstones with `filter(col("value").isNotNull())`
- Uses `coalesce(after.id, before.id)` to handle deletes (where `after` is null)

**`upsert_orders()` function:**
- Deduplicates within each batch: `Window.partitionBy("id").orderBy(lsn.desc())`, keep `row_number() = 1`
- LSN guard on MERGE update: `s.lsn > t.lsn` — makes replays idempotent
- `whenNotMatchedInsert` has `condition="s.op != 'd'"` — don't insert a row for a delete event

**Reading from Bronze, not Kafka:**
- Bronze is permanent, Kafka has 7-day retention
- Enables full replay of Silver from Bronze any time a bug is found

### What broke
- **Typo: `change.withColumn` instead of `changes.withColumn`** — Python NameError.
  Fix: renamed to `changes`.

- **`.execute()` inside parentheses** — caused syntax error in the MERGE chain.
  Fix: moved `.execute()` outside the parentheses block.

### Verified
```
Silver row count: 154
Postgres count: 154  (SELECT count(*) FROM orders)
→ MATCH ✅
```

Sample Silver row:
```
id=1, amount=1500.00 (decimal ✅), updated_at=2026-11-02 04:54:12 (timestamp ✅)
```

---

## Upcoming

### Phase E — Gold (daily revenue aggregates)
- Batch job over Silver
- Daily revenue by status
- Write to Gold Delta

### Phase F — Tests
- Unit test `parse_changes()` with synthetic events
- Reconciliation test: Silver count == Postgres count automatically

### Phase G — MinIO
- Add MinIO container to docker-compose
- Swap local paths for `s3a://lake/...`
- Handle `hadoop-aws` version matching

### Failure Experiments (planned)
| Experiment | Expected behavior |
|---|---|
| Kill Silver mid-batch (`kill -9`) | Restarts from checkpoint, no duplicates |
| Stop Kafka Connect 5 minutes | Replication slot holds WAL, catches up on restart |
| Delete Silver checkpoint + table | Replay from Bronze, same final result |
| Stop Kafka, restart it | Silver pauses, resumes when Kafka is back |
