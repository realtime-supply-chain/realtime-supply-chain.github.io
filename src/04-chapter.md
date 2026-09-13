# Chapter 4: The Analytics Layer

## What the Analytics Layer Does

Snowflake Postgres in Chapter 3 gives us a fast operational store for live shipment events. But it's optimized for writes and point reads, not for the heavier aggregation queries that answer questions like "which warehouses are causing the most delays over time?" or "how has the average delay changed across the last hour?"

For those questions we need an analytics layer -- somewhere to accumulate historical data and run aggregations without affecting the operational store. The Snowflake warehouse is that layer.

The analytics layer needs to:

- Store a full history of shipment events as they accumulate over a run.
- Stay current with new events arriving from Snowflake Postgres.
- Serve the analytics Streamlit dashboard with the five charts it displays on every refresh.
- Handle heavier aggregation queries efficiently without impacting live operations.

## The Incremental Sync Pattern

Rather than writing directly to the Snowflake warehouse from the simulator, we use an incremental sync pattern. The analytics app (and the notebook in Chapter 6) reads new rows from the `shipments` table in Snowflake Postgres and appends them to `shipment_history` in the Snowflake warehouse.

The sync tracks a watermark -- the `MAX(recorded_at)` already in `shipment_history`. On each sync call, it pulls only rows newer than that watermark and appends them:

```python
def sync_to_warehouse(sf_cur):
    sf_cur.execute("SELECT MAX(recorded_at) FROM shipment_history")
    last_synced_at = sf_cur.fetchone()[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)

    pg_cur.execute("""
        SELECT * FROM shipments
        WHERE recorded_at > %s
        ORDER BY recorded_at
    """, (last_synced_at,))

    rows = pg_cur.fetchall()
    # batch insert into shipment_history ...
    return len(rows)
```

This pattern is the same one used by the analytics Streamlit app on every 30-second refresh. Running it in the notebook here confirms the mechanism works before the Kafka layer is running.

The first run will find no rows since the simulator hasn't started yet. That's expected.

## The Shipment History Table

The `shipment_history` table mirrors the `shipments` table in Snowflake Postgres but is designed for analytics rather than transactions. It has no primary key constraint since it's append-only and adds a `synced_at` column recording when each row was pulled from Snowflake Postgres:

```sql
CREATE TABLE shipment_history (
    shipment_id      TEXT,
    supplier_id      TEXT,
    warehouse_id     TEXT,
    dist_center_id   TEXT,
    retailer_id      TEXT,
    status           TEXT,
    delay_minutes    INTEGER,
    rerouted         BOOLEAN,
    alt_warehouse_id TEXT,
    recorded_at      TIMESTAMP_TZ,
    synced_at        TIMESTAMP_TZ
)
```

The table is dropped and recreated on each notebook run, so the notebook is safe to re-run from the start during development. In the analytics Streamlit app the table persists across refreshes and the incremental sync keeps it current.

## Connecting to Snowflake

We use the `snowflake-connector-python` library to connect to the Snowflake warehouse. The account identifier is the part of the Snowflake URL:

```python
sf = snowflake.connector.connect(
    account  = SNOWFLAKE_ACCOUNT,
    user     = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSWORD,
)
sf_cursor = sf.cursor()
sf_cursor.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
sf_cursor.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
sf_cursor.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
```

## The Compute Warehouse

The Snowflake compute warehouse runs the analytics queries. We use an X-SMALL warehouse with auto-suspend and auto-resume to keep costs low. It spins up when a query arrives and suspends automatically after 60 seconds of inactivity:

```sql
CREATE WAREHOUSE IF NOT EXISTS SUPPLY_CHAIN_WH
WITH WAREHOUSE_SIZE      = 'X-SMALL'
     AUTO_SUSPEND        = 60
     AUTO_RESUME         = TRUE
     INITIALLY_SUSPENDED = TRUE
```

`INITIALLY_SUSPENDED = TRUE` means the warehouse doesn't consume credits until the first query runs.

## Batch Inserts

The sync function inserts rows in batches of 500 using `executemany` with `%s` placeholders:

```python
BATCH_SIZE = 500
for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i + BATCH_SIZE]
    placeholders = ", ".join(["%s"] * 11)
    sf_cur.executemany(
        f"INSERT INTO shipment_history VALUES ({placeholders})",
        values_list,
    )
```

Timestamps are converted to strings before insertion to ensure the Snowflake connector handles them correctly.

## Verifying the Result

After creating the table and running the initial sync, we confirm the column definitions match expectations and check the row count:

```
Table: shipment_history
  Column               Type
  ----------------------------------------
  SHIPMENT_ID          TEXT
  SUPPLIER_ID          TEXT
  WAREHOUSE_ID         TEXT
  DIST_CENTER_ID       TEXT
  RETAILER_ID          TEXT
  STATUS               TEXT
  DELAY_MINUTES        NUMBER
  REROUTED             BOOLEAN
  ALT_WAREHOUSE_ID     TEXT
  RECORDED_AT          TIMESTAMP_TZ
  SYNCED_AT            TIMESTAMP_TZ

Row count: 0
```

Zero rows is correct at this stage -- the simulator hasn't run yet.

## Gotchas

**`mogrify` behaves differently on the Snowflake connector than on psycopg2.** The Snowflake connector's `mogrify` may not correctly escape all Python types, particularly timestamps and booleans. Use `executemany` with `%s` placeholders instead. This is the pattern used throughout the analytics notebook and the Streamlit app.

**Timestamp binding.** Convert datetime objects to strings before inserting into Snowflake. The connector doesn't always handle timezone-aware datetime objects correctly. Wrapping with `str(v) if hasattr(v, 'isoformat') else v` ensures safe binding for all timestamp columns.

**`CREATE TABLE IF NOT EXISTS` in the analytics app.** The analytics notebook drops and recreates `shipment_history` on each run. The analytics Streamlit app doesn't -- it creates the table only if it doesn't already exist, then appends to it incrementally. Ensure the notebook and the app aren't run simultaneously, as the notebook's `DROP TABLE` will delete data the app has already accumulated.
