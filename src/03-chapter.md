# Chapter 3: The Operational Layer

## What We Need From the Operational Layer

The supply chain graph in Chapter 2 gives us the structure -- the nodes and relationships that define how suppliers connect to retailers through warehouses and distribution centers. What it doesn't give us is a place to write live shipment events as they arrive from Kafka.

We need an operational store that can:

- Accept high-frequency writes from the Kafka consumer in real time.
- Record the current status of every shipment.
- Flag rerouted shipments and store the alternative warehouse used.
- Log every warehouse disruption and restore event with a timestamp.
- Serve the live operations Streamlit dashboard with fast reads.

Snowflake Postgres is that store. It's a fully managed Postgres database hosted inside Snowflake that accepts connections from standard `psycopg2` clients. The connection pattern is identical to any other managed Postgres service.

## The Schema

Two tables hold all the operational data.

**`shipments`** is the primary table. Every event consumed from the `shipment-events` Kafka topic is written here. If a shipment is rerouted, the `rerouted` flag is set to `true` and the alternative warehouse is recorded in `alt_warehouse_id`:

```sql
CREATE TABLE shipments (
    shipment_id      TEXT PRIMARY KEY,
    supplier_id      TEXT        NOT NULL,
    warehouse_id     TEXT        NOT NULL,
    dist_center_id   TEXT        NOT NULL,
    retailer_id      TEXT        NOT NULL,
    status           TEXT        NOT NULL,
    delay_minutes    INTEGER     NOT NULL DEFAULT 0,
    rerouted         BOOLEAN     NOT NULL DEFAULT FALSE,
    alt_warehouse_id TEXT,
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

**`disruption_events`** records every warehouse disruption and restore event. The simulator writes a row each time a warehouse goes offline and again when it comes back online. The live operations dashboard reads this table to determine the current phase and display the disruption banner:

```sql
CREATE TABLE disruption_events (
    id            SERIAL PRIMARY KEY,
    warehouse_id  TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
```

The `event_type` column holds either `disrupted` or `restored`. The most recent row tells the dashboard which phase the system is currently in.

Both tables are created using a loop to keep the notebook concise:

```python
tables = {
    "shipments":         "CREATE TABLE shipments ...",
    "disruption_events": "CREATE TABLE disruption_events ...",
}

for name, ddl in tables.items():
    cursor.execute(f"DROP TABLE IF EXISTS {name}")
    cursor.execute(ddl)
    print(f"Table {name} created.")
```

## Indexes

We create indexes on the columns most commonly used in dashboard queries:

```sql
CREATE INDEX idx_shipments_status      ON shipments (status);
CREATE INDEX idx_shipments_warehouse   ON shipments (warehouse_id);
CREATE INDEX idx_shipments_recorded_at ON shipments (recorded_at DESC);
CREATE INDEX idx_shipments_rerouted    ON shipments (rerouted);
```

The `status` index speeds up the per-status counts shown in the metric tiles. The `warehouse_id` index speeds up the delayed-by-warehouse bar chart. The `recorded_at` index speeds up the incremental sync used by the analytics layer in Chapter 4. The `rerouted` index speeds up the rerouted shipments table.

## Role Setup

We create a dedicated application role with the minimum permissions needed to read and write both tables. No superuser privileges are granted. The `DO` block skips creation cleanly if the role already exists, making the cell safe to re-run:

```sql
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supply_chain_app') THEN
        CREATE ROLE supply_chain_app LOGIN PASSWORD 'change-me-in-production';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO supply_chain_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE shipments TO supply_chain_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE disruption_events TO supply_chain_app;
```

The `ALTER DEFAULT PRIVILEGES` statement ensures any future tables in the schema are also accessible to the role without an additional grant.

## Connecting From Python

The connection pattern is standard `psycopg2`. SSL is required for all Snowflake Postgres connections:

```python
conn = psycopg2.connect(
    host     = SNOWFLAKE_PG_HOST,
    user     = SNOWFLAKE_PG_USER,
    password = SNOWFLAKE_PG_PASSWORD,
    dbname   = SNOWFLAKE_PG_DBNAME,
    sslmode  = "require",
    port     = 5432,
)
conn.autocommit = True
```

`autocommit = True` is set for DDL statements. The simulator uses the same connection pattern and relies on `autocommit` for the high-frequency event writes.

## Verifying the Result

After creating both tables and indexes, we confirm the column definitions and index names match expectations:

```
Table: shipments
  Column               Type                     Nullable   Default
  ----------------------------------------------------------------------
  shipment_id          text                     NO
  supplier_id          text                     NO
  warehouse_id         text                     NO
  dist_center_id       text                     NO
  retailer_id          text                     NO
  status               text                     NO
  delay_minutes        integer                  NO         0
  rerouted             boolean                  NO         false
  alt_warehouse_id     text                     YES
  recorded_at          timestamp with time zone NO         now()

Table: disruption_events
  Column               Type                     Nullable   Default
  ----------------------------------------------------------------------
  id                   integer                  NO
  warehouse_id         text                     NO
  event_type           text                     NO
  recorded_at          timestamp with time zone NO         now()

Indexes (shipments):
  idx_shipments_recorded_at
  idx_shipments_rerouted
  idx_shipments_status
  idx_shipments_warehouse
  shipments_pkey
```

## What the Simulator Writes

The simulator writes to both tables on every lifecycle event. For shipment events, it uses `ON CONFLICT DO UPDATE` to handle rerouted shipments correctly -- if a shipment arrives that was initially routed through a warehouse that then went offline, the update overwrites the routing fields rather than creating a duplicate row:

```python
INSERT INTO shipments (...)
VALUES (...)
ON CONFLICT (shipment_id) DO UPDATE
    SET rerouted         = EXCLUDED.rerouted,
        alt_warehouse_id = EXCLUDED.alt_warehouse_id,
        status           = EXCLUDED.status
```

For disruption events, it's a plain insert each time a warehouse changes state:

```python
INSERT INTO disruption_events (warehouse_id, event_type)
VALUES (%s, %s)
```

The `recorded_at` timestamp is set by the database using `DEFAULT NOW()`, which ensures the timestamp reflects when the event landed in Postgres rather than when it was generated by the simulator.

## Gotchas

**`ON CONFLICT DO UPDATE` not `DO NOTHING`.** Using `DO NOTHING` for shipment inserts means rerouted events are silently dropped when the original shipment row already exists. The rerouted flag and alternative warehouse ID never get written. Always use `DO UPDATE` and explicitly set the rerouting columns.

**`disruption_events` grant must be explicit.** The `ALTER DEFAULT PRIVILEGES` statement only covers tables created after it runs. Since `disruption_events` is created before the default privileges are set, it needs its own explicit `GRANT` statement. Relying on default privileges alone leaves the table inaccessible to the application role.
