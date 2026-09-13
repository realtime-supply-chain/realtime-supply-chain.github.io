# Chapter 5: The Simulator

## What the Simulator Does

The simulator generates synthetic shipment events, publishes them to a Confluent Cloud Kafka topic and consumes them back in real time. For each consumed event, it writes the shipment to both Neo4j and Snowflake Postgres -- and if the shipment passes through a warehouse that's currently offline, it reroutes it through an alternative warehouse found via the Neo4j graph.

The simulator has four responsibilities:

1. Publish synthetic shipment events to the `shipment-events` Kafka topic.
2. Consume those events from the same topic.
3. Inject warehouse disruptions at random intervals and restore them after a random duration.
4. Write every event to Neo4j and Snowflake Postgres, with rerouting applied where needed.

It runs directly in a Jupyter notebook cell. The loop stops on keyboard interrupt or automatically after `MAX_RUNTIME_SECONDS`.

## The Kafka Producer and Consumer

The simulator uses `confluent-kafka` for both producing and consuming events. The producer publishes one event per iteration of the loop. The consumer reads one event back before writing to the databases. This tight produce-consume loop ensures that what gets written to Neo4j and Snowflake Postgres is exactly what landed in Kafka -- the same event, with the same timestamp.

Both producer and consumer authenticate with SASL/SSL:

```python
producer = Producer({
    "bootstrap.servers": CONFLUENT_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     CONFLUENT_API_KEY,
    "sasl.password":     CONFLUENT_API_SECRET,
})
```

The consumer uses a fixed `group.id` and `auto.offset.reset = "earliest"` to ensure it picks up from the beginning of the topic on each run.

## Generating Shipment Events

Each shipment event is generated with a fixed random seed, which makes the sequence reproducible:

```python
rng  = np.random.default_rng(RANDOM_SEED)

def make_shipment_event():
    status = str(rng.choice(STATUSES, p=STATUS_WEIGHTS))
    return {
        "shipment_id":    str(uuid.uuid4()),
        "supplier_id":    str(rng.choice(SUPPLIER_IDS)),
        "warehouse_id":   str(rng.choice(WAREHOUSE_IDS)),
        "dist_center_id": str(rng.choice(DIST_CENTER_IDS)),
        "retailer_id":    str(rng.choice(RETAILER_IDS)),
        "status":         status,
        "timestamp":      time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "delay_minutes":  int(rng.integers(15, 240)) if status == "delayed" else 0,
    }
```

Status weights are `[0.25, 0.45, 0.15, 0.15]` for departed, in_transit, delayed and delivered. Roughly 15% of shipments are delayed, which produces enough delay data for the analytics charts without overwhelming the status breakdown.

## The Disruption Lifecycle

Warehouse disruptions fire randomly during the simulation. Two configuration parameters control the timing:

- `DISRUPTION_INTERVAL_MIN` / `DISRUPTION_INTERVAL_MAX` -- how many events pass between disruptions.
- `DISRUPTION_DURATION_MIN` / `DISRUPTION_DURATION_MAX` -- how long each disruption lasts in events.

Both the gap between disruptions and the duration of each disruption are drawn randomly from these ranges using the seeded `rng`. This produces a realistic pattern -- disruptions are not predictable but the system is reproducible.

The first disruption always hits `W003`, which is the warehouse used in the Chapter 2 graph verification test. Subsequent disruptions target a randomly chosen warehouse drawn after each restore.

The lifecycle has three phases:

```
normal -> disrupted -> restored -> normal -> ...
```

Transitioning to `disrupted` sets the warehouse's `active` property to `false` in Neo4j and writes a `disrupted` row to `disruption_events` in Snowflake Postgres. Transitioning to `restored` reverses both.

## Rerouting

When the simulator is in the `disrupted` phase and a consumed event routes through the disrupted warehouse, it calls `find_alternative_route()` to ask Neo4j for a path that avoids it:

```python
if phase == "disrupted" and consumed["warehouse_id"] == current_disruption:
    alt_warehouse_id = find_alternative_route(
        session,
        consumed["supplier_id"],
        consumed["retailer_id"],
    )
```

The rerouting query uses a variable-length path with a `WHERE NONE()` filter:

```cypher
MATCH path = (s:Supplier {id: $supplier_id})-[:SHIPS_TO*1..6]->(r:Retailer {id: $retailer_id})
WHERE NONE(n IN nodes(path) WHERE n:Warehouse AND coalesce(n.active, true) = false)
RETURN [n IN nodes(path) WHERE n:Warehouse | n.id][0] AS warehouse_id
ORDER BY length(path)
LIMIT 1
```

If an alternative is found, `alt_warehouse_id` is passed to both `write_to_neo4j` and `write_to_postgres`, which record the rerouting in both systems.

## Writing to Neo4j and Snowflake Postgres

Each consumed event is written to both systems before the loop advances.

**Neo4j** -- a `MERGE` on `shipment_id` creates or updates the `Shipment` node and its four relationships. If the shipment was rerouted, the `VIA_WAREHOUSE` relationship points to the alternative warehouse rather than the original:

```cypher
MERGE (sh:Shipment {shipment_id: $shipment_id})
SET sh.status        = $status,
    sh.delay_minutes = $delay_minutes,
    sh.timestamp     = $timestamp,
    sh.rerouted      = $rerouted
```

Neo4j writes use `write_to_neo4j_with_retry()`, which opens a fresh session per attempt and retries up to twice on connection failure. This handles the session timeouts that Aura enforces on long-running connections.

**Snowflake Postgres** -- an `INSERT ... ON CONFLICT DO UPDATE` handles rerouted shipments correctly. If a shipment arrives that was initially routed through a warehouse that then went offline, the update overwrites the routing fields:

```python
INSERT INTO shipments (...)
ON CONFLICT (shipment_id) DO UPDATE
    SET rerouted         = EXCLUDED.rerouted,
        alt_warehouse_id = EXCLUDED.alt_warehouse_id,
        status           = EXCLUDED.status
```

## The Display

The simulator refreshes its output every `PRINT_EVERY` events, showing the current phase, the latest event and the running status breakdown:

```
Events processed : 1440
Phase            : DISRUPTION ACTIVE -- W003 offline
Rerouted         : 36
Refresh every    : 10

Latest event:
  !! a894fcfa... S010 -> W003 -> DC002 -> R011 [delayed]

Last reroute:
  5fbb903f... W003 -> W005

Status breakdown:
  departed     : 396
  in_transit   : 598
  delayed      : 232
  delivered    : 214
```

The `!!` prefix marks delayed events. The `->` prefix marks normal events.

## Runtime Limit

The simulator stops automatically after `MAX_RUNTIME_SECONDS` even when `N_EVENTS` is set to `-1` for continuous operation:

```python
start_time = time.time()
while (N_EVENTS == -1 or counter < N_EVENTS) and \
      (time.time() - start_time < MAX_RUNTIME_SECONDS):
    ...
```

This ensures the simulator doesn't run indefinitely if left unattended.

## Verifying the Result

A verify cell at the end of the notebook connects to Snowflake Postgres and prints a summary of both tables:

```
Snowflake Postgres -- shipments table by status:
  departed     : 396
  in_transit   : 598
  delayed      : 232
  delivered    : 214

  Total        : 1440
  Rerouted     : 36

Snowflake Postgres -- disruption_events:
  Warehouse    Event        Timestamp
  W003         disrupted    2026-09-07 15:18:42+00:00
  W003         restored     2026-09-07 15:21:15+00:00
  W007         disrupted    2026-09-07 15:24:03+00:00
  W007         restored     2026-09-07 15:26:44+00:00
```

## Gotchas

**Long-lived Neo4j Aura sessions time out.** Aura closes idle connections after a period of inactivity, throwing `SessionExpired`. The fix is to open a fresh session per operation rather than holding one open for the duration of the loop. All Neo4j calls in the simulator use short-lived `with neo4j_driver.session() as session:` blocks and the main shipment write uses `write_to_neo4j_with_retry()` to handle dropped connections automatically.

**Only one rerouted shipment.** If the disruption window is too short relative to the event rate and the number of warehouses, very few shipments will pass through the disrupted warehouse during the disruption. Increase `DISRUPTION_DURATION_MAX` or decrease `DISRUPTION_INTERVAL_MIN` to produce more rerouting events.

**`ON CONFLICT DO NOTHING` drops rerouted events.** Using `DO NOTHING` for shipment inserts means rerouted events are silently dropped when the original shipment row already exists. Always use `DO UPDATE` and explicitly set the rerouting columns.

**Tables are truncated, not dropped, at simulator startup.** The `shipments` and `disruption_events` tables are owned by the Chapter 3 notebook. The simulator truncates them at startup rather than dropping and recreating them. Re-running the Chapter 3 notebook after the simulator has run will drop and recreate the tables, clearing all data.
