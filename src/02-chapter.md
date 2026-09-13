# Chapter 2: The Supply Chain Graph

## What We Need From the Graph

The supply chain graph is the foundation of the routing system. Without it, we can't find paths from suppliers to retailers, identify alternative routes when a warehouse goes offline or understand the connectivity of the network. Everything the simulator and the Streamlit application do around routing depends on having an accurate, connected graph of the supply chain.

We specifically need:

- Every supplier, warehouse, distribution center and retailer as a node.
- Every shipping relationship as a directed edge.
- A way to find paths from any supplier to any retailer through active warehouses.
- A way to exclude disrupted warehouses from routing without changing the graph structure.

## The Aura Graph Model

The supply chain maps naturally to Aura's property graph model. Here's what we're storing:

```text
(:Supplier {id, name, location})
    -[:SHIPS_TO]->
(:Warehouse {id, name, location, active})
    -[:SHIPS_TO]->
(:DistributionCenter {id, name, location})
    -[:SHIPS_TO]->
(:Retailer {id, name, location})
```

Each node label represents a distinct tier of the supply chain. The `active` property on `Warehouse` nodes is the key to disruption routing -- setting it to `false` marks a warehouse as offline without deleting it from the graph. The routing query filters out inactive warehouses mid-path using a `WHERE NONE()` clause.

Shipment events from the Kafka consumer are stored as `Shipment` nodes with relationships back to the supplier, warehouse, distribution center and retailer involved:

```text
(:Supplier)-[:HAS_SHIPMENT]->(:Shipment)-[:VIA_WAREHOUSE]->(:Warehouse)
(:Shipment)-[:VIA_DIST_CENTER]->(:DistributionCenter)
(:Shipment)-[:DESTINED_FOR]->(:Retailer)
```

## Building the Network

The supply chain is generated synthetically using a fixed random seed, which guarantees that the same node IDs appear in every notebook and in the simulator. The node counts are:

- 20 suppliers: `S000` to `S019`
- 12 warehouses: `W000` to `W011`
- 10 distribution centers: `DC000` to `DC009`
- 30 retailers: `R000` to `R029`

Each supplier is connected to a random subset of warehouses, each warehouse to a random subset of distribution centers and each distribution center to a random subset of retailers. The connections are sparse enough to make routing non-trivial but dense enough to ensure there's always an alternative path when a warehouse goes offline.

## Loading Into Aura

We load nodes and relationships in separate passes using `MERGE` to make the operation idempotent. Running the notebook twice or more won't create duplicate nodes.

**Nodes.** Each tier is loaded with a uniqueness constraint on `id`:

```cypher
UNWIND $rows AS row
MERGE (s:Supplier {id: row.id})
SET s.name     = row.name,
    s.location = row.location
```

The same pattern applies for `Warehouse`, `DistributionCenter` and `Retailer` nodes. Warehouses get an additional `active` property, defaulting to `true`.

**Relationships.** Each shipping connection becomes a `SHIPS_TO` relationship:

```cypher
UNWIND $rows AS row
MATCH (a {id: row.from_id})
MATCH (b {id: row.to_id})
MERGE (a)-[:SHIPS_TO]->(b)
```

We load in batches of 500 rows at a time.

## Clearing Before Reload

We clear the graph before each run to keep the notebook idempotent:

```cypher
MATCH (n)
CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 1000 ROWS
```

## Indexes

We create indexes on `id` for all four node labels before loading and an index on `Shipment.status` for the queries used by the analytics notebook:

```cypher
CREATE INDEX supplier_id     IF NOT EXISTS FOR (n:Supplier)           ON (n.id)
CREATE INDEX warehouse_id    IF NOT EXISTS FOR (n:Warehouse)          ON (n.id)
CREATE INDEX dist_center_id  IF NOT EXISTS FOR (n:DistributionCenter) ON (n.id)
CREATE INDEX retailer_id     IF NOT EXISTS FOR (n:Retailer)           ON (n.id)
CREATE INDEX shipment_status IF NOT EXISTS FOR (n:Shipment)           ON (n.status)
```

## Routing Queries

With the graph in place, two routing queries drive the rest of the system.

**Normal routing** -- finding the path from a supplier to a retailer:

```cypher
MATCH path = shortestPath(
    (s:Supplier {id: $supplier_id})-[:SHIPS_TO*]->(r:Retailer {id: $retailer_id})
)
RETURN [n IN nodes(path) WHERE n:Warehouse | n.id][0] AS warehouse_id
```

**Disruption routing** -- finding a path that avoids the disrupted warehouse:

```cypher
MATCH path = (s:Supplier {id: $supplier_id})-[:SHIPS_TO*1..6]->(r:Retailer {id: $retailer_id})
WHERE NONE(n IN nodes(path) WHERE n:Warehouse AND coalesce(n.active, true) = false)
RETURN [n IN nodes(path) WHERE n:Warehouse | n.id][0] AS warehouse_id
ORDER BY length(path)
LIMIT 1
```

The disruption query uses a variable-length path rather than `shortestPath()` because `shortestPath()` can't filter nodes mid-path. The `WHERE NONE()` clause checks every node in the path and rejects any path that passes through an inactive warehouse.

## Disruption and Restore

Marking a warehouse as offline is a single `SET` operation:

```cypher
MATCH (w:Warehouse {id: $warehouse_id})
SET w.active = false
```

Restoring it is the same in reverse:

```cypher
MATCH (w:Warehouse {id: $warehouse_id})
SET w.active = true
```

The graph structure doesn't change. Only the `active` property changes and the routing query filters accordingly on the next call.

## Verifying the Result

After loading, we verify the node and relationship counts and confirm the disruption routing query returns a result for a known supplier-retailer pair:

```
Suppliers            : 20
Warehouses           : 12
Distribution centers : 10
Retailers            : 30
SHIPS_TO             : 172

Disruption test (S000 -> R001, W003 offline):
  Alternative route via: W001
```

## Gotchas

**`shortestPath()` can't filter mid-path nodes.** The normal routing query uses `shortestPath()`, which is fast but can't exclude inactive nodes mid-path. The disruption routing query switches to a variable-length `[:SHIPS_TO*1..6]` pattern with a `WHERE NONE()` filter. The two queries look similar but are not interchangeable -- using `shortestPath()` for disruption routing will return paths through the disrupted warehouse.

**`NONE()` and three-valued logic.** The `WHERE NONE()` clause must use `coalesce(n.active, true) = false` rather than `n.active = false`. If `active` is missing from a node (for example, on a `DistributionCenter`), `n.active = false` evaluates to `null` rather than `false`, which `NONE()` treats as a match and rejects the path incorrectly. Wrapping with `coalesce()` makes the check safe for all node types.

**Fixed random seed.** The supply chain topology is generated with `RANDOM_SEED = 42`. Every notebook and the simulator use the same seed, which guarantees that node IDs match across the system. Changing the seed will produce a different graph and break the cross-notebook consistency.
