# Chapter 1: Architecture and Setup

## What We're Building

Real-time supply chain routing sits at the intersection of three distinct data problems:

1. **Graph problem** -- which route connects a supplier to a retailer through active warehouses, and what's the best alternative when one goes offline?
2. **Operational problem** -- what's the current status of each shipment and which ones have been rerouted?
3. **Analytical problem** -- which warehouses are causing the most delays, which suppliers have the highest delay rates and where are SLA breaches concentrated?

Each of those problems has a natural home in a different kind of database. That's the core idea behind this book. We're not going to use one database and make it do everything. We're going to use three and let each one do what it does best.

The system we'll build is a real-time supply chain dashboard with 20 suppliers, 12 warehouses, 10 distribution centers and 30 retailers. A simulator generates shipment events that flow through a Confluent Cloud Kafka topic. Warehouse disruptions are injected automatically and the Neo4j graph finds alternative routes in real time. Two Streamlit dashboards show live operations and historical analytics.

## The Three-System Architecture

The architecture has three layers, each serving a different purpose.

**Neo4j Aura** is the graph layer. It holds the supply chain network -- suppliers, warehouses, distribution centers and retailers connected by shipping relationships. Aura answers the questions that a relational database handles with difficulty:

- What's the shortest path from a supplier to a retailer through active warehouses?
- When a warehouse goes offline, which alternative route avoids it?
- Which nodes are the most connected hubs of the network?

The variable-length path queries and inline filtering that Cypher provides make disruption routing fast and clean.

**Snowflake Postgres** is the operational layer. It's a fully managed Postgres database, hosted inside Snowflake, that accepts the high-frequency shipment writes from our Kafka consumer. Every event consumed from the `shipment-events` topic is written here in real time. Snowflake Postgres handles this with standard `psycopg2` connectivity. The `shipments` and `disruption_events` tables give the live operations dashboard everything it needs.

**Snowflake** is the analytics layer. Shipment data flows from Snowflake Postgres into a `shipment_history` table using an incremental sync pattern. We run aggregations here -- delay trends over time, warehouse health scores, supplier delay rates and SLA breach analysis. The analytics Streamlit app syncs and queries this layer on every refresh.

The relationship between these systems is as follows:

- Aura is where the *structure* lives -- the supply chain graph that neither Snowflake Postgres nor Snowflake knows anything about.
- Snowflake Postgres is where data is *written*, in real time, by the Kafka consumer.
- Snowflake is where we *analyze* the history that Snowflake Postgres accumulates.

Each system has a clear role and none of the roles overlap.

## What Aura Adds

A natural question is: what does Aura add? We already have a Postgres database. Can't we just store supply chain relationships there too? We could store them there. But querying them could be difficult. Finding the shortest path from a supplier to a retailer that avoids a disrupted warehouse requires traversing a graph -- following edges from node to node, filtering out inactive nodes mid-path. In SQL this means recursive CTEs with exclusion logic, which become unwieldy fast. In Cypher, Aura's query language, it's a concise pattern match with an inline filter:

```cypher
MATCH path = (s:Supplier {id: $supplier_id})-[:SHIPS_TO*1..6]->(r:Retailer {id: $retailer_id})
WHERE NONE(n IN nodes(path) WHERE n:Warehouse AND coalesce(n.active, true) = false)
RETURN [n IN nodes(path) WHERE n:Warehouse | n.id][0] AS warehouse_id
ORDER BY length(path)
LIMIT 1
```

The supply chain *is* a graph. Suppliers ship to warehouses, warehouses ship to distribution centers, distribution centers ship to retailers. The connectivity between them is the point. Storing that as rows and columns in a relational table is possible but goes against the natural structure of the data.

## What You'll Need

To follow along you'll need accounts and access to these services:

1. **Neo4j Aura** -- the free tier is sufficient. Create an account at [Get Started for Free](https://console.neo4j.io/graphacademy) and note your URI, username and password.
2. **Snowflake** -- a free trial account gives us access to both Snowflake Postgres and the Snowflake warehouse. Create an account at [Try Snowflake for Free](https://signup.snowflake.com/). Create a Snowflake Postgres instance and a warehouse before running the notebooks.
3. **Confluent Cloud** -- a free trial account is sufficient. Create an account at [Get Started with Confluent](https://www.confluent.io/get-started/) and create a cluster with a topic named `shipment-events`.
4. **Python** -- we'll use Python 3.12 throughout. The notebooks run in a local Jupyter environment. A virtual environment is highly recommended. For example:

```bash
python3 -m venv ~/supply-chain-env
source ~/supply-chain-env/bin/activate
```

All dependencies are installed using `%pip install` cells at the top of each notebook, with pinned versions for stability.

## Environment Variables

Credentials are passed to notebooks and the Streamlit applications using environment variables. Set these before starting Jupyter:

```bash
# Neo4j Aura
export NEO4J_URI="neo4j+s://xxxx.databases.neo4j.io"
export NEO4J_USERNAME="your-username"
export NEO4J_PASSWORD="your-password"

# Snowflake Postgres
export SNOWFLAKE_PG_HOST="your-instance.eu-west-2.aws.postgres.snowflake.app"
export SNOWFLAKE_PG_USER="snowflake_admin"
export SNOWFLAKE_PG_PASSWORD="your-password"
export SNOWFLAKE_PG_DBNAME="postgres"

# Snowflake warehouse
export SNOWFLAKE_ACCOUNT="your-account-identifier"
export SNOWFLAKE_USER="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_DATABASE="SUPPLY_CHAIN"
export SNOWFLAKE_SCHEMA="PUBLIC"
export SNOWFLAKE_WAREHOUSE="SUPPLY_CHAIN_WH"

# Confluent Cloud
export CONFLUENT_BOOTSTRAP_SERVERS="your-cluster.confluent.cloud:9092"
export CONFLUENT_API_KEY="your-api-key"
export CONFLUENT_API_SECRET="your-api-secret"
```

## Chapter Overview

Here's what each chapter covers:

**Chapter 2: The Supply Chain Graph** walks through building the supply chain network in Neo4j Aura -- 20 suppliers, 12 warehouses, 10 distribution centers and 30 retailers connected by shipping relationships. We'll cover the Cypher queries that make routing and disruption handling efficient, including shortest path and variable-length path queries.

**Chapter 3: The Operational Layer** sets up Snowflake Postgres as the operational store for live shipment records. We'll cover the table design, indexes and the `disruption_events` table that records every warehouse disruption and restore event.

**Chapter 4: The Analytics Layer** sets up the Snowflake warehouse as the analytics layer for historical shipment data. We'll cover the `shipment_history` table, the incremental sync pattern that keeps it current and how the analytics app uses it on every refresh.

**Chapter 5: The Simulator** writes a Kafka producer and consumer that generate synthetic shipment events and write them to both Neo4j and Snowflake Postgres in real time. Warehouse disruptions fire randomly, triggering live rerouting through the Neo4j graph.

**Chapter 6: The Streamlit Apps** covers the two Streamlit dashboards -- the live operations dashboard (`app.py`) showing current shipment status, active disruptions and rerouted shipments and the analytics dashboard (`analytics_app.py`) showing delay trends, warehouse health, shipment flow and supplier performance. There is also an optional analytics notebook for deeper one-off analysis.

**Chapter 7: Bringing It All Together** explains how the three systems work as a unit, covers the order of operations for running the full stack end-to-end and suggests directions for taking the project further.

Let's start by building the supply chain graph.
