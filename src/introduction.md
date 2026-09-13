# Real-Time Supply Chain Routing with Neo4j, Snowflake Postgres and Confluent Kafka

This book builds a real-time supply chain operations dashboard using three database systems:

1. Neo4j Aura for the supply chain graph.
2. Snowflake Postgres for live shipment records.
3. Snowflake for historical analytics.

A simulated supply chain generates shipment events that flow through a Confluent Cloud Kafka topic in real time. Warehouse disruptions are injected automatically, triggering live rerouting through the Neo4j graph. Two Streamlit dashboards show live operations and analytics.

## How the Code Is Organized

All runnable code lives in the `code/` directory.

Notebooks are numbered to match the chapters -- `02_neo4j_supply_chain.ipynb` corresponds to Chapter 2, and so on.
