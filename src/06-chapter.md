# Chapter 6: The Streamlit Apps

## Two Dashboards

The system has two Streamlit dashboards. The live operations dashboard (`app.py`, port 8501) shows current shipment status, active disruptions, rerouted shipments and a disruption events log. The analytics dashboard (`analytics_app.py`, port 8502) shows status breakdown, delay trends, warehouse health, shipment flow and supplier performance, updated every 30 seconds from the Snowflake warehouse.

Run them alongside the simulator:

```bash
streamlit run app.py
streamlit run analytics_app.py --server.port 8502
```

## The Live Operations Dashboard

### Auto-Refresh

The live operations dashboard refreshes every 10 seconds. `streamlit-autorefresh` handles this with a single call near the top of the script:

```python
st_autorefresh(interval=REFRESH_SECONDS * 1000)
```

Every 10 seconds the entire script re-runs from top to bottom, re-querying Snowflake Postgres for the latest shipment and disruption data.

### Connections

All queries read from Snowflake Postgres using `psycopg2`. The connection is opened per query using `@st.cache_data(ttl=REFRESH_SECONDS)`, which caches each query result for 10 seconds and avoids hammering the database on every rerun:

```python
@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_status_counts():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*) AS count
                FROM shipments
                GROUP BY status
            """)
            return {row[0]: row[1] for row in cur.fetchall()}
```

### The Disruption Banner

The most prominent element on the dashboard is the disruption banner -- a full-width coloured box at the top of the page. It shows green for normal operations, red when a warehouse is offline and amber immediately after a restore. After a configurable grace period (`RESTORED_GRACE_SECONDS`, defaulting to 60 seconds), the banner returns to green even if the last event in `disruption_events` was a restore.

The current phase is determined by the most recent row in `disruption_events`:

```python
cur.execute("""
    SELECT event_type, warehouse_id, recorded_at
    FROM disruption_events
    ORDER BY recorded_at DESC
    LIMIT 1
""")
```

If the last event was `disrupted`, the banner is red. If it was `restored` and fewer than `RESTORED_GRACE_SECONDS` have elapsed, the banner is amber. Otherwise it's green.

### Status Metric Tiles

Four metric tiles below the banner show shipment counts for each status -- departed, in_transit, delayed and delivered. Each tile uses the same color palette as the analytics dashboard's doughnut chart for consistency across both apps.

### Delayed Shipments by Warehouse

A vertical bar chart shows delayed shipment counts per warehouse, using a Reds colorscale. Warehouses with more delayed shipments appear with a deeper red fill, making the worst-performing warehouses immediately visible.

### Recent Rerouted Shipments

A 10-row table shows the most recent rerouted shipments with the original warehouse, the alternative warehouse and the timestamp. This table is the most direct way to see the disruption routing in action.

### Disruption Events Log

A second 10-row table at the bottom of the page shows the most recent rows from `disruption_events` -- every warehouse disruption and restore, with timestamps. This gives an at-a-glance history of disruption activity during the current simulator run.

## The Analytics Dashboard

### Auto-Refresh and Sync

The analytics dashboard refreshes every 30 seconds. On each refresh it syncs new rows from `shipments` in Snowflake Postgres into `shipment_history` in the Snowflake warehouse using the incremental watermark pattern from Chapter 4:

```python
sf_cur.execute("SELECT MAX(recorded_at) FROM shipment_history")
last_synced_at = sf_cur.fetchone()[0] or datetime(1970, 1, 1, tzinfo=timezone.utc)
```

Only rows newer than the watermark are pulled and appended. The first refresh after a long simulator run may sync thousands of rows; subsequent refreshes sync only the handful of rows that arrived in the last 30 seconds.

The table is created on first use with `CREATE TABLE IF NOT EXISTS`, so the app works whether or not the Chapter 4 notebook has been run:

```python
sf_cur.execute("""
    CREATE TABLE IF NOT EXISTS shipment_history (...)
""")
```

### Connections

The analytics dashboard connects to two systems:

1. **Snowflake Postgres** -- for the incremental sync source.
2. **Snowflake warehouse** -- for all analytics queries.

Both connections are opened at the start of each refresh and closed at the end. The Snowflake warehouse compute is an X-SMALL instance with auto-suspend, so it only consumes credits while queries are running.

### Status Breakdown

The first chart is a doughnut using `go.Pie` with `hole=0.4`. The same `COLOR_MAP` is used throughout both apps for consistency:

```python
COLOR_MAP = {
    "departed":   "#4C9BE8",
    "in_transit": "#2A9D8F",
    "delayed":    "#E63946",
    "delivered":  "#57CC99",
}
```

### Delay Trends Over Time

A dual-axis line chart shows delayed shipment count per minute on the left axis and average delay duration in minutes on the right axis. The dashed line for average delay makes it easy to distinguish the two series without relying on color alone.

### Warehouse Health Heatmap

A `go.Heatmap` shows each warehouse's health score -- the percentage of its shipments that were not delayed -- on a red-to-orange-to-green colorscale. Warehouses that were disrupted during the simulator run tend to show lower health scores, which makes the heatmap a quick way to see which warehouses caused the most disruption.

### Shipment Flow Sankey

A `go.Sankey` diagram shows the volume of shipments flowing from each supplier to each warehouse. The left nodes are suppliers (blue) and the right nodes are warehouses (teal). The width of each link is proportional to the number of shipments that took that route. Rerouting events show up as unexpected links -- suppliers that don't normally connect to a warehouse appearing with a thin link when a disruption forced their shipments there.

### Top Suppliers and SLA Breach

Two horizontal bar charts sit side by side at the bottom of the dashboard. The first ranks suppliers by delay rate using a Reds colorscale. The second ranks warehouses by SLA breach rate -- the percentage of shipments where `delay_minutes` exceeded the threshold -- using an Oranges colorscale. Both charts exclude nodes with fewer than three shipments to avoid misleading results from small samples.

## Gotchas

**`use_container_width` deprecated.** Streamlit deprecated `use_container_width` in favor of `width='stretch'` in recent versions. Use `st.plotly_chart(fig, width='stretch')` rather than `use_container_width=True`.

**`streamlit-autorefresh` `silent` argument removed.** The `silent` keyword argument was removed from `st_autorefresh` in recent versions. Use `st_autorefresh(interval=N)` with no additional arguments.

**`@st.cache_data` and live data.** Setting `ttl` too high on cached queries means the dashboard shows stale data. Set `ttl` to match the refresh interval so that each auto-refresh fetches fresh data from the database.

**Snowflake warehouse auto-suspend.** The X-SMALL compute warehouse suspends after 60 seconds of inactivity. The first query after a suspension has a cold-start delay of a few seconds while the warehouse resumes. This is normal and expected -- the analytics dashboard shows a spinner while the sync and queries run.

**`mogrify` on the Snowflake connector.** The Snowflake connector's `mogrify` may not correctly escape all Python types. Use `executemany` with `%s` placeholders for batch inserts into `shipment_history`.
