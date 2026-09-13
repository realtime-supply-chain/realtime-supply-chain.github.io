"""
analytics_app.py -- Supply Chain Analytics Dashboard
Reads from Snowflake warehouse. Run with:
    streamlit run analytics_app.py --server.port 8502
"""

import os
from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import snowflake.connector
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title = "Supply Chain Analytics",
    page_icon  = "📊",
    layout     = "wide",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_HOST     = os.environ["SNOWFLAKE_PG_HOST"]
PG_USER     = os.environ["SNOWFLAKE_PG_USER"]
PG_PASSWORD = os.environ["SNOWFLAKE_PG_PASSWORD"]
PG_DBNAME   = os.environ["SNOWFLAKE_PG_DBNAME"]

SF_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER      = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SF_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SF_SCHEMA    = os.environ["SNOWFLAKE_SCHEMA"]
SF_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]

SLA_THRESHOLD_MINUTES = 60

STATUS_COLORS = {
    "departed":   "#4C9BE8",
    "in_transit": "#2A9D8F",
    "delayed":    "#E63946",
    "delivered":  "#57CC99",
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_pg_conn():
    """Return a psycopg2 connection to Snowflake Postgres."""
    conn = psycopg2.connect(
        host     = PG_HOST,
        user     = PG_USER,
        password = PG_PASSWORD,
        dbname   = PG_DBNAME,
        sslmode  = "require",
        port     = 5432,
    )
    conn.autocommit = True
    return conn

def get_sf_cursor():
    """Return a connected Snowflake cursor pointed at the analytics schema."""
    sf = snowflake.connector.connect(
        account  = SF_ACCOUNT,
        user     = SF_USER,
        password = SF_PASSWORD,
    )
    cur = sf.cursor()
    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")
    cur.execute(f"USE WAREHOUSE {SF_WAREHOUSE}")
    return sf, cur

def sync_to_warehouse(sf_cur):
    """Pull new rows from Snowflake Postgres and append to shipment_history."""
    sf_cur.execute("""
        CREATE TABLE IF NOT EXISTS shipment_history (
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
    """)

    # Get watermark
    sf_cur.execute("SELECT MAX(recorded_at) FROM shipment_history")
    result = sf_cur.fetchone()[0]
    last_synced_at = result if result else datetime(1970, 1, 1, tzinfo=timezone.utc)
    synced_at = datetime.now(timezone.utc)

    pg_conn = get_pg_conn()
    pg_cur  = pg_conn.cursor()
    pg_cur.execute("""
        SELECT
            shipment_id, supplier_id, warehouse_id, dist_center_id,
            retailer_id, status, delay_minutes, rerouted,
            alt_warehouse_id, recorded_at
        FROM shipments
        WHERE recorded_at > %s
        ORDER BY recorded_at
    """, (last_synced_at,))

    rows = pg_cur.fetchall()
    pg_cur.close()
    pg_conn.close()

    if not rows:
        return 0

    BATCH_SIZE = 500
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch        = rows[i:i + BATCH_SIZE]
        placeholders = ", ".join(["%s"] * 11)
        values_list  = [
            tuple(
                str(v) if hasattr(v, "isoformat") else (None if v is None else v)
                for v in (*row, synced_at)
            )
            for row in batch
        ]
        sf_cur.executemany(
            f"INSERT INTO shipment_history VALUES ({placeholders})",
            values_list,
        )
        total += len(batch)

    return total

# ---------------------------------------------------------------------------
# Query helpers (all read from shipment_history in Snowflake warehouse)
# ---------------------------------------------------------------------------
def fetch_status_breakdown(sf_cur):
    sf_cur.execute("""
        SELECT
            status,
            COUNT(*)                                           AS count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM shipment_history
        GROUP BY status
        ORDER BY count DESC
    """)
    return pd.DataFrame(sf_cur.fetchall(), columns=["status", "count", "pct"])

def fetch_delay_trends(sf_cur):
    sf_cur.execute("""
        SELECT
            DATE_TRUNC('minute', recorded_at) AS minute,
            COUNT(*)                          AS delayed_count,
            ROUND(AVG(delay_minutes), 1)      AS avg_delay_mins
        FROM shipment_history
        WHERE status = 'delayed'
        GROUP BY DATE_TRUNC('minute', recorded_at)
        ORDER BY minute
    """)
    df = pd.DataFrame(sf_cur.fetchall(), columns=["minute", "delayed_count", "avg_delay_mins"])
    df["minute"] = df["minute"].astype(str)
    return df

def fetch_warehouse_health(sf_cur):
    sf_cur.execute("""
        SELECT
            warehouse_id,
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END)        AS delayed,
            SUM(CASE WHEN rerouted = TRUE    THEN 1 ELSE 0 END)        AS rerouted,
            ROUND(
                (1 - SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(*), 0)) * 100, 1
            )                                                           AS health_score
        FROM shipment_history
        GROUP BY warehouse_id
        ORDER BY warehouse_id
    """)
    return pd.DataFrame(
        sf_cur.fetchall(),
        columns=["warehouse_id", "total", "delayed", "rerouted", "health_score"]
    )

def fetch_sankey_data(sf_cur):
    sf_cur.execute("""
        SELECT supplier_id, warehouse_id, COUNT(*) AS flow
        FROM shipment_history
        GROUP BY supplier_id, warehouse_id
        ORDER BY flow DESC
        LIMIT 50
    """)
    return sf_cur.fetchall()

def fetch_top_suppliers(sf_cur):
    sf_cur.execute("""
        SELECT
            supplier_id,
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END)        AS delayed,
            ROUND(
                SUM(CASE WHEN status = 'delayed' THEN 1 ELSE 0 END)
                * 100.0 / COUNT(*), 1
            )                                                           AS delay_rate_pct
        FROM shipment_history
        GROUP BY supplier_id
        HAVING COUNT(*) >= 3
        ORDER BY delay_rate_pct DESC
        LIMIT 10
    """)
    return pd.DataFrame(
        sf_cur.fetchall(),
        columns=["supplier_id", "total", "delayed", "delay_rate_pct"]
    )

def fetch_sla_breach(sf_cur):
    sf_cur.execute("""
        SELECT
            warehouse_id,
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN delay_minutes > %s THEN 1 ELSE 0 END)        AS breaches,
            ROUND(
                SUM(CASE WHEN delay_minutes > %s THEN 1 ELSE 0 END)
                * 100.0 / NULLIF(COUNT(*), 0), 1
            )                                                           AS breach_rate_pct
        FROM shipment_history
        GROUP BY warehouse_id
        HAVING COUNT(*) >= 3
        ORDER BY breach_rate_pct DESC
        LIMIT 10
    """, (SLA_THRESHOLD_MINUTES, SLA_THRESHOLD_MINUTES))
    return pd.DataFrame(
        sf_cur.fetchall(),
        columns=["warehouse_id", "total", "breaches", "breach_rate_pct"]
    )

# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------
def chart_status_doughnut(df):
    fig = go.Figure(go.Pie(
        labels        = df["status"].tolist(),
        values        = df["count"].tolist(),
        marker_colors = [STATUS_COLORS.get(s, "#999") for s in df["status"]],
        hole          = 0.4,
        textinfo      = "label+percent+value",
        showlegend    = False,
    ))
    fig.update_layout(
        title  = "Shipment Status Breakdown",
        height = 350,
        margin = dict(l=0, r=0, t=40, b=0),
    )
    return fig

def chart_delay_trends(df):
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x     = df["minute"],
        y     = df["delayed_count"],
        name  = "Delayed count",
        mode  = "lines+markers",
        yaxis = "y1",
    ))
    fig.add_trace(go.Scatter(
        x     = df["minute"],
        y     = df["avg_delay_mins"],
        name  = "Avg delay (mins)",
        mode  = "lines+markers",
        yaxis = "y2",
        line  = dict(dash="dash"),
    ))
    fig.update_layout(
        title  = "Delay Trends Over Time",
        height = 350,
        margin = dict(l=0, r=0, t=40, b=0),
        yaxis  = dict(title="Delayed count"),
        yaxis2 = dict(title="Avg delay (mins)", overlaying="y", side="right"),
        legend = dict(orientation="h", yanchor="top", y=-0.2),
    )
    return fig

def chart_warehouse_heatmap(df):
    fig = go.Figure(go.Heatmap(
        z          = [df["health_score"].tolist()],
        x          = df["warehouse_id"].tolist(),
        y          = ["Health score"],
        colorscale = [[0, "#E63946"], [0.5, "#F4A261"], [1, "#57CC99"]],
        zmin       = 0,
        zmax       = 100,
        text       = [[f"{v}%" for v in df["health_score"].tolist()]],
        texttemplate = "%{text}",
        showscale  = True,
    ))
    fig.update_layout(
        title  = "Warehouse Health Score",
        height = 200,
        margin = dict(l=0, r=0, t=40, b=0),
    )
    return fig

def chart_sankey(rows):
    suppliers  = sorted(set(r[0] for r in rows))
    warehouses = sorted(set(r[1] for r in rows))
    all_nodes  = suppliers + warehouses
    node_index = {n: i for i, n in enumerate(all_nodes)}

    fig = go.Figure(go.Sankey(
        node = dict(
            label = all_nodes,
            color = (
                ["#4C9BE8"] * len(suppliers) +
                ["#2A9D8F"] * len(warehouses)
            ),
        ),
        link = dict(
            source = [node_index[r[0]] for r in rows],
            target = [node_index[r[1]] for r in rows],
            value  = [r[2] for r in rows],
        ),
    ))
    fig.update_layout(
        title  = "Shipment Flow: Suppliers to Warehouses",
        height = 450,
        margin = dict(l=0, r=10, t=40, b=0),
    )
    return fig

def chart_top_suppliers(df):
    fig = px.bar(
        df.sort_values("delay_rate_pct"),
        x           = "delay_rate_pct",
        y           = "supplier_id",
        orientation = "h",
        title       = "Top Suppliers by Delay Rate",
        labels      = {"delay_rate_pct": "Delay Rate (%)", "supplier_id": "Supplier"},
        color       = "delay_rate_pct",
        color_continuous_scale = "Reds",
    )
    fig.update_layout(
        height              = 350,
        margin              = dict(l=0, r=0, t=40, b=0),
        coloraxis_showscale = False,
        showlegend          = False,
    )
    return fig

def chart_sla_breach(df):
    fig = px.bar(
        df.sort_values("breach_rate_pct"),
        x           = "breach_rate_pct",
        y           = "warehouse_id",
        orientation = "h",
        title       = f"SLA Breach Rate by Warehouse (threshold: {SLA_THRESHOLD_MINUTES} mins)",
        labels      = {"breach_rate_pct": "Breach Rate (%)", "warehouse_id": "Warehouse"},
        color       = "breach_rate_pct",
        color_continuous_scale = "Oranges",
    )
    fig.update_layout(
        height              = 350,
        margin              = dict(l=0, r=0, t=40, b=0),
        coloraxis_showscale = False,
        showlegend          = False,
    )
    return fig

# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------
st.title("Supply Chain Analytics")

st_autorefresh(interval=30 * 1000)

try:
    sf, sf_cur = get_sf_cursor()
except Exception as e:
    st.error(f"Snowflake connection error: {e}")
    st.stop()

with st.spinner("Syncing latest data from Snowflake Postgres..."):
    try:
        rows_synced = sync_to_warehouse(sf_cur)
        st.caption(f"Synced {rows_synced:,} new rows into the warehouse.")
    except Exception as e:
        st.warning(f"Sync warning: {e}")

try:
    status_df   = fetch_status_breakdown(sf_cur)
    delay_df    = fetch_delay_trends(sf_cur)
    health_df   = fetch_warehouse_health(sf_cur)
    sankey_rows = fetch_sankey_data(sf_cur)
    supplier_df = fetch_top_suppliers(sf_cur)
    sla_df      = fetch_sla_breach(sf_cur)
except Exception as e:
    st.error(f"Query error: {e}")
    sf_cur.close()
    sf.close()
    st.stop()

sf_cur.close()
sf.close()

if status_df.empty:
    st.info("No data in the warehouse yet. Run the simulator in Chapter 5 first.")
    st.stop()

# --- Row 1: Status doughnut + Delay trends ---
col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(chart_status_doughnut(status_df), width="stretch")
with col2:
    st.plotly_chart(chart_delay_trends(delay_df), width="stretch")

# --- Row 2: Warehouse heatmap (full width) ---
st.plotly_chart(chart_warehouse_heatmap(health_df), width="stretch")

# --- Row 3: Sankey (full width) ---
if sankey_rows:
    st.plotly_chart(chart_sankey(sankey_rows), width="stretch")

# --- Row 4: Top suppliers + SLA breach ---
col3, col4 = st.columns(2)
with col3:
    st.plotly_chart(chart_top_suppliers(supplier_df), width="stretch")
with col4:
    st.plotly_chart(chart_sla_breach(sla_df), width="stretch")

total = status_df["count"].sum()
st.caption(f"Total shipments in warehouse: {total:,}")
