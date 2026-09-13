"""
app.py -- Supply Chain Live Operations Dashboard
Reads from Snowflake Postgres. Run with:
    streamlit run app.py --server.port 8501
"""

import os

import pandas as pd
import psycopg2
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title = "Supply Chain Live Ops",
    page_icon  = "🚚",
    layout     = "wide",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_HOST     = os.environ["SNOWFLAKE_PG_HOST"]
PG_USER     = os.environ["SNOWFLAKE_PG_USER"]
PG_PASSWORD = os.environ["SNOWFLAKE_PG_PASSWORD"]
PG_DBNAME   = os.environ["SNOWFLAKE_PG_DBNAME"]

REFRESH_SECONDS        = 10
TABLE_ROWS             = 10
RESTORED_GRACE_SECONDS = 60   # show amber for this long after a restore, then go green

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_conn():
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

@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_status_counts():
    """Return shipment counts grouped by status."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status, COUNT(*) AS count
                FROM shipments
                GROUP BY status
            """)
            rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}

@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_delayed_by_warehouse():
    """Return delayed shipment counts per warehouse."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT warehouse_id, COUNT(*) AS delayed
                FROM shipments
                WHERE status = 'delayed'
                GROUP BY warehouse_id
                ORDER BY delayed DESC
            """)
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["warehouse_id", "delayed"])

@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_recent_reroutes():
    """Return the most recent rerouted shipments."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    LEFT(shipment_id, 8)  AS shipment,
                    warehouse_id          AS original,
                    alt_warehouse_id      AS alternative,
                    recorded_at           AS "recorded at"
                FROM shipments
                WHERE rerouted = TRUE
                ORDER BY recorded_at DESC
                LIMIT %s
            """, (TABLE_ROWS,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)

@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_disruption_events():
    """Return the most recent disruption events."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    warehouse_id  AS warehouse,
                    event_type    AS event,
                    recorded_at   AS "recorded at"
                FROM disruption_events
                ORDER BY recorded_at DESC
                LIMIT %s
            """, (TABLE_ROWS,))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(rows, columns=cols)

@st.cache_data(ttl=REFRESH_SECONDS)
def fetch_current_phase():
    """Return (phase, warehouse_id) from the most recent disruption event."""
    from datetime import datetime, timezone
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT event_type, warehouse_id, recorded_at
                FROM disruption_events
                ORDER BY recorded_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
    if row is None:
        return "normal", None
    event_type, warehouse_id, recorded_at = row
    if event_type == "disrupted":
        return "disrupted", warehouse_id
    elapsed = (datetime.now(timezone.utc) - recorded_at).total_seconds()
    if elapsed > RESTORED_GRACE_SECONDS:
        return "normal", None
    return "restored", warehouse_id

# ---------------------------------------------------------------------------
# Styling helpers
# ---------------------------------------------------------------------------
BANNER_CSS = {
    "normal":    ("background:#1a7f4b; color:#fff;", "Normal operations"),
    "disrupted": ("background:#c0392b; color:#fff;", "DISRUPTION ACTIVE"),
    "restored":  ("background:#d4820a; color:#fff;", "Restored"),
}

STATUS_COLORS = {
    "departed":   "#4C9BE8",
    "in_transit": "#2A9D8F",
    "delayed":    "#E63946",
    "delivered":  "#57CC99",
}

def render_banner(phase, warehouse_id):
    css, label = BANNER_CSS.get(phase, BANNER_CSS["normal"])
    detail = f" -- {warehouse_id} offline" if phase == "disrupted" and warehouse_id else \
             f" -- {warehouse_id} back online" if phase == "restored" and warehouse_id else ""
    st.markdown(
        f"""
        <div style="{css} padding:16px 24px; border-radius:6px;
                    font-size:1.15rem; font-weight:600; margin-bottom:1rem;">
            {label}{detail}
        </div>
        """,
        unsafe_allow_html=True,
    )

def metric_tile(label, value, color):
    st.markdown(
        f"""
        <div style="background:{color}18; border-left:4px solid {color};
                    padding:16px 20px; border-radius:4px;">
            <div style="font-size:0.8rem; color:#666; text-transform:uppercase;
                        letter-spacing:0.05em;">{label}</div>
            <div style="font-size:2rem; font-weight:700; color:{color};">{value:,}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ---------------------------------------------------------------------------
# Main layout
# ---------------------------------------------------------------------------
st_autorefresh(interval=REFRESH_SECONDS * 1000)

st.title("Supply Chain Live Operations")

try:
    phase, warehouse_id   = fetch_current_phase()
    counts                = fetch_status_counts()
    delayed_wh_df         = fetch_delayed_by_warehouse()
    reroutes_df           = fetch_recent_reroutes()
    disruption_df         = fetch_disruption_events()
except Exception as e:
    st.error(f"Database connection error: {e}")
    st.stop()

# --- Disruption banner ---
render_banner(phase, warehouse_id)

# --- Status metric tiles ---
cols = st.columns(4)
for col, status in zip(cols, ["departed", "in_transit", "delayed", "delivered"]):
    with col:
        metric_tile(
            status.replace("_", " ").title(),
            counts.get(status, 0),
            STATUS_COLORS[status],
        )

st.markdown("<br>", unsafe_allow_html=True)

# --- Delayed by warehouse / Recent reroutes ---
left, right = st.columns(2)

with left:
    st.subheader("Delayed shipments by warehouse")
    if delayed_wh_df.empty:
        st.info("No delayed shipments.")
    else:
        import plotly.express as px
        fig = px.bar(
            delayed_wh_df,
            x     = "warehouse_id",
            y     = "delayed",
            color = "delayed",
            color_continuous_scale = "Reds",
            labels = {"warehouse_id": "Warehouse", "delayed": "Delayed shipments"},
        )
        fig.update_layout(
            height              = 300,
            margin              = dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale = False,
            showlegend          = False,
        )
        st.plotly_chart(fig, width="stretch")

with right:
    st.subheader("Recent rerouted shipments")
    if reroutes_df.empty:
        st.info("No rerouted shipments yet.")
    else:
        st.dataframe(reroutes_df, width="stretch", hide_index=True)

# --- Disruption events log ---
st.subheader("Disruption events log")
if disruption_df.empty:
    st.info("No disruption events recorded yet.")
else:
    st.dataframe(disruption_df, width="stretch", hide_index=True)

# --- Footer + auto-refresh ---
total = sum(counts.values())
st.caption(f"Total shipments: {total:,} · Refreshing every {REFRESH_SECONDS}s")
