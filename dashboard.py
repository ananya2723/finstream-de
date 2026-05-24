"""
FinStream — Gold Layer Dashboard
Queries star schema (fact + dims + aggregates) from gold.db only.
Auto-refreshes every 3 seconds.
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

from finstream_config import settings

GOLD_DB = settings.gold_db

st.set_page_config(
    page_title="FinStream DE — Gold Layer Analytics",
    page_icon="🏆",
    layout="wide"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Bebas+Neue&display=swap');

    html, body, [class*="css"] { font-family: 'IBM Plex Mono', monospace; }
    h1, h2, h3 { font-family: 'Bebas Neue', sans-serif !important; letter-spacing: 0.05em; }
    .stApp { background: #060b14; color: #cbd5e1; }

    .layer-badge {
        display: inline-block;
        background: linear-gradient(90deg, #f59e0b, #d97706);
        color: #000;
        font-family: 'Bebas Neue', sans-serif;
        font-size: 0.9rem;
        letter-spacing: 0.1em;
        padding: 3px 14px;
        border-radius: 3px;
        margin-bottom: 8px;
    }
    .kpi-box {
        background: #0d1526;
        border: 1px solid #1e3a5f;
        border-top: 3px solid #f59e0b;
        border-radius: 8px;
        padding: 18px;
        text-align: center;
    }
    .kpi-val {
        font-family: 'Bebas Neue', sans-serif;
        font-size: 2.2rem;
        color: #f59e0b;
        line-height: 1;
    }
    .kpi-lbl {
        font-size: 0.65rem;
        color: #475569;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin-top: 4px;
    }
    .pipeline-step {
        background: #0d1526;
        border: 1px solid #1e3a5f;
        border-radius: 6px;
        padding: 10px 14px;
        font-size: 0.75rem;
        color: #64748b;
    }
    .pipeline-active { border-color: #f59e0b44; color: #f59e0b; }
    .alert-row {
        background: #1a0a0a;
        border-left: 3px solid #ef4444;
        padding: 8px 12px;
        margin: 4px 0;
        border-radius: 0 6px 6px 0;
        font-size: 0.75rem;
    }
</style>
""", unsafe_allow_html=True)


def query(sql, params=()):
    try:
        conn = sqlite3.connect(GOLD_DB)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    except:
        return pd.DataFrame()


# ── Header ──
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown('<div class="layer-badge">GOLD LAYER</div>', unsafe_allow_html=True)
    st.markdown("## FinStream — Real-Time Financial Analytics")
with col_h2:
    st.markdown(f"<div style='text-align:right;color:#475569;font-size:0.75rem;margin-top:28px'>🕐 {datetime.now().strftime('%H:%M:%S UTC')}<br>Auto-refresh: 3s</div>", unsafe_allow_html=True)

# ── Pipeline Lineage Banner ──
p1, p2, p3, p4, p5 = st.columns(5)
with p1: st.markdown('<div class="pipeline-step">📡 Kafka Producer</div>', unsafe_allow_html=True)
with p2: st.markdown('<div class="pipeline-step pipeline-active">🟤 Bronze Layer<br><span style="font-size:0.65rem">raw_transactions</span></div>', unsafe_allow_html=True)
with p3: st.markdown('<div class="pipeline-step pipeline-active">⬜ Silver Layer<br><span style="font-size:0.65rem">clean_transactions</span></div>', unsafe_allow_html=True)
with p4: st.markdown('<div class="pipeline-step pipeline-active">🏆 Gold Layer<br><span style="font-size:0.65rem">fact + dims + agg</span></div>', unsafe_allow_html=True)
with p5: st.markdown('<div class="pipeline-step pipeline-active">📊 Dashboard<br><span style="font-size:0.65rem">Gold queries only</span></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

placeholder = st.empty()

while True:
    # ── KPIs from Gold ──
    kpi_df = query("""
        SELECT
            COUNT(*) as total_txns,
            SUM(amount) as total_volume,
            SUM(is_anomaly) as anomalies,
            AVG(amount) as avg_txn,
            SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END)*100.0/COUNT(*) as success_rate,
            COUNT(DISTINCT account_id) as unique_accounts
        FROM fact_transactions
    """)

    anomaly_df = query("""
        SELECT f.transaction_id, f.date_id, f.account_id,
               m.merchant_name, m.category, f.amount, f.anomaly_score, f.event_hour
        FROM fact_transactions f
        JOIN dim_merchant m ON f.merchant_id = m.merchant_id
        WHERE f.is_anomaly = 1
        ORDER BY f.loaded_at DESC LIMIT 8
    """)

    cat_df = query("""
        SELECT category, SUM(total_amount) as total, SUM(txn_count) as count
        FROM agg_daily_category
        GROUP BY category ORDER BY total DESC
    """)

    hourly_df = query("""
        SELECT hour, SUM(txn_count) as txns, SUM(total_amount) as volume
        FROM agg_hourly_volume GROUP BY hour ORDER BY hour
    """)

    recent_df = query("""
        SELECT f.transaction_id, f.date_id, f.event_hour,
               f.account_id, m.merchant_name, m.category,
               f.amount, f.status, f.payment_mode, f.city, f.is_anomaly
        FROM fact_transactions f
        JOIN dim_merchant m ON f.merchant_id = m.merchant_id
        ORDER BY f.loaded_at DESC LIMIT 20
    """)

    top_accounts = query("""
        SELECT f.account_id, a.city,
               COUNT(*) as txns, SUM(f.amount) as total_spend,
               SUM(f.is_anomaly) as flags
        FROM fact_transactions f
        JOIN dim_account a ON f.account_id = a.account_id
        GROUP BY f.account_id ORDER BY total_spend DESC LIMIT 8
    """)

    dq_df = query("""
        SELECT checked_at, input_rows, duplicate_rows, invalid_amount_rows,
               invalid_event_time_rows, valid_rows, anomaly_rows
        FROM data_quality_runs
        ORDER BY checked_at DESC LIMIT 1
    """)

    market_latest_df = query("""
        SELECT symbol, event_time, price, volume, price_change_pct,
               is_price_anomaly, anomaly_score, source
        FROM latest_market_prices
        ORDER BY symbol
    """)

    market_bars_df = query("""
        SELECT symbol, event_minute, close_price, total_volume, tick_count, anomaly_count
        FROM agg_market_minute_bars
        ORDER BY event_minute
    """)

    market_quality_df = query("""
        SELECT checked_at, input_rows, duplicate_rows, invalid_price_rows,
               invalid_event_time_rows, valid_rows, anomaly_rows
        FROM market_quality_runs
        ORDER BY checked_at DESC LIMIT 1
    """)

    with placeholder.container():
        if kpi_df.empty or kpi_df["total_txns"].iloc[0] == 0:
            st.warning("Gold layer is empty — run `python bronze_to_silver.py` and `python silver_to_gold.py` after Bronze has data.")
        else:
            row = kpi_df.iloc[0]

            # KPI Cards
            c1,c2,c3,c4,c5,c6 = st.columns(6)
            for col, val, label in zip(
                [c1,c2,c3,c4,c5,c6],
                [
                    f"₹{row['total_volume']/1e6:.2f}M",
                    f"{int(row['total_txns'])}",
                    f"{int(row['anomalies'])}",
                    f"₹{row['avg_txn']:.0f}",
                    f"{row['success_rate']:.1f}%",
                    f"{int(row['unique_accounts'])}"
                ],
                ["Total Volume","Transactions","Anomalies","Avg Transaction","Success Rate","Unique Accounts"]
            ):
                with col:
                    color = "#ef4444" if label == "Anomalies" and int(row['anomalies']) > 0 else "#f59e0b"
                    st.markdown(f"""<div class="kpi-box">
                        <div class="kpi-val" style="color:{color}">{val}</div>
                        <div class="kpi-lbl">{label}</div>
                    </div>""", unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            if not dq_df.empty:
                dq = dq_df.iloc[0]
                st.caption(
                    "Latest data quality run: "
                    f"{int(dq['valid_rows'])}/{int(dq['input_rows'])} valid rows, "
                    f"{int(dq['duplicate_rows'])} duplicates, "
                    f"{int(dq['invalid_amount_rows'])} invalid amounts, "
                    f"{int(dq['anomaly_rows'])} anomalies."
                )

            # Charts row 1
            ch1, ch2 = st.columns([3, 2])
            with ch1:
                st.markdown("**Hourly Transaction Volume** *(from agg_hourly_volume)*")
                if not hourly_df.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=hourly_df["hour"], y=hourly_df["volume"],
                        marker_color="#f59e0b", opacity=0.85, name="Volume"
                    ))
                    fig.add_trace(go.Scatter(
                        x=hourly_df["hour"], y=hourly_df["txns"],
                        yaxis="y2", line=dict(color="#60a5fa", width=2), name="Txn Count"
                    ))
                    fig.update_layout(
                        plot_bgcolor="#060b14", paper_bgcolor="#060b14",
                        font=dict(color="#94a3b8", size=10),
                        height=230, margin=dict(l=0,r=0,t=10,b=0),
                        legend=dict(bgcolor="#0d1526", bordercolor="#1e3a5f"),
                        xaxis=dict(gridcolor="#0d1526", title="Hour of Day"),
                        yaxis=dict(gridcolor="#1e3a5f", title="Amount (₹)"),
                        yaxis2=dict(overlaying="y", side="right", title="Txn Count", gridcolor="#1e3a5f")
                    )
                    st.plotly_chart(fig, use_container_width=True)

            with ch2:
                st.markdown("**Category Spend** *(from agg_daily_category)*")
                if not cat_df.empty:
                    fig2 = px.pie(
                        cat_df, values="total", names="category",
                        color_discrete_sequence=px.colors.sequential.Oranges_r,
                        hole=0.5
                    )
                    fig2.update_layout(
                        plot_bgcolor="#060b14", paper_bgcolor="#060b14",
                        font=dict(color="#94a3b8", size=10),
                        height=230, margin=dict(l=0,r=0,t=10,b=0),
                        legend=dict(bgcolor="#0d1526", font=dict(size=9))
                    )
                    st.plotly_chart(fig2, use_container_width=True)

            # Charts row 2
            ch3, ch4 = st.columns([1, 2])
            with ch3:
                st.markdown("**Anomaly Alerts** *(fact_transactions)*")
                if anomaly_df.empty:
                    st.markdown("<span style='color:#475569;font-size:0.8rem'>No anomalies in Gold layer.</span>", unsafe_allow_html=True)
                else:
                    for _, r in anomaly_df.iterrows():
                        st.markdown(f"""<div class="alert-row">
                            <b style="color:#f87171">₹{r['amount']:,.0f}</b>
                            · {r['merchant_name']}<br>
                            <span style="color:#475569">{r['account_id']} · score={r['anomaly_score']:.1f}</span>
                        </div>""", unsafe_allow_html=True)

            with ch4:
                st.markdown("**Top Accounts by Spend** *(dim_account JOIN fact)*")
                if not top_accounts.empty:
                    fig3 = px.bar(
                        top_accounts, x="account_id", y="total_spend",
                        color="flags", color_continuous_scale=["#1e3a5f","#ef4444"],
                        hover_data=["city","txns","flags"]
                    )
                    fig3.update_layout(
                        plot_bgcolor="#060b14", paper_bgcolor="#060b14",
                        font=dict(color="#94a3b8", size=10),
                        height=240, margin=dict(l=0,r=0,t=10,b=0),
                        xaxis=dict(gridcolor="#0d1526"),
                        yaxis=dict(gridcolor="#1e3a5f"),
                        coloraxis_colorbar=dict(title="Anomalies", tickfont=dict(size=9))
                    )
                    st.plotly_chart(fig3, use_container_width=True)

            # Live fact table
            st.markdown("**📋 Live Fact Table** *(fact_transactions JOIN dim_merchant)*")
            if not recent_df.empty:
                recent_df["amount"] = recent_df["amount"].apply(lambda x: f"₹{x:,.0f}")
                recent_df["is_anomaly"] = recent_df["is_anomaly"].apply(lambda x: "Yes" if x else "-")
                st.dataframe(
                    recent_df.rename(columns={
                        "transaction_id":"TXN ID","date_id":"Date","event_hour":"Hr",
                        "account_id":"Account","merchant_name":"Merchant","category":"Category",
                        "amount":"Amount","status":"Status","payment_mode":"Mode",
                        "city":"City","is_anomaly":"Anomaly"
                    }),
                    use_container_width=True, height=280
                )

            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("### Real Market Data Gold Layer")
            if market_latest_df.empty:
                st.info("No real market ticks loaded yet. Start the real-data profile with FINNHUB_API_KEY to populate market analytics.")
            else:
                if not market_quality_df.empty:
                    mq = market_quality_df.iloc[0]
                    st.caption(
                        "Latest market quality run: "
                        f"{int(mq['valid_rows'])}/{int(mq['input_rows'])} valid ticks, "
                        f"{int(mq['duplicate_rows'])} duplicates, "
                        f"{int(mq['invalid_price_rows'])} invalid prices, "
                        f"{int(mq['anomaly_rows'])} anomalies."
                    )

                m1, m2, m3, m4 = st.columns(4)
                market_metrics = [
                    (len(market_latest_df), "Tracked Symbols"),
                    (f"{market_latest_df['is_price_anomaly'].sum():.0f}", "Price Anomalies"),
                    (f"{market_latest_df['price_change_pct'].abs().max():.2f}%", "Max Move"),
                    (market_latest_df["source"].iloc[0], "Market Source"),
                ]
                for col, (val, label) in zip([m1, m2, m3, m4], market_metrics):
                    with col:
                        st.markdown(f"""<div class="kpi-box">
                            <div class="kpi-val">{val}</div>
                            <div class="kpi-lbl">{label}</div>
                        </div>""", unsafe_allow_html=True)

                if not market_bars_df.empty:
                    price_fig = px.line(
                        market_bars_df,
                        x="event_minute",
                        y="close_price",
                        color="symbol",
                        title="Minute Close Price"
                    )
                    price_fig.update_layout(
                        plot_bgcolor="#060b14", paper_bgcolor="#060b14",
                        font=dict(color="#94a3b8", size=10),
                        height=280, margin=dict(l=0,r=0,t=40,b=0),
                        xaxis=dict(gridcolor="#0d1526"),
                        yaxis=dict(gridcolor="#1e3a5f")
                    )
                    st.plotly_chart(price_fig, use_container_width=True)

                market_table = market_latest_df.copy()
                market_table["price"] = market_table["price"].map(lambda x: f"{x:,.4f}")
                market_table["volume"] = market_table["volume"].map(lambda x: f"{x:,.2f}")
                market_table["price_change_pct"] = market_table["price_change_pct"].map(lambda x: f"{x:.3f}%")
                market_table["is_price_anomaly"] = market_table["is_price_anomaly"].map(lambda x: "Yes" if x else "-")
                st.dataframe(
                    market_table.rename(columns={
                        "symbol": "Symbol",
                        "event_time": "Event Time",
                        "price": "Price",
                        "volume": "Volume",
                        "price_change_pct": "Move",
                        "is_price_anomaly": "Anomaly",
                        "anomaly_score": "Score",
                        "source": "Source",
                    }),
                    use_container_width=True,
                    height=240,
                )

    time.sleep(3)
    placeholder.empty()
