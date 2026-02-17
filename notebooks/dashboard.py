import streamlit as st
import plotly.express as px
import pandas as pd
from pyspark.sql import SparkSession
import time

# --- PAGE CONFIGURATION (Dark Mode Financial Terminal) ---
st.set_page_config(
    page_title="Streamflix | Executive Terminal",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- CUSTOM CSS (The "Pro" Look) ---
st.markdown("""
    <style>
    .metric-card {
        background-color: #1e1e1e;
        border-left: 5px solid #ff4b4b;
        padding: 15px;
        border-radius: 5px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
    }
    .metric-label {
        color: #a0a0a0;
        font-size: 14px;
        font-weight: bold;
    }
    .metric-value {
        color: #ffffff;
        font-size: 28px;
        font-weight: bold;
    }
    .metric-delta {
        color: #00ff00;
        font-size: 14px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- INITIALIZE SPARK ---
@st.cache_resource
def get_spark_session():
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
    return SparkSession.builder \
        .appName("Streamflix-Dashboard") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.demo.type", "hadoop") \
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

spark = get_spark_session()

# --- HEADER ---
col_head1, col_head2 = st.columns([3, 1])
with col_head1:
    st.title("🎬 Streamflix | Executive Command")
    st.caption("Real-Time Revenue Ingestion & Analytics Node")
with col_head2:
    # THE LIVE SWITCH
    live_mode = st.checkbox("🔴 LIVE UPDATE", value=False)

# --- DATA FETCHING ---
try:
    df_spark = spark.sql("SELECT * FROM demo.streamflix.revenue_table ORDER BY timestamp DESC")
    # Limit for dashboard performance
    df = df_spark.limit(2000).toPandas()
    
    # FIX: Handle NULL countries so the chart works
    df['country'] = df['country'].fillna("Global/Unknown")

    if not df.empty:
        # --- METRICS CALCULATIONS ---
        total_rev = df['amount'].sum()
        txn_count = len(df)
        avg_ticket = df['amount'].mean()
        
        # Simulating a "trend" (just for visuals in this MVP)
        rev_delta = "+2.4%"
        txn_delta = "+1.8%"

        # --- KPI ROW (Using Custom HTML) ---
        c1, c2, c3 = st.columns(3)
        
        with c1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">TOTAL REVENUE (YTD)</div>
                <div class="metric-value">${total_rev:,.2f}</div>
                <div class="metric-delta">▲ {rev_delta} vs last hr</div>
            </div>
            """, unsafe_allow_html=True)
            
        with c2:
            st.markdown(f"""
            <div class="metric-card" style="border-left: 5px solid #4b94ff;">
                <div class="metric-label">TRANSACTION VOL</div>
                <div class="metric-value">{txn_count}</div>
                <div class="metric-delta">▲ {txn_delta} vs last hr</div>
            </div>
            """, unsafe_allow_html=True)

        with c3:
            st.markdown(f"""
            <div class="metric-card" style="border-left: 5px solid #00ff00;">
                <div class="metric-label">AVG TICKET SIZE</div>
                <div class="metric-value">${avg_ticket:.2f}</div>
                <div class="metric-label">Global Benchmark: $14.50</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")

        # --- CHARTS ROW ---
        chart1, chart2 = st.columns([1, 2])

        with chart1:
            st.subheader("🌍 Geographic Revenue Mix")
            # Group by country
            country_df = df.groupby("country")['amount'].sum().reset_index()
            fig_pie = px.pie(country_df, values='amount', names='country', hole=0.6,
                 color_discrete_sequence=px.colors.sequential.RdBu)
            fig_pie.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig_pie, use_container_width=True)

        with chart2:
            st.subheader("📉 Recent Transaction Ledger")
            # Clean table display
            display_df = df[['timestamp', 'event_type', 'user_id', 'country', 'amount']].head(8)
            st.dataframe(
                display_df, 
                use_container_width=True,
                hide_index=True,
                column_config={
                    "timestamp": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
                    "amount": st.column_config.NumberColumn("Revenue", format="$%.2f")
                }
            )

    else:
        st.warning("⚠️ Connected to Data Lake, but table is empty. Waiting for producer...")

except Exception as e:
    st.error(f"System Error: {e}")

# --- AUTO REFRESH LOOP ---
if live_mode:
    time.sleep(2)
    st.rerun()