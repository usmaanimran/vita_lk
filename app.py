import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time
from datetime import datetime, timedelta


st.set_page_config(
    page_title="Vita.lk | Command Center",
    page_icon="📡",
    layout="wide"
)


st.markdown("""
    <style>
    .st-emotion-cache-18ni7ap { padding-top: 1rem; } 
    .big-font { 
        font-size: 70px !important; 
        font-weight: 800; 
        line-height: 1.1;
    }
    .hot-topic-marquee { 
        background-color: #262730; 
        padding: 12px; 
        border-radius: 8px; 
        border-left: 6px solid #FF4B4B; 
        margin-bottom: 25px;
        color: #ffffff;
        font-weight: 500;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    div[data-testid="stMetric"] {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 15px;
        border-radius: 10px;
    }

 
    @media (max-width: 768px) {

        .big-font {
            font-size: 42px !important;
        }

        .st-emotion-cache-18ni7ap {
            padding-top: 0.2rem !important;
        }

        div[data-testid="stMetric"] {
            padding: 10px !important;
        }

        .hot-topic-marquee {
            font-size: 14px !important;
            padding: 8px !important;
        }

        .block-container {
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }

        [data-testid="column"] {
            width: 100% !important;
            flex-direction: column !important;
            flex-basis: 100% !important;
        }

        .js-plotly-plot, .plot-container {
            width: 100% !important;
            height: auto !important;
        }
        
        .stProgress > div > div {
            height: 15px !important;
        }
    }

    </style>
    """, unsafe_allow_html=True)


GITHUB_USER = "usmaanimran" 
REPO_NAME = "vita_lk"
BRANCH = "main"


BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/{BRANCH}/"


@st.cache_data(ttl=30)  
def load_data(filename, source_type="Cloud"):
    local_path = os.path.join("data", filename)
    remote_url = BASE_URL + "data/" + filename

    if source_type == "Cloud":
        try:
            return pd.read_csv(remote_url, storage_options={'User-Agent': 'Mozilla/5.0'})
        except Exception as e:
            if os.path.exists(local_path):
                return pd.read_csv(local_path)
            return None
    else:
        if os.path.exists(local_path):
            return pd.read_csv(local_path)
        return None


@st.fragment(run_every=900)
def main_dashboard(source_mode):
    
    df_risk = load_data("risk_history.csv", source_mode)
    df_news = load_data("daily_news_scan.csv", source_mode)
    df_market = load_data("market_data.csv", source_mode)
    
    if df_risk is None or df_risk.empty:
        st.warning("📡 Waiting for Intelligence Stream...")
        if source_mode == "Cloud":
            st.info(f"Connecting to: {BASE_URL}")
        else:
            st.info("Checking local 'data/' folder.")
        return

    latest = df_risk.iloc[-1]
    
    risk_score = int(latest.get("Total_Risk", 0))
    top_headline = latest.get("Top_Headline", "No Headlines Available")
    timestamp = latest.get("Timestamp", "N/A")
    
    anomaly_val = latest.get('Anomaly_Flag', False)
    if str(anomaly_val).lower() in ['true', '1']:
        st.error(
            "🚨 **STATISTICAL ANOMALY DETECTED:** "
            "Risk Score deviating > 2 Sigma from 24h mean. Immediate attention required."
        )

    if df_news is not None and not df_news.empty:
        emerging = df_news[df_news['Headline'].str.contains('Emerging Trend', case=False, na=False)]
        
        if not emerging.empty:
            clean_trends = [h.replace("⚠️ Emerging Trend:", "").strip() for h in emerging['Headline'].tolist()]
            threat_text = "   🛑   ".join(clean_trends)
            
            st.markdown(f"""
            <div class="hot-topic-marquee">
                <marquee scrollamount="12">🔥 <b>AI DETECTED EMERGING THREATS:</b> {threat_text}</marquee>
            </div>
            """, unsafe_allow_html=True)

    if risk_score > 75:
        status_color = "🔴 CRITICAL"
        status_msg = "ACTIVATE CONTINGENCY"
        color_code = "#FF4B4B"
    elif risk_score > 40:
        status_color = "🟠 ELEVATED"
        status_msg = "MONITOR CLOSELY"
        color_code = "#FF8C00"
    else:
        status_color = "🟢 STABLE"
        status_msg = "BUSINESS AS USUAL"
        color_code = "#3CB371"

    st.markdown("### 📡 Vita.LK")
    
    col1, col2, col3 = st.columns([1.2, 1.8, 1])

    with col1:
        st.markdown("##### National Risk Index")
        st.markdown(f'<div class="big-font" style="color:{color_code};">{risk_score}/100</div>', unsafe_allow_html=True)
        st.caption(f"Last Updated: {timestamp}")

    with col2:
        st.markdown(f"##### System Status: {status_color}")
        st.markdown(f"## {status_msg}")
        st.progress(risk_score / 100)
        
        m1, m2 = st.columns(2)
        with m1:
            usd_val = float(latest.get('USD', 0))
            st.metric("USD/LKR Rate", f"LKR {usd_val:.2f}")
        with m2:
            if df_market is not None and not df_market.empty:
                oil_val = float(df_market.iloc[-1].get('oil_price', 0))
            else:
                oil_val = 0.0
            st.metric("Brent Crude Oil", f"${oil_val:.2f}")

    with col3:
        st.markdown("##### PESTLE Drivers")
        st.metric("Economic Stress", f"{latest.get('Economic_Risk', 0)}/100")
        st.metric("Social Unrest", f"{latest.get('Social_Risk', 0)}/100")
        st.metric("Environmental", f"{latest.get('Environmental_Risk', 0)}/100")

    st.divider()

    st.markdown("### 📊 Strategic Analysis")
    
    
    try:
        df_risk['Timestamp'] = pd.to_datetime(df_risk['Timestamp'])
        df_risk = df_risk.sort_values('Timestamp')
        df_chart = df_risk
    except:
        df_chart = df_risk

    chart_col1, chart_col2 = st.columns([2, 1])

    with chart_col1:
        st.markdown("**Synergy Risk Trend (Multi-Factor Interaction)**")
        
        risk_cols = ['Total_Risk', 'Economic_Risk', 'Social_Risk', 'Environmental_Risk', 'News_Risk']
        available_cols = [c for c in risk_cols if c in df_chart.columns]
        
        if not df_chart.empty:
            fig = px.line(
                df_chart, 
                x='Timestamp', 
                y=available_cols,
                markers=True, 
                color_discrete_map={
                    "Total_Risk": "#FF4B4B",
                    "Economic_Risk": "#1E88E5",
                    "Social_Risk": "#FFC107", 
                    "Environmental_Risk": "#00C853",
                    "News_Risk": "#9C27B0"
                }
            )
            fig.update_traces(line=dict(width=2))
            fig.for_each_trace(lambda t: t.update(line=dict(width=4)) if t.name == 'Total_Risk' else None)
            
            if not df_chart.empty and 'Timestamp' in df_chart.columns:
                last_time = df_chart['Timestamp'].iloc[-1]
                start_zoom = last_time - timedelta(hours=4)
                initial_range = [start_zoom, last_time]
            else:
                initial_range = None

            fig.update_layout(
                legend_title="Risk Factors",
                margin=dict(l=0, r=0, t=30, b=0),
                height=380,
                hovermode="x unified",
                xaxis_title=None,
                yaxis_title="Risk Score (0-100)",
                xaxis=dict(
                    rangeslider=dict(visible=True, thickness=0.05),
                    type="date",
                    range=initial_range 
                )
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Insufficient history for chart.")

    with chart_col2:
        st.markdown("**Current Risk Distribution**")
        
        pie_data = {
            "Economy": latest.get('Economic_Risk', 0),
            "Social": latest.get('Social_Risk', 0),
            "Environment": latest.get('Environmental_Risk', 0),
            "News/Politics": latest.get('News_Risk', 0)
        }
     
        pie_data = {k: v for k, v in pie_data.items() if v > 0}
        
        if pie_data:
            df_pie = pd.DataFrame(list(pie_data.items()), columns=['Factor', 'Score'])
            fig_pie = px.pie(
                df_pie, 
                values='Score', 
                names='Factor',
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig_pie.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No significant risk drivers active.")

    st.markdown("### 📰 Live Intelligence Feed")
    if df_news is not None and not df_news.empty:

        df_news = df_news.sort_values(by='Risk', ascending=False)
        
        st.dataframe(
            df_news,
            column_config={
                "Link": st.column_config.LinkColumn("Source"),
                "Risk": st.column_config.ProgressColumn(
                    "Risk Score",
                    format="%d",
                    min_value=0,
                    max_value=100,
                ),
            },
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No news data available.")


def system_footer():
    st.sidebar.title("Vita.lk")
    
    st.sidebar.markdown("### 🛠 Data Source")
    source_mode = st.sidebar.radio(
        "Select Input Stream:",
        ["Cloud", "Local"],
        index=0,
        help="Cloud: Fetches from GitHub Repo (Robot). Local: Fetches from your laptop."
    )
    
    st.sidebar.markdown("---")
    
    df_market = load_data("market_data.csv", source_mode)
    
    if df_market is not None and not df_market.empty:
        source = df_market.iloc[-1].get('source', 'Unknown')
        if "Live" in str(source):
            status = "🟢 **ONLINE** (Live Feeds)"
        else:
            status = "🟠 **FAIL-SAFE** (Cached Data)"
    else:
        status = "🔴 **OFFLINE**"

    st.sidebar.markdown(f"**System Integrity:**")
    st.sidebar.markdown(status)
    st.sidebar.caption(f"Mode: {source_mode} Stream")
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Force Refresh Cache"):
        st.cache_data.clear()
        st.rerun()
        
    return source_mode


selected_mode = system_footer()
main_dashboard(selected_mode)
