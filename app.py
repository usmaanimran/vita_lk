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


try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    
    cred_path = os.path.join("data", "serviceAccountKey.json")
    
    
    if firebase_admin._apps:
        DB = firestore.client()
        ST_FIRESTORE_ENABLED = True
    
   
    elif os.path.exists(cred_path):
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        DB = firestore.client()
        ST_FIRESTORE_ENABLED = True
        st.sidebar.success("🔥 Firestore Connected.")
        
   
    else:
        DB = None
        ST_FIRESTORE_ENABLED = False
        if not os.path.exists(cred_path):
             st.sidebar.warning("⚠️ Missing 'data/serviceAccountKey.json'. Live mode disabled.")
        else:
            st.sidebar.warning("⚠️ Firebase Admin error.")

except ImportError:
    DB = None
    ST_FIRESTORE_ENABLED = False
    st.sidebar.error("❌ Install 'firebase-admin' dependency.")
except Exception as e:
    DB = None
    ST_FIRESTORE_ENABLED = False
    st.sidebar.error(f"❌ Firebase Setup Failed: {e}")


GITHUB_USER = "usmaanimran" 
REPO_NAME = "vita_lk"
BRANCH = "main"
BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_USER}/{REPO_NAME}/{BRANCH}/"
CANVAS_APP_ID = "sl_risk_monitor"
CANVAS_USER_ID = "backend_service_user"



@st.cache_data(ttl=30)
def fetch_risk_history_for_charting(source_mode):
    """Fetches historical CSV data for charts (not real-time metrics)."""
    try:
        if source_mode == "Cloud":
            remote_url = BASE_URL + "data/risk_history.csv"
            return pd.read_csv(remote_url, storage_options={'User-Agent': 'Mozilla/5.0'})
        else: 
            local_path = os.path.join("data", "risk_history.csv")
            if os.path.exists(local_path):
                return pd.read_csv(local_path)
            return None
    except Exception as e:
        
        return None

def fetch_live_data():
    """Fetches real-time data from Firestore."""
    if not DB: return None
    try:
       
        doc_ref = DB.collection('artifacts').document(CANVAS_APP_ID).collection('users').document(CANVAS_USER_ID).collection('riskData').document('latest')
        doc = doc_ref.get()
        if doc.exists:
            return doc.to_dict()
        return None
    except Exception as e:
       
        st.warning(f"Live data stream interrupted: {e}") 
        return None


st.markdown("""
    <style>
    .st-emotion-cache-18ni7ap { padding-top: 1rem; } 
    .big-font { font-size: 70px !important; font-weight: 800; line-height: 1.1; }
    .hot-topic-marquee { background-color: #262730; padding: 12px; border-radius: 8px; border-left: 6px solid #FF4B4B; margin-bottom: 25px; color: #ffffff; font-weight: 500; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
    div[data-testid="stMetric"] { background-color: #1E1E1E; border: 1px solid #333; padding: 15px; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)



def main_dashboard(source_mode, live_data):
    
   
    if live_data is None:
        st.warning("📡 Waiting for Live Intelligence Stream or CSV data...")
        return
        
    latest = live_data
    
    risk_score = int(latest.get("Total_Risk", 0))
    timestamp = latest.get("Timestamp", "N/A")
    
   
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

    st.markdown("### 📡 Vita.LK Command Center")
    
    col1, col2, col3 = st.columns([1.2, 1.8, 1])

    with col1:
        st.markdown("##### National Risk Index")
        st.markdown(f'<div class="big-font" style="color:{color_code};">{risk_score}/100</div>', unsafe_allow_html=True)
        st.caption(f"Last Pushed: {timestamp}")

    with col2:
        st.markdown(f"##### System Status: {status_color}")
        st.markdown(f"## {status_msg}")
        st.progress(risk_score / 100)
        
        m1, m2 = st.columns(2)
        with m1:
           
            usd_val = float(latest.get('USD', 0))
            st.metric("USD/LKR Rate", f"LKR {usd_val:.2f}")
        with m2:
           
            oil_val = float(latest.get('Oil_Price', 0))
            st.metric("Brent Crude Oil", f"${oil_val:.2f}")

    with col3:
        st.markdown("##### PESTLE Drivers")
        st.metric("Economic Stress", f"{latest.get('Economic_Risk', 0)}/100")
        st.metric("Social Unrest", f"{latest.get('Social_Risk', 0)}/100")
        st.metric("Environmental", f"{latest.get('Environmental_Risk', 0)}/100")

    st.divider()

   
    st.markdown("### 📊 Strategic Analysis")
    df_chart = fetch_risk_history_for_charting(source_mode)
    
    chart_col1, chart_col2 = st.columns([2, 1])
    
    if df_chart is not None and not df_chart.empty:
        try:
            df_chart['Timestamp'] = pd.to_datetime(df_chart['Timestamp'])
            df_chart = df_chart.sort_values('Timestamp')
        except:
            pass 

        with chart_col1:
            st.markdown("**Synergy Risk Trend (Multi-Factor Interaction)**")
            
            risk_cols = ['Total_Risk', 'Economic_Risk', 'Social_Risk', 'Environmental_Risk', 'News_Risk']
            available_cols = [c for c in risk_cols if c in df_chart.columns]
            
           
            if not df_chart.empty:
                fig = px.line(df_chart, x='Timestamp', y=available_cols, markers=True, 
                              color_discrete_map={"Total_Risk": "#FF4B4B", "Economic_Risk": "#1E88E5", 
                                                  "Social_Risk": "#FFC107", "Environmental_Risk": "#00C853", "News_Risk": "#9C27B0"})
                fig.update_traces(line=dict(width=2))
                fig.for_each_trace(lambda t: t.update(line=dict(width=4)) if t.name == 'Total_Risk' else None)
                fig.update_layout(legend_title="Risk Factors", margin=dict(l=0, r=0, t=30, b=0), height=380, hovermode="x unified", xaxis_title=None, yaxis_title="Risk Score (0-100)")
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
                fig_pie = px.pie(df_pie, values='Score', names='Factor', hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
                fig_pie.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No significant risk drivers active.")
    else:
        st.info("Charts cannot be rendered. Historical data not found.")

    st.markdown("### 📰 Live Intelligence Feed")
   
    headlines_data = latest.get("Headlines", [])
    if headlines_data:
        
        df_news = pd.DataFrame(headlines_data)
        df_news = df_news.sort_values(by='Risk', ascending=False)
        
        st.dataframe(
            df_news[['Headline', 'Risk', 'Sector', 'Link']],
            column_config={
                "Link": st.column_config.LinkColumn("Source"),
                "Risk": st.column_config.ProgressColumn(
                    "Risk Score", format="%d", min_value=0, max_value=100
                ),
            },
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No live news data available.")


def system_footer():
    st.sidebar.markdown("---")
    
   
    source_mode = st.sidebar.radio(
        "Chart Data Source", 
        ["Local (Laptop)", "Cloud (GitHub)"],
        index=0 
    )
    
    if st.sidebar.button("🔄 Force Refresh Data"):
        st.cache_data.clear()
        st.rerun()

   
    live_data = fetch_live_data() 
    
    
    mode_keyword = "Local" if "Local" in source_mode else "Cloud"
    
    
    main_dashboard(mode_keyword, live_data)
    
system_footer()
