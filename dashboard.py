import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import plotly.express as px

st.set_page_config(page_title="Personal Analytics", layout="wide")
st.title("📊 Personal Habits Dashboard")

try:
    # Try loading from Streamlit Cloud Secrets
    DATABASE_URL = st.secrets["DATABASE_URL"]
except (FileNotFoundError, KeyError):
    # If running locally without secrets.toml, use .env
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_resource
def get_db_connection():
    if not DATABASE_URL:
        st.error("Database URL not found. Check your .env file.")
        return None
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            connect_args={"sslmode": "require"}
        )
        return engine
    except Exception as e:
        st.error(f"Connection failed: {e}")
        return None

def load_data():
    engine = get_db_connection()
    if engine:
        # Joining tables to get readable names instead of IDs
        query = """
        SELECT 
            d.date, 
            d.day_name, 
            d.is_weekend,
            h.habit_name, 
            f.status, 
            f.is_completed
        FROM fact_habits f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_habit h ON f.habit_id = h.habit_id
        ORDER BY d.date DESC
        """
        df = pd.read_sql(query, engine)
        return df
    return pd.DataFrame()

df = load_data()

if not df.empty:
    # --- METRICS ROW ---
    col1, col2, col3 = st.columns(3)
    total_logs = len(df)
    completion_rate = df['is_completed'].mean() * 100
    
    col1.metric("Total Logs", total_logs)
    col2.metric("Completion Rate", f"{completion_rate:.1f}%")
    
    # --- CHARTS ---
    st.subheader("📅 Consistency Over Time")
    
    # Simple Heatmap approximation using Scatter
    fig = px.scatter(
        df, 
        x="date", 
        y="habit_name", 
        color="is_completed",
        symbol="is_completed",
        title="Habit Streak View",
        color_discrete_map={True: "green", False: "red"}
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- DATA TABLE ---
    with st.expander("🔍 View Raw Data"):
        st.dataframe(df)

else:
    st.warning("Error: No data found.")