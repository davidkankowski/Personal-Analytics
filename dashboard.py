import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
import plotly.express as px

# Page configuration
st.set_page_config(page_title="Personal Analytics", layout="wide")
st.title("📊 Personal Habits Dashboard")

# Secure Connection Logic
def get_db_connection():
    # Try Cloud Secrets first, then local .env
    if "DATABASE_URL" in st.secrets:
        db_url = st.secrets["DATABASE_URL"]
    else:
        from dotenv import load_dotenv
        load_dotenv()
        db_url = os.getenv("DATABASE_URL")

    if not db_url:
        return None

    # Force the correct driver for Streamlit
    if "postgresql://" in db_url and "postgresql+psycopg2" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://")

    return create_engine(db_url, connect_args={"sslmode": "require"})

# Load Data
def load_data():
    engine = get_db_connection()
    if not engine:
        st.error("Database URL not found.")
        return pd.DataFrame()
        
    try:
        query = """
        SELECT 
            d.date, 
            d.day_name, 
            h.habit_name, 
            f.is_completed
        FROM fact_habits f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_habit h ON f.habit_id = h.habit_id
        ORDER BY d.date DESC
        """
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# Visualization
df = load_data()

if not df.empty:
    # Metrics
    col1, col2, col3 = st.columns(3)
    total_logs = len(df)
    completion_rate = df['is_completed'].mean() * 100
    
    col1.metric("Total Logs", total_logs)
    col2.metric("Completion Rate", f"{completion_rate:.1f}%")
    col3.metric("Latest Log", str(df['date'].iloc[0]))
    
    # Charts
    st.subheader("📅 Consistency Heatmap")
    
    # Clean Heatmap
    fig = px.scatter(
        df, 
        x="date", 
        y="habit_name", 
        color="is_completed",
        symbol="is_completed",
        size_max=10,
        color_discrete_map={True: "#00CC96", False: "#EF553B"},
        title="Daily Habit Performance"
    )
    fig.update_layout(xaxis_title="Date", yaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    with st.expander("🔍 View Raw Data"):
        st.dataframe(df)

else:
    st.info("Waiting for data - Ensure your GitHub Action runs successfully")