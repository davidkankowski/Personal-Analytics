import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import plotly.express as px

st.set_page_config(page_title="Personal Analytics", layout="wide")
st.title("📊 Personal Habits Dashboard")

# Debug method
DATABASE_URL = None
try:
    # Try Cloud Secrets
    if "DATABASE_URL" in st.secrets:
        DATABASE_URL = st.secrets["DATABASE_URL"]
        st.success("Found credentials in Streamlit Cloud Secrets!")
    else:
        # Try Local .env
        load_dotenv()
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL:
            st.info("Found credentials in local .env file.")
        else:
            st.error("No credentials found in Secrets or .env.")
except FileNotFoundError:

    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL:
        st.info("Found credentials in local .env file (Local Mode).")

if not DATABASE_URL:
    st.stop()

# Connection test
try:
    if "postgresql://" in DATABASE_URL and "postgresql+psycopg2" not in DATABASE_URL:
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://")

    engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})
    
    # Simple query to test connection
    with engine.connect() as conn:
        st.toast("Database Connection Successful!")
        
        query = """
        SELECT d.date, h.habit_name, f.is_completed
        FROM fact_habits f
        JOIN dim_date d ON f.date_id = d.date_id
        JOIN dim_habit h ON f.habit_id = h.habit_id
        ORDER BY d.date DESC
        LIMIT 100
        """
        df = pd.read_sql(query, conn)

except Exception as e:
    st.error(f"💥 Connection Error: {e}")
    st.stop()

# Display
if not df.empty:
    st.metric("Total Logs", len(df))
    st.dataframe(df)
else:
    st.warning("No data found in database.")