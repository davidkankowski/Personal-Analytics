import os
import requests
import json
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

headers = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_pages(num_pages=None):
    """
    Extraction: calls API as many times as needed to get all pages
    """
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

    get_all = num_pages is None
    page_size = 100 if get_all else num_pages

    payload = {"page_size": page_size}
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print("Error:", response.status_code)
        print(response.text)
        return []

    data = response.json()
    results = data["results"]

    # Loop for pagination
    while data["has_more"] and get_all:
        payload = {
            "page_size": page_size, 
            "start_cursor": data["next_cursor"]
        }
        response = requests.post(url, json=payload, headers=headers)
        data = response.json()
        results.extend(data["results"])

    return results

def clean_notion_data(data):
    """
    Transformation: cleans raw Notion JSON into a flat DataFrame
    """
    clean_rows = []
    
    for page in data:
        page_id = page['id'] 
        props = page['properties']
        
        try:
            # Habit column            
            if props['Habit']['title']:
                habit_name = props['Habit']['title'][0]['text']['content']
            else:
                habit_name = "Untitled"

            # Date column
            raw_date = props['Date']['date']['start']

            # Status column
            status_name = props['Status']['status']['name']

            # Convert Status to boolean value
            is_completed = (status_name == "Done")
            
            clean_rows.append({
                            "page_id": page_id,
                            "habit_name": habit_name,
                            "date": raw_date,
                            "status": status_name,       
                            "is_completed": is_completed 
            })

        except KeyError as e:
            print(f"Row skipped. Missing column {e}. Check Notion column names.")
            continue
        except TypeError as e:
            print(f"Row skipped. Data format issue: {e}")
            continue

    df = pd.DataFrame(clean_rows)

    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])
    
    return df

def transform_to_star_schema(df):
    """
    Transformation: transform flat DataFrame into Star Schema (Fact + Dimension)
    """
    # Date Dimension
    dim_date = df[['date']].drop_duplicates().reset_index(drop=True)
    dim_date['date_id'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['date'].dt.year
    dim_date['month'] = dim_date['date'].dt.month
    dim_date['day'] = dim_date['date'].dt.day
    dim_date['day_name'] = dim_date['date'].dt.day_name()
    dim_date['is_weekend'] = dim_date['date'].dt.weekday >= 5

    # Habit Dimension
    dim_habit = df[['habit_name']].drop_duplicates().reset_index(drop=True)
    dim_habit['habit_id'] = dim_habit.index + 100 

    # Fact Table
    fact_table = df.merge(dim_date, on='date', how='left')
    fact_table = fact_table.merge(dim_habit, on='habit_name', how='left')
    
    fact_habits = fact_table[[
        'page_id', 'date_id', 'habit_id', 'is_completed', 'status'
    ]]

    return {
        "dim_date": dim_date,
        "dim_habit": dim_habit,
        "fact_habits": fact_habits
    }

def load_to_supabase(data_model):
    """
    Load: Connects to Supabase and loads the 3 tables
    """
    if not DATABASE_URL:
        print("Error: DATABASE_URL is missing from .env")
        return
    
    try:
        # Create SQL Connection with Pessimistic checks
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            connect_args={"sslmode": "require"}
            )
        print("Connecting to Supabase")

        # Load Dimensions (replace allows updates to definitions)
        print("Loading dim_date.")
        data_model['dim_date'].to_sql('dim_date', engine, if_exists='replace', index=False)
        
        print("Loading dim_habit.")
        data_model['dim_habit'].to_sql('dim_habit', engine, if_exists='replace', index=False)
        
        # Load Fact Table (replace allows simple/idempotent)
        print("Loading fact_habits.")
        data_model['fact_habits'].to_sql('fact_habits', engine, if_exists='replace', index=False)
        
        print("ETL Pipeline finished successfully")

    except Exception as e:
        print(f"Database Load Failed: {e}")


if __name__ == "__main__":
    print("Starting ETL Pipeline.")

    # Extract
    raw_pages = get_pages()
    print(f"Extracted {len(raw_pages)} raw rows.")

    # Transform
    if raw_pages:
        df = clean_notion_data(raw_pages)
        print(f"Cleaned data.")
        
        if not df.empty:
            star_schema = transform_to_star_schema(df)
            print("Star Schema generated.")
            
            # Load
            load_to_supabase(star_schema)
        else:
            print("Transformation resulted in empty data.")
    else:
        print("No data found in Notion.")