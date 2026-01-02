import os
import requests
import json
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")

headers = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_pages(num_pages=None):
    """
    Pagination: calls API as many times as needed to get all pages
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

if __name__ == "__main__":
    # Extract
    raw_pages = get_pages()
    print(f"Extracted {len(raw_pages)} rows.")
    
    # Transform
    df = clean_notion_data(raw_pages)
    print(f"Transformed data.")
    
    if not df.empty:
        print("\n Clean Data Preview:")
        print(df.head())
    else:
        print("No data found or all rows failed transformation.")