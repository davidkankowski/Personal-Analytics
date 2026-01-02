import os
import requests
import json
from dotenv import load_dotenv

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
        print("Error before loop:", response.status_code)
        print(response.text)
        return []

    data = response.json()
    results = data["results"]

    # Loop for pagination
    while data["has_more"] and get_all:
        print("Looping to get all pages")
        payload = {
            "page_size": page_size, 
            "start_cursor": data["next_cursor"]
        }
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code != 200:
            print("Error in loop:", response.status_code)
            break
            
        data = response.json()
        results.extend(data["results"])

    print(f"Successfully fetched {len(results)} total rows.")
    return results

if __name__ == "__main__":
    pages = get_pages()

    # To save to a JSON file locally
    # with open('my_data.json', 'w', encoding='utf8') as f:
    #     json.dump(pages, f, ensure_ascii=False, indent=4)
    #     print("Saved raw data to my_data.json")