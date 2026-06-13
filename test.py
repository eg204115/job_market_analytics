from dotenv import load_dotenv
import os

import requests  

load_dotenv() 

X_API_KEY = os.getenv('X-API-Key')

def fetch_jobs(query="data engineer"):
    params = {
        "query": query,
    }
    headers = {
        "X-API-Key": X_API_KEY
    }

    response = requests.get(
        'https://api.openwebninja.com/jsearch/search-v2',
        headers=headers,
        params=params
    )

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching jobs: {response.status_code}")
        return None
    