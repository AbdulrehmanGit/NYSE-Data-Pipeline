import os 
import requests
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
API_KEY =os.getenv("POLYGON_API_KEY")
# print(f"Using API Key: {API_KEY}")

RAW_DIR = Path("D:/Workspace/NYSE/data/raw/tickers")
RAW_DIR.mkdir(parents=True,exist_ok=True)

url = "https://api.polygon.io/v3/reference/tickers"


def fetch_nyse_data(limit=50):
    params = {
        "exchange": "XNYS",  # NYSE
        "market": "stocks",
        "active": "true",
        "limit": limit,
        "apiKey": API_KEY
    }
     
    response  = requests.get(url,params=params)

    response.raise_for_status()

    
    return response.json()

def save_raw_json(data,date_str):
    filename = RAW_DIR / f"nyse_tickers_{date_str}.json"
    with open(filename,'w') as f:
        json.dump(data,f,indent=2)
    print(f"saved ticker data to {filename}")

today_str = datetime.today().strftime("%Y-%m-%d")

try:
    print(f"fetching data nyse data for {today_str}")
    nyse_data= fetch_nyse_data(limit=50)
    save_raw_json(nyse_data,today_str)
except Exception as e:
    print(f"Encountered Error {e}")
        