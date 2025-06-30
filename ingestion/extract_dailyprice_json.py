import os 
import requests
import json
from datetime import datetime,timedelta
from pathlib import Path
from dotenv import load_dotenv
from utillity import *


load_dotenv()
API_KEY =os.getenv("POLYGON_API_KEY")


RAW_DIR = Path("D:/Workspace/NYSE/data/raw/daily_agg")
RAW_DIR.mkdir(parents=True,exist_ok=True)

def isweeknd(date):
    return date.weekday() >=5

def fetch_data_for_date(date_str):
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}?adjusted=true&apiKey={API_KEY}"
    response  = requests.get(url)
    if response.status_code != 200:
        print(f"[{date_str}] Error fetching data: {response.status_code}")
        return None
    return response.json()
def find_latest_unloaded_data():
    today = datetime.today()
    date = today - timedelta(days=1)

    while True:
        if isweeknd(date):
            date -= timedelta(days=1)
            continue

        date_str = date.strftime("%Y-%m-%d")
        print(f"Checking data for {date_str}...")

        json_data = fetch_data_for_date(date_str)

        if json_data and json_data.get("resultsCount", 0) > 0:
            print(f"Data exists for {date_str}")

            
            if is_date_already_loaded("nyse_stg.dailyprice_loaded_file", date_str):
                print(f"Data for {date_str} is already loaded. Skipping extraction.")
                return None, None

            return date_str, json_data
        else:
            print(f"No data for {date_str}, going back a day.")
            date -= timedelta(days=1)
def save_raw_json(data,date_str):
    filename = RAW_DIR / f"daily_agg_{date_str}.json"
    with open(filename,'w') as f:
        json.dump(data,f,indent=2)
    print(f"saved daily agg data to {filename}")

def main():
    date_str, json_data = find_latest_unloaded_data()
    if date_str and json_data:
        save_raw_json(json_data, date_str)

        con = get_pg_connection()
        log_ingestion("nyse_stg.dailyprice_loaded_file", date_str)
        con.close()
    else:
        print("No new data to extract.")

main()