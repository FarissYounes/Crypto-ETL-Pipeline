import os
import requests
from dotenv import load_dotenv
from time import sleep
import logging

from utils.load_save import DataType, save_file

# Set .env file path
env_path = ".env"
load_dotenv(env_path)

# Log path
LOG_PATH = os.environ.get("LOG_PATH")
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load API variables
BASE_API_URL = os.environ.get("BASE_API_URL")
API_KEY = os.environ.get("API_KEY")
API_KEY_VALUE = os.environ.get("API_KEY_VALUE")

# API URLs
COINS_METRICS_URL = os.environ.get("COINS_METRICS_URL")
TRENDING_COINS_URL = os.environ.get("TRENDING_COINS_URL")
HISTORICAL_DATA_BY_ID_URL = os.environ.get("HISTORICAL_DATA_BY_ID_URL")

# Data files
TRENDING_COINS_IDS = os.environ.get("TRENDING_COINS_IDS")
COINS_METRICS_1h = os.environ.get("COINS_METRICS_1h")
HISTORICAL_DATA_BY_ID = os.environ.get("HISTORICAL_DATA_BY_ID")

def trending_coins_list() -> list[str] :
    logging.info("Starting to fetch trending coins...")
    
    try:
        response = requests.get(BASE_API_URL + TRENDING_COINS_URL, headers={API_KEY: API_KEY_VALUE})
        logging.info(f"Trending coins fetched successfully (status code {response.status_code})")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error while fetching trending coins: {e.response.status_code} - {e.response.text}")
    
    except requests.exceptions.ConnectionError:
        logging.error("Connection error: unable to reach the server while fetching trending coins")
    
    except requests.exceptions.Timeout:
        logging.error("Request timed out while fetching trending coins")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Unexpected error while fetching trending coins: {e}")

    trending_coins_ids = [coin_info["item"]["id"] for coin_info in response.json()["coins"]]
    logging.info(f"Total trending coins retrieved: {len(trending_coins_ids)}")

    return trending_coins_ids

def extract_coins_market_metrics(ids: list) -> list[dict]:
    logging.info("Starting to fetch market metrics for coins...")

    res = []
    rate_limit = 20

    for i_min in range(0, len(ids), 100):
        
        i_max = min(i_min + 100, len(ids))
        batch = ids[i_min:i_max]
        
        try:
            response = requests.get(
                BASE_API_URL + COINS_METRICS_URL,
                headers={API_KEY: API_KEY_VALUE},
                params={"vs_currency": "usd", "ids": ", ".join(batch), "price_change_percentage": "1h"}
            )
            logging.info(f"Batch fetched successfully ({len(response.json())} coins)")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching market metrics for batch {i_min + 1}-{i_max}: {e}")

        res += response.json()

        rate_limit -= 1

        if rate_limit == 0:
            logging.info("Rate limit reached, sleeping for 50 seconds...")
            sleep(50)
            rate_limit = 20

    logging.info(f"Market metrics fetching completed: {len(res)} coins total")
    return res


def historical_data_by_id(ids: list, date: str) -> list[dict]:
    logging.info(f"Starting to fetch historical data for date {date}...")
    res = []

    for coin_id in ids:
        logging.info(f"Fetching historical data for coin: {coin_id}")
        
        try:
            response = requests.get(
                BASE_API_URL + f"coins/{coin_id}" + HISTORICAL_DATA_BY_ID_URL,
                headers={API_KEY: API_KEY_VALUE},
                params={"date": date}
            )
            logging.info(f"Historical data fetched for {coin_id} (status {response.status_code})")
        
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching historical data for {coin_id}: {e}")

        res.append(response.json())

    logging.info(f"Historical data fetching completed for {len(res)} coins")
    return res

if __name__ == "__main__" :
    
    ids = trending_coins_list()
    save_file(TRENDING_COINS_IDS, ids, DataType.RAW)

    coins_metrics = extract_coins_market_metrics(ids)
    save_file(COINS_METRICS_1h, coins_metrics, DataType.RAW)

    historical_data = historical_data_by_id(ids, "13-11-2025")
    save_file(HISTORICAL_DATA_BY_ID, historical_data, DataType.RAW)
    