from dotenv import load_dotenv
from utils.config import POLYMARKET_URL
from etl.extract import Extractor
from etl.transform import Polymarket_transformer
from etl.load import Loader
import os, logging


load_dotenv()
host = os.getenv("LOCAL_DB_host")
user = os.getenv("LOCAL_DB_USER")
password = os.getenv("LOCAL_DB_PASSWORD")
port = os.getenv("LOCAL_DB_PORT")

etl_log_path = os.path.join("logs", "etl.txt")



with open(etl_log_path, "a") as f:
    pass

logging.basicConfig(
    filename = etl_log_path,
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)

polymarket_loader = Loader(host, user, password, port, dbname = "polymarket")

events_format = {
    "event_id" : "VARCHAR(20) PRIMARY KEY",
    "title" : "TEXT",
    "startDate" : "TIMESTAMP",
    "endDate" : "TIMESTAMP",
    "liquidity" : "NUMERIC(20, 4)",
    "volume" : "NUMERIC(20, 4)",
    "competitive" : "DOUBLE PRECISION",
    "commentCount" : "INTEGER",
    "tags" : "TEXT[]",
    "lastUpdated" : "TIMESTAMP"
}
polymarket_loader.create_table("events", events_format)

markets_format = {
    "event_id" : "VARCHAR(20)",
    "market_id" : "VARCHAR(20)",
    "question" : "TEXT",
    "startDate" : "TIMESTAMP",
    "endDate" : "TIMESTAMP",
    "yesPrice" : "NUMERIC(20, 4)",
    "noPrice" : "NUMERIC(20, 4)",
    "competitive" : "DOUBLE PRECISION",
    "volume" : "NUMERIC(20, 4)",
    "liquidity" : "NUMERIC(20, 4)",
    "lastUpdated" : "TIMESTAMP"
}
polymarket_loader.create_table("markets", markets_format)

polymarket_data = Extractor.get_json_from_api(POLYMARKET_URL)
events_data = Polymarket_transformer.get_events_table(polymarket_data)
markets_data = Polymarket_transformer.get_markets_table(polymarket_data)

polymarket_loader.update_table("events", events_data, "event_id")
polymarket_loader.update_table("markets", markets_data)

polymarket_loader.close()
