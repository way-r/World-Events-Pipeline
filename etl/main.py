from dotenv import load_dotenv
from utils.config import POLYMARKET_URL
from etl.extract import Extractor
from etl.transform import Polymarket_transformer
from etl.load import Loader
import os, logging, argparse


load_dotenv()
host = os.getenv("LOCAL_DB_HOST")
user = os.getenv("LOCAL_DB_USER")
password = os.getenv("LOCAL_DB_PASSWORD")
port = os.getenv("LOCAL_DB_PORT")

etl_log_path = os.path.join("logs", "etl.txt")
logging.basicConfig(
    filename = etl_log_path,
    level = logging.INFO,
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S"
)


def setup():
    '''
    Set up by creating all the required databases and tables

    Returns
    None
    '''
    with open(etl_log_path, "a"):
        pass
    
    try:
        Polymarket_db_setup = Loader(host, user, password, port)
        Polymarket_db_setup.create_database("Polymarket")
        logging.info("Successfully created database 'Polymarket'")
        Polymarket_db_setup.close()

    except Exception as e:
        logging.error(f"Error while trying to create database 'Polymarket' : {e}")

    try:
        Polymarket_table_setup = Loader(host, user, password, port, dbname = "Polymarket")
        
        events_format = {
            "event_id" : "VARCHAR(20) PRIMARY KEY",
            "title" : "TEXT",
            "start_date" : "TIMESTAMP",
            "end_date" : "TIMESTAMP",
            "liquidity" : "NUMERIC(20, 4)",
            "volume" : "NUMERIC(20, 4)",
            "competitive" : "DOUBLE PRECISION",
            "comment_count" : "INTEGER",
            "tags" : "TEXT[]",
            "last_updated" : "TIMESTAMP"
        }
        Polymarket_table_setup.create_table("events", events_format)
        logging.info("Successfully created table 'events' in database 'Polymarket'")

        markets_format = {
            "event_id" : "VARCHAR(20)",
            "market_id" : "VARCHAR(20)",
            "question" : "TEXT",
            "start_date" : "TIMESTAMP",
            "end_date" : "TIMESTAMP",
            "yes_price" : "NUMERIC(20, 4)",
            "no_price" : "NUMERIC(20, 4)",
            "competitive" : "DOUBLE PRECISION",
            "volume" : "NUMERIC(20, 4)",
            "liquidity" : "NUMERIC(20, 4)",
            "last_updated" : "TIMESTAMP"
        }
        Polymarket_table_setup.create_table("markets", markets_format)
        logging.info("Successfully created table 'markets' in database 'Polymarket'")

        Polymarket_table_setup.close()

    except Exception as e:
        Polymarket_table_setup.close()
        logging.error(f"Error while trying to create tables for database 'Polymarket' : {e}")


def update():
    '''
    Get data from sources and update the database

    Returns
    None
    '''
    with open(etl_log_path, "a"):
        pass

    try:
        polymarket_data = Extractor.get_json_from_api(POLYMARKET_URL)
        events_data = Polymarket_transformer.get_events_table(polymarket_data)
        markets_data = Polymarket_transformer.get_markets_table(polymarket_data)

    except Exception as e:
        logging.error(f"Error while trying to get data from Polymarket gammaAPI: {e}")

    try:
        polymarket_loader = Loader(host, user, password, port, dbname = "Polymarket")
        
        polymarket_loader.update_table("events", events_data, "event_id")
        logging.info("Successfully updated table 'events' in database 'Polymarket'")

        polymarket_loader.update_table("markets", markets_data)
        logging.info("Successfully updated table 'markets' in database 'Polymarket'")

        polymarket_loader.close()
    
    except Exception as e:
        polymarket_loader.close()
        logging.error(f"Error while trying to write data to database 'Polymarket': {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "update"])
    args = parser.parse_args()

    if args.action == "setup":
        setup()

    if args.action == "update":
        update()
