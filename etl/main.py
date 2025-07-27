from dotenv import load_dotenv
from utils.config import POLYMARKET_URL
from etl.extract import Extractor
from etl.transform import Polymarket_transformer
from etl.load import Loader
import os, argparse


load_dotenv()
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
port = os.getenv("DB_PORT")


def set_up():
    '''
    Set up by creating all the required databases and tables

    Returns
    None
    '''
    try:
        Polymarket_db_setup = Loader(host, user, password, port)
        Polymarket_db_setup.create_database("Polymarket")
        print("Successfully created database 'Polymarket'")
        Polymarket_db_setup.close()

    except Exception as e:
        print(f"Error while trying to create database 'Polymarket' : {e}")

    try:
        Polymarket_table_setup = Loader(host, user, password, port, dbname = "Polymarket")
        
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
        Polymarket_table_setup.create_table("events", events_format)
        print("Successfully created table 'events' in database 'Polymarket'")

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
        Polymarket_table_setup.create_table("markets", markets_format)
        print("Successfully created table 'markets' in database 'Polymarket'")

        Polymarket_table_setup.close()

    except Exception as e:
        print(f"Error while trying to create tables for database 'Polymarket' : {e}")


def update():
    '''
    Get data from sources and update the database

    Returns
    None
    '''
    try:
        polymarket_data = Extractor.get_json_from_api(POLYMARKET_URL)
        events_data = Polymarket_transformer.get_events_table(polymarket_data)
        markets_data = Polymarket_transformer.get_markets_table(polymarket_data)

    except Exception as e:
        print(f"Error while trying to get data from Polymarket gammaAPI: {e}")

    try:
        polymarket_loader = Loader(host, user, password, port, dbname = "Polymarket")
        
        polymarket_loader.update_table("events", events_data, "event_id")
        print("Successfully updated table 'events' in database 'Polymarket'")

        polymarket_loader.update_table("markets", markets_data)
        print("Successfully updated table 'markets' in database 'Polymarket'")

        polymarket_loader.close()
    
    except Exception as e:
        print(f"Error while trying to write data to database 'Polymarket': {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "update"])
    args = parser.parse_args()

    if args.action == "setup":
        set_up()

    if args.action == "update":
        update()
