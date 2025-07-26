from dateutil import parser
from datetime import datetime, timezone
import json, logging

class Transformer:
    '''
    To transform data from sources into a format that can be injested into database
    '''
    pass

class Polymarket_transformer(Transformer):
    '''
    Transform a json from Polymarket api
    '''

    @staticmethod
    def get_events_table(data):
        '''
        Create a table that has information about all events

        Parameter
        data (list) : json form Polymarket api

        Returns
        list of dicts : 
            [
                {
                    "event_id" : id,
                    "title" : title,
                    "startDate" : startDate,
                    "endDate" : endDate,
                    "liquidity" : liquidity,
                    "volume" : volume,
                    "competitive" : competitive,
                    "commentCount" : comment_count,
                    "tags" : [tags,],
                    "lastUpdated" : timestamp
                },
            ]
        '''
        res = []
        timestamp = datetime.now(timezone.utc).isoformat(timespec = "microseconds").replace("+00:00", "Z")

        for event in data:
            event_details = dict()

            try:
                event_details["event_id"] = str(event.get("id"))
                event_details["title"] = event.get("title")

                try:
                    startDate = parser.parse(event.get("startDate"))
                    startDate = startDate.replace(tzinfo = timezone.utc)
                    event_details["startDate"] = startDate
                except (TypeError, ValueError):
                    event_details["startDate"] = None

                try:
                    endDate = parser.parse(event.get("endDate"))
                    endDate = endDate.replace(tzinfo = timezone.utc)
                    event_details["endDate"] = endDate
                except (TypeError, ValueError):
                    event_details["endDate"] = None

                try:
                    event_details["liquidity"] = float(event.get("liquidity"))
                except (TypeError, ValueError):
                    event_details["liquidity"] = None

                try:
                    event_details["volume"] = float(event.get("volume"))
                except (TypeError, ValueError):
                    event_details["volume"] = None

                try:
                    event_details["competitive"] = float(event.get("competitive"))
                except (TypeError, ValueError):
                    event_details["competitive"] = None
                
                try:
                    event_details["commentCount"] = int(event.get("commentCount"))
                except (TypeError, ValueError):
                    event_details["commentCount"] = None

                try:
                    tags = []
                    for tag_content in event.get("tags"):
                        tags.append(tag_content.get("label"))
                    event_details["tags"] = tags
                except (TypeError, ValueError):
                    event_details["tags"] = None

                event_details["lastUpdated"] = timestamp

                res.append(event_details)

            except Exception as e:
                logging.warning(f"Error while getting details for event {event_details["event_id"]}: {e}")
                continue

        return res


    @staticmethod
    def get_markets_table(data):
        '''
        Create a table that has information about all markets

        Parameter
        data (list) : json form Polymarket api

        Returns
        list of dicts : 
            [
                {
                    "event_id" : event_id,
                    "market_id" : id,
                    "question" : question,
                    "startDate" : startDate,
                    "endDate" : endDate,
                    "yesPrice" : yesPrice,
                    "noPrice" : noPrice,
                    "volume" : volume,
                    "liquidity" : liquidity,
                    "lastUpdated" : timestamp
                },
            ]
        '''
        res = []
        timestamp = datetime.now(timezone.utc).isoformat(timespec = "microseconds").replace("+00:00", "Z")

        for event in data:
            event_id = str(event.get("id"))

            for market in event["markets"]:
                market_details = dict()

                try:
                    market_details["event_id"] = event_id
                    market_details["market_id"] = str(market.get("id"))
                    market_details["question"] = market.get("question")
                    
                    try:
                        startDate = parser.parse(market.get("startDate"))
                        startDate = startDate.replace(tzinfo = timezone.utc)
                        market_details["startDate"] = startDate
                    except (TypeError, ValueError):
                        market_details["startDate"] = None

                    try:
                        endDate = parser.parse(market.get("endDate"))
                        endDate = endDate.replace(tzinfo = timezone.utc)
                        market_details["endDate"] = endDate
                    except (TypeError, ValueError):
                        market_details["endDate"] = None

                    try:
                        prices = json.loads(market.get("outcomePrices"))
                        market_details["yesPrice"] = float(prices[0])
                        market_details["noPrice"] = float(prices[1])

                    except (json.JSONDecodeError, TypeError, ValueError):
                        market_details["yesPrice"] = None
                        market_details["noPrice"] = None

                    try:
                        market_details["competitive"] = float(market.get("competitive"))
                    except (TypeError, ValueError):
                        market_details["competitive"] = None

                    try:
                        market_details["volume"] = float(market.get("volume"))
                    except (TypeError, ValueError):
                        market_details["volume"] = None

                    try:
                        market_details["liquidity"] = float(market.get("liquidity"))
                    except (TypeError, ValueError):
                        market_details["liquidity"] = None

                    market_details["lastUpdated"] = timestamp

                    res.append(market_details)

                except Exception as e:
                    logging.warning(f"Error while getting details for market {market_details["market_id"]}: {e}")
                    continue

        return res
