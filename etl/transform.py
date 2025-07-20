class Transformer:
    '''
    To transform data from sources into a format that can be injested into database
    '''
    pass

class Polymarket_transformer(Transformer):
    '''
    Transform a json from Polymarket api
    '''

    def get_market_table(data):
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
        pass

    def get_event_table(data):
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
        pass
