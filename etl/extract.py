import requests


class Extractor:
    '''
    To extract from sources
    '''

    @staticmethod
    def get_json_from_api(url):
        '''
        Retrieves a a structured json from an api directly

        Parameter
        url (str) : the url of the source

        Returns
        dict or list : source json

        Raises
        requests.exceptions.HTTPError : the requests fail
        '''
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
