from polygon import RESTClient
import requests_cache
import requests
from datetime import datetime
import pytz


class CachedRESTClient(RESTClient):
    def __init__(self, auth_key: str, cache_location: str = 'polygon-cache'):
        requests_cache.install_cache(cache_location, filter_fn=self._cache_filter)
        super().__init__(auth_key)

    def _cache_filter(self, resp: requests.Response):
        parsed_response = resp.json()

        try:
            return self._filter_by_from(parsed_response)
        # a key error will be thrown if from is not found in the json response
        # a value error will be thrown if the value cannot be parsed as a date
        # this is important because some api calls to polygon return from not as a date
        except (KeyError, ValueError):
            pass

        try:
            return self._filter_by_unix_timestamp(parsed_response)
        # a key error is thrown if a unix timestamp is not found
        except KeyError:
            pass

        return False

    @staticmethod
    def _filter_by_from(parsed_response):
        # all polygon api requests that use a
        # singular historical date use this format
        return datetime.strptime(parsed_response['from'],
                                 '%Y-%m-%d').date() < datetime.now(
            pytz.timezone('EST')).date()

    @staticmethod
    def _filter_by_unix_timestamp(parsed_response):
        # aggregate results and historic quotes
        # that need to be cached use this format
        return datetime.utcfromtimestamp(
            parsed_response['results'][-1]['t'] / 1000).date() < datetime.now(
            pytz.UTC).date()
