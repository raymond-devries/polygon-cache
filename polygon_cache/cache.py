from polygon import RESTClient
import requests_cache
import requests
from datetime import datetime


class CachedRESTClient(RESTClient):
    def __init__(self, auth_key: str, cache_location: str = 'polygon-cache'):
        requests_cache.install_cache(cache_location, filter_fn=self._cache_filter)
        super().__init__(auth_key)

    @staticmethod
    def _cache_filter(resp: requests.Response):
        parsed_response = resp.json()
        # if any filter function returns True, cache the result
        filter_functions = [
            lambda: datetime.strptime(parsed_response['from'],
                                     '%Y-%m-%d').date() < datetime.today().date(),
        ]

        for func in filter_functions:
            try:
                return func()
            except KeyError:
                pass
        return False
