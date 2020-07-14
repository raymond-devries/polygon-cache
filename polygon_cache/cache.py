from typing import List

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from polygon import RESTClient
from polygon.rest.models import StocksEquitiesAggregatesApiResponse
import requests_cache
import requests
import pytz


class CachedRESTClient(RESTClient):
    def __init__(self, auth_key: str, cache_location: str = 'polygon-cache'):
        requests_cache.install_cache(cache_location, filter_fn=self._cache_filter)
        super().__init__(auth_key)

    def _cache_filter(self, resp: requests.Response) -> bool:
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
    def _filter_by_from(parsed_response: dict) -> bool:
        # all polygon api requests that use a
        # singular historical date use this format
        return datetime.strptime(parsed_response['from'],
                                 '%Y-%m-%d').date() < datetime.now(
            pytz.timezone('EST')).date()

    @staticmethod
    def _filter_by_unix_timestamp(parsed_response: dict) -> bool:
        # aggregate results and historic quotes
        # that need to be cached use this format
        return datetime.utcfromtimestamp(
            parsed_response['results'][-1]['t'] / 1000).date() < datetime.now(
            pytz.UTC).date()

    def stocks_equities_aggregates(self, ticker, multiplier, timespan, from_, to,
                                   max_threads=20, **query_params
                                   ) -> StocksEquitiesAggregatesApiResponse:
        start = datetime.strptime(from_, '%Y-%m-%d')
        end = datetime.strptime(to, '%Y-%m-%d')
        if timespan == 'minute' or timespan == 'hour':
            max_days_calls = 5
        else:
            max_days_calls = 3000

        dates_api_calls = self._calculate_aggregate_api_calls(start, end,
                                                              max_days_calls)

        executor = ThreadPoolExecutor(max_threads)
        api_responses = []
        for dates in dates_api_calls:
            date1 = dates[0].strftime('%Y-%m-%d')
            date2 = dates[1].strftime('%Y-%m-%d')
            api_responses.append(executor.submit(
                super().stocks_equities_aggregates, ticker, multiplier, timespan,
                date1, date2))

        api_responses = [result.result() for result in api_responses]
        return self._combine_aggregate_results(api_responses)

    @staticmethod
    def _calculate_aggregate_api_calls(start: datetime, end: datetime,
                                       days: int) -> List[tuple]:
        current_day = start
        period = timedelta(days=days)
        dates = []
        while current_day <= end:
            value1 = current_day
            next_date = current_day + period
            if end > next_date:
                value2 = next_date
            else:
                value2 = end
            dates.append((value1, value2))
            current_day = value2 + timedelta(1)

        return dates

    @staticmethod
    def _combine_aggregate_results(
            api_responses: List[StocksEquitiesAggregatesApiResponse]
            ) -> StocksEquitiesAggregatesApiResponse:
        first_result = api_responses[0]
        expected_ticker = first_result.ticker
        expected_status = first_result.status
        expected_adjusted = first_result.adjusted
        query_count_combined = 0
        results_count_combined = 0
        results_combined = []
        for api_response in api_responses:
            if api_response.ticker != expected_ticker:
                raise ValueError(
                    f'Multiple tickers encountered while trying to combine results: '
                    f'{api_response.ticker} and {expected_ticker}')

            if api_response.status != expected_status:
                raise ValueError(
                    f'Multiple statuses encountered while trying to combine results: '
                    f'{api_response.status} and {expected_status}')

            if api_response.adjusted != expected_adjusted:
                raise ValueError(
                    f'Multiple adjusted values encountered while trying to '
                    f'combine results: {api_response.adjusted} and {expected_adjusted}')

            query_count_combined += api_response.queryCount
            results_count_combined += api_response.resultsCount
            results_combined += api_response.results

        combined_api_response = StocksEquitiesAggregatesApiResponse()
        combined_api_response.ticker = expected_ticker
        combined_api_response.status = expected_status
        combined_api_response.adjusted = expected_adjusted
        combined_api_response.queryCount = query_count_combined
        combined_api_response.resultsCount = results_count_combined
        combined_api_response.results = results_combined

        return combined_api_response
