from polygon_cache.cache import CachedRESTClient
import pytest
import requests
import responses
import os


@pytest.fixture
def fake_json_request_on_2020_01_17(tmp_path, freezer):
    def _fake_request(json_data: dict):
        freezer.move_to('2020-01-17')
        temp = str(tmp_path)
        responses.add(responses.GET, 'http://url.com',
                      json=json_data)
        resp = requests.get('http://url.com')
        client = CachedRESTClient('api_key', cache_location=temp)
        return resp, client

    return _fake_request


def test_cache_creation(tmpdir):
    temp = str(tmpdir.join('polygon-cache'))
    CachedRESTClient('api_key', cache_location=temp)
    assert os.path.isfile(temp + '.sqlite')


@pytest.mark.parametrize('date,expected_filter_response',
                         [('2020-01-15', True),
                          ('2020-01-17', False),
                          ('2020-01-30', False)])
@responses.activate
def test_cache_filter_from(date, expected_filter_response,
                           fake_json_request_on_2020_01_17):
    data = {'from': date}
    resp, client = fake_json_request_on_2020_01_17(data)
    assert client._cache_filter(resp) is expected_filter_response
