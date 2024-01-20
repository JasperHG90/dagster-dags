import datetime as dt

import pandas as pd
from luchtmeetnet_ingestion.luchtmeetnet.api import (
    _join_endpoint_to_base_url,
    get_results_luchtmeetnet_endpoint,
)


def test_join_endpoint_to_base_url():
    endpoint = "test"
    joined_endpoint = _join_endpoint_to_base_url(endpoint)
    assert joined_endpoint == "https://api.luchtmeetnet.nl/open_api/test"


def test_get_results_luchtmeetnet_endpoint():
    end = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    start = (dt.datetime.now() - dt.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")
    request_params = {
        "station_number": "NL01494",
        "start": start,
        "end": end,
    }
    r = get_results_luchtmeetnet_endpoint("measurements", request_params=request_params)
    _ = pd.DataFrame(r)
