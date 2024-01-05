import logging
import os
from typing import Any, Dict, List, Optional

import requests  # type: ignore

from luchtmeetnet_ingestion.luchtmeetnet.const import LUCHTMEETNET_BASE_URL

logger = logging.getLogger("dagster_orchestrators.IO.api")


def get_results_luchtmeetnet_endpoint(
    endpoint: str,
    request_params: Optional[Dict[str, Any]] = None,
    page: int = 1,
    responses: Optional[List] = None,
) -> List[Dict[str, Any]]:
    """Recursively retrieve all results from a luchtmeetnet endpoint.

    See <https://api-docs.luchtmeetnet.nl/#intro>

    Parameters
    ----------
    endpoint : str
        An endpoint from the luchtmeetnet API. Listed here <https://api-docs.luchtmeetnet.nl/#methods>
    request_params : Optional[Dict[str, Any]], optional
        Dictionary with request parameters. These are listed in the method documentation, by default None
    page : int, optional
        page number for which to retrieve results, by default 1
    responses : Optional[List], optional
        list used to append results to, by default None

    Returns
    -------
    List[Dict[str, Any]]
        List of records retrieved from the endpoint
    """
    url = _join_endpoint_to_base_url(endpoint)
    logger.debug(f"Page: {url}")
    logger.debug(f"Request params: {request_params}")
    logger.debug(f"Page: {page}")
    if responses is None:
        responses = []
    if request_params is None:
        _request_params = {"page": page}
    else:
        _request_params = request_params.copy()
        _request_params["page"] = page
    resp = requests.get(url, params=_request_params)
    resp.raise_for_status()
    resp_json = resp.json()
    logger.debug(f"Results: {len(resp_json['data'])}")
    for record in resp_json["data"]:
        responses.append(record)
    if resp_json.get("pagination") is not None:
        current_page = _current_page(resp_json)
        next_page = _next_page(resp_json)
        logger.debug(f"Current page: {current_page}; Next page: {next_page}")
        if current_page != next_page:
            responses = get_results_luchtmeetnet_endpoint(
                endpoint=endpoint,
                request_params=request_params,
                page=next_page,
                responses=responses,
            )
    return responses


def _join_endpoint_to_base_url(endpoint: str) -> str:
    return os.path.join(LUCHTMEETNET_BASE_URL, endpoint)


def _current_page(response_json: Dict[str, Any]) -> int:
    return response_json["pagination"]["current_page"]


def _next_page(response_json: Dict[str, Any]) -> int:
    return response_json["pagination"]["next_page"]
