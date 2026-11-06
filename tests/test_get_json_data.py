from unittest.mock import MagicMock, patch
import pytest
import requests
from scripts.raw.get_json_data import (
    build_headers,
    build_url,
    create_paged_params,
    fetch_paginated_data,
    make_json_request,
    validate_date,
    validate_date_tuple,
)


def test_validate_date_and_tuple():
  validate_date("2026-11-06")
  validate_date_tuple(("2026-10-01", "2026-11-06"))

  with pytest.raises(ValueError, match="Invalid date format"):
    validate_date("11/06/2026")

  with pytest.raises(ValueError, match="is before start date"):
    validate_date_tuple(("2026-11-06", "2026-10-01"))


def test_create_paged_params():
  params = create_paged_params(
      media_id="9k4tbcdfg0",
      start_date="2026-10-01",
      end_date="2026-11-01",
      per_page=50,
      page=2,
  )
  assert params == {
      "media_id": "9k4tbcdfg0",
      "start_date": "2026-10-01",
      "end_date": "2026-11-01",
      "per_page": 50,
      "page": 2,
  }


def test_build_url_path_and_query_patterns():
  path_url = build_url(
      "https://api.wistia.com/",
      "{api_base}/modern/medias/{mediaHashedId}/stats",
      "9k4tbcdfg0",
  )
  assert path_url == "https://api.wistia.com/modern/medias/9k4tbcdfg0/stats"

  events_url = build_url(
      "https://api.wistia.com",
      "{api_base}/modern/stats/events",
      "9k4tbcdfg0",
  )
  assert events_url == "https://api.wistia.com/modern/stats/events"


def test_build_headers():
  headers = build_headers("secret_token")
  assert headers["authorization"] == "Bearer secret_token"
  assert headers["accept"] == "application/json"


@patch("scripts.raw.get_json_data.requests.get")
def test_make_json_request_success_and_error(mock_get: MagicMock):
  mock_resp = MagicMock()
  mock_resp.json.return_value = {"id": 123}
  mock_get.return_value = mock_resp

  res = make_json_request("https://api.wistia.com/test", "token123", {"page": 1})
  assert res == {"id": 123}

  mock_get.side_effect = requests.exceptions.ConnectionError("Timeout")
  with pytest.raises(requests.exceptions.RequestException):
    make_json_request("https://api.wistia.com/test", "token123")


@patch("scripts.raw.get_json_data.make_json_request")
def test_fetch_paginated_data_multi_page_list(mock_make_req: MagicMock):
  page_1 = [{"event_key": f"e_{i}"} for i in range(50)]
  page_2 = [{"event_key": f"e_{i}"} for i in range(50, 100)]
  page_3 = [{"event_key": f"e_{i}"} for i in range(100, 115)]
  mock_make_req.side_effect = [page_1, page_2, page_3]

  results = fetch_paginated_data(
      url="https://api.wistia.com/modern/stats/events",
      api_key="token",
      per_page=50,
      base_params={"media_id": "9k4tbcdfg0"},
  )

  assert isinstance(results, list)
  assert len(results) == 115
  assert mock_make_req.call_count == 3


@patch("scripts.raw.get_json_data.make_json_request")
def test_fetch_paginated_data_single_dict_endpoint(mock_make_req: MagicMock):
  mock_make_req.return_value = {"id": 128512814, "hashed_id": "9k4tbcdfg0"}

  result = fetch_paginated_data(
      url="https://api.wistia.com/modern/medias/9k4tbcdfg0/stats",
      api_key="token",
      per_page=50,
  )

  assert result == {"id": 128512814, "hashed_id": "9k4tbcdfg0"}
  assert mock_make_req.call_count == 1
