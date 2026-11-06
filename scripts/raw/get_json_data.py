from datetime import datetime
from typing import Any
import requests


def validate_date(date_str: str) -> None:
  try:
    datetime.strptime(date_str, "%Y-%m-%d").date()
  except ValueError as exc:
    raise ValueError(f"Invalid date format '{date_str}'. Expected 'YYYY-MM-DD'.") from exc


def validate_date_tuple(date_tup: tuple[str, str]) -> None:
  start, end = date_tup
  validate_date(start)
  validate_date(end)
  if end < start:
    raise ValueError(f"End date '{end}' is before start date '{start}'.")


def create_paged_params(
    media_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    per_page: int | None = None,
    page: int | None = None,
) -> dict[str, Any]:
  params: dict[str, Any] = {}

  if start_date and end_date:
    validate_date_tuple((start_date, end_date))
    params["start_date"] = start_date
    params["end_date"] = end_date

  if media_id:
    params["media_id"] = media_id
  if per_page is not None:
    params["per_page"] = per_page
  if page is not None:
    params["page"] = page

  return params


def build_url(api_base: str, url_pattern: str, media_hash_id: str | None = None) -> str:
  return url_pattern.format(
      api_base=api_base.rstrip("/"),
      mediaHashedId=media_hash_id or "",
  )


def build_headers(api_token: str) -> dict[str, str]:
  return {
      "accept": "application/json",
      "X-Wistia-API-Version": "2026-09",
      "authorization": f"Bearer {api_token}",
  }


def make_json_request(
    url: str,
    api_key: str,
    params: dict[str, Any] | None = None,
) -> Any:
  headers = build_headers(api_key)

  try:
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()
  except requests.exceptions.RequestException as e:
    print(f"API Connection Error for {url}: {e}")
    raise


def fetch_paginated_data(
    url: str,
    api_key: str,
    per_page: int = 50,
    base_params: dict[str, Any] | None = None,
) -> list[Any] | dict[str, Any]:
  """Fetches data from a Wistia endpoint, automatically looping through pages
  if the endpoint returns a list of records reaching `per_page`."""
  all_records: list[Any] = []
  page = 1

  while True:
    page_params = {**(base_params or {}), "per_page": per_page, "page": page}
    data = make_json_request(url=url, api_key=api_key, params=page_params)

    # Single-object endpoints (stats, engagement, metadata) return a dict
    if isinstance(data, dict):
      return data

    # List endpoints (e.g. media_events) return a list of records per page
    if isinstance(data, list):
      all_records.extend(data)
      if len(data) < per_page:
        break
      page += 1
    else:
      break

  return all_records






