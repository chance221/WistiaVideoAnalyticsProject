import requests
from datetime import datetime
from scripts.aws.secrets_manager import get_secret
from scripts.models.config_models import (
  MediaConfig,
)



RESULTS_PER_PAGE:  # get this from the media.yaml config

def create_paged_params(
      media_id: str | None, 
      start_date: str | None, 
      end_date: str | None, 
      per_page: int | None
      ) -> dict:    
  
  params = {}
  
  if start_date & end_date:
    validate_date_tuple = (start_date, end_date)
    params["start_date"] = start_date
    params['end_date'] = end_date
  
  if media_id:
    params["media_id"] =  per_page
  if per_page:
    params["per_page"]
  return params 
  


def validate_date(date: str):
  try:
    valid_start_date = datetime.strptime(date, "%Y-%m-%d").date()
  except ValueError:
        raise ValueError(f"Invalid date format '{str}'. Expected 'YYYY-MM-DD'.")

def validate_date_tuple(date_tup: tuple):
  
  start, end = date_tup
  
  if end < start :
    raise ValueError("End Date is before the start date")
  else:
      validate_date(start)
      validate_date(end)


def build_url(api_base:str, url_pattern: str, media_hash_id: str) -> str :
  return  f"{api_base}/{url_pattern}/{media_hash_id}/{url_subfix}"


def build_headers(api_token: str):
  headers = {
    "accept": "application/json",
    "X-Wistia-API-Version": "2026-09",
    "authorization": f"Bearer {api_token}"
  }

  return headers


def make_json_request(url: str, apiKey: str, results_per_page: int, params:dict) : 
  headers = build_headers(apiKey)

  params = params

  try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    json_response = response.json()
    return  json_response

  except requests.exceptions.RequestException as e:
    print(f"API Connection Error: {e}")





