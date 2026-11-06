import requests
from datetime import datetime



def make_json_request(url: str, apiKey: str, realuts_per_page: int, params:dict) : 
  headers = {
    "Authorization": f"Bearer {apiKey}"
  }

  params = params

  try:
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    json_response = response.json()
    return  json_response

  except requests.exceptions.RequestException as e:
    print(f"API Connection Error: {e}")


def create_paged_params(
      media_id: str | None, 
      start_date: str | None, 
      end_date: str | None, 
      per_page: int | None) -> dict:    
  
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


def build_url(api_base:str, after_base_prefix: str, media_hash_id: str, url_subfix:str) -> str :
  return  f"{api_base}/{after_base_prefix}/{media_hash_id}/{url_subfix}"


EVENT_KEY = "1776883_daa69fba-127a-47e2-8742-fa2af6bb8fda-983016d26-6b3f1358f61c-354f"
MEDIA_ID_LIST = ["9k4tbcdfg0", "8hunphufxp "]

mediaHashedId = "9k4tbcdfg0"

media_url = "https://api.wistia.com/modern/stats/medias/9k4tbcdfg0"

show_media_api = "https://api.wistia.com/modern/medias/9k4tbcdfg0"
#

show_media_analytics_url = f"https://api.wistia.com/modern/analytics/medias/{mediaHashedId}?start_date=2026-10-03&end_date=2026-11-04"

event_data_url = f"https://api.wistia.com/modern/stats/events/{EVENT_KEY}"

media_engagement_url = f"https://api.wistia.com/modern/stats/medias/{mediaHashedId}/engagement"
# This provides an engagement matrix each second of the video
# egagement score is the percentage of how much the video was watched. Don't care about the engagement data. See if this can come from somewhere else

media_views_url = f"https://api.wistia.com/modern/medias/{mediaHashedId}/stats"
#gets


media_embed_locations = "https://api.wistia.com/modern/analytics/medias/9k4tbcdfg0/embed_locations?start_date=2026-10-03&end_date=2026-11-04"
#where the video is embeded and number of plays

visitor_url = "https://api.wistia.com/modern/stats/visitors"
headers = {
    "accept": "application/json",
    "X-Wistia-API-Version": "2026-09",
    "authorization": "Bearer "
}






response = requests.get(event_data_url, headers=headers)

print(response.text)