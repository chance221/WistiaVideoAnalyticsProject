from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, HttpUrl, ValidationError


class Stats(BaseModel):
  model_config = ConfigDict(frozen=True)
  pageLoads: int
  visitors: int
  percentOfVisitorsClickingPlay: int
  plays: int
  averagePercentWatched: int


class Endpoints(BaseModel):
  description: str
  url_pattern: str
  write_strategy: str
  expected_response_columns: Dict[str, Union[str, Dict]]


class MediaConfig(BaseModel):
  model_config = ConfigDict(frozen=True)

  secret_name: str
  api_base: str
  media_ids: List[str]
  url_pattern: str
  endpoints: Dict[str, Endpoints]
  results_per_page: int


class AwsConfig(BaseModel):
  model_config = ConfigDict(frozen=True)
  bucket_name: str
  region: str
  api_secret: str


class EnvironmentConfig(BaseModel):
  model_config = ConfigDict(frozen=True)
  env: str
  aws: AwsConfig


class UserAgentDetails(BaseModel):
  model_config = ConfigDict(frozen=True)
  browser: str
  browser_version: str
  platform: str
  mobile: bool


class Thumbnail(BaseModel):
  model_config = ConfigDict(frozen=True)
  url: HttpUrl  # Automatically validates that this is a properly formed URL
  width: int
  height: int
  fileSize: int
  contentType: str
  type: str


class MediaEvent(BaseModel):
  model_config = ConfigDict(frozen=True)
  received_at: datetime  # Automatically parses ISO timestamp string into a Python datetime object
  event_key: str
  ip: str
  country: str
  region: str
  city: str
  lat: float
  lon: float
  org: str
  email: Optional[str] = None
  percent_viewed: float
  embed_url: str
  conversion_type: str
  conversion_data: Dict[str, Any]
  iframe_heatmap_url: HttpUrl
  visitor_key: str
  user_agent_details: UserAgentDetails
  media_id: str
  media_name: str
  media_url: HttpUrl
  thumbnail: Thumbnail



