from datetime import datetime
from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, ConfigDict, HttpUrl, ValidationError


class Stats(BaseModel):
  model_config = ConfigDict(frozen=True)
  pageLoads: int
  visitors: int
  percentOfVisitorsClickingPlay: int
  plays: int
  averagePercentWatched: int


class Endpoints(BaseModel):
  model_config = ConfigDict(frozen=True)
  description: str
  url_pattern: str
  write_strategy: str
  expected_response_columns: Dict[str, Union[str, Dict[str, Any]]]


class MediaConfig(BaseModel):
  model_config = ConfigDict(frozen=True)

  api_base: str
  media_ids: List[str]
  endpoints: Dict[str, Endpoints]
  results_per_page: int


class AwsConfig(BaseModel):
  model_config = ConfigDict(frozen=True)
  bucket_name: str
  region: str
  api_secret: str
  sns_topic_arn: Optional[str] = None
  alert_email: Optional[str] = None


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
  fileSize: Optional[int] = None
  contentType: Optional[str] = None
  type: Optional[str] = None


class MediaViews(BaseModel):
  model_config = ConfigDict(frozen=True)
  id: int
  hashed_id: str
  name: str
  stats: Stats


class MediaEngagement(BaseModel):
  model_config = ConfigDict(frozen=True)
  media_id: Optional[str] = None
  engagement: float
  engagement_data: List[int]
  rewatch_data: Optional[List[int]] = None


class MediaMetadata(BaseModel):
  model_config = ConfigDict(frozen=True)
  id: int
  hashed_id: str
  progress: float
  type: str
  archived: bool
  name: str
  duration: float
  created: datetime
  updated: datetime
  description: Optional[str] = None
  protected: Optional[Union[str, bool]] = None
  status: Optional[Union[str, bool]] = None
  thumbnail: Thumbnail
  section: Optional[Union[str, bool]] = None
  tags: List[Any]
  assets: List[Dict[str, Any]]
  folder: Optional[Dict[str, Any]] = None


class MediaEvent(BaseModel):
  model_config = ConfigDict(frozen=True)
  received_at: datetime  # Automatically parses ISO timestamp string into a Python datetime object
  event_key: str
  ip: Optional[str] = None
  country: Optional[str] = None
  region: Optional[str] = None
  city: Optional[str] = None
  lat: Optional[float] = None
  lon: Optional[float] = None
  org: Optional[str] = None
  email: Optional[str] = None
  percent_viewed: float
  embed_url: Optional[str] = None
  conversion_type: Optional[str] = None
  conversion_data: Dict[str, Any]
  iframe_heatmap_url: HttpUrl
  visitor_key: str
  user_agent_details: UserAgentDetails
  media_id: str
  media_name: str
  media_url: HttpUrl
  thumbnail: Thumbnail


ENDPOINT_MODEL_MAP: Dict[str, type[BaseModel]] = {
    "media_views": MediaViews,
    "media_engagement": MediaEngagement,
    "media_events": MediaEvent,
    "media_metadata": MediaMetadata,
}




