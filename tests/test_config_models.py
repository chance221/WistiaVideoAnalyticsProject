from pydantic import ValidationError
import pytest
from scripts.models.config_models import (
    ENDPOINT_MODEL_MAP,
    MediaEngagement,
    MediaEvent,
    MediaMetadata,
    MediaViews,
)


def test_media_views_validation_and_immutability():
  payload = {
      "id": 128512814,
      "hashed_id": "9k4tbcdfg0",
      "name": "The Gap Method",
      "stats": {
          "pageLoads": 940,
          "visitors": 706,
          "percentOfVisitorsClickingPlay": 42,
          "plays": 328,
          "averagePercentWatched": 26,
      },
  }
  model = MediaViews.model_validate(payload)
  assert model.hashed_id == "9k4tbcdfg0"
  assert model.stats.plays == 328

  with pytest.raises(ValidationError):
    model.name = "Mutated"  # type: ignore[misc]


def test_media_engagement_validation_happy_and_sad_path():
  payload = {
      "engagement": 0.255894,
      "engagement_data": [297, 252, 233],
      "rewatch_data": [12, 8, 5],
  }
  model = MediaEngagement.model_validate(payload)
  assert model.engagement == 0.255894
  assert len(model.engagement_data) == 3

  with pytest.raises(ValidationError):
    MediaEngagement.model_validate({"engagement": "not-a-float", "engagement_data": "not-a-list"})


def test_media_metadata_validation_happy_and_sad_path():
  payload = {
      "id": 128512814,
      "hashed_id": "9k4tbcdfg0",
      "progress": 1,
      "type": "Video",
      "archived": False,
      "name": "The Gap Method",
      "duration": 2195.01,
      "created": "2025-01-04T00:35:10+00:00",
      "updated": "2026-06-29T22:50:15+00:00",
      "description": "",
      "protected": None,
      "status": "ready",
      "thumbnail": {
          "url": "https://embed-ssl.wistia.com/deliveries/thumb.jpg",
          "width": 200,
          "height": 120,
      },
      "section": None,
      "tags": [],
      "assets": [],
  }
  model = MediaMetadata.model_validate(payload)
  assert model.id == 128512814
  assert model.created.year == 2025

  with pytest.raises(ValidationError):
    MediaMetadata.model_validate({**payload, "created": "invalid-datetime"})


def test_media_event_validation_and_invalid_url_rejection():
  valid_event = {
      "received_at": "2026-07-26T10:27:31.000Z",
      "event_key": "evt_123",
      "ip": "172.59.223.55",
      "country": "US",
      "region": "New Jersey",
      "city": "Newark",
      "lat": 40.7641,
      "lon": -74.1654,
      "org": "T-Mobile USA",
      "email": None,
      "percent_viewed": 0.5,
      "embed_url": "https://dataengineerinterviews.com/",
      "conversion_type": "",
      "conversion_data": {},
      "iframe_heatmap_url": "https://api.wistia.com/modern/stats/events/evt_123/iframe.html",
      "visitor_key": "vis_123",
      "user_agent_details": {
          "browser": "Chrome",
          "browser_version": "150",
          "platform": "Android",
          "mobile": True,
      },
      "media_id": "9k4tbcdfg0",
      "media_name": "The Gap Method",
      "media_url": "https://chrisgarzon19.wistia.com/medias/9k4tbcdfg0",
      "thumbnail": {
          "url": "http://embed.wistia.com/deliveries/5859ba.bin",
          "width": 1920,
          "height": 1080,
          "fileSize": 1466800,
          "contentType": "image/png",
          "type": "StillImageFile",
      },
  }
  event = MediaEvent.model_validate(valid_event)
  assert event.event_key == "evt_123"
  assert event.user_agent_details.mobile is True

  invalid_event = {**valid_event, "media_url": "not-a-valid-url"}
  with pytest.raises(ValidationError):
    MediaEvent.model_validate(invalid_event)


def test_endpoint_model_map_has_all_configured_endpoints():
  assert set(ENDPOINT_MODEL_MAP.keys()) == {
      "media_views",
      "media_engagement",
      "media_events",
      "media_metadata",
  }
