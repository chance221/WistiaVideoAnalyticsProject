import pytest
from scripts.silver.transformer import (
    deduplicate_by_primary_key,
    transform_validated_records_for_silver,
)


def test_transform_all_endpoints_happy_path():
  views_in = [{
      "id": 1,
      "hashed_id": "9k4tbcdfg0",
      "name": "Video A",
      "stats": {
          "pageLoads": 100,
          "visitors": 80,
          "percentOfVisitorsClickingPlay": 50,
          "plays": 40,
          "averagePercentWatched": 60,
      },
      "ingested_at": "2026-11-06T17:00:00+00:00",
  }]
  views_out = transform_validated_records_for_silver("media_views", views_in)
  assert views_out[0]["page_loads"] == 100
  assert views_out[0]["plays"] == 40

  eng_in = [{
      "media_id": None,
      "engagement": 0.25,
      "engagement_data": [10, 20],
      "rewatch_data": None,
      "ingested_at": "2026-11-06T17:00:00+00:00",
  }]
  eng_out = transform_validated_records_for_silver(
      "media_engagement", eng_in, media_id="9k4tbcdfg0"
  )
  assert eng_out[0]["media_id"] == "9k4tbcdfg0"
  assert eng_out[0]["rewatch_data"] == []

  meta_in = [{
      "id": 1,
      "hashed_id": "9k4tbcdfg0",
      "name": "Video A",
      "type": "Video",
      "archived": False,
      "duration": 120.5,
      "created": "2025-01-04T00:35:10+00:00",
      "updated": "2026-06-29T22:50:15+00:00",
      "description": "Test",
      "protected": None,
      "status": "ready",
      "thumbnail": {"url": "https://wistia.com/t.jpg", "width": 200, "height": 120},
      "folder": {"id": 99, "name": "Main Folder"},
      "ingested_at": "2026-11-06T17:00:00+00:00",
  }]
  meta_out = transform_validated_records_for_silver("media_metadata", meta_in)
  assert meta_out[0]["thumbnail_url"] == "https://wistia.com/t.jpg"
  assert meta_out[0]["folder_id"] == 99

  evt_in = [{
      "event_key": "evt_1",
      "received_at": "2026-07-26T10:27:31+00:00",
      "media_id": "9k4tbcdfg0",
      "media_name": "Video A",
      "media_url": "https://wistia.com/medias/9k4tbcdfg0",
      "visitor_key": "v_1",
      "ip": "1.2.3.4",
      "country": "US",
      "region": "NY",
      "city": "NYC",
      "lat": 40.7,
      "lon": -74.0,
      "org": "ISP",
      "email": None,
      "percent_viewed": 0.85,
      "embed_url": "",
      "conversion_type": "",
      "iframe_heatmap_url": "https://api.wistia.com/heatmap",
      "user_agent_details": {
          "browser": "Chrome",
          "browser_version": "150",
          "platform": "Windows",
          "mobile": False,
      },
      "thumbnail": {"url": "https://wistia.com/t.jpg", "width": 1920, "height": 1080},
      "ingested_at": "2026-11-06T17:00:00+00:00",
  }]
  evt_out = transform_validated_records_for_silver("media_events", evt_in)
  assert evt_out[0]["event_date"] == "2026-07-26"
  assert evt_out[0]["browser"] == "Chrome"


def test_transform_and_deduplicate_sad_paths():
  with pytest.raises(ValueError, match="Unsupported endpoint"):
    transform_validated_records_for_silver("bad_endpoint", [])

  with pytest.raises(ValueError, match="requires 'media_id'"):
    transform_validated_records_for_silver(
        "media_engagement",
        [{"engagement": 0.5, "engagement_data": [1], "ingested_at": "2026-11-06"}],
        media_id=None,
    )

  with pytest.raises(ValueError, match="Unsupported endpoint"):
    deduplicate_by_primary_key("bad_endpoint", [])

  with pytest.raises(ValueError, match="Missing primary key field 'event_key'"):
    deduplicate_by_primary_key("media_events", [{"ingested_at": "2026-11-06"}])


def test_deduplicate_by_primary_key_keeps_latest_record():
  rows = [
      {"event_key": "evt_1", "percent_viewed": 0.25, "ingested_at": "2026-11-06T10:00:00Z"},
      {"event_key": "evt_1", "percent_viewed": 0.90, "ingested_at": "2026-11-06T18:00:00Z"},
      {"event_key": "evt_2", "percent_viewed": 0.50, "ingested_at": "2026-11-06T12:00:00Z"},
  ]
  deduped = deduplicate_by_primary_key("media_events", rows)
  assert len(deduped) == 2
  by_key = {r["event_key"]: r for r in deduped}
  assert by_key["evt_1"]["percent_viewed"] == 0.90
