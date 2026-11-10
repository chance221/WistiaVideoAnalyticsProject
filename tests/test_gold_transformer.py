import pytest
from scripts.gold.transformer import (
    build_dim_media,
    build_dim_visitor,
    build_fact_media_engagement,
    infer_channel_from_embed_url,
    validate_and_build_gold_tables,
)


def test_infer_channel_from_embed_url_happy_and_sad_path():
  assert infer_channel_from_embed_url("https://www.youtube.com/watch?v=123") == "YouTube"
  assert infer_channel_from_embed_url("https://facebook.com/video") == "Facebook"
  assert infer_channel_from_embed_url("https://www.linkedin.com/feed") == "LinkedIn"
  assert infer_channel_from_embed_url("https://dataengineerinterviews.com/page") == "dataengineerinterviews.com"
  assert infer_channel_from_embed_url("") == "Direct / Wistia"
  assert infer_channel_from_embed_url(None) == "Direct / Wistia"
  assert infer_channel_from_embed_url("not-a-url-without-host") == "Direct / Wistia"


def test_build_dim_media_happy_and_sad_path():
  metadata = [
      {
          "id": 128512814,
          "hashed_id": "9k4tbcdfg0",
          "name": "The Gap Method",
          "duration": 2195.01,
          "type": "Video",
          "status": "ready",
          "archived": False,
          "thumbnail_url": "https://wistia.com/t.jpg",
          "created": "2025-01-04T00:35:10+00:00",
          "updated": "2026-06-29T22:50:15+00:00",
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {"name": "Missing Hashed ID"},
      {"hashed_id": "bad_dur", "duration": "not-a-float"},
      "non-dict-item",  # type: ignore[list-item]
  ]
  events = [{
      "media_id": "9k4tbcdfg0",
      "media_url": "https://chrisgarzon19.wistia.com/medias/9k4tbcdfg0",
      "embed_url": "https://dataengineerinterviews.com/",
  }]

  quarantine: list[dict] = []
  dim_media = build_dim_media(metadata, events, quarantine_records=quarantine)
  assert len(dim_media) == 1
  assert dim_media[0]["media_id"] == "9k4tbcdfg0"
  assert dim_media[0]["title"] == "The Gap Method"
  assert dim_media[0]["channel"] == "dataengineerinterviews.com"

  # Bad records were quarantined without crashing the transform
  assert len(quarantine) == 3
  assert all(q["layer"] == "gold" and q["table"] == "dim_media" for q in quarantine)

  with pytest.raises(TypeError, match="silver_metadata must be a list"):
    build_dim_media("invalid")  # type: ignore[arg-type]


def test_build_dim_visitor_happy_and_sad_path():
  events = [
      {
          "visitor_key": "v_1",
          "received_at": "2026-07-20T10:00:00Z",
          "ip": "1.1.1.1",
          "country": "US",
          "region": "NJ",
          "city": "Newark",
          "org": "ISP",
          "email": "user@example.com",
          "browser": "Chrome",
          "platform": "Mac",
          "mobile": False,
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {
          "visitor_key": "v_1",
          "received_at": "2026-07-26T10:00:00Z",
          "ip": "2.2.2.2",
          "country": "US",
          "region": "NY",
          "city": "NYC",
          "org": "ISP",
          "email": None,
          "browser": "Safari",
          "platform": "iOS",
          "mobile": True,
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {"ip": "3.3.3.3"},  # Missing visitor_key
      "non-dict-event",  # type: ignore[list-item]
  ]

  quarantine: list[dict] = []
  visitors = build_dim_visitor(events, quarantine_records=quarantine)
  assert len(visitors) == 1
  assert visitors[0]["visitor_id"] == "v_1"
  assert visitors[0]["ip_address"] == "2.2.2.2"
  assert visitors[0]["email"] == "user@example.com"
  assert len(quarantine) == 2
  assert all(q["layer"] == "gold" and q["table"] == "dim_visitor" for q in quarantine)

  with pytest.raises(TypeError, match="silver_events must be a list"):
    build_dim_visitor("invalid")  # type: ignore[arg-type]


def test_build_fact_media_engagement_happy_and_sad_path():
  views = [
      {
          "hashed_id": "9k4tbcdfg0",
          "page_loads": 940,
          "visitors": 706,
          "plays": 328,
          "percent_of_visitors_clicking_play": 42,
          "average_percent_watched": 26,
      },
      {"hashed_id": "bad_views", "page_loads": "not-an-int"},
  ]
  engagement = [
      {
          "media_id": "9k4tbcdfg0",
          "engagement": 0.255894,
          "engagement_data": [297, 252, 233],
      },
      {"media_id": "bad_eng", "engagement": "not-a-float"},
  ]
  metadata = [{"hashed_id": "9k4tbcdfg0", "duration": 100.0}]
  events = [
      {
          "event_key": "e1",
          "media_id": "9k4tbcdfg0",
          "event_date": "2026-11-05",
          "visitor_key": "v1",
          "percent_viewed": 0.50,
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {
          "event_key": "e2",
          "media_id": "9k4tbcdfg0",
          "event_date": "2026-11-05",
          "visitor_key": "v2",
          "percent_viewed": 1.00,
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {
          "event_key": "e3",
          "media_id": "9k4tbcdfg0",
          "event_date": "2026-11-05",
          "visitor_key": "v2",
          "percent_viewed": 0.0,
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {
          "event_key": "e4_zero_plays_day",
          "media_id": "9k4tbcdfg0",
          "event_date": "2026-11-06",
          "visitor_key": "v3",
          "percent_viewed": 0.0,
          "ingested_at": "2026-11-06T17:00:00Z",
      },
      {"event_key": "bad_missing_media_id"},
      {
          "event_key": "bad_percent",
          "media_id": "9k4tbcdfg0",
          "event_date": "2026-11-05",
          "percent_viewed": "invalid-float",
      },
  ]

  quarantine: list[dict] = []
  facts = build_fact_media_engagement(
      views, engagement, metadata, events, quarantine_records=quarantine
  )
  assert len(facts) == 2
  row = facts[0]
  assert row["engagement_key"] == "9k4tbcdfg0_2026-11-05"
  assert row["daily_event_count"] == 3
  assert row["daily_play_count"] == 2
  assert row["daily_unique_visitors"] == 2
  assert row["daily_watched_percent"] == 75.0
  assert row["daily_total_watch_time_seconds"] == 150.0
  assert row["daily_avg_watch_time_seconds"] == 75.0
  assert row["peak_concurrent_viewers"] == 297

  # Verify zero-plays day produces strict Python floats (0.0, not int 0) for Spark DoubleType()
  zero_play_row = facts[1]
  assert zero_play_row["engagement_key"] == "9k4tbcdfg0_2026-11-06"
  assert isinstance(zero_play_row["daily_watched_percent"], float)
  assert isinstance(zero_play_row["daily_total_watch_time_seconds"], float)
  assert isinstance(zero_play_row["daily_avg_watch_time_seconds"], float)
  assert isinstance(zero_play_row["overall_engagement_score"], float)
  assert len(quarantine) == 4
  assert all(
      q["layer"] == "gold" and q["table"] == "fact_media_engagement"
      for q in quarantine
  )

  with pytest.raises(TypeError, match="must be lists"):
    build_fact_media_engagement(views, engagement, metadata, "not-a-list")  # type: ignore[arg-type]


def test_validate_and_build_gold_tables_happy_and_sad_path():
  gold_tables, quarantine_by_table = validate_and_build_gold_tables(
      silver_views=[{"hashed_id": "9k4tbcdfg0", "page_loads": 10}],
      silver_engagement=[{"media_id": "9k4tbcdfg0", "engagement": 0.5, "engagement_data": [5]}],
      silver_metadata=[
          {"hashed_id": "9k4tbcdfg0", "name": "Valid Video", "duration": 50.0},
          {"name": "Invalid Video Missing Hashed ID"},
      ],
      silver_events=[
          {
              "event_key": "e1",
              "media_id": "9k4tbcdfg0",
              "event_date": "2026-11-05",
              "visitor_key": "v1",
              "percent_viewed": 0.5,
          },
          {"event_key": "e_bad"},
      ],
  )
  assert len(gold_tables["dim_media"]) == 1
  assert len(gold_tables["dim_visitor"]) == 1
  assert len(gold_tables["fact_media_engagement"]) == 1
  assert len(quarantine_by_table["dim_media"]) == 1
  assert len(quarantine_by_table["dim_visitor"]) == 1
  assert len(quarantine_by_table["fact_media_engagement"]) == 1

  with pytest.raises(TypeError, match="must be lists"):
    validate_and_build_gold_tables([], [], [], "not-a-list")  # type: ignore[arg-type]
