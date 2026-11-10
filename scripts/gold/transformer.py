from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

GOLD_PRIMARY_KEYS: dict[str, str] = {
    "dim_media": "media_id",
    "dim_visitor": "visitor_id",
    "fact_media_engagement": "engagement_key",
}

GOLD_SPARK_SCHEMAS: dict[str, str] = {
    "dim_media": (
        "media_id STRING, numeric_id BIGINT, title STRING, url STRING, "
        "channel STRING, duration DOUBLE, type STRING, status STRING, "
        "archived BOOLEAN, thumbnail_url STRING, created_at STRING, "
        "updated_at STRING, ingested_at STRING"
    ),
    "dim_visitor": (
        "visitor_id STRING, ip_address STRING, country STRING, region STRING, "
        "city STRING, org STRING, email STRING, browser STRING, "
        "platform STRING, mobile BOOLEAN, last_seen_at STRING, ingested_at STRING"
    ),
    "fact_media_engagement": (
        "engagement_key STRING, media_id STRING, date STRING, "
        "daily_event_count BIGINT, daily_play_count BIGINT, "
        "daily_unique_visitors BIGINT, daily_watched_percent DOUBLE, "
        "daily_total_watch_time_seconds DOUBLE, daily_avg_watch_time_seconds DOUBLE, "
        "cumulative_page_loads BIGINT, cumulative_visitors BIGINT, "
        "cumulative_play_count BIGINT, cumulative_play_rate BIGINT, "
        "cumulative_watched_percent BIGINT, overall_engagement_score DOUBLE, "
        "peak_concurrent_viewers BIGINT, ingested_at STRING"
    ),
}


def _record_gold_quarantine(
    quarantine_records: list[dict[str, Any]] | None,
    table_name: str,
    raw_record: Any,
    error_msg: str,
    ts_iso: str,
) -> None:
  """Appends a Gold-layer quarantine entry if a quarantine list is provided."""
  if quarantine_records is not None:
    quarantine_records.append({
        "layer": "gold",
        "table": table_name,
        "raw_record": raw_record,
        "errors": [{"msg": error_msg}],
        "quarantined_at": ts_iso,
    })


def infer_channel_from_embed_url(embed_url: str | None) -> str:
  """Infers a human-readable channel name from an embed URL."""
  if not embed_url or not embed_url.strip():
    return "Direct / Wistia"

  parsed = urlparse(embed_url.strip())
  host = (parsed.netloc or "").lower()
  if not host:
    return "Direct / Wistia"

  if "youtube.com" in host or "youtu.be" in host:
    return "YouTube"
  if "facebook.com" in host or "fb.watch" in host:
    return "Facebook"
  if "linkedin.com" in host:
    return "LinkedIn"
  return host.removeprefix("www.")


def build_dim_media(
    silver_metadata: list[dict[str, Any]],
    silver_events: list[dict[str, Any]] | None = None,
    quarantine_records: list[dict[str, Any]] | None = None,
    validated_at: datetime | None = None,
) -> list[dict[str, Any]]:
  """Builds `dim_media` records (SCD1 PK: `media_id`) from Silver metadata and events.
  Bad records are routed to `quarantine_records` without stopping execution."""
  if not isinstance(silver_metadata, list):
    raise TypeError("silver_metadata must be a list of dicts")

  ts_iso = (validated_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()

  media_url_by_id: dict[str, str] = {}
  channel_by_id: dict[str, str] = {}

  for evt in silver_events or []:
    if not isinstance(evt, dict):
      continue
    m_id = evt.get("media_id")
    if not m_id:
      continue
    if evt.get("media_url") and m_id not in media_url_by_id:
      media_url_by_id[m_id] = str(evt["media_url"])
    if evt.get("embed_url") and m_id not in channel_by_id:
      channel_by_id[m_id] = infer_channel_from_embed_url(evt.get("embed_url"))

  dim_rows: dict[str, dict[str, Any]] = {}
  for meta in silver_metadata:
    if not isinstance(meta, dict):
      _record_gold_quarantine(
          quarantine_records,
          "dim_media",
          meta,
          "Expected dict record in silver_metadata",
          ts_iso,
      )
      continue

    media_id = meta.get("hashed_id")
    if not media_id:
      _record_gold_quarantine(
          quarantine_records,
          "dim_media",
          meta,
          "Missing 'hashed_id' in silver_metadata record",
          ts_iso,
      )
      continue

    try:
      row = {
          "media_id": str(media_id),
          "numeric_id": meta.get("id"),
          "title": meta.get("name"),
          "url": media_url_by_id.get(
              media_id, f"https://fast.wistia.com/medias/{media_id}"
          ),
          "channel": channel_by_id.get(media_id, "Direct / Wistia"),
          "duration": float(meta.get("duration") or 0.0),
          "type": meta.get("type"),
          "status": meta.get("status"),
          "archived": bool(meta.get("archived", False)),
          "thumbnail_url": meta.get("thumbnail_url"),
          "created_at": meta.get("created"),
          "updated_at": meta.get("updated"),
          "ingested_at": meta.get("ingested_at"),
      }
      dim_rows[str(media_id)] = row
    except (ValueError, TypeError) as exc:
      _record_gold_quarantine(
          quarantine_records,
          "dim_media",
          meta,
          f"Invalid data in silver_metadata record: {exc}",
          ts_iso,
      )

  return list(dim_rows.values())


def build_dim_visitor(
    silver_events: list[dict[str, Any]],
    quarantine_records: list[dict[str, Any]] | None = None,
    validated_at: datetime | None = None,
) -> list[dict[str, Any]]:
  """Builds `dim_visitor` records (SCD1 PK: `visitor_id`) from Silver media events,
  retaining the latest state per visitor. Bad records are routed to `quarantine_records`
  without stopping execution."""
  if not isinstance(silver_events, list):
    raise TypeError("silver_events must be a list of dicts")

  ts_iso = (validated_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
  visitors: dict[str, dict[str, Any]] = {}

  for evt in silver_events:
    if not isinstance(evt, dict):
      _record_gold_quarantine(
          quarantine_records,
          "dim_visitor",
          evt,
          "Expected dict record in silver_events",
          ts_iso,
      )
      continue

    visitor_id = evt.get("visitor_key")
    if not visitor_id:
      _record_gold_quarantine(
          quarantine_records,
          "dim_visitor",
          evt,
          "Missing 'visitor_key' in silver_events record",
          ts_iso,
      )
      continue

    received_at = str(evt.get("received_at") or "")
    existing = visitors.get(visitor_id)

    if existing is None or received_at >= str(existing.get("last_seen_at") or ""):
      email = evt.get("email") or (existing.get("email") if existing else None)
      visitors[visitor_id] = {
          "visitor_id": str(visitor_id),
          "ip_address": evt.get("ip"),
          "country": evt.get("country"),
          "region": evt.get("region"),
          "city": evt.get("city"),
          "org": evt.get("org"),
          "email": email,
          "browser": evt.get("browser"),
          "platform": evt.get("platform"),
          "mobile": evt.get("mobile"),
          "last_seen_at": received_at,
          "ingested_at": evt.get("ingested_at"),
      }

  return list(visitors.values())


def build_fact_media_engagement(
    silver_views: list[dict[str, Any]],
    silver_engagement: list[dict[str, Any]],
    silver_metadata: list[dict[str, Any]],
    silver_events: list[dict[str, Any]],
    quarantine_records: list[dict[str, Any]] | None = None,
    validated_at: datetime | None = None,
) -> list[dict[str, Any]]:
  """Builds `fact_media_engagement` records (SCD1 PK: `engagement_key` = `<media_id>_<date>`)
  combining daily event-level watch metrics with cumulative video engagement metrics.
  Bad records are routed to `quarantine_records` without stopping execution."""
  if not all(
      isinstance(x, list)
      for x in (silver_views, silver_engagement, silver_metadata, silver_events)
  ):
    raise TypeError("All Silver inputs to build_fact_media_engagement must be lists")

  ts_iso = (validated_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()

  views_by_id: dict[str, dict[str, Any]] = {}
  for v in silver_views:
    if not isinstance(v, dict) or not v.get("hashed_id"):
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          v,
          "Missing or invalid 'hashed_id' in silver_views record",
          ts_iso,
      )
      continue
    try:
      int(v.get("page_loads") or 0)
      int(v.get("visitors") or 0)
      int(v.get("plays") or 0)
      int(v.get("percent_of_visitors_clicking_play") or 0)
      int(v.get("average_percent_watched") or 0)
      views_by_id[str(v["hashed_id"])] = v
    except (ValueError, TypeError) as exc:
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          v,
          f"Invalid numeric metric in silver_views record: {exc}",
          ts_iso,
      )

  eng_by_id: dict[str, dict[str, Any]] = {}
  for e in silver_engagement:
    if not isinstance(e, dict) or not e.get("media_id"):
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          e,
          "Missing or invalid 'media_id' in silver_engagement record",
          ts_iso,
      )
      continue
    try:
      float(e.get("engagement") or 0.0)
      eng_data = e.get("engagement_data") or []
      if not isinstance(eng_data, list):
        raise TypeError("engagement_data must be a list")
      if eng_data:
        int(max(eng_data))
      eng_by_id[str(e["media_id"])] = e
    except (ValueError, TypeError) as exc:
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          e,
          f"Invalid numeric metric in silver_engagement record: {exc}",
          ts_iso,
      )

  meta_by_id: dict[str, dict[str, Any]] = {}
  for m in silver_metadata:
    if isinstance(m, dict) and m.get("hashed_id"):
      try:
        float(m.get("duration") or 0.0)
        meta_by_id[str(m["hashed_id"])] = m
      except (ValueError, TypeError):
        continue

  events_by_media_date: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
  for evt in silver_events:
    if not isinstance(evt, dict):
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          evt,
          "Expected dict record in silver_events",
          ts_iso,
      )
      continue

    m_id = evt.get("media_id")
    evt_date = evt.get("event_date") or str(evt.get("received_at") or "")[:10]
    if not m_id or not evt_date:
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          evt,
          "Missing 'media_id' or 'event_date' in silver_events record",
          ts_iso,
      )
      continue

    try:
      float(evt.get("percent_viewed") or 0.0)
      events_by_media_date[(str(m_id), str(evt_date))].append(evt)
    except (ValueError, TypeError) as exc:
      _record_gold_quarantine(
          quarantine_records,
          "fact_media_engagement",
          evt,
          f"Invalid 'percent_viewed' in silver_events record: {exc}",
          ts_iso,
      )

  fact_rows: list[dict[str, Any]] = []

  for (media_id, event_date), day_events in sorted(events_by_media_date.items()):
    meta = meta_by_id.get(media_id, {})
    views = views_by_id.get(media_id, {})
    eng = eng_by_id.get(media_id, {})

    duration = float(meta.get("duration") or 0.0)
    total_events = len(day_events)
    play_events = [e for e in day_events if float(e.get("percent_viewed") or 0.0) > 0.0]
    daily_play_count = len(play_events)
    unique_visitors = len({e.get("visitor_key") for e in day_events if e.get("visitor_key")})

    if play_events:
      avg_fraction_watched = sum(
          (float(e.get("percent_viewed") or 0.0) for e in play_events),
          0.0,
      ) / daily_play_count
    else:
      avg_fraction_watched = 0.0

    daily_watched_percent = float(round(avg_fraction_watched * 100.0, 2))
    daily_total_watch_time = float(
        round(
            sum(
                (float(e.get("percent_viewed") or 0.0) * duration for e in play_events),
                0.0,
            ),
            2,
        )
    )
    daily_avg_watch_time = float(
        round(daily_total_watch_time / daily_play_count, 2)
        if daily_play_count > 0
        else 0.0
    )

    eng_data = eng.get("engagement_data") or []
    peak_concurrent_viewers = max(eng_data) if eng_data else 0
    latest_ingested_at = max(
        (str(e.get("ingested_at") or "") for e in day_events),
        default="",
    )

    fact_rows.append({
        "engagement_key": f"{media_id}_{event_date}",
        "media_id": media_id,
        "date": event_date,
        "daily_event_count": int(total_events),
        "daily_play_count": int(daily_play_count),
        "daily_unique_visitors": int(unique_visitors),
        "daily_watched_percent": float(daily_watched_percent),
        "daily_total_watch_time_seconds": float(daily_total_watch_time),
        "daily_avg_watch_time_seconds": float(daily_avg_watch_time),
        "cumulative_page_loads": int(views.get("page_loads") or 0),
        "cumulative_visitors": int(views.get("visitors") or 0),
        "cumulative_play_count": int(views.get("plays") or 0),
        "cumulative_play_rate": int(
            views.get("percent_of_visitors_clicking_play") or 0
        ),
        "cumulative_watched_percent": int(
            views.get("average_percent_watched") or 0
        ),
        "overall_engagement_score": float(eng.get("engagement") or 0.0),
        "peak_concurrent_viewers": int(peak_concurrent_viewers),
        "ingested_at": latest_ingested_at,
    })

  return fact_rows


def validate_and_build_gold_tables(
    silver_views: list[dict[str, Any]],
    silver_engagement: list[dict[str, Any]],
    silver_metadata: list[dict[str, Any]],
    silver_events: list[dict[str, Any]],
    validated_at: datetime | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
  """Builds Gold tables (`dim_media`, `dim_visitor`, `fact_media_engagement`) while
  routing any bad Silver records into per-table Quarantine lists without crashing."""
  if not all(
      isinstance(x, list)
      for x in (silver_views, silver_engagement, silver_metadata, silver_events)
  ):
    raise TypeError("All Silver inputs to validate_and_build_gold_tables must be lists")

  if validated_at is None:
    validated_at = datetime.now(timezone.utc)

  quarantine_by_table: dict[str, list[dict[str, Any]]] = {
      "dim_media": [],
      "dim_visitor": [],
      "fact_media_engagement": [],
  }

  gold_tables = {
      "dim_media": build_dim_media(
          silver_metadata=silver_metadata,
          silver_events=silver_events,
          quarantine_records=quarantine_by_table["dim_media"],
          validated_at=validated_at,
      ),
      "dim_visitor": build_dim_visitor(
          silver_events=silver_events,
          quarantine_records=quarantine_by_table["dim_visitor"],
          validated_at=validated_at,
      ),
      "fact_media_engagement": build_fact_media_engagement(
          silver_views=silver_views,
          silver_engagement=silver_engagement,
          silver_metadata=silver_metadata,
          silver_events=silver_events,
          quarantine_records=quarantine_by_table["fact_media_engagement"],
          validated_at=validated_at,
      ),
  }

  return gold_tables, quarantine_by_table
