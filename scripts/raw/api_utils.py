from collections.abc import Callable
import logging
import os
from typing import Any
import backoff
import requests
from scripts.aws.sns_client import send_sns_failure_alert
from scripts.models.config_loader import load_environment_config


logger = logging.getLogger("api_ingestion")
logger.setLevel(logging.INFO)

TRANSIENT_STATUS_CODES = {408, 429}


def is_transient_error(exception: Exception) -> bool:
  """Filters for retriable HTTP status codes (408, 429, 5xx) and transient network drops."""
  if isinstance(exception, requests.exceptions.HTTPError):
    if exception.response is None:
      return False
    status_code = exception.response.status_code
    return status_code in TRANSIENT_STATUS_CODES or status_code >= 500

  return isinstance(
      exception,
      (
          requests.exceptions.ConnectionError,
          requests.exceptions.Timeout,
          requests.exceptions.ChunkedEncodingError,
      ),
  )


def _extract_url_from_details(details: dict[str, Any]) -> str:
  args = details.get("args") or ()
  kwargs = details.get("kwargs") or {}
  return str(args[0]) if args else str(kwargs.get("url", "Unknown URL"))


def log_backoff_details(details: dict[str, Any]) -> None:
  """Custom handler invoked by backoff on every retry.
  Extracts the URL from the decorated function's positional or keyword arguments.
  """
  url = _extract_url_from_details(details)

  logger.warning(
      f"[API RETRY] Attempt #{details['tries']} failed for URL: {url}. "
      f"Retrying in {details['wait']:.2f}s due to exception: {type(details['exception']).__name__}"
  )


def _resolve_sns_notification_settings() -> tuple[str | None, str, str | None]:
  """Resolves `(topic_arn, region, alert_email)` from environment variables or environment YAML config."""
  topic_arn = os.getenv("SNS_TOPIC_ARN")
  region = os.getenv("AWS_REGION", "us-east-1")
  alert_email = os.getenv("ALERT_EMAIL")

  if not topic_arn:
    env_name = os.getenv("ENV", "dev")
    try:
      env_cfg = load_environment_config(env_name)
      topic_arn = env_cfg.aws.sns_topic_arn
      region = env_cfg.aws.region
      alert_email = alert_email or env_cfg.aws.alert_email
    except Exception:
      pass

  return topic_arn, region, alert_email


def log_giveup_and_notify(details: dict[str, Any]) -> None:
  """Custom handler invoked by backoff `on_giveup` when a request fails permanently
  (either due to a non-transient error or after exhausting all retry attempts).
  Logs the failure and sends an AWS SNS email notification with the failure reason.
  """
  url = _extract_url_from_details(details)
  tries = details.get("tries", 1)
  elapsed = float(details.get("elapsed", 0.0))
  exc = details.get("exception")
  exc_type = type(exc).__name__ if exc is not None else "UnknownException"

  status_code = (
      exc.response.status_code
      if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None
      else None
  )
  failure_category = (
      "Retry attempts exhausted (transient error persisted)"
      if isinstance(exc, Exception) and is_transient_error(exc)
      else "Non-transient error (immediate giveup)"
  )
  status_info = f" (HTTP {status_code})" if status_code is not None else ""
  reason_summary = f"{failure_category}: {exc_type}{status_info} - {exc}"

  logger.error(
      f"[API GIVEUP] Request aborted after {tries} attempt(s) ({elapsed:.2f}s) for URL: {url}. "
      f"Reason: {reason_summary}"
  )

  topic_arn, region, alert_email = _resolve_sns_notification_settings()
  if not topic_arn:
    logger.warning(
        "[API GIVEUP] No SNS_TOPIC_ARN configured; skipping SNS email notification."
    )
    return

  subject = f"[Wistia Pipeline Alert] API Failure{status_info}: {exc_type}"
  message = (
      "Wistia Raw Ingestion API Request Failed\n"
      "----------------------------------------\n"
      f"URL: {url}\n"
      f"Failure Category: {failure_category}\n"
      f"Exception Type: {exc_type}{status_info}\n"
      f"Error Details: {exc}\n"
      f"Attempts Made: {tries}\n"
      f"Elapsed Time: {elapsed:.2f}s\n"
  )

  try:
    send_sns_failure_alert(
        topic_arn=topic_arn,
        region=region,
        subject=subject,
        message=message,
        recipient_email=alert_email,
    )
  except Exception as sns_exc:
    logger.error(
        f"[API GIVEUP] Failed to send SNS notification to {topic_arn}: {sns_exc}"
    )


def resilient_api_request(
    max_tries: int = 5,
    max_value: int = 10,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
  """Pre-configured exponential backoff decorator factory with full jitter and SNS giveup alerting."""
  return backoff.on_exception(
      backoff.expo,
      requests.exceptions.RequestException,
      giveup=lambda e: not is_transient_error(e),
      max_tries=max_tries,
      max_value=max_value,
      jitter=backoff.full_jitter,
      on_backoff=log_backoff_details,
      on_giveup=log_giveup_and_notify,
  )
