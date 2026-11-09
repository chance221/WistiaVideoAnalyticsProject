from unittest.mock import MagicMock, patch
import pytest
import requests
from scripts.raw.api_utils import (
    is_transient_error,
    log_backoff_details,
    log_giveup_and_notify,
    resilient_api_request,
)


def _make_http_error(status_code: int | None) -> requests.exceptions.HTTPError:
  if status_code is None:
    return requests.exceptions.HTTPError("No response attached")
  resp = MagicMock()
  resp.status_code = status_code
  return requests.exceptions.HTTPError(f"HTTP {status_code}", response=resp)


def test_is_transient_error_happy_path_retriable_errors():
  assert is_transient_error(_make_http_error(408)) is True
  assert is_transient_error(_make_http_error(429)) is True
  assert is_transient_error(_make_http_error(500)) is True
  assert is_transient_error(_make_http_error(502)) is True
  assert is_transient_error(_make_http_error(503)) is True
  assert is_transient_error(_make_http_error(504)) is True
  assert is_transient_error(requests.exceptions.ConnectionError("Drop")) is True
  assert is_transient_error(requests.exceptions.Timeout("Timed out")) is True
  assert is_transient_error(requests.exceptions.ChunkedEncodingError("Stream cut")) is True


def test_is_transient_error_sad_path_non_retriable_errors():
  # 4xx client errors (other than 408/429) should NOT retry
  assert is_transient_error(_make_http_error(400)) is False
  assert is_transient_error(_make_http_error(401)) is False
  assert is_transient_error(_make_http_error(403)) is False
  assert is_transient_error(_make_http_error(404)) is False
  # HTTPError with response=None should safely return False without AttributeError
  assert is_transient_error(_make_http_error(None)) is False
  # Non-requests exception
  assert is_transient_error(ValueError("bad value")) is False


def test_log_backoff_details_positional_and_keyword_url(caplog: pytest.LogCaptureFixture):
  details_pos = {
      "args": ("https://api.wistia.com/modern/stats/events",),
      "kwargs": {},
      "tries": 2,
      "wait": 1.25,
      "exception": requests.exceptions.Timeout("timeout"),
  }
  log_backoff_details(details_pos)
  assert "Attempt #2 failed for URL: https://api.wistia.com/modern/stats/events" in caplog.text

  caplog.clear()
  details_kw = {
      "args": (),
      "kwargs": {"url": "https://api.wistia.com/modern/medias/9k4tbcdfg0"},
      "tries": 1,
      "wait": 0.5,
      "exception": requests.exceptions.ConnectionError("reset"),
  }
  log_backoff_details(details_kw)
  assert "https://api.wistia.com/modern/medias/9k4tbcdfg0" in caplog.text


@patch("scripts.raw.api_utils.send_sns_failure_alert")
def test_log_giveup_and_notify_sends_sns_on_exhausted_and_non_transient(
    mock_send_sns: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
  monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:440107864885:wistia-alerts")
  monkeypatch.setenv("ALERT_EMAIL", "engineer@example.com")

  # 1. Exhausted retries on transient 503 error
  details_transient = {
      "args": ("https://api.wistia.com/modern/stats/events",),
      "kwargs": {},
      "tries": 5,
      "elapsed": 12.4,
      "exception": _make_http_error(503),
  }
  log_giveup_and_notify(details_transient)

  mock_send_sns.assert_called_once()
  call_kwargs = mock_send_sns.call_args.kwargs
  assert call_kwargs["topic_arn"] == "arn:aws:sns:us-east-1:440107864885:wistia-alerts"
  assert call_kwargs["recipient_email"] == "engineer@example.com"
  assert "Retry attempts exhausted" in call_kwargs["message"]
  assert "HTTP 503" in call_kwargs["subject"]

  # 2. Sad path: SNS send raises RuntimeError -> logged cleanly without crashing giveup handler
  mock_send_sns.side_effect = RuntimeError("SNS unreachable")
  log_giveup_and_notify(details_transient)
  assert "Failed to send SNS notification" in caplog.text


def test_resilient_api_request_retries_transient_then_succeeds():
  call_counter = {"count": 0}

  @resilient_api_request(max_tries=3, max_value=0)
  def flaky_call(url: str) -> dict[str, str]:
    call_counter["count"] += 1
    if call_counter["count"] < 3:
      raise _make_http_error(429)
    return {"status": "ok", "url": url}

  result = flaky_call("https://api.wistia.com/test")
  assert result == {"status": "ok", "url": "https://api.wistia.com/test"}
  assert call_counter["count"] == 3


@patch("scripts.raw.api_utils.send_sns_failure_alert")
def test_resilient_api_request_sad_path_gives_up_immediately_on_404_and_notifies(
    mock_send_sns: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
):
  monkeypatch.setenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:440107864885:wistia-alerts")
  call_counter = {"count": 0}

  @resilient_api_request(max_tries=5, max_value=0)
  def not_found_call(url: str) -> None:
    call_counter["count"] += 1
    raise _make_http_error(404)

  with pytest.raises(requests.exceptions.HTTPError):
    not_found_call("https://api.wistia.com/missing")

  # Should NOT retry on 404, and should trigger on_giveup SNS alert immediately
  assert call_counter["count"] == 1
  mock_send_sns.assert_called_once()
  assert "Non-transient error (immediate giveup)" in mock_send_sns.call_args.kwargs["message"]
