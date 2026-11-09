from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError
import pytest
from scripts.aws.sns_client import (
    resolve_topic_email_recipients,
    send_sns_failure_alert,
)


def test_resolve_topic_email_recipients_happy_and_sad_path():
  mock_sns = MagicMock()
  mock_sns.list_subscriptions_by_topic.return_value = {
      "Subscriptions": [
          {"Protocol": "email", "Endpoint": "ops@example.com"},
          {"Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123:queue"},
      ]
  }
  topic_arn = "arn:aws:sns:us-east-1:440107864885:wistia-alerts"

  recipients = resolve_topic_email_recipients(
      mock_sns, topic_arn, configured_email="admin@example.com"
  )
  assert recipients == ["admin@example.com", "ops@example.com"]

  # Sad path: ClientError on list_subscriptions_by_topic falls back to topic_arn
  mock_sns.list_subscriptions_by_topic.side_effect = ClientError(
      {"Error": {"Code": "AuthorizationError", "Message": "Denied"}},
      "ListSubscriptionsByTopic",
  )
  fallback = resolve_topic_email_recipients(mock_sns, topic_arn)
  assert fallback == [topic_arn]


@patch("scripts.aws.sns_client.boto3.session.Session")
def test_send_sns_failure_alert_happy_path_logs_recipient(
    mock_session_cls: MagicMock,
    caplog: pytest.LogCaptureFixture,
):
  mock_sns = MagicMock()
  mock_sns.list_subscriptions_by_topic.return_value = {
      "Subscriptions": [{"Protocol": "email", "Endpoint": "alerts@example.com"}]
  }
  mock_sns.publish.return_value = {"MessageId": "msg-12345"}
  mock_session_cls.return_value.client.return_value = mock_sns

  topic_arn = "arn:aws:sns:us-east-1:440107864885:wistia-alerts"
  res = send_sns_failure_alert(
      topic_arn=topic_arn,
      region="us-east-1",
      subject="API 401 Unauthorized",
      message="Request failed due to invalid token",
  )

  assert res["message_id"] == "msg-12345"
  assert res["recipients"] == ["alerts@example.com"]
  assert "sent to recipient(s): alerts@example.com" in caplog.text
  mock_sns.publish.assert_called_once()


@patch("scripts.aws.sns_client.boto3.session.Session")
def test_send_sns_failure_alert_sad_path_validation_and_client_error(
    mock_session_cls: MagicMock,
):
  with pytest.raises(ValueError, match="topic_arn must be a non-empty string"):
    send_sns_failure_alert("", "us-east-1", "Subj", "Msg")

  with pytest.raises(ValueError, match="region must be a non-empty string"):
    send_sns_failure_alert("arn:aws:sns:us-east-1:123:topic", "", "Subj", "Msg")

  with pytest.raises(ValueError, match="message must be a non-empty string"):
    send_sns_failure_alert("arn:aws:sns:us-east-1:123:topic", "us-east-1", "Subj", "   ")

  mock_sns = MagicMock()
  mock_sns.list_subscriptions_by_topic.return_value = {"Subscriptions": []}
  mock_sns.publish.side_effect = ClientError(
      {"Error": {"Code": "NotFound", "Message": "Topic does not exist"}},
      "Publish",
  )
  mock_session_cls.return_value.client.return_value = mock_sns

  with pytest.raises(RuntimeError, match="Failed to publish SNS failure alert"):
    send_sns_failure_alert(
        "arn:aws:sns:us-east-1:123:missing",
        "us-east-1",
        "Subj",
        "Failure message",
    )
