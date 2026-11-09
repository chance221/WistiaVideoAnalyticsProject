import logging
from typing import Any
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("api_ingestion")
logger.setLevel(logging.INFO)


def resolve_topic_email_recipients(
    sns_client: Any,
    topic_arn: str,
    configured_email: str | None = None,
) -> list[str]:
  """Resolves email recipients subscribed to the SNS topic (or uses `configured_email`)."""
  recipients: list[str] = []
  if configured_email and configured_email.strip():
    recipients.append(configured_email.strip())

  try:
    response = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
    for sub in response.get("Subscriptions", []):
      protocol = sub.get("Protocol", "")
      endpoint = sub.get("Endpoint", "")
      if protocol in ("email", "email-json") and endpoint and endpoint not in recipients:
        recipients.append(endpoint)
  except ClientError:
    # If the IAM role only has sns:Publish and not sns:ListSubscriptionsByTopic, fall back gracefully
    pass

  return recipients or [topic_arn]


def send_sns_failure_alert(
    topic_arn: str,
    region: str,
    subject: str,
    message: str,
    recipient_email: str | None = None,
) -> dict[str, Any]:
  """Publishes a failure notification to an AWS SNS topic and logs who received it."""
  if not topic_arn or not topic_arn.strip():
    raise ValueError("topic_arn must be a non-empty string")
  if not region or not region.strip():
    raise ValueError("region must be a non-empty string")
  if not message or not message.strip():
    raise ValueError("message must be a non-empty string")

  clean_subject = (subject or "Wistia API Ingestion Failure").strip()[:100]

  session = boto3.session.Session()
  sns_client = session.client("sns", region_name=region)

  recipients = resolve_topic_email_recipients(
      sns_client=sns_client,
      topic_arn=topic_arn,
      configured_email=recipient_email,
  )

  try:
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=clean_subject,
        Message=message,
    )
  except ClientError as exc:
    raise RuntimeError(
        f"Failed to publish SNS failure alert to {topic_arn}: {exc}"
    ) from exc

  message_id = response.get("MessageId", "unknown")
  recipient_str = ", ".join(recipients)
  logger.info(
      f"[SNS ALERT SENT] Notification (MessageId={message_id}) sent to recipient(s): "
      f"{recipient_str} | Reason Summary: {clean_subject}"
  )

  return {
      "message_id": message_id,
      "topic_arn": topic_arn,
      "recipients": recipients,
  }
