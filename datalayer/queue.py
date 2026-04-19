"""
datalayer.queue
───────────────
SQS queue helpers for the AssetEra data layer.

Supports two modes:
  - SQS mode (production): publish/consume via AWS SQS
  - Local mode (dev/test): in-process list queue, no AWS required

Mode is selected automatically:
  SQS mode  — when QUEUE_EQUITIES env var is set AND boto3 can reach SQS
  Local mode — otherwise

Usage
─────
  # Publish
  from datalayer.queue import get_queue, publish_message
  q = get_queue(asset_class)
  publish_message(q, msg)

  # Consume (worker loop)
  for receipt_handle, msg in receive_messages(q):
      process(msg)
      delete_message(q, receipt_handle)
"""
from __future__ import annotations

import json
import logging
import os
from typing import Iterator

from datalayer.schemas import ASSET_CLASS_QUEUE

logger = logging.getLogger(__name__)

# ── In-process local queue (for dev / testing) ─────────────────────────
_LOCAL_QUEUES: dict[str, list[dict]] = {}


def _use_sqs() -> bool:
    """True when SQS is reachable (boto3 available + queue URL resolvable)."""
    try:
        import boto3  # noqa: F401
        return bool(
            os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ROLE_ARN")
        )
    except ImportError:
        return False


# ── Queue resolution ───────────────────────────────────────────────────

def get_queue_url(queue_name: str) -> str | None:
    """Resolve the SQS queue URL for a given queue name."""
    if not _use_sqs():
        return None
    try:
        import boto3
        sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
        resp = sqs.get_queue_url(QueueName=queue_name)
        return resp["QueueUrl"]
    except Exception as e:
        logger.warning("[queue] could not resolve %s: %s", queue_name, e)
        return None


def get_queue(asset_class: str) -> str:
    """Return queue identifier for an asset class (URL in SQS mode, name in local mode)."""
    queue_name = ASSET_CLASS_QUEUE.get(asset_class, f"assetera-ingest-{asset_class}")
    if _use_sqs():
        url = get_queue_url(queue_name)
        return url or queue_name
    return queue_name


# ── Publish ────────────────────────────────────────────────────────────

def publish_message(queue: str, message: dict) -> str | None:
    """
    Publish one message to SQS or local queue.
    Returns message ID (SQS) or None (local).
    """
    if _use_sqs():
        return _sqs_send(queue, message)
    return _local_send(queue, message)


def publish_batch(queue: str, messages: list[dict]) -> dict:
    """
    Publish up to 10 messages in one SQS batch call.
    Returns {"ok": int, "fail": int}.
    """
    if not messages:
        return {"ok": 0, "fail": 0}
    if _use_sqs():
        return _sqs_send_batch(queue, messages)
    for msg in messages:
        _local_send(queue, msg)
    return {"ok": len(messages), "fail": 0}


def _sqs_send(queue_url: str, message: dict) -> str | None:
    try:
        import boto3
        sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
        resp = sqs.send_message(
            QueueUrl    = queue_url,
            MessageBody = json.dumps(message, default=str),
        )
        return resp.get("MessageId")
    except Exception as e:
        logger.error("[queue] SQS send failed: %s", e)
        return None


def _sqs_send_batch(queue_url: str, messages: list[dict]) -> dict:
    import boto3
    sqs   = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
    ok = fail = 0
    # SQS batch size limit: 10 messages
    for i in range(0, len(messages), 10):
        chunk = messages[i:i+10]
        entries = [
            {"Id": str(j), "MessageBody": json.dumps(m, default=str)}
            for j, m in enumerate(chunk)
        ]
        try:
            resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
            ok   += len(resp.get("Successful", []))
            fail += len(resp.get("Failed", []))
        except Exception as e:
            logger.error("[queue] batch send failed: %s", e)
            fail += len(chunk)
    return {"ok": ok, "fail": fail}


def _local_send(queue_name: str, message: dict) -> None:
    _LOCAL_QUEUES.setdefault(queue_name, []).append(message)
    logger.debug("[queue/local] enqueued → %s", queue_name)


# ── Consume ────────────────────────────────────────────────────────────

def receive_messages(
    queue: str,
    max_messages: int = 1,
    wait_seconds: int = 20,
) -> Iterator[tuple[str, dict]]:
    """
    Yield (receipt_handle, message_dict) tuples.
    In local mode, receipt_handle is a synthetic string.
    """
    if _use_sqs():
        yield from _sqs_receive(queue, max_messages, wait_seconds)
    else:
        yield from _local_receive(queue, max_messages)


def delete_message(queue: str, receipt_handle: str) -> None:
    """Acknowledge (delete) a processed message."""
    if _use_sqs():
        _sqs_delete(queue, receipt_handle)
    # local mode: already removed by _local_receive


def _sqs_receive(
    queue_url: str, max_messages: int, wait_seconds: int
) -> Iterator[tuple[str, dict]]:
    try:
        import boto3
        sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
        resp = sqs.receive_message(
            QueueUrl            = queue_url,
            MaxNumberOfMessages = max(1, min(max_messages, 10)),
            WaitTimeSeconds     = wait_seconds,
            AttributeNames      = ["All"],
        )
        for m in resp.get("Messages", []):
            try:
                body = json.loads(m["Body"])
                yield m["ReceiptHandle"], body
            except json.JSONDecodeError as e:
                logger.warning("[queue] malformed SQS message: %s", e)
    except Exception as e:
        logger.error("[queue] SQS receive failed: %s", e)


def _sqs_delete(queue_url: str, receipt_handle: str) -> None:
    try:
        import boto3
        sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    except Exception as e:
        logger.warning("[queue] SQS delete failed: %s", e)


def _local_receive(
    queue_name: str, max_messages: int
) -> Iterator[tuple[str, dict]]:
    q = _LOCAL_QUEUES.get(queue_name, [])
    delivered = 0
    while q and delivered < max_messages:
        msg = q.pop(0)
        yield f"local-{delivered}", msg
        delivered += 1


# ── Queue depth ────────────────────────────────────────────────────────

def queue_depth(queue: str) -> int:
    """Approximate number of messages in queue."""
    if _use_sqs():
        try:
            import boto3
            sqs = boto3.client("sqs", region_name=os.getenv("AWS_REGION", "us-east-1"))
            resp = sqs.get_queue_attributes(
                QueueUrl       = queue,
                AttributeNames = ["ApproximateNumberOfMessages"],
            )
            return int(resp["Attributes"].get("ApproximateNumberOfMessages", 0))
        except Exception:
            return -1
    return len(_LOCAL_QUEUES.get(queue, []))


# ── DLQ helper ────────────────────────────────────────────────────────

def dlq_name() -> str:
    return os.getenv("QUEUE_DLQ", "assetera-ingest-dlq")
