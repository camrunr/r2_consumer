import argparse
import base64
import datetime
import gzip
import json
import os
import re
import sys
from typing import Any

import boto3
import requests
from botocore.config import Config as BotocoreConfig

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
QUEUE_NAME = os.environ.get("QUEUE_NAME", f"{BUCKET_NAME}-notifications")

CF_API_BASE = "https://api.cloudflare.com/client/v4"
# R2 uses the AWS S3 protocol; credentials come from Manage R2 API Tokens,
# not from the Cloudflare Bearer token above.
R2_ENDPOINT = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com"

# Notification actions that cover every "new object" scenario.
OBJECT_CREATE_ACTIONS = ["PutObject", "CopyObject", "CompleteMultipartUpload"]

# Pull-consumer tuning: pull up to 100 messages per batch; give the consumer
# 60 s to ack before a message is considered unacknowledged and requeued.
PULL_BATCH_SIZE = 100
VISIBILITY_TIMEOUT_MS = 60_000


# ---------------------------------------------------------------------------
# Cloudflare API helpers
# ---------------------------------------------------------------------------

def _cf_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }


def _cf_raise(resp: requests.Response, method: str, path: str) -> None:
    """Raise requests.HTTPError with the full Cloudflare response body included.

    Raising HTTPError (not RuntimeError) preserves exc.response so callers
    can inspect the status code — e.g. to treat 404 as "not found yet".
    """
    if not resp.ok:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(
            f"{method} {path} → HTTP {resp.status_code}\n"
            f"Response: {detail}\n"
            "Check that your API token has the required permissions — see README.md\n"
            "for details.",
            response=resp,
        )


def _cf_get(path: str) -> Any:
    """GET from the Cloudflare v4 API, raise on HTTP or API errors."""
    url = f"{CF_API_BASE}{path}"
    resp = requests.get(url, headers=_cf_headers(), timeout=30)
    _cf_raise(resp, "GET", path)
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"API error for GET {path}: {body.get('errors')}")
    return body


def _cf_post(path: str, payload: dict) -> Any:
    """POST to the Cloudflare v4 API, raise on HTTP or API errors."""
    url = f"{CF_API_BASE}{path}"
    resp = requests.post(url, headers=_cf_headers(), json=payload, timeout=30)
    _cf_raise(resp, "POST", path)
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"API error for POST {path}: {body.get('errors')}")
    return body


def _cf_put(path: str, payload: dict) -> Any:
    """PUT to the Cloudflare v4 API, raise on HTTP or API errors."""
    url = f"{CF_API_BASE}{path}"
    resp = requests.put(url, headers=_cf_headers(), json=payload, timeout=30)
    _cf_raise(resp, "PUT", path)
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"API error for PUT {path}: {body.get('errors')}")
    return body


# ---------------------------------------------------------------------------
# Setup: queue
# ---------------------------------------------------------------------------

def ensure_queue() -> str:
    """Return the queue_id for QUEUE_NAME, creating the queue if absent."""
    print(f"[setup] checking for queue '{QUEUE_NAME}' ...", file=sys.stderr)

    # Paginate through all queues to find a matching name.
    page = 1
    while True:
        body = _cf_get(f"/accounts/{ACCOUNT_ID}/queues?page={page}&per_page=100")
        queues = body.get("result") or []
        for q in queues:
            if q["queue_name"] == QUEUE_NAME:
                queue_id = q["queue_id"]
                print(f"[setup] found existing queue id={queue_id}", file=sys.stderr)
                return queue_id
        info = body.get("result_info", {})
        if page >= info.get("total_pages", 1):
            break
        page += 1

    # Queue does not exist — create it.
    print(f"[setup] creating queue '{QUEUE_NAME}' ...", file=sys.stderr)
    body = _cf_post(f"/accounts/{ACCOUNT_ID}/queues", {"queue_name": QUEUE_NAME})
    queue_id = body["result"]["queue_id"]
    print(f"[setup] created queue id={queue_id}", file=sys.stderr)
    return queue_id


# ---------------------------------------------------------------------------
# Setup: HTTP pull consumer
# ---------------------------------------------------------------------------

def ensure_http_pull_consumer(queue_id: str) -> None:
    """Attach an HTTP pull consumer to the queue if one does not exist.

    Raises a descriptive error if a Worker consumer already exists, because
    a queue supports only one consumer type at a time.
    """
    print("[setup] checking consumers on queue ...", file=sys.stderr)
    body = _cf_get(f"/accounts/{ACCOUNT_ID}/queues/{queue_id}")
    consumers = (body.get("result") or {}).get("consumers") or []

    for c in consumers:
        if c.get("type") == "http_pull":
            print("[setup] http_pull consumer already present", file=sys.stderr)
            return
        if c.get("type") == "worker":
            raise RuntimeError(
                "A Worker consumer is already attached to this queue.  "
                "HTTP pull cannot be added until the Worker consumer is removed.\n"
                "  npx wrangler queues consumer worker remove "
                f"{QUEUE_NAME} <WORKER_NAME>\n"
                "See https://developers.cloudflare.com/queues/configuration/pull-consumers/"
            )

    print("[setup] attaching http_pull consumer ...", file=sys.stderr)
    _cf_post(
        f"/accounts/{ACCOUNT_ID}/queues/{queue_id}/consumers",
        {"type": "http_pull"},
    )
    print("[setup] http_pull consumer attached", file=sys.stderr)


# ---------------------------------------------------------------------------
# Setup: R2 event notification rules
# ---------------------------------------------------------------------------

def ensure_notification_rules(queue_id: str) -> None:
    """Create or update the R2 event notification rule for object-create events."""
    print("[setup] checking R2 notification rules ...", file=sys.stderr)

    # Retrieve existing rules for this bucket + queue pair.
    try:
        body = _cf_get(
            f"/accounts/{ACCOUNT_ID}/event_notifications/r2/{BUCKET_NAME}"
            f"/configuration/queues/{queue_id}"
        )
        rules = (body.get("result") or {}).get("rules") or []
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            rules = []
        else:
            raise

    # Check whether ALL required actions are already covered by at least one rule.
    covered = set()
    for rule in rules:
        covered.update(rule.get("actions", []))

    missing = set(OBJECT_CREATE_ACTIONS) - covered
    if not missing:
        print("[setup] notification rules already cover all object-create actions", file=sys.stderr)
        return

    print(f"[setup] configuring notification rules (missing: {missing}) ...", file=sys.stderr)
    _cf_put(
        f"/accounts/{ACCOUNT_ID}/event_notifications/r2/{BUCKET_NAME}"
        f"/configuration/queues/{queue_id}",
        {
            "rules": [
                {
                    "actions": OBJECT_CREATE_ACTIONS,
                    "description": "Notify on new object creation",
                }
            ]
        },
    )
    print("[setup] notification rules configured", file=sys.stderr)


# ---------------------------------------------------------------------------
# R2 object download
# ---------------------------------------------------------------------------

def make_r2_client():
    """Build a boto3 S3 client pointed at the R2 S3-compatible endpoint.

    R2 credentials (access key + secret) are distinct from the Cloudflare
    API Bearer token — they come from "Manage R2 API Tokens" in the dashboard.
    """
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        # R2 does not use AWS regions; us-east-1 is a required placeholder.
        region_name="us-east-1",
        config=BotocoreConfig(signature_version="s3v4"),
    )


def download_object(r2: Any, bucket: str, key: str) -> bytes:
    """Fetch raw bytes for key from R2, transparently decompressing gzip.

    For gzip-compressed objects (key ends with .gz or Content-Encoding is
    gzip), the stored compressed bytes are decompressed before returning so
    that stdout always receives plain text / uncompressed content.
    """
    resp = r2.get_object(Bucket=bucket, Key=key)
    body_bytes = resp["Body"].read()

    content_encoding = resp.get("ContentEncoding", "")
    # Decompress if the object is gzip-encoded (either by Content-Encoding or
    # by file extension convention).
    if content_encoding == "gzip" or key.endswith(".gz"):
        body_bytes = gzip.decompress(body_bytes)

    return body_bytes


# ---------------------------------------------------------------------------
# Pull loop: consume messages, download objects, ack on success
# ---------------------------------------------------------------------------

def pull_and_process(queue_id: str, r2: Any) -> int:
    """Pull one batch of messages, process each, ack successes.

    Returns the number of messages successfully processed.

    Processing order for each message (critical for single-delivery):
      1. Decode the queue message body.
      2. Download the R2 object referenced by the notification.
      3. Write the object bytes to stdout.
      4. Add the lease_id to the ack list.
    Only after ALL steps succeed is the lease_id included in the ack call.
    """
    # Pull up to PULL_BATCH_SIZE messages; Cloudflare short-polls — if there
    # are no messages it returns an empty batch immediately rather than blocking.
    print(
        f"[pull] pulling up to {PULL_BATCH_SIZE} messages "
        f"(visibility_timeout={VISIBILITY_TIMEOUT_MS}ms) ...",
        file=sys.stderr,
    )
    pull_resp = _cf_post(
        f"/accounts/{ACCOUNT_ID}/queues/{queue_id}/messages/pull",
        {
            "batch_size": PULL_BATCH_SIZE,
            "visibility_timeout_ms": VISIBILITY_TIMEOUT_MS,
        },
    )

    messages = (pull_resp.get("result") or {}).get("messages") or []
    if not messages:
        print("[pull] no messages in queue", file=sys.stderr)
        return 0

    print(f"[pull] received {len(messages)} message(s)", file=sys.stderr)

    ack_lease_ids: list[str] = []

    for msg in messages:
        lease_id = msg["lease_id"]
        raw_body = msg["body"]

        # Queues encodes messages with content-type "json" or "bytes" as
        # base64 to allow safe transport inside the JSON response envelope.
        # Attempt base64 decode; if it fails the body is plain text/JSON.
        try:
            decoded = base64.b64decode(raw_body).decode("utf-8")
        except Exception:
            decoded = raw_body

        try:
            notification = json.loads(decoded)
        except json.JSONDecodeError:
            print(f"[pull] WARN: non-JSON message body, skipping: {decoded!r}", file=sys.stderr)
            # Do not ack — let it expire and potentially surface for investigation.
            continue

        bucket = notification.get("bucket", BUCKET_NAME)
        key = notification.get("object", {}).get("key")
        action = notification.get("action", "unknown")

        if not key:
            print(f"[pull] WARN: notification has no object.key — skipping: {notification}", file=sys.stderr)
            continue

        print(f"[pull] downloading s3://{bucket}/{key} (action={action}) ...", file=sys.stderr)

        try:
            # Download the object from R2 using the S3-compatible credentials.
            data = download_object(r2, bucket, key)
        except Exception as exc:
            print(f"[pull] ERROR downloading {key}: {exc}", file=sys.stderr)
            # Do not ack; the message will be redelivered after visibility_timeout.
            continue

        try:
            # Write raw bytes to stdout — separate from stderr diagnostics.
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
        except Exception as exc:
            print(f"[pull] ERROR writing to stdout for {key}: {exc}", file=sys.stderr)
            continue

        print(f"[pull] wrote {len(data)} bytes for {key}", file=sys.stderr)
        # Queue the lease_id for acknowledgment only after a successful write.
        # This is what removes the notification from the queue permanently.
        ack_lease_ids.append(lease_id)

    if ack_lease_ids:
        # Acknowledge all successfully processed messages in a single request.
        # Each acked lease_id is permanently removed from the queue backlog.
        print(f"[pull] acknowledging {len(ack_lease_ids)} message(s) ...", file=sys.stderr)
        _cf_post(
            f"/accounts/{ACCOUNT_ID}/queues/{queue_id}/messages/ack",
            {"acks": [{"lease_id": lid} for lid in ack_lease_ids], "retries": []},
        )
        print(f"[pull] acknowledged {len(ack_lease_ids)} message(s)", file=sys.stderr)

    return len(ack_lease_ids)


# ---------------------------------------------------------------------------
# Age-based deletion
# ---------------------------------------------------------------------------

# Supported suffixes for --delete-older-than values.
_AGE_UNITS: dict[str, int] = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}
_AGE_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)([smhdw])$", re.IGNORECASE)


def parse_age(value: str) -> datetime.timedelta:
    """Parse a human age string such as '1w', '7d', '24h', '90m', '3600s'.

    Raises argparse.ArgumentTypeError on invalid input so the error appears
    as a standard argparse usage message.
    """
    m = _AGE_PATTERN.match(value.strip())
    if not m:
        raise argparse.ArgumentTypeError(
            f"Invalid age '{value}'. Use a number followed by a unit: "
            "s (seconds), m (minutes), h (hours), d (days), w (weeks). "
            "Examples: 1w  7d  24h  90m"
        )
    amount, unit = float(m.group(1)), m.group(2).lower()
    return datetime.timedelta(seconds=amount * _AGE_UNITS[unit])


def delete_old_objects(r2: Any, max_age: datetime.timedelta) -> int:
    """Delete all objects in BUCKET_NAME whose LastModified is older than max_age.

    Uses paginated list_objects_v2 (1000 keys per page) then issues batched
    delete_objects requests (up to 1000 keys each — the S3 API maximum).
    Returns the total number of objects deleted.
    """
    cutoff = datetime.datetime.now(tz=datetime.timezone.utc) - max_age
    print(
        f"[prune] scanning '{BUCKET_NAME}' for objects older than {max_age} "
        f"(before {cutoff.isoformat()}) ...",
        file=sys.stderr,
    )

    to_delete: list[dict[str, str]] = []
    paginator = r2.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                to_delete.append({"Key": obj["Key"]})

    if not to_delete:
        print("[prune] no objects found older than the specified age", file=sys.stderr)
        return 0

    print(f"[prune] deleting {len(to_delete)} object(s) ...", file=sys.stderr)

    deleted_count = 0
    # delete_objects accepts at most 1000 keys per call.
    batch_size = 1000
    for i in range(0, len(to_delete), batch_size):
        batch = to_delete[i : i + batch_size]
        resp = r2.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": batch, "Quiet": False})
        deleted = resp.get("Deleted", [])
        errors = resp.get("Errors", [])
        deleted_count += len(deleted)
        for err in errors:
            print(
                f"[prune] ERROR deleting {err.get('Key')}: "
                f"{err.get('Code')} — {err.get('Message')}",
                file=sys.stderr,
            )

    print(f"[prune] deleted {deleted_count} object(s)", file=sys.stderr)
    return deleted_count


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def validate_config() -> None:
    missing = [
        name
        for name, val in [
            ("CLOUDFLARE_API_TOKEN", API_TOKEN),
            ("CLOUDFLARE_ACCOUNT_ID", ACCOUNT_ID),
            ("BUCKET_NAME", BUCKET_NAME),
            ("R2_ACCESS_KEY_ID", R2_ACCESS_KEY_ID),
            ("R2_SECRET_ACCESS_KEY", R2_SECRET_ACCESS_KEY),
        ]
        if not val
    ]
    if missing:
        print(f"ERROR: missing required environment variables: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consume Cloudflare R2 object-create notifications from a Queue.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Set CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, BUCKET_NAME,\n"
            "R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY before running.\n"
            "See README.md for full setup and credential instructions."
        ),
    )
    parser.add_argument(
        "--delete-older-than",
        metavar="AGE",
        type=parse_age,
        default=None,
        help=(
            "Delete objects from the R2 bucket whose last-modified time is older "
            "than AGE, then exit.  AGE is a number followed by a unit: "
            "s (seconds), m (minutes), h (hours), d (days), w (weeks). "
            "Examples: --delete-older-than 1w  --delete-older-than 30d"
        ),
    )
    parser.add_argument(
        "--setup-only",
        action="store_true",
        help="Only ensure the queue, consumer, and notification rules exist; do not pull.",
    )
    parser.add_argument(
        "--no-setup",
        action="store_true",
        help="Skip the idempotent setup check and go straight to pulling messages.",
    )
    args = parser.parse_args()

    validate_config()

    # --delete-older-than is independent of setup/pull; runs and exits.
    if args.delete_older_than is not None:
        r2 = make_r2_client()
        delete_old_objects(r2, args.delete_older_than)
        return

    queue_id: str

    if not args.no_setup:
        queue_id = ensure_queue()
        ensure_http_pull_consumer(queue_id)
        ensure_notification_rules(queue_id)
    else:
        # Without setup we still need the queue_id — look it up by name.
        print(f"[init] resolving queue id for '{QUEUE_NAME}' ...", file=sys.stderr)
        page = 1
        queue_id = ""
        while True:
            body = _cf_get(f"/accounts/{ACCOUNT_ID}/queues?page={page}&per_page=100")
            for q in (body.get("result") or []):
                if q["queue_name"] == QUEUE_NAME:
                    queue_id = q["queue_id"]
                    break
            if queue_id:
                break
            info = body.get("result_info", {})
            if page >= info.get("total_pages", 1):
                break
            page += 1
        if not queue_id:
            print(f"ERROR: queue '{QUEUE_NAME}' not found.  Run without --no-setup first.", file=sys.stderr)
            sys.exit(1)
        print(f"[init] queue id={queue_id}", file=sys.stderr)

    if args.setup_only:
        print("[done] setup complete", file=sys.stderr)
        return

    r2 = make_r2_client()
    processed = pull_and_process(queue_id, r2)
    print(f"[done] processed {processed} object(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
