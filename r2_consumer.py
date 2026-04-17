import argparse
import base64
import datetime
import gzip
import json
import os
import re
import signal
import sys
import time
from typing import Any

import boto3
import requests
from botocore.config import Config as BotocoreConfig

# Exit cleanly when stdout is closed early (e.g. piped into `head`) instead of
# dumping a BrokenPipeError traceback to stderr.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN", "")
ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
# QUEUE_NAME is resolved in main() after config validation — otherwise an
# empty BUCKET_NAME produces the nonsense default "-notifications".
QUEUE_NAME = ""

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


# Transient failures worth retrying: CF rate-limit + 5xx, plus network errors.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 5
_BASE_BACKOFF_S = 1.0


def _cf_request(method: str, path: str, payload: dict | None = None) -> Any:
    """Issue a Cloudflare v4 API request with bounded exponential-backoff retry.

    Retries on 429/5xx and network errors up to _MAX_RETRIES times.  Honors
    Retry-After on 429 when present.  Raises on non-retryable HTTP errors or
    after retries are exhausted.
    """
    url = f"{CF_API_BASE}{path}"
    kwargs: dict[str, Any] = {"headers": _cf_headers(), "timeout": 30}
    if payload is not None:
        kwargs["json"] = payload

    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, **kwargs)
        except requests.RequestException as exc:
            last_exc = exc
            if attempt == _MAX_RETRIES:
                raise
            delay = _BASE_BACKOFF_S * (2 ** attempt)
            print(
                f"[retry] {method} {path} network error ({exc}); "
                f"sleeping {delay:.1f}s (attempt {attempt + 1}/{_MAX_RETRIES})",
                file=sys.stderr,
            )
            time.sleep(delay)
            continue

        if resp.status_code in _RETRYABLE_STATUS and attempt < _MAX_RETRIES:
            retry_after = resp.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else _BASE_BACKOFF_S * (2 ** attempt)
            except ValueError:
                delay = _BASE_BACKOFF_S * (2 ** attempt)
            print(
                f"[retry] {method} {path} HTTP {resp.status_code}; "
                f"sleeping {delay:.1f}s (attempt {attempt + 1}/{_MAX_RETRIES})",
                file=sys.stderr,
            )
            time.sleep(delay)
            continue

        _cf_raise(resp, method, path)
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"API error for {method} {path}: {body.get('errors')}")
        return body

    # Retries exhausted on network errors.
    raise last_exc if last_exc else RuntimeError(f"{method} {path}: retries exhausted")


def _cf_get(path: str) -> Any:
    return _cf_request("GET", path)


def _cf_post(path: str, payload: dict) -> Any:
    return _cf_request("POST", path, payload)


def _cf_put(path: str, payload: dict) -> Any:
    return _cf_request("PUT", path, payload)


# ---------------------------------------------------------------------------
# Setup: queue
# ---------------------------------------------------------------------------

def find_queue_id(name: str) -> str | None:
    """Return the queue_id for name by paginating /queues, or None if absent."""
    page = 1
    while True:
        body = _cf_get(f"/accounts/{ACCOUNT_ID}/queues?page={page}&per_page=100")
        for q in (body.get("result") or []):
            if q["queue_name"] == name:
                return q["queue_id"]
        info = body.get("result_info", {})
        if page >= info.get("total_pages", 1):
            return None
        page += 1


def ensure_queue() -> str:
    """Return the queue_id for QUEUE_NAME, creating the queue if absent."""
    print(f"[setup] checking for queue '{QUEUE_NAME}' ...", file=sys.stderr)

    queue_id = find_queue_id(QUEUE_NAME)
    if queue_id:
        print(f"[setup] found existing queue id={queue_id}", file=sys.stderr)
        return queue_id

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
    """Ensure an unfiltered object-create notification rule exists on this bucket→queue.

    The R2 configuration PUT replaces the entire rule set for a bucket→queue
    pair, so we fetch existing rules first and only add a rule when no
    existing *unfiltered* rule already covers the required actions.  Rules
    scoped with a prefix/suffix don't count — we want object-create events
    for every key in the bucket.
    """
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

    required = set(OBJECT_CREATE_ACTIONS)
    for rule in rules:
        # Only unfiltered rules (no prefix/suffix) count as full coverage.
        if rule.get("prefix") or rule.get("suffix"):
            continue
        if required.issubset(set(rule.get("actions", []))):
            print(
                "[setup] notification rules already cover all object-create actions",
                file=sys.stderr,
            )
            return

    # Add our rule alongside any existing rules rather than replacing them.
    new_rule = {
        "actions": OBJECT_CREATE_ACTIONS,
        "description": "Notify on new object creation",
    }
    preserved_rules = [
        {k: v for k, v in r.items() if k in ("actions", "prefix", "suffix", "description")}
        for r in rules
    ]
    print(
        f"[setup] configuring notification rules "
        f"(preserving {len(preserved_rules)} existing rule(s)) ...",
        file=sys.stderr,
    )
    _cf_put(
        f"/accounts/{ACCOUNT_ID}/event_notifications/r2/{BUCKET_NAME}"
        f"/configuration/queues/{queue_id}",
        {"rules": preserved_rules + [new_rule]},
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


def stream_object_to_stdout(r2: Any, bucket: str, key: str) -> int:
    """Stream key from R2 to stdout, transparently decompressing gzip.

    Streams in chunks so multi-GB objects don't blow memory.  Returns the
    number of uncompressed bytes written.
    """
    resp = r2.get_object(Bucket=bucket, Key=key)
    body = resp["Body"]
    content_encoding = resp.get("ContentEncoding", "")
    is_gzip = content_encoding == "gzip" or key.endswith(".gz")

    out = sys.stdout.buffer
    total = 0
    chunk_size = 1 << 20  # 1 MiB

    if is_gzip:
        with gzip.GzipFile(fileobj=body, mode="rb") as gz:
            while True:
                chunk = gz.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                total += len(chunk)
    else:
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)

    out.flush()
    return total


# ---------------------------------------------------------------------------
# Pull loop: emit one JSON record per queued message (no download, no ack)
# ---------------------------------------------------------------------------

def pull_and_list(queue_id: str) -> int:
    """Pull one batch of messages and print each as a JSON line to stdout.

    Each line contains all fields a fetch worker needs to download the object
    and acknowledge the message independently:
      queue_id, lease_id, bucket, key, action

    Does NOT download objects or acknowledge messages — that is left to a
    separate fetch worker (see fetch_and_ack).  Returns the number of records
    printed.
    """
    # Cloudflare short-polls: returns an empty batch immediately when the
    # queue has no messages rather than blocking.
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

    count = 0
    for msg in messages:
        lease_id = msg["lease_id"]
        raw_body = msg["body"]

        # Queues reports the body's content-type in metadata.  "json" and
        # "text" are delivered verbatim; "bytes" is base64-encoded for safe
        # transport inside the JSON envelope.  Fall back to parsing as JSON
        # directly when no metadata is present.
        metadata = msg.get("metadata") or {}
        content_type = (metadata.get("content-type") or metadata.get("contentType") or "").lower()

        if content_type == "bytes":
            try:
                decoded = base64.b64decode(raw_body).decode("utf-8")
            except Exception as exc:
                print(f"[pull] WARN: base64 decode failed ({exc}); skipping", file=sys.stderr)
                continue
        else:
            decoded = raw_body

        try:
            notification = json.loads(decoded)
        except json.JSONDecodeError:
            print(f"[pull] WARN: non-JSON message body, skipping: {decoded!r}", file=sys.stderr)
            continue

        bucket = notification.get("bucket", BUCKET_NAME)
        key = notification.get("object", {}).get("key")
        action = notification.get("action", "unknown")

        if not key:
            print(f"[pull] WARN: notification has no object.key — skipping: {notification}", file=sys.stderr)
            continue

        # Emit one compact JSON line to stdout. This is the unit of work
        # passed to a fetch worker: it carries everything needed to download
        # the object and ack the queue entry without any shared state.
        record = {
            "queue_id": queue_id,
            "lease_id": lease_id,
            "bucket": bucket,
            "key": key,
            "action": action,
        }
        print(json.dumps(record, separators=(",", ":")))
        count += 1

    return count


# ---------------------------------------------------------------------------
# Fetch worker: download one object and ack its queue entry
# ---------------------------------------------------------------------------

def fetch_and_ack(record: dict) -> None:
    """Download the R2 object described by record, write bytes to stdout, then ack.

    record must contain: queue_id, lease_id, bucket, key.
    The queue message is acknowledged only after the object has been fully
    written to stdout, guaranteeing the entry is removed at most once per
    successful write.  Any exception before the ack leaves the message in
    the queue to be retried after the visibility timeout.
    """
    bucket = record["bucket"]
    key = record["key"]
    queue_id = record["queue_id"]
    lease_id = record["lease_id"]

    print(f"[fetch] downloading s3://{bucket}/{key} ...", file=sys.stderr)

    r2 = make_r2_client()
    # Stream directly to stdout — avoids buffering the whole object in memory
    # so multi-GB downloads don't OOM.  Diagnostic output goes to stderr.
    total = stream_object_to_stdout(r2, bucket, key)

    print(f"[fetch] wrote {total} bytes; acknowledging queue entry ...", file=sys.stderr)

    # Ack only after a successful write — this removes the notification entry
    # from the queue permanently so no other worker re-processes it.
    _cf_post(
        f"/accounts/{ACCOUNT_ID}/queues/{queue_id}/messages/ack",
        {"acks": [{"lease_id": lease_id}], "retries": []},
    )
    print(f"[fetch] acked lease {lease_id[:16]}...", file=sys.stderr)


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
        description=(
            "Cloudflare R2 event-notification queue consumer.\n\n"
            "LIST MODE (default): pulls the queue and prints one JSON record per\n"
            "pending object to stdout. No downloads, no acks.\n\n"
            "FETCH MODE (RECORD arg): downloads the object in the given JSON record\n"
            "and acks the queue entry after a successful write."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Set CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, BUCKET_NAME,\n"
            "R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY before running.\n"
            "See README.md for full setup and credential instructions."
        ),
    )
    parser.add_argument(
        "record",
        nargs="?",
        metavar="RECORD",
        help=(
            "JSON record emitted by list mode.  When supplied the script runs in\n"
            "fetch mode: downloads the object, writes it to stdout, and acks the\n"
            "queue entry.  Omit to run in list mode."
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
        help="Skip the idempotent setup check and go straight to listing messages.",
    )
    args = parser.parse_args()

    validate_config()

    # Compute the queue name default now that BUCKET_NAME has been validated.
    global QUEUE_NAME
    QUEUE_NAME = os.environ.get("QUEUE_NAME") or f"{BUCKET_NAME}-notifications"

    # CRIBL_COLLECT_ARG env var acts as an implicit RECORD argument, allowing
    # the script to be driven in fetch mode by a Cribl collector without
    # requiring explicit CLI construction.
    raw_record = args.record if args.record is not None else os.environ.get("CRIBL_COLLECT_ARG")

    # Fetch mode: RECORD positional arg supplied (or CRIBL_COLLECT_ARG set) —
    # download + ack, then exit.
    if raw_record is not None:
        try:
            record = json.loads(raw_record)
        except json.JSONDecodeError as exc:
            print(f"ERROR: RECORD is not valid JSON: {exc}", file=sys.stderr)
            sys.exit(1)
        for field in ("queue_id", "lease_id", "bucket", "key"):
            if field not in record:
                print(f"ERROR: RECORD is missing required field '{field}'", file=sys.stderr)
                sys.exit(1)
        fetch_and_ack(record)
        return

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
        resolved = find_queue_id(QUEUE_NAME)
        if not resolved:
            print(f"ERROR: queue '{QUEUE_NAME}' not found.  Run without --no-setup first.", file=sys.stderr)
            sys.exit(1)
        queue_id = resolved
        print(f"[init] queue id={queue_id}", file=sys.stderr)

    if args.setup_only:
        print("[done] setup complete", file=sys.stderr)
        return

    count = pull_and_list(queue_id)
    print(f"[done] listed {count} object(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
