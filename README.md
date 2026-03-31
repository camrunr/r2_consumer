# r2-consumer

Connects to the Cloudflare API to monitor an R2 bucket for newly created
objects via an event-notification queue, downloads each object, and prints
its uncompressed content to stdout.

On first run the script creates any missing infrastructure (queue, HTTP pull
consumer, notification rules) before pulling. Subsequent runs skip setup if
everything is already in place.

## How it works

1. Ensures a Cloudflare Queue exists (creates one if absent).
2. Ensures an HTTP pull consumer is attached to that queue.
3. Ensures R2 event notification rules are configured so the bucket emits
   object-create events (`PutObject`, `CopyObject`, `CompleteMultipartUpload`)
   to the queue.
4. Pulls messages from the queue, downloads each referenced object from R2,
   writes its bytes to stdout, then acknowledges the message to permanently
   remove it from the queue.

Each notification is processed **at most once**: the queue entry is only
removed after the object has been fully written to stdout. A failed download
or write leaves the message unacknowledged; it reappears in the queue after
the visibility timeout (60 s by default) and can be retried.

## Dependencies

```
pip install requests boto3
```

Or with uv:

```
uv add requests boto3
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `CLOUDFLARE_API_TOKEN` | yes | Cloudflare API Bearer token (see [API token permissions](#api-token-permissions)) |
| `CLOUDFLARE_ACCOUNT_ID` | yes | Your Cloudflare account ID (visible in the dashboard URL) |
| `BUCKET_NAME` | yes | Name of the R2 bucket to watch |
| `R2_ACCESS_KEY_ID` | yes | R2 S3-compatible access key ID |
| `R2_SECRET_ACCESS_KEY` | yes | R2 S3-compatible secret access key |
| `QUEUE_NAME` | no | Queue name — defaults to `<BUCKET_NAME>-notifications` |

## Usage

```bash
# Ensure setup then pull all pending notifications (default):
python3 r2_consumer.py

# Only create the queue / consumer / notification rules, do not pull:
python3 r2_consumer.py --setup-only

# Skip setup and go straight to pulling (queue must already exist):
python3 r2_consumer.py --no-setup

# Delete all objects in the bucket older than a given age, then exit:
python3 r2_consumer.py --delete-older-than 1w   # older than 1 week
python3 r2_consumer.py --delete-older-than 30d  # older than 30 days
python3 r2_consumer.py --delete-older-than 24h  # older than 24 hours
python3 r2_consumer.py --delete-older-than 90m  # older than 90 minutes
```

Supported age units: `s` seconds, `m` minutes, `h` hours, `d` days, `w` weeks.

`--delete-older-than` runs independently of setup/pull — it does not touch
the notification queue, only the bucket objects themselves.

Object bytes are written to **stdout**. Diagnostic messages (queue IDs,
download progress, errors) go to **stderr** and never mix with object content.
If the object is binary, redirect stdout to a file:

```bash
python3 r2_consumer.py > output.bin
```

## API token permissions

Create a token at <https://dash.cloudflare.com/profile/api-tokens> using
**Create Custom Token** and grant:

- **Account > Queues > Edit** — list, create, and configure queues; pull and
  acknowledge messages. Both read and write are required because acknowledging
  a message writes state back to the queue. Choose **Edit**, not Read.
- **Account > Workers R2 Storage > Edit** — read and write R2
  event-notification configuration.

> **Tip:** A `403 Forbidden` on `/queues` almost always means the token is
> missing the **Queues Edit** permission. Re-create the token with both
> permissions ticked.

## R2 S3 credentials

`R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY` are **not** the same as the
Cloudflare API Bearer token. Obtain them from:

> Cloudflare dashboard → R2 Object Storage → **Manage R2 API Tokens**
> → Create API Token (choose *Object Read* or *Object Read & Write*)

Note the Access Key ID and Secret Access Key when shown — they are not
recoverable afterwards. The S3-compatible endpoint used internally is:

```
https://<CLOUDFLARE_ACCOUNT_ID>.r2.cloudflarestorage.com
```

## Worker consumer conflict

A Cloudflare Queue supports only one consumer type at a time. If a Worker
consumer already exists on the queue, adding an HTTP pull consumer will fail.
Remove the Worker consumer first:

```bash
npx wrangler queues consumer worker remove <QUEUE_NAME> <WORKER_NAME>
```

See [Cloudflare pull consumers docs](https://developers.cloudflare.com/queues/configuration/pull-consumers/) for details.
