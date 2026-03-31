# r2-consumer

Monitors a Cloudflare R2 bucket for newly created objects via an
event-notification queue.  The workflow is split into two composable steps
designed for distributed processing:

**List mode** (default): pulls the notification queue and prints one JSON
record per pending object to stdout.  No objects are downloaded; no queue
entries are acknowledged.

**Fetch mode** (positional `RECORD` arg): accepts a JSON record from list
mode, downloads the referenced object, writes its bytes to stdout, then
acknowledges the queue entry to permanently remove it.

```bash
# Step 1 — collect work (one JSON line per object):
python3 r2_consumer.py > records.jsonl

# Step 2 — distribute fetch across 8 parallel workers:
cat records.jsonl | parallel -j8 python3 r2_consumer.py {}
```

## How it works

On first run the script sets up any missing infrastructure, then lists:

1. Ensures a Cloudflare Queue exists (creates one if absent).
2. Ensures an HTTP pull consumer is attached to that queue.
3. Ensures R2 event notification rules are configured so the bucket emits
   object-create events (`PutObject`, `CopyObject`, `CompleteMultipartUpload`)
   to the queue.
4. Pulls messages from the queue and emits one JSON record per object to
   stdout.

Each notification is processed **at most once**: the queue entry is only
acknowledged (and permanently removed) after the object has been fully written
to stdout.  A failed download or write leaves the message unacknowledged; it
reappears in the queue after the visibility timeout (60 s) and can be retried.

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

The script has two primary modes and two utility modes.

### List mode (default)

Pulls the notification queue and prints one JSON line per pending object to
stdout.  No objects are downloaded; no queue entries are acknowledged.

```bash
python3 r2_consumer.py           # ensure setup, then list
python3 r2_consumer.py --no-setup  # skip setup, just list
```

Each stdout line is a self-contained JSON record:

```json
{"queue_id":"ed0e359b...","lease_id":"eyJhbGci...","bucket":"my-bucket","key":"logs/2024-01-01.log.gz","action":"PutObject"}
```

### Fetch mode (RECORD arg)

Pass a JSON record from list mode as a positional argument.  The script
downloads the object, writes its bytes to stdout, then acknowledges the queue
entry so it is permanently removed.

```bash
python3 r2_consumer.py '{"queue_id":"...","lease_id":"...","bucket":"...","key":"..."}'
```

### Distributed pipeline

Because list mode only emits records and fetch mode only consumes one, the two
steps compose freely.  Use any parallelism tool to fan out the fetch step:

```bash
# GNU parallel — 8 concurrent fetch workers:
python3 r2_consumer.py | parallel -j8 python3 r2_consumer.py {}

# xargs:
python3 r2_consumer.py | xargs -P8 -I{} python3 r2_consumer.py {}

# Save the list first, then distribute across machines or processes:
python3 r2_consumer.py > records.jsonl
cat records.jsonl | parallel -j8 python3 r2_consumer.py {}
```

Diagnostic messages (queue IDs, download progress, errors) go to **stderr**
and never mix with the JSON records or object bytes on stdout.

If downloaded objects are binary, redirect stdout to a file:

```bash
python3 r2_consumer.py '{"queue_id":"...","key":"..."}' > output.bin
```

### Setup only

```bash
python3 r2_consumer.py --setup-only
```

Creates the queue, HTTP pull consumer, and R2 notification rules if absent,
then exits without pulling any messages.

### Delete old objects

```bash
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
