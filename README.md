# wildduck-s3-storage

> **Tenforward AB fork** of [Reusable-email/wildduck-s3-storage](https://github.com/Reusable-email/wildduck-s3-storage),
> published as `@tenforwardab/wildduck-s3-storage`. Changes over upstream:
>
> - **Partial fetches are byte-identical to GridFS.** IMAP `BODY[n]<start.len>`
>   offsets address the base64 output, but decoded attachments hold binary in S3.
>   Reads now go through the same `base64Offset()` mapping WildDuck's GridFS
>   backend uses (binary window + encoder `skipStartBytes` / `startPadding` /
>   limit), for both the S3 path and the dual-mode GridFS fallback. Previously
>   the base64 offsets were sent as a raw byte `Range`, which corrupted partial
>   downloads in clients that fetch attachments in chunks (Apple Mail, mobile).
> - **End-offset fix.** WildDuck's `base64-offset.js` reads one byte too few when
>   the request starts three characters into a base64 group; the vendored copy
>   in `lib/base64-offset.js` covers the skipped characters. (Reported upstream.)
> - Requests at or past EOF return an empty stream instead of an S3 `416`.
> - `npm test` runs a randomized byte-identity suite against `libbase64` folding.


S3/R2-compatible object storage backend for [WildDuck](https://github.com/zone-eu/wildduck) attachment storage. Drop-in replacement for the built-in GridFS backend.

Stores attachment blobs in any S3-compatible object store (Cloudflare R2, AWS S3, MinIO, etc.) while keeping deduplication metadata in MongoDB. Supports zero-downtime migration from existing GridFS installations.

## Why?

WildDuck stores email attachments in MongoDB GridFS by default. This works but has drawbacks at scale:

- **Cost** — MongoDB storage is expensive; S3/R2 is cheap (R2 has free egress)
- **Scalability** — GridFS chunks bloat MongoDB, slowing backups and replication
- **Separation of concerns** — hot metadata on fast SSDs, cold blobs in object storage

## Features

- **Drop-in replacement** — same interface as `gridstore-storage.js`
- **Deduplication preserved** — SHA-256 hash-based dedup, same as GridFS backend
- **Reference counting** — same dual-counter (c + magic) approach
- **Server-side encryption** — AES-256 or KMS
- **Dual mode** — zero-downtime migration with GridFS fallback reads
- **Migration CLI** — batch migrate existing attachments with progress tracking
- **R2 native** — built for Cloudflare R2 (works with any S3-compatible store)

## Installation

```bash
npm install @tenforwardab/wildduck-s3-storage
```

## Quick Start

### 1. Patch WildDuck's attachment-storage.js

Add the `s3` case to the storage type switch in `lib/attachment-storage.js`:

```javascript
switch (type) {
    case 's3': {
        const mode = (options.options && options.options.mode) || 's3';
        if (mode === 'dual') {
            const DualStorage = require('@tenforwardab/wildduck-s3-storage/lib/dual-storage');
            this.storage = new DualStorage(options);
        } else {
            const S3Storage = require('@tenforwardab/wildduck-s3-storage/lib/s3-storage');
            this.storage = new S3Storage(options);
        }
        break;
    }
    case 'gridstore':
    default:
        this.storage = new GridstoreStorage(this.options);
        break;
}
```

### 2. Configure (attachments.toml)

```toml
type="s3"
mode="dual"                # Start with "dual" for migration, switch to "s3" after
bucket="attachments"
decodeBase64=true

[s3]
bucket="wildduck-attachments"
region="auto"
endpoint="https://<account-id>.r2.cloudflarestorage.com"
accessKeyId="your-access-key"
secretAccessKey="your-secret-key"
forcePathStyle=true

# Optional: server-side encryption
[s3.encryption]
algorithm="AES256"
```

### 3. Deploy

Restart WildDuck. In dual mode:
- **New attachments** → stored in S3
- **Existing attachments** → read from S3 first, GridFS fallback

### 4. Migrate existing data

```bash
npx @tenforwardab/wildduck-s3-storage migrate \
  --mongo-url "mongodb://user:pass@host:27017/wildduck" \
  --s3-bucket wildduck-attachments \
  --s3-endpoint "https://<account-id>.r2.cloudflarestorage.com" \
  --s3-access-key-id "..." \
  --s3-secret-access-key "..." \
  --concurrency 20
```

Safe to run during live traffic. Safe to run multiple times (skips already-migrated files).

### 5. Switch to S3-only mode

Once migration completes, update `attachments.toml`:

```toml
mode="s3"   # was "dual"
```

Restart WildDuck. Then reclaim MongoDB space:

```javascript
db.attachments.chunks.drop()
```

## Architecture

```
┌──────────────┐     ┌─────────────────┐
│   WildDuck   │────▶│ attachment-      │
│  (IMAP/API)  │     │ storage.js       │
└──────────────┘     └────────┬─────────┘
                              │
                    ┌─────────▼──────────┐
                    │   s3-storage.js     │
                    │   (this plugin)     │
                    └──┬──────────────┬──┘
                       │              │
              ┌────────▼───┐   ┌──────▼──────┐
              │  MongoDB   │   │   S3 / R2   │
              │ (metadata) │   │   (blobs)   │
              └────────────┘   └─────────────┘

Metadata (dedup index, refcounts): MongoDB
Binary attachment data: S3/R2
```

## API

### S3Storage

Drop-in replacement for `GridstoreStorage`. Same method signatures:

| Method | Description |
|--------|-------------|
| `create(attachment, hash, callback)` | Store attachment (dedup + upload) |
| `get(attachmentId)` | Get attachment metadata |
| `createReadStream(id, attachmentData, options)` | Stream attachment data |
| `delete(id, magic, callback)` | Decrement reference count |
| `update(ids, count, magic, callback)` | Batch update reference counts |
| `deleteOrphaned(callback)` | Clean up zero-reference attachments |

### DualStorage (extends S3Storage)

Migration mode. Writes to S3, reads from S3 with GridFS fallback.

### migrateAttachments(options)

Batch migrate GridFS blobs to S3. Returns `{ migrated, skipped, failed, total }`.

## S3 Object Layout

Objects are stored with a two-level hash prefix to avoid listing bottlenecks:

```
<prefix>/<first-2-chars-of-hash>/<full-hash>
```

Example: `ab/abc123def456...`

## License

EUPL-1.2 — Compatible with GPL, AGPL, and most open-source licenses.

## Contributing

Built by [Reusable Email](https://reusable.email). Issues and PRs welcome.
