'use strict';

/**
 * @module s3-storage
 *
 * Drop-in replacement for WildDuck's gridstore-storage.js.
 * Stores attachment blobs in any S3-compatible object store (AWS S3, Cloudflare R2,
 * MinIO, etc.) while keeping reference-counting metadata in MongoDB.
 *
 * The MongoDB collection (`<bucket>.files`) is retained for:
 *   - Deduplication via SHA-256 hash lookups
 *   - Reference counting (metadata.c / metadata.m)
 *   - Attachment metadata (contentType, transferEncoding, etc.)
 *
 * Binary data is stored in S3 keyed by the SHA-256 content hash, so identical
 * attachments are naturally deduplicated at the object level.
 *
 * Configuration (attachments.toml):
 *   type="s3"
 *   bucket="attachments"           # MongoDB collection prefix
 *   decodeBase64=true
 *
 *   [s3]
 *   bucket="wildduck-attachments"  # S3 bucket name
 *   region="auto"
 *   endpoint="https://<account>.r2.cloudflarestorage.com"
 *   accessKeyId="..."
 *   secretAccessKey="..."
 *   forcePathStyle=true
 *
 *   # Optional: server-side encryption
 *   [s3.encryption]
 *   algorithm="AES256"             # or "aws:kms"
 *   keyId=""                       # KMS key ID (only for aws:kms)
 */

const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const crypto = require('crypto');
const { planRead, rangeHeader } = require('./read-range');
const { analyzeBase64 } = require('./decode-plan');
const { s3Key } = require('./hash-key');

const CHUNK_SIZE = 255 * 1024; // Match WildDuck's 255 KB threshold
const ORPHAN_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours before orphan cleanup

class S3Storage {
    /**
     * @param {Object} options - WildDuck storage options
     * @param {Object} options.gridfs - MongoDB database connection (for metadata)
     * @param {Object} options.redis - Redis client (for distributed locking)
     * @param {Object} options.options - Storage-specific config from attachments.toml
     */
    constructor(options) {
        this.bucketName = (options.options && options.options.bucket) || 'attachments';
        this.decodeBase64 = (options.options && options.options.decodeBase64) || false;

        // MongoDB — metadata only (reference counts, dedup index)
        this.gridfs = options.gridfs;
        this.filesCollection = this.gridfs.collection(this.bucketName + '.files');

        // Distributed lock for large attachment dedup (same as gridstore-storage)
        // Newer WildDuck versions use 'ioredfour', older ones use 'redfour'
        let RedFour;
        try { RedFour = require('ioredfour'); } catch { RedFour = require('redfour'); }
        this.lock = new RedFour({
            redis: options.redis,
            namespace: 'wildduck'
        });

        // S3 client
        const s3Opts = (options.options && options.options.s3) || {};
        this.s3Bucket = s3Opts.bucket || 'wildduck-attachments';
        this.s3Prefix = s3Opts.prefix || '';
        this.s3Encryption = s3Opts.encryption || null;

        this.s3 = new S3Client({
            region: s3Opts.region || 'auto',
            endpoint: s3Opts.endpoint,
            credentials: s3Opts.accessKeyId ? {
                accessKeyId: s3Opts.accessKeyId,
                secretAccessKey: s3Opts.secretAccessKey
            } : undefined,
            forcePathStyle: s3Opts.forcePathStyle !== false
        });
    }

    _s3Key(hash) {
        return s3Key(this.s3Prefix, hash);
    }

    /**
     * Build S3 encryption params if configured.
     */
    _encryptionParams() {
        if (!this.s3Encryption) return {};
        const params = {};
        if (this.s3Encryption.algorithm === 'AES256') {
            params.ServerSideEncryption = 'AES256';
        } else if (this.s3Encryption.algorithm === 'aws:kms') {
            params.ServerSideEncryption = 'aws:kms';
            if (this.s3Encryption.keyId) {
                params.SSEKMSKeyId = this.s3Encryption.keyId;
            }
        }
        return params;
    }

    /**
     * Retrieve attachment metadata from MongoDB.
     * Binary data lives in S3, but WildDuck needs metadata for IMAP responses.
     */
    async get(attachmentId) {
        let attachmentData = await this.filesCollection.findOne({ _id: attachmentId });

        if (!attachmentData) {
            let err = new Error('This attachment does not exist');
            err.responseCode = 404;
            err.code = 'FileNotFound';
            throw err;
        }

        return {
            contentType: attachmentData.contentType,
            transferEncoding: attachmentData.metadata.transferEncoding,
            length: attachmentData.length,
            count: attachmentData.metadata.c,
            hash: attachmentData._id,
            metadata: attachmentData.metadata
        };
    }

    /**
     * Store an attachment. Implements deduplication via SHA-256 hash:
     *   - If hash already exists in MongoDB → increment reference count
     *   - If new → upload blob to S3 + create metadata doc in MongoDB
     *
     * Reference counting uses the same dual-counter (c + magic) approach
     * as WildDuck's gridstore-storage to prevent premature deletion.
     */
    create(attachment, hash, callback) {
        const attachmentCallback = (err, id, attachmentData) => {
            if (err) return callback(err);
            callback(null, id, attachmentData);
        };

        this._tryCreate(attachment, hash, 0, attachmentCallback);
    }

    _tryCreate(attachment, hash, tryCount, callback) {
        const id = hash;
        const magic = attachment.magic || 0;

        // Try to increment reference count on existing attachment
        this.filesCollection.findOneAndUpdate(
            { _id: id },
            {
                $inc: { 'metadata.c': 1, 'metadata.m': magic }
            },
            { returnDocument: 'after' }
        ).then(result => {
            if (result && result.value) {
                // Attachment already exists — just incremented refcount
                return callback(null, id, result.value);
            }

            // New attachment — need to upload to S3
            this._createNew(attachment, hash, magic, tryCount, callback);
        }).catch(err => callback(err));
    }

    _createNew(attachment, hash, magic, tryCount, callback) {
        const id = hash;
        const isLarge = attachment.body && attachment.body.length > CHUNK_SIZE;

        const doCreate = () => {
            let body = attachment.body;

            // Same rule as GridFS: decode only when the folding is regular
            // enough to be reproduced exactly on partial reads.
            const metadata = {
                c: 1,
                m: magic,
                cu: new Date(),
                esize: body.length,
                transferEncoding: attachment.transferEncoding || ''
            };
            Object.keys(attachment.metadata || {}).forEach(key => {
                if (!(key in metadata)) {
                    metadata[key] = attachment.metadata[key];
                }
            });
            if (this.decodeBase64) {
                const plan = analyzeBase64(attachment);
                if (plan.decoded) {
                    body = this._decodeBase64(body);
                    metadata.decoded = true;
                    metadata.lineLen = plan.lineLen;
                }
            }

            const fileDoc = {
                _id: id,
                length: body.length,
                contentType: attachment.contentType || 'application/octet-stream',
                metadata,
                uploadDate: new Date()
            };

            // Upload blob to S3
            const s3Key = this._s3Key(hash);
            const putParams = {
                Bucket: this.s3Bucket,
                Key: s3Key,
                Body: body,
                ContentType: attachment.contentType || 'application/octet-stream',
                ...this._encryptionParams()
            };

            this.s3.send(new PutObjectCommand(putParams)).then(() => {
                // Insert metadata into MongoDB
                this.filesCollection.insertOne(fileDoc).then(() => {
                    callback(null, id, fileDoc);
                }).catch(err => {
                    if (err.code === 11000) {
                        // Race condition: another process inserted first
                        // Retry as increment instead
                        if (tryCount < 5) {
                            return this._tryCreate(
                                { ...attachment, magic },
                                hash,
                                tryCount + 1,
                                callback
                            );
                        }
                    }
                    callback(err);
                });
            }).catch(err => callback(err));
        };

        if (isLarge) {
            // Acquire distributed lock for large attachments to prevent parallel S3 uploads
            const lockId = 'atts3:' + hash;
            this.lock.waitAcquireLock(lockId, 2 * 60 * 1000, false, (err, lock) => {
                if (err) return callback(err);

                // Re-check after acquiring lock — another process may have created it
                this.filesCollection.findOneAndUpdate(
                    { _id: hash },
                    { $inc: { 'metadata.c': 1, 'metadata.m': magic } },
                    { returnDocument: 'after' }
                ).then(result => {
                    if (result && result.value) {
                        this.lock.releaseLock(lock, () => {});
                        return callback(null, hash, result.value);
                    }
                    doCreate();
                    // Release lock after create
                    this.lock.releaseLock(lock, () => {});
                }).catch(err => {
                    this.lock.releaseLock(lock, () => {});
                    callback(err);
                });
            });
        } else {
            doCreate();
        }
    }

    /**
     * Create a readable stream for an attachment from S3.
     *
     * IMAP partial fetches (BODY[n]<start.len>) address the *base64 output*,
     * while decoded attachments hold binary in S3. planRead() maps the request
     * to the binary window and encoder trimming exactly like WildDuck's GridFS
     * backend, so both backends return byte-identical slices.
     */
    createReadStream(id, attachmentData, options) {
        const { PassThrough } = require('stream');
        const plan = planRead(attachmentData, options);

        let output;
        let sink;
        if (plan.decoded) {
            const libbase64 = require('libbase64');
            output = new libbase64.Encoder(plan.encoderOptions);
            sink = output;
        } else {
            output = new PassThrough();
            sink = output;
        }

        if (plan.empty) {
            // Nothing to read (offset at or past EOF): an empty stream, never a
            // Range request S3 would answer with 416.
            setImmediate(() => sink.end());
            return output;
        }

        const getParams = {
            Bucket: this.s3Bucket,
            Key: this._s3Key(id),
            Range: rangeHeader(plan.start, plan.end)
        };

        // Deferred so a synchronous throw (bad key, client misconfiguration)
        // surfaces as a stream error too; the API has sent headers by now.
        Promise.resolve().then(() => this.s3.send(new GetObjectCommand(getParams))).then(response => {
            const s3Stream = response.Body;
            s3Stream.once('error', err => output.destroy(err));
            s3Stream.pipe(sink);
        }).catch(err => {
            output.destroy(err);
        });

        return output;
    }

    /**
     * Decrement reference counters for an attachment.
     * Actual deletion happens via deleteOrphaned().
     */
    delete(id, magic, callback) {
        this.filesCollection.findOneAndUpdate(
            { _id: id },
            {
                $inc: { 'metadata.c': -1, 'metadata.m': -(magic || 0) }
            }
        ).then(() => callback(null, true))
         .catch(err => callback(err));
    }

    /**
     * Batch update reference counts for multiple attachments.
     */
    update(ids, count, magic, callback) {
        if (!ids || !ids.length) return callback(null, true);

        this.filesCollection.updateMany(
            { _id: { $in: ids } },
            {
                $inc: { 'metadata.c': count, 'metadata.m': magic }
            }
        ).then(() => callback(null, true))
         .catch(err => callback(err));
    }

    /**
     * Find and remove attachments with zero references that are older than 24 hours.
     * Deletes from both MongoDB (metadata) and S3 (blob).
     */
    deleteOrphaned(callback) {
        const cutoff = new Date(Date.now() - ORPHAN_AGE_MS);

        const cursor = this.filesCollection.find({
            'metadata.c': 0,
            'metadata.m': 0,
            uploadDate: { $lt: cutoff }
        });

        let deleted = 0;
        const deleteNext = () => {
            cursor.next().then(doc => {
                if (!doc) {
                    return cursor.close().then(() => callback(null, deleted));
                }

                // Delete from S3 first, then MongoDB
                const s3Key = this._s3Key(doc._id);
                this.s3.send(new DeleteObjectCommand({
                    Bucket: this.s3Bucket,
                    Key: s3Key
                })).then(() => {
                    return this.filesCollection.deleteOne({ _id: doc._id });
                }).then(() => {
                    deleted++;
                    setImmediate(deleteNext);
                }).catch(err => {
                    // Log but continue — don't block cleanup on individual failures
                    if (typeof this.gridfs?.emit === 'function') {
                        this.gridfs.emit('error', err);
                    }
                    setImmediate(deleteNext);
                });
            }).catch(err => {
                cursor.close().then(() => callback(err));
            });
        };

        deleteNext();
    }

    /**
     * Base64 decode helper — matches gridstore-storage behavior.
     */
    _decodeBase64(body) {
        if (Buffer.isBuffer(body)) {
            body = body.toString();
        }
        // Strip whitespace and decode
        return Buffer.from(body.replace(/\s/g, ''), 'base64');
    }
}

module.exports = S3Storage;
