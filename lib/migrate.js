'use strict';

/**
 * @module migrate
 *
 * Background migration worker: streams attachment blobs from GridFS → S3.
 * Designed to run alongside WildDuck in dual mode without downtime.
 *
 * Usage:
 *   const { migrateAttachments } = require('@tenforwardab/wildduck-s3-storage/lib/migrate');
 *
 *   await migrateAttachments({
 *     mongoUrl: 'mongodb://...',
 *     dbName: 'wildduck',
 *     s3: {
 *       bucket: 'wildduck-attachments',
 *       region: 'auto',
 *       endpoint: 'https://...',
 *       accessKeyId: '...',
 *       secretAccessKey: '...',
 *     },
 *     bucket: 'attachments',      // MongoDB collection prefix
 *     batchSize: 100,             // Docs per batch
 *     concurrency: 10,            // Parallel S3 uploads
 *     onProgress: ({ migrated, skipped, failed, total }) => {},
 *   });
 */

const { MongoClient, GridFSBucket } = require('mongodb');
const { S3Client, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');

/**
 * @typedef {Object} MigrateOptions
 * @property {string} mongoUrl - MongoDB connection string
 * @property {string} [dbName='wildduck'] - Database name
 * @property {Object} s3 - S3 configuration
 * @property {string} [bucket='attachments'] - MongoDB collection prefix
 * @property {string} [s3Prefix=''] - S3 key prefix
 * @property {number} [batchSize=100] - Documents per cursor batch
 * @property {number} [concurrency=10] - Parallel S3 uploads
 * @property {Object} [encryption] - S3 server-side encryption options
 * @property {Function} [onProgress] - Progress callback
 * @property {AbortSignal} [signal] - Abort signal to stop migration
 */

/**
 * Migrate all attachment blobs from GridFS to S3.
 *
 * For each attachment in the MongoDB files collection:
 *   1. Check if already in S3 (HeadObject)
 *   2. If not, stream from GridFS and upload to S3
 *   3. Mark as migrated in metadata
 *
 * Safe to run multiple times — already-migrated files are skipped.
 * Safe to run during live traffic — dual-storage handles reads.
 */
async function migrateAttachments(options) {
    const {
        mongoUrl,
        dbName = 'wildduck',
        s3: s3Config,
        bucket = 'attachments',
        s3Prefix = '',
        batchSize = 100,
        concurrency = 10,
        encryption = null,
        onProgress,
        signal
    } = options;

    const mongo = new MongoClient(mongoUrl);
    await mongo.connect();
    const db = mongo.db(dbName);

    const filesCollection = db.collection(bucket + '.files');
    const gridstore = new GridFSBucket(db, {
        bucketName: bucket,
        chunkSizeBytes: 255 * 1024
    });

    const s3 = new S3Client({
        region: s3Config.region || 'auto',
        endpoint: s3Config.endpoint,
        credentials: s3Config.accessKeyId ? {
            accessKeyId: s3Config.accessKeyId,
            secretAccessKey: s3Config.secretAccessKey
        } : undefined,
        forcePathStyle: s3Config.forcePathStyle !== false
    });

    function s3Key(hash) {
        const prefix = s3Prefix ? s3Prefix + '/' : '';
        return `${prefix}${String(hash).substring(0, 2)}/${hash}`;
    }

    function encryptionParams() {
        if (!encryption) return {};
        const params = {};
        if (encryption.algorithm === 'AES256') {
            params.ServerSideEncryption = 'AES256';
        } else if (encryption.algorithm === 'aws:kms') {
            params.ServerSideEncryption = 'aws:kms';
            if (encryption.keyId) params.SSEKMSKeyId = encryption.keyId;
        }
        return params;
    }

    // Count total for progress
    const total = await filesCollection.countDocuments({
        'metadata.s3Migrated': { $ne: true }
    });

    const stats = { migrated: 0, skipped: 0, failed: 0, total };
    if (onProgress) onProgress({ ...stats });

    const cursor = filesCollection.find({
        'metadata.s3Migrated': { $ne: true }
    }).batchSize(batchSize);

    // Process in batches with controlled concurrency
    let batch = [];

    const processBatch = async (docs) => {
        await Promise.all(docs.map(async (doc) => {
            if (signal && signal.aborted) return;

            const hash = doc._id;
            const key = s3Key(hash);

            try {
                // Check if already in S3
                try {
                    await s3.send(new HeadObjectCommand({
                        Bucket: s3Config.bucket,
                        Key: key
                    }));
                    // Already exists in S3 — just mark and skip
                    await filesCollection.updateOne(
                        { _id: hash },
                        { $set: { 'metadata.s3Migrated': true } }
                    );
                    stats.skipped++;
                    return;
                } catch (headErr) {
                    if (headErr.name !== 'NotFound' && headErr.$metadata?.httpStatusCode !== 404) {
                        throw headErr;
                    }
                }

                // Stream from GridFS
                const body = await new Promise((resolve, reject) => {
                    const stream = gridstore.openDownloadStream(hash);
                    const chunks = [];
                    let length = 0;
                    stream.on('data', chunk => {
                        chunks.push(chunk);
                        length += chunk.length;
                    });
                    stream.on('end', () => resolve(Buffer.concat(chunks, length)));
                    stream.on('error', reject);
                });

                // Upload to S3
                await s3.send(new PutObjectCommand({
                    Bucket: s3Config.bucket,
                    Key: key,
                    Body: body,
                    ContentType: doc.contentType || 'application/octet-stream',
                    ...encryptionParams()
                }));

                // Mark as migrated
                await filesCollection.updateOne(
                    { _id: hash },
                    { $set: { 'metadata.s3Migrated': true } }
                );

                stats.migrated++;
            } catch (err) {
                stats.failed++;
                if (typeof options.onError === 'function') {
                    options.onError({ hash: String(hash), error: err });
                }
            }

            if (onProgress && (stats.migrated + stats.skipped + stats.failed) % 100 === 0) {
                onProgress({ ...stats });
            }
        }));
    };

    while (await cursor.hasNext()) {
        if (signal && signal.aborted) break;

        const doc = await cursor.next();
        if (!doc) break;

        batch.push(doc);

        if (batch.length >= concurrency) {
            await processBatch(batch);
            batch = [];
        }
    }

    // Process remaining
    if (batch.length > 0) {
        await processBatch(batch);
    }

    await cursor.close();
    await mongo.close();

    if (onProgress) onProgress({ ...stats });

    return stats;
}

module.exports = { migrateAttachments };
