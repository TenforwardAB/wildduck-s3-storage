#!/usr/bin/env node
'use strict';

/**
 * CLI tool to migrate WildDuck attachments from GridFS to S3/R2.
 *
 * Usage:
 *   npx @tenforwardab/wildduck-s3-storage migrate \
 *     --mongo-url mongodb://... \
 *     --s3-bucket wildduck-attachments \
 *     --s3-endpoint https://xxx.r2.cloudflarestorage.com \
 *     --s3-access-key-id ... \
 *     --s3-secret-access-key ... \
 *     --concurrency 20
 *
 * Environment variables are also accepted:
 *   MONGO_URL, S3_BUCKET, S3_ENDPOINT, S3_REGION,
 *   S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_PREFIX
 */

const { migrateAttachments } = require('../lib/migrate');

function parseArgs() {
    const args = process.argv.slice(2);
    const opts = {};

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];
        const next = args[i + 1];

        switch (arg) {
            case '--mongo-url': opts.mongoUrl = next; i++; break;
            case '--db-name': opts.dbName = next; i++; break;
            case '--bucket': opts.bucket = next; i++; break;
            case '--s3-bucket': opts.s3Bucket = next; i++; break;
            case '--s3-endpoint': opts.s3Endpoint = next; i++; break;
            case '--s3-region': opts.s3Region = next; i++; break;
            case '--s3-access-key-id': opts.s3AccessKeyId = next; i++; break;
            case '--s3-secret-access-key': opts.s3SecretAccessKey = next; i++; break;
            case '--s3-prefix': opts.s3Prefix = next; i++; break;
            case '--concurrency': opts.concurrency = parseInt(next, 10); i++; break;
            case '--batch-size': opts.batchSize = parseInt(next, 10); i++; break;
            case '--encryption': opts.encryption = next; i++; break;
            case '--help': case '-h':
                console.log(`
  wildduck-s3-storage migrate

  Migrate WildDuck attachments from MongoDB GridFS to S3/R2.
  Safe to run multiple times. Safe to run during live traffic.

  Options:
    --mongo-url <url>              MongoDB connection string
    --db-name <name>               Database name (default: wildduck)
    --bucket <name>                Collection prefix (default: attachments)
    --s3-bucket <name>             S3 bucket name
    --s3-endpoint <url>            S3 endpoint URL
    --s3-region <region>           S3 region (default: auto)
    --s3-access-key-id <key>       S3 access key
    --s3-secret-access-key <key>   S3 secret key
    --s3-prefix <prefix>           S3 key prefix
    --concurrency <n>              Parallel uploads (default: 10)
    --batch-size <n>               Cursor batch size (default: 100)
    --encryption <AES256|aws:kms>  Server-side encryption

  Environment variables (fallbacks):
    MONGO_URL, S3_BUCKET, S3_ENDPOINT, S3_REGION,
    S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_PREFIX
`);
                process.exit(0);
        }
    }

    return opts;
}

async function main() {
    const opts = parseArgs();

    const mongoUrl = opts.mongoUrl || process.env.MONGO_URL;
    const s3Bucket = opts.s3Bucket || process.env.S3_BUCKET;
    const s3Endpoint = opts.s3Endpoint || process.env.S3_ENDPOINT;
    const s3AccessKeyId = opts.s3AccessKeyId || process.env.S3_ACCESS_KEY_ID;
    const s3SecretAccessKey = opts.s3SecretAccessKey || process.env.S3_SECRET_ACCESS_KEY;

    if (!mongoUrl) {
        console.error('Error: --mongo-url or MONGO_URL is required');
        process.exit(1);
    }
    if (!s3Bucket) {
        console.error('Error: --s3-bucket or S3_BUCKET is required');
        process.exit(1);
    }

    const encryption = opts.encryption ? { algorithm: opts.encryption } : null;

    const controller = new AbortController();
    process.on('SIGINT', () => {
        console.log('\nGracefully stopping migration...');
        controller.abort();
    });
    process.on('SIGTERM', () => controller.abort());

    const startTime = Date.now();
    let lastLog = 0;

    console.log('Starting migration from GridFS to S3...');
    console.log(`  MongoDB: ${mongoUrl.replace(/\/\/[^@]+@/, '//***@')}`);
    console.log(`  S3 bucket: ${s3Bucket}`);
    console.log(`  Endpoint: ${s3Endpoint || '(default)'}`);
    console.log(`  Concurrency: ${opts.concurrency || 10}`);
    console.log('');

    const stats = await migrateAttachments({
        mongoUrl,
        dbName: opts.dbName || 'wildduck',
        s3: {
            bucket: s3Bucket,
            endpoint: s3Endpoint,
            region: opts.s3Region || process.env.S3_REGION || 'auto',
            accessKeyId: s3AccessKeyId,
            secretAccessKey: s3SecretAccessKey,
            forcePathStyle: true
        },
        bucket: opts.bucket || 'attachments',
        s3Prefix: opts.s3Prefix || process.env.S3_PREFIX || '',
        batchSize: opts.batchSize || 100,
        concurrency: opts.concurrency || 10,
        encryption,
        signal: controller.signal,
        onProgress: (progress) => {
            const now = Date.now();
            if (now - lastLog < 2000) return; // Log at most every 2s
            lastLog = now;

            const elapsed = ((now - startTime) / 1000).toFixed(1);
            const done = progress.migrated + progress.skipped + progress.failed;
            const pct = progress.total > 0 ? ((done / progress.total) * 100).toFixed(1) : 0;
            const rate = elapsed > 0 ? (done / (elapsed)).toFixed(0) : 0;

            process.stdout.write(
                `\r  [${pct}%] ${done}/${progress.total} — ` +
                `${progress.migrated} migrated, ${progress.skipped} skipped, ` +
                `${progress.failed} failed — ${rate}/s — ${elapsed}s elapsed`
            );
        },
        onError: ({ hash, error }) => {
            console.error(`\n  Error migrating ${hash}: ${error.message}`);
        }
    });

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n\nMigration complete in ${elapsed}s`);
    console.log(`  Migrated: ${stats.migrated}`);
    console.log(`  Skipped:  ${stats.skipped}`);
    console.log(`  Failed:   ${stats.failed}`);

    if (stats.failed > 0) {
        console.log('\nSome attachments failed to migrate. Re-run to retry.');
        process.exit(1);
    }

    if (!controller.signal.aborted) {
        console.log('\nAll attachments migrated. You can now switch from "dual" to "s3" mode.');
    }
}

main().catch(err => {
    console.error('Migration failed:', err);
    process.exit(1);
});
