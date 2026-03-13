'use strict';

/**
 * @reusable-email/wildduck-s3-storage
 *
 * S3/R2-compatible attachment storage for WildDuck.
 * Drop-in replacement for the built-in GridFS storage.
 *
 * Supports three modes:
 *   - "s3"   — all operations go to S3 (default after migration)
 *   - "dual" — writes to S3, reads from S3 with GridFS fallback (migration mode)
 *   - "gridstore" — original GridFS behavior (passthrough, not used)
 *
 * Usage in WildDuck's attachment-storage.js:
 *
 *   // Replace the storage type switch:
 *   case 's3': {
 *     const mode = (options.options && options.options.mode) || 's3';
 *     if (mode === 'dual') {
 *       const DualStorage = require('@reusable-email/wildduck-s3-storage/lib/dual-storage');
 *       this.storage = new DualStorage(options);
 *     } else {
 *       const S3Storage = require('@reusable-email/wildduck-s3-storage/lib/s3-storage');
 *       this.storage = new S3Storage(options);
 *     }
 *     break;
 *   }
 */

const S3Storage = require('./lib/s3-storage');
const DualStorage = require('./lib/dual-storage');
const { migrateAttachments } = require('./lib/migrate');

module.exports = { S3Storage, DualStorage, migrateAttachments };
