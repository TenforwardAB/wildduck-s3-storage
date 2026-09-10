'use strict';

/**
 * @module dual-storage
 *
 * Migration-mode storage backend for WildDuck.
 * Reads from both S3 and GridFS (S3 first), writes only to S3.
 *
 * This allows zero-downtime migration from GridFS → S3:
 *   1. Deploy with mode="dual" → new attachments go to S3
 *   2. Run migration worker to copy existing GridFS blobs to S3
 *   3. Switch to mode="s3" → GridFS no longer used
 *   4. Drop attachments.chunks collection to reclaim MongoDB space
 *
 * Configuration (attachments.toml):
 *   type="s3"
 *   mode="dual"              # "s3" (default) or "dual" (migration)
 *   bucket="attachments"
 *   decodeBase64=true
 *
 *   [s3]
 *   bucket="wildduck-attachments"
 *   endpoint="https://<account>.r2.cloudflarestorage.com"
 *   ...
 */

const S3Storage = require('./s3-storage');
const { GridFSBucket } = require('mongodb');
const { planRead } = require('./read-range');

class DualStorage extends S3Storage {
    constructor(options) {
        super(options);

        // GridFS for reading legacy data
        this.gridstore = new GridFSBucket(this.gridfs, {
            bucketName: this.bucketName,
            chunkSizeBytes: 255 * 1024
        });

        this.chunksCollection = this.gridfs.collection(this.bucketName + '.chunks');
    }

    /**
     * Read stream: try S3 first, fall back to GridFS.
     * Once migration is complete, GridFS reads will naturally stop.
     */
    createReadStream(id, attachmentData, options) {
        const { PassThrough } = require('stream');
        const output = new PassThrough();
        let settled = false;

        // Try S3 first
        const s3Stream = super.createReadStream(id, attachmentData, options);

        s3Stream.once('error', () => {
            if (settled) return;
            settled = true;

            // S3 failed — fall back to GridFS
            try {
                const gridStream = this._createGridFSReadStream(id, attachmentData, options);
                gridStream.pipe(output);
                gridStream.once('error', err => output.destroy(err));
            } catch (err) {
                output.destroy(err);
            }
        });

        s3Stream.once('readable', () => {
            if (settled) return;
            settled = true;
            s3Stream.pipe(output);
        });

        // If S3 stream ends without emitting readable (empty file edge case)
        s3Stream.once('end', () => {
            if (settled) return;
            settled = true;
            output.end();
        });

        return output;
    }

    /**
     * Read from GridFS with the same window/encoder planning as the S3 path,
     * so a not-yet-migrated attachment answers partial fetches identically.
     */
    _createGridFSReadStream(id, attachmentData, options) {
        const plan = planRead(attachmentData, options);
        const stream = this.gridstore.openDownloadStream(id, { start: plan.start, end: plan.end });

        if (plan.decoded) {
            const libbase64 = require('libbase64');
            const encoder = new libbase64.Encoder(plan.encoderOptions);
            stream.once('error', err => encoder.emit('error', err));
            stream.pipe(encoder);
            return encoder;
        }

        return stream;
    }

    /**
     * Delete: clean up from both S3 and GridFS.
     */
    deleteOrphaned(callback) {
        // Run S3 orphan cleanup (which handles the metadata + S3 blob)
        super.deleteOrphaned((err, s3Deleted) => {
            if (err) return callback(err);

            // Also clean up any orphaned GridFS chunks that might remain
            // from attachments that were migrated to S3
            this._cleanupGridFSOrphans((err2, gridDeleted) => {
                callback(err2, (s3Deleted || 0) + (gridDeleted || 0));
            });
        });
    }

    /**
     * Remove GridFS chunks for attachments that no longer have a files entry
     * (because the metadata doc was already handled by S3 storage).
     */
    _cleanupGridFSOrphans(callback) {
        const filesCol = this.filesCollection;
        const chunksCol = this.chunksCollection;

        // Find chunk file IDs that have no corresponding files entry
        chunksCol.distinct('files_id').then(chunkFileIds => {
            if (!chunkFileIds.length) return callback(null, 0);

            // Check which ones still have metadata
            return filesCol.find(
                { _id: { $in: chunkFileIds } },
                { projection: { _id: 1 } }
            ).toArray().then(existingFiles => {
                const existingIds = new Set(existingFiles.map(f => f._id.toString()));
                const orphanIds = chunkFileIds.filter(id => !existingIds.has(id.toString()));

                if (!orphanIds.length) return callback(null, 0);

                return chunksCol.deleteMany({
                    files_id: { $in: orphanIds }
                }).then(result => {
                    callback(null, result.deletedCount || 0);
                });
            });
        }).catch(err => callback(err));
    }
}

module.exports = DualStorage;
