'use strict';

/**
 * WildDuck identifies attachments by the raw SHA-256 digest: a Buffer at
 * runtime, a BSON Binary when read back from MongoDB. The S3 key is the hex
 * form, split on its first two characters to spread the listing.
 */
function hashHex(hash) {
    if (Buffer.isBuffer(hash)) {
        return hash.toString('hex');
    }
    if (hash && typeof hash === 'object') {
        if (Buffer.isBuffer(hash.buffer)) {
            return Buffer.from(hash.buffer).toString('hex');
        }
        if (typeof hash.value === 'function') {
            return Buffer.from(hash.value(true)).toString('hex');
        }
    }
    const str = String(hash);
    if (/^[0-9a-f]{64}$/i.test(str)) {
        return str.toLowerCase();
    }
    throw new TypeError('attachment id is not a SHA-256 digest');
}

function s3Key(prefix, hash) {
    const hex = hashHex(hash);
    return `${prefix ? prefix + '/' : ''}${hex.substring(0, 2)}/${hex}`;
}

module.exports = { hashHex, s3Key };
