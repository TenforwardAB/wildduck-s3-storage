'use strict';

const base64Offset = require('./base64-offset');

/**
 * Turns an IMAP partial-fetch request (offsets in the base64 *output*) into a
 * binary read window plus encoder settings, exactly like WildDuck's GridFS
 * backend does. Attachments stored decoded (decodeBase64=true) hold binary in
 * the object store, so the requested base64 slice must be mapped back to the
 * binary bytes that produce it, re-encoded, and trimmed with skip/limit/padding
 * so line folding lands where the client expects.
 *
 * Returns { start, end, encoderOptions, decoded } where [start, end) is the
 * binary window to read (end exclusive, like GridFS) and encoderOptions feed
 * libbase64.Encoder when `decoded` is true.
 */
function planRead(attachmentData, options) {
    options = options || {};
    const meta = (attachmentData && attachmentData.metadata) || {};
    const length = Number(attachmentData && attachmentData.length) || 0;
    const decoded = !!meta.decoded;

    const encoderOptions = { lineLength: meta.lineLen };
    let start = 0;
    let end;

    const partial = options.startFrom !== undefined || options.maxLength !== undefined;
    if (partial && decoded) {
        const o = base64Offset(meta.lineLen, options.startFrom || 0, options.maxLength);
        encoderOptions.skipStartBytes = o.base64SkipStartBytes;
        // libbase64 reads the misspelt key; the intended one is kept for parity with GridFS.
        encoderOptions.limitOutputBytes = o.base64LimitBytes;
        encoderOptions.limitOutbutBytes = o.base64LimitBytes;
        encoderOptions.startPadding = o.base64Padding;
        start = o.binaryStartOffset || 0;
        if (o.binaryEndOffset) {
            end = o.binaryEndOffset;
        }
    } else if (partial) {
        start = options.startFrom || 0;
        if (options.maxLength) {
            end = start + options.maxLength;
        }
    }

    start = Math.min(start, length);
    end = Math.min(end === undefined ? length : end, length);
    if (start >= length) {
        start = end = length;
    } else if (start >= end) {
        start = Math.min(end, length);
    }

    return { start, end, encoderOptions, decoded, empty: start >= end };
}

/** HTTP Range header for a [start, end) window; null when nothing is to be read. */
function rangeHeader(start, end) {
    if (start >= end) {
        return null;
    }
    return `bytes=${start}-${end - 1}`;
}

module.exports = { planRead, rangeHeader };
