'use strict';

// Copied from WildDuck (zone-eu/wildduck, lib/attachments/base64-offset.js),
// EUPL-1.1+. One change: the binary end offset also covers the skipped start
// characters (see below); everything else is kept verbatim.


/**
 * Calculates binary start and end offsets for folded base64 offsets.
 *
 * @param {Number} lineLength Line length
 * @param {Number} base64StartOffset Base64 offset to start reading from
 * @param {Number} base64MaxLength Max response length for base64 response
 * @returns {Object} Offsets and padding for binary input and base64 output
 */
const base64Offset = (lineLength, base64StartOffset, base64MaxLength) => {
    // current line
    let base64ExpectedStartLine = Math.floor(base64StartOffset / (lineLength + 2));

    // which byte of current b64 line is the start
    let base64LineOffset = base64StartOffset - base64ExpectedStartLine * (lineLength + 2);

    // b64 line start without line breaks
    let unfoldedBase64LineStart = base64ExpectedStartLine * lineLength;

    // is current b64 start byte \r (1) or \n (2)
    let base64LineEndChar = 0;
    if (base64LineOffset >= lineLength) {
        base64LineEndChar = base64LineOffset + 1 - lineLength;
        base64LineOffset -= base64LineEndChar; // stay with real last byte on current line
    }

    // actual b64 start byte without line breaks
    let unfoldedBase64StartOffset = unfoldedBase64LineStart + base64LineOffset;

    // find start of binary bytes that corresponds to the start of current base64 chunk
    let binaryStartOffset = Math.floor(unfoldedBase64StartOffset / 4) * 3; // in the middle of b64 chunk

    // WE START READING BYTES FROM binaryStartOffset

    // next:
    // 1. how many b64 bytes to skip from output
    // 2. where to put newlines

    let reversedBase64StartOffset = (binaryStartOffset / 3) * 4; // start of b64 chunk that corresponds to binaryStartOffset
    let base64StartDiff = unfoldedBase64StartOffset - reversedBase64StartOffset; // how many b64 bytes to skip (without newlines)

    // base64 chunk might have started on previous line which would give us an extra line break
    let base64RealStartLine = Math.floor(reversedBase64StartOffset / lineLength);

    let extraNewlines = (base64ExpectedStartLine - base64RealStartLine) * 2;
    //base64StartDiff += extraNewlines;

    // base64 (for binaryStartOffset) line offset without newlines
    let startBase64LineOffset = reversedBase64StartOffset - base64RealStartLine * lineLength;

    let binaryEndOffset = 0;
    if (base64MaxLength) {
        // The window must cover the skipped chars of the first base64 group as
        // well as the requested ones, otherwise the last group is short one
        // byte and its final character is wrong. (Upstream uses
        // ceil(maxLength / 4) * 3 + 2, which misses that case.)
        let binaryMaxLength = Math.ceil((base64StartDiff + base64MaxLength) / 4) * 3;
        binaryEndOffset = binaryStartOffset + binaryMaxLength + 3;
    }

    return {
        // line length
        lineLength,

        // start reading binary input from this offset
        binaryStartOffset,
        // end reading binary input at this offset
        binaryEndOffset,

        // padding string for base64 output to get line folding correct
        base64Padding: '#'.repeat(startBase64LineOffset),

        // how many bytes to ignore from the start of base64 output (includes padding)
        base64SkipStartBytes: base64StartDiff + startBase64LineOffset + base64LineEndChar + extraNewlines,

        // how many base64 bytes to return
        base64LimitBytes: base64MaxLength
    };
};

module.exports = base64Offset;
