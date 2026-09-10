'use strict';

/**
 * Decides whether a base64 attachment can be stored decoded, the same way
 * WildDuck's GridFS backend does: the first line's length must be a plain
 * base64 run terminated by CRLF, and the folded line count must match the
 * body. Only then can partial fetches be re-folded identically on the way out.
 * Returns { decoded, lineLen }.
 */
function analyzeBase64(attachment) {
    const body = attachment.body;
    if (!body || attachment.transferEncoding !== 'base64') {
        return { decoded: false, lineLen: 0 };
    }
    let lineLen = 0;
    let expectBr = false;
    for (let i = 0, len = Math.min(1000, body.length); i < len; i++) {
        const chr = body[i];
        if (expectBr && chr === 0x0a) {
            break;
        } else if (expectBr) {
            lineLen = 0;
            break;
        } else if (
            (chr >= 0x30 && chr <= 0x39) ||
            (chr >= 0x41 && chr <= 0x5a) ||
            (chr >= 0x61 && chr <= 0x7a) ||
            chr === 0x2b ||
            chr === 0x2f ||
            chr === 0x3d
        ) {
            lineLen++;
        } else if (chr === 0x0d) {
            expectBr = true;
        } else {
            lineLen = 0;
            break;
        }
    }
    if (!lineLen || lineLen > 998) {
        return { decoded: false, lineLen: 0 };
    }
    if (body.length === lineLen && lineLen < 76) {
        lineLen = 76;
    }
    const expectedLineCount = Math.ceil(body.length / (lineLen + 2));
    const lineCount = attachment.lineCount;
    if (typeof lineCount === 'number' && lineCount >= expectedLineCount - 1 && lineCount <= expectedLineCount + 1) {
        return { decoded: true, lineLen };
    }
    return { decoded: false, lineLen: 0 };
}

module.exports = { analyzeBase64 };
