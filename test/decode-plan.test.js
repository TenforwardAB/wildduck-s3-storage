'use strict';

const test = require('node:test');
const assert = require('node:assert');
const crypto = require('crypto');
const { PassThrough } = require('stream');
const libbase64 = require('libbase64');
const { analyzeBase64 } = require('../lib/decode-plan');

const fold = (binary, lineLen) =>
    new Promise(resolve => {
        const chunks = [];
        new PassThrough().end(binary).pipe(new libbase64.Encoder({ lineLength: lineLen })).on('data', c => chunks.push(c)).on('end', () => resolve(Buffer.concat(chunks)));
    });
const countLines = body => body.toString().split('\r\n').filter(Boolean).length;

test('regularly folded base64 is stored decoded with its line length', async () => {
    for (const lineLen of [76, 64]) {
        const body = await fold(crypto.randomBytes(3000), lineLen);
        assert.deepStrictEqual(analyzeBase64({ body, transferEncoding: 'base64', lineCount: countLines(body) }), { decoded: true, lineLen });
    }
});

test('irregular or non-base64 bodies stay raw', () => {
    const raw = Buffer.from('AAAA\r\nBB\r\nCCCCCCCC\r\n');
    assert.strictEqual(analyzeBase64({ body: raw, transferEncoding: 'base64', lineCount: 1 }).decoded, false);
    assert.strictEqual(analyzeBase64({ body: Buffer.from('hello world'), transferEncoding: 'base64', lineCount: 1 }).decoded, false);
    assert.strictEqual(analyzeBase64({ body: Buffer.from('QUJD\r\n'), transferEncoding: 'quoted-printable', lineCount: 1 }).decoded, false);
});

test('a single short line counts as 76-column folding', () => {
    const body = Buffer.from('QUJDREVG');
    assert.deepStrictEqual(analyzeBase64({ body, transferEncoding: 'base64', lineCount: 1 }), { decoded: true, lineLen: 76 });
});
