'use strict';

// The contract: for any (startFrom, maxLength) a partial read of a *decoded*
// attachment must equal the same slice of the folded base64 that WildDuck's
// GridFS backend (and the original message) would produce.
const test = require('node:test');
const assert = require('node:assert');
const crypto = require('crypto');
const { PassThrough } = require('stream');
const libbase64 = require('libbase64');
const { planRead, rangeHeader } = require('../lib/read-range');

const collect = stream =>
    new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', c => chunks.push(c));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', reject);
    });

const encodeFull = (binary, lineLen) => collect(new PassThrough().end(binary).pipe(new libbase64.Encoder({ lineLength: lineLen })));

// Simulates the backend: read [start, end) of the binary, then encode with the planned options.
async function partialRead(binary, meta, options) {
    const plan = planRead({ length: binary.length, metadata: meta }, options);
    if (plan.empty) {
        return Buffer.alloc(0);
    }
    const window = binary.subarray(plan.start, plan.end);
    if (!plan.decoded) {
        return window;
    }
    return collect(new PassThrough().end(window).pipe(new libbase64.Encoder(plan.encoderOptions)));
}

test('decoded partial reads match slices of the folded base64', async () => {
    for (const lineLen of [76, 60]) {
        const binary = crypto.randomBytes(5000);
        const full = await encodeFull(binary, lineLen);
        const meta = { decoded: true, lineLen };
        // [3, 4], [7, 4] and [11, 8] start three characters into a base64 group with a
        // multiple-of-four length: the case upstream's end offset gets one byte short.
        const cases = [[0, 100], [1, 1], [3, 4], [7, 4], [11, 8], [75, 5], [76, 5], [77, 5], [78, 3], [155, 160], [1000, 2000], [0, full.length], [full.length - 10, 50]];
        for (let i = 0; i < 200; i++) {
            const start = crypto.randomInt(0, full.length);
            cases.push([start, crypto.randomInt(1, 400)]);
        }
        for (const [startFrom, maxLength] of cases) {
            const expected = full.subarray(startFrom, startFrom + maxLength);
            const got = await partialRead(binary, meta, { startFrom, maxLength });
            assert.strictEqual(got.toString(), expected.toString(), `lineLen=${lineLen} start=${startFrom} len=${maxLength}`);
        }
    }
});

test('decoded read without offsets returns the whole folded base64', async () => {
    const binary = crypto.randomBytes(1234);
    const full = await encodeFull(binary, 76);
    const got = await partialRead(binary, { decoded: true, lineLen: 76 }, {});
    assert.strictEqual(got.toString(), full.toString());
});

test('raw (not decoded) partial reads are plain byte windows', async () => {
    const binary = crypto.randomBytes(300);
    assert.deepStrictEqual(await partialRead(binary, { decoded: false }, { startFrom: 10, maxLength: 20 }), binary.subarray(10, 30));
    assert.deepStrictEqual(await partialRead(binary, { decoded: false }, { startFrom: 290, maxLength: 100 }), binary.subarray(290));
    assert.strictEqual((await partialRead(binary, { decoded: false }, { startFrom: 300, maxLength: 5 })).length, 0);
    assert.strictEqual((await partialRead(binary, { decoded: false }, { startFrom: 999 })).length, 0);
});

test('range header is inclusive and absent for empty windows', () => {
    assert.strictEqual(rangeHeader(0, 10), 'bytes=0-9');
    assert.strictEqual(rangeHeader(5, 6), 'bytes=5-5');
    assert.strictEqual(rangeHeader(7, 7), null);
    const plan = planRead({ length: 100, metadata: { decoded: true, lineLen: 76 } }, { startFrom: 5000, maxLength: 10 });
    assert.ok(plan.empty);
});
