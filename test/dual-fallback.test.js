'use strict';

// Dual mode must fall back to GridFS when S3 has no object, without ever
// emitting an empty stream or an error the API layer cannot handle.
const test = require('node:test');
const assert = require('node:assert');
const { Readable } = require('stream');
const Module = require('module');

// Stub mongodb's GridFSBucket and the redis lock before loading the backends.
const stubs = {
    mongodb: { GridFSBucket: class { constructor() {} openDownloadStream(id, opts) { return Readable.from([Buffer.from('grid-' + JSON.stringify(opts))]); } } },
    ioredfour: class { constructor() {} }
};
const origLoad = Module._load;
Module._load = function (request, ...rest) {
    if (stubs[request]) return stubs[request];
    return origLoad.call(this, request, ...rest);
};
const DualStorage = require('../lib/dual-storage');
const S3Storage = require('../lib/s3-storage');

const collect = stream =>
    new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', c => chunks.push(c));
        stream.on('end', () => resolve(Buffer.concat(chunks).toString()));
        stream.on('error', reject);
    });

const makeStorage = (Cls, sendImpl) => {
    const storage = new Cls({ gridfs: { collection: () => ({}) }, redis: {}, options: { bucket: 'attachments', s3: { bucket: 'b', endpoint: 'http://s3.local' } } });
    storage.s3 = { send: sendImpl };
    return storage;
};
const noSuchKey = () => Promise.reject(Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey' }));

test('dual: S3 miss falls back to GridFS for raw attachments', async () => {
    const storage = makeStorage(DualStorage, noSuchKey);
    const out = await collect(storage.createReadStream(Buffer.alloc(32, 1), { length: 10, metadata: { decoded: false } }, { startFrom: 2, maxLength: 4 }));
    assert.strictEqual(out, 'grid-{"start":2,"end":6}');
});

test('dual: S3 miss falls back to GridFS for decoded attachments (re-encoded)', async () => {
    const storage = makeStorage(DualStorage, noSuchKey);
    const out = await collect(storage.createReadStream(Buffer.alloc(32, 1), { length: 10, metadata: { decoded: true, lineLen: 76 } }, {}));
    assert.ok(out.length > 0 && /^[A-Za-z0-9+/=\r\n]+$/.test(out), out);
});

test('s3: a failing GET surfaces as a stream error, never a silent empty body', async () => {
    const storage = makeStorage(S3Storage, noSuchKey);
    await assert.rejects(collect(storage.createReadStream(Buffer.alloc(32, 1), { length: 10, metadata: { decoded: false } }, {})), /NoSuchKey/);
});

test('s3 keys are the hex digest for Buffer, BSON Binary and hex string ids', () => {
    const storage = makeStorage(S3Storage, noSuchKey);
    const buf = Buffer.from('a1b2'.repeat(16), 'hex');
    const hex = buf.toString('hex');
    assert.strictEqual(storage._s3Key(buf), `${hex.slice(0, 2)}/${hex}`);
    assert.strictEqual(storage._s3Key({ buffer: buf, _bsontype: 'Binary' }), `${hex.slice(0, 2)}/${hex}`);
    assert.strictEqual(storage._s3Key(hex), `${hex.slice(0, 2)}/${hex}`);
});
