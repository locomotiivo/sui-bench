import { A as Address, b as bcs, s as suiBcs, t as toBase64, f as fromBase64, T as TypeTagSerializer, i as isSerializedBcs, a as toBase58 } from '../_/index.mjs';
import { n as normalizeSuiAddress, i as isValidSuiAddress, a as normalizeSuiObjectId } from '../_/sui-types.mjs';
import { M as MOVE_STDLIB_ADDRESS, S as SUI_FRAMEWORK_ADDRESS, a as SUI_TYPE_ARG } from '../_/constants2.mjs';
import { q as queue, a as serialExecutor, s as suiClient, k as keypair } from '../_/executor.mjs';
import { d as defineLoggedTask } from '../_/logger.mjs';
import '@mysten/sui/client';
import '@mysten/sui/transactions';
import '@mysten/sui/keypairs/ed25519';
import '@mysten/sui/keypairs/secp256k1';
import '@mysten/sui/keypairs/secp256r1';
import '@mysten/sui/cryptography';
import '@opentelemetry/sdk-metrics';
import '@opentelemetry/exporter-prometheus';
import 'pino';
import 'path';
import 'fs';
import '../runtime.mjs';
import 'node:http';
import 'node:https';
import 'node:fs';
import 'node:url';
import 'std-env';

function chunk(array, size) {
  return Array.from({ length: Math.ceil(array.length / size) }, (_, i) => {
    return array.slice(i * size, (i + 1) * size);
  });
}

const SUI_NS_NAME_REGEX = /^(?!.*(^(?!@)|[-.@])($|[-.@]))(?:[a-z0-9-]{0,63}(?:\.[a-z0-9-]{0,63})*)?@[a-z0-9-]{0,63}$/i;
const SUI_NS_DOMAIN_REGEX = /^(?!.*(^|[-.])($|[-.]))(?:[a-z0-9-]{0,63}\.)+sui$/i;
const MAX_SUI_NS_NAME_LENGTH = 235;
function isValidSuiNSName(name) {
  if (name.length > MAX_SUI_NS_NAME_LENGTH) {
    return false;
  }
  if (name.includes("@")) {
    return SUI_NS_NAME_REGEX.test(name);
  }
  return SUI_NS_DOMAIN_REGEX.test(name);
}

const NAME_PATTERN = /^([a-z0-9]+(?:-[a-z0-9]+)*)$/;
const VERSION_REGEX = /^\d+$/;
const MAX_APP_SIZE = 64;
const NAME_SEPARATOR$1 = "/";
const isValidNamedPackage = (name) => {
  const parts = name.split(NAME_SEPARATOR$1);
  if (parts.length < 2 || parts.length > 3) return false;
  const [org, app, version] = parts;
  if (version !== void 0 && !VERSION_REGEX.test(version)) return false;
  if (!isValidSuiNSName(org)) return false;
  return NAME_PATTERN.test(app) && app.length < MAX_APP_SIZE;
};
const isValidNamedType = (type) => {
  const splitType = type.split(/::|<|>|,/);
  for (const t of splitType) {
    if (t.includes(NAME_SEPARATOR$1) && !isValidNamedPackage(t)) return false;
  }
  return true;
};

function pureBcsSchemaFromTypeName(name) {
  switch (name) {
    case "u8":
      return bcs.u8();
    case "u16":
      return bcs.u16();
    case "u32":
      return bcs.u32();
    case "u64":
      return bcs.u64();
    case "u128":
      return bcs.u128();
    case "u256":
      return bcs.u256();
    case "bool":
      return bcs.bool();
    case "string":
      return bcs.string();
    case "id":
    case "address":
      return Address;
  }
  const generic = name.match(/^(vector|option)<(.+)>$/);
  if (generic) {
    const [kind, inner] = generic.slice(1);
    if (kind === "vector") {
      return bcs.vector(pureBcsSchemaFromTypeName(inner));
    } else {
      return bcs.option(pureBcsSchemaFromTypeName(inner));
    }
  }
  throw new Error(`Invalid Pure type name: ${name}`);
}

/**
 * Utilities for hex, bytes, CSPRNG.
 * @module
 */
/*! noble-hashes - MIT License (c) 2022 Paul Miller (paulmillr.com) */
// We use WebCrypto aka globalThis.crypto, which exists in browsers and node.js 16+.
// node.js versions earlier than v19 don't declare it in global scope.
// For node.js, package.json#exports field mapping rewrites import
// from `crypto` to `cryptoNode`, which imports native module.
// Makes the utils un-importable in browsers without a bundler.
// Once node.js 18 is deprecated (2025-04-30), we can just drop the import.
/** Checks if something is Uint8Array. Be careful: nodejs Buffer will return true. */
function isBytes(a) {
    return a instanceof Uint8Array || (ArrayBuffer.isView(a) && a.constructor.name === 'Uint8Array');
}
/** Asserts something is positive integer. */
function anumber(n) {
    if (!Number.isSafeInteger(n) || n < 0)
        throw new Error('positive integer expected, got ' + n);
}
/** Asserts something is Uint8Array. */
function abytes(b, ...lengths) {
    if (!isBytes(b))
        throw new Error('Uint8Array expected');
    if (lengths.length > 0 && !lengths.includes(b.length))
        throw new Error('Uint8Array expected of length ' + lengths + ', got length=' + b.length);
}
/** Asserts a hash instance has not been destroyed / finished */
function aexists(instance, checkFinished = true) {
    if (instance.destroyed)
        throw new Error('Hash instance has been destroyed');
    if (checkFinished && instance.finished)
        throw new Error('Hash#digest() has already been called');
}
/** Asserts output is properly-sized byte array */
function aoutput(out, instance) {
    abytes(out);
    const min = instance.outputLen;
    if (out.length < min) {
        throw new Error('digestInto() expects output buffer of length at least ' + min);
    }
}
/** Cast u8 / u16 / u32 to u32. */
function u32(arr) {
    return new Uint32Array(arr.buffer, arr.byteOffset, Math.floor(arr.byteLength / 4));
}
/** Zeroize a byte array. Warning: JS provides no guarantees. */
function clean(...arrays) {
    for (let i = 0; i < arrays.length; i++) {
        arrays[i].fill(0);
    }
}
/** Is current platform little-endian? Most are. Big-Endian platform: IBM */
const isLE = /* @__PURE__ */ (() => new Uint8Array(new Uint32Array([0x11223344]).buffer)[0] === 0x44)();
/** The byte swap operation for uint32 */
function byteSwap(word) {
    return (((word << 24) & 0xff000000) |
        ((word << 8) & 0xff0000) |
        ((word >>> 8) & 0xff00) |
        ((word >>> 24) & 0xff));
}
/** Conditionally byte swap if on a big-endian platform */
const swap8IfBE = isLE
    ? (n) => n
    : (n) => byteSwap(n);
/** In place byte swap for Uint32Array */
function byteSwap32(arr) {
    for (let i = 0; i < arr.length; i++) {
        arr[i] = byteSwap(arr[i]);
    }
    return arr;
}
const swap32IfBE = isLE
    ? (u) => u
    : byteSwap32;
/**
 * Converts string to bytes using UTF8 encoding.
 * @example utf8ToBytes('abc') // Uint8Array.from([97, 98, 99])
 */
function utf8ToBytes(str) {
    if (typeof str !== 'string')
        throw new Error('string expected');
    return new Uint8Array(new TextEncoder().encode(str)); // https://bugzil.la/1681809
}
/**
 * Normalizes (non-hex) string or Uint8Array to Uint8Array.
 * Warning: when Uint8Array is passed, it would NOT get copied.
 * Keep in mind for future mutable operations.
 */
function toBytes(data) {
    if (typeof data === 'string')
        data = utf8ToBytes(data);
    abytes(data);
    return data;
}
/** For runtime check if class implements interface */
class Hash {
}
function createOptHasher(hashCons) {
    const hashC = (msg, opts) => hashCons(opts).update(toBytes(msg)).digest();
    const tmp = hashCons({});
    hashC.outputLen = tmp.outputLen;
    hashC.blockLen = tmp.blockLen;
    hashC.create = (opts) => hashCons(opts);
    return hashC;
}

/**
 * Internal helpers for blake hash.
 * @module
 */
/**
 * Internal blake variable.
 * For BLAKE2b, the two extra permutations for rounds 10 and 11 are SIGMA[10..11] = SIGMA[0..1].
 */
// prettier-ignore
const BSIGMA = /* @__PURE__ */ Uint8Array.from([
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3,
    11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4,
    7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8,
    9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13,
    2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9,
    12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11,
    13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10,
    6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5,
    10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0,
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3,
    // Blake1, unused in others
    11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4,
    7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8,
    9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13,
    2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9,
]);

/**
 * Internal helpers for u64. BigUint64Array is too slow as per 2025, so we implement it using Uint32Array.
 * @todo re-check https://issues.chromium.org/issues/42212588
 * @module
 */
const U32_MASK64 = /* @__PURE__ */ BigInt(2 ** 32 - 1);
const _32n = /* @__PURE__ */ BigInt(32);
function fromBig(n, le = false) {
    if (le)
        return { h: Number(n & U32_MASK64), l: Number((n >> _32n) & U32_MASK64) };
    return { h: Number((n >> _32n) & U32_MASK64) | 0, l: Number(n & U32_MASK64) | 0 };
}
// Right rotate for Shift in [1, 32)
const rotrSH = (h, l, s) => (h >>> s) | (l << (32 - s));
const rotrSL = (h, l, s) => (h << (32 - s)) | (l >>> s);
// Right rotate for Shift in (32, 64), NOTE: 32 is special case.
const rotrBH = (h, l, s) => (h << (64 - s)) | (l >>> (s - 32));
const rotrBL = (h, l, s) => (h >>> (s - 32)) | (l << (64 - s));
// Right rotate for shift===32 (just swaps l&h)
const rotr32H = (_h, l) => l;
const rotr32L = (h, _l) => h;
// JS uses 32-bit signed integers for bitwise operations which means we cannot
// simple take carry out of low bit sum by shift, we need to use division.
function add(Ah, Al, Bh, Bl) {
    const l = (Al >>> 0) + (Bl >>> 0);
    return { h: (Ah + Bh + ((l / 2 ** 32) | 0)) | 0, l: l | 0 };
}
// Addition with more than 2 elements
const add3L = (Al, Bl, Cl) => (Al >>> 0) + (Bl >>> 0) + (Cl >>> 0);
const add3H = (low, Ah, Bh, Ch) => (Ah + Bh + Ch + ((low / 2 ** 32) | 0)) | 0;

/**
 * blake2b (64-bit) & blake2s (8 to 32-bit) hash functions.
 * b could have been faster, but there is no fast u64 in js, so s is 1.5x faster.
 * @module
 */
// Same as SHA512_IV, but swapped endianness: LE instead of BE. iv[1] is iv[0], etc.
const B2B_IV = /* @__PURE__ */ Uint32Array.from([
    0xf3bcc908, 0x6a09e667, 0x84caa73b, 0xbb67ae85, 0xfe94f82b, 0x3c6ef372, 0x5f1d36f1, 0xa54ff53a,
    0xade682d1, 0x510e527f, 0x2b3e6c1f, 0x9b05688c, 0xfb41bd6b, 0x1f83d9ab, 0x137e2179, 0x5be0cd19,
]);
// Temporary buffer
const BBUF = /* @__PURE__ */ new Uint32Array(32);
// Mixing function G splitted in two halfs
function G1b(a, b, c, d, msg, x) {
    // NOTE: V is LE here
    const Xl = msg[x], Xh = msg[x + 1]; // prettier-ignore
    let Al = BBUF[2 * a], Ah = BBUF[2 * a + 1]; // prettier-ignore
    let Bl = BBUF[2 * b], Bh = BBUF[2 * b + 1]; // prettier-ignore
    let Cl = BBUF[2 * c], Ch = BBUF[2 * c + 1]; // prettier-ignore
    let Dl = BBUF[2 * d], Dh = BBUF[2 * d + 1]; // prettier-ignore
    // v[a] = (v[a] + v[b] + x) | 0;
    let ll = add3L(Al, Bl, Xl);
    Ah = add3H(ll, Ah, Bh, Xh);
    Al = ll | 0;
    // v[d] = rotr(v[d] ^ v[a], 32)
    ({ Dh, Dl } = { Dh: Dh ^ Ah, Dl: Dl ^ Al });
    ({ Dh, Dl } = { Dh: rotr32H(Dh, Dl), Dl: rotr32L(Dh) });
    // v[c] = (v[c] + v[d]) | 0;
    ({ h: Ch, l: Cl } = add(Ch, Cl, Dh, Dl));
    // v[b] = rotr(v[b] ^ v[c], 24)
    ({ Bh, Bl } = { Bh: Bh ^ Ch, Bl: Bl ^ Cl });
    ({ Bh, Bl } = { Bh: rotrSH(Bh, Bl, 24), Bl: rotrSL(Bh, Bl, 24) });
    (BBUF[2 * a] = Al), (BBUF[2 * a + 1] = Ah);
    (BBUF[2 * b] = Bl), (BBUF[2 * b + 1] = Bh);
    (BBUF[2 * c] = Cl), (BBUF[2 * c + 1] = Ch);
    (BBUF[2 * d] = Dl), (BBUF[2 * d + 1] = Dh);
}
function G2b(a, b, c, d, msg, x) {
    // NOTE: V is LE here
    const Xl = msg[x], Xh = msg[x + 1]; // prettier-ignore
    let Al = BBUF[2 * a], Ah = BBUF[2 * a + 1]; // prettier-ignore
    let Bl = BBUF[2 * b], Bh = BBUF[2 * b + 1]; // prettier-ignore
    let Cl = BBUF[2 * c], Ch = BBUF[2 * c + 1]; // prettier-ignore
    let Dl = BBUF[2 * d], Dh = BBUF[2 * d + 1]; // prettier-ignore
    // v[a] = (v[a] + v[b] + x) | 0;
    let ll = add3L(Al, Bl, Xl);
    Ah = add3H(ll, Ah, Bh, Xh);
    Al = ll | 0;
    // v[d] = rotr(v[d] ^ v[a], 16)
    ({ Dh, Dl } = { Dh: Dh ^ Ah, Dl: Dl ^ Al });
    ({ Dh, Dl } = { Dh: rotrSH(Dh, Dl, 16), Dl: rotrSL(Dh, Dl, 16) });
    // v[c] = (v[c] + v[d]) | 0;
    ({ h: Ch, l: Cl } = add(Ch, Cl, Dh, Dl));
    // v[b] = rotr(v[b] ^ v[c], 63)
    ({ Bh, Bl } = { Bh: Bh ^ Ch, Bl: Bl ^ Cl });
    ({ Bh, Bl } = { Bh: rotrBH(Bh, Bl, 63), Bl: rotrBL(Bh, Bl, 63) });
    (BBUF[2 * a] = Al), (BBUF[2 * a + 1] = Ah);
    (BBUF[2 * b] = Bl), (BBUF[2 * b + 1] = Bh);
    (BBUF[2 * c] = Cl), (BBUF[2 * c + 1] = Ch);
    (BBUF[2 * d] = Dl), (BBUF[2 * d + 1] = Dh);
}
function checkBlake2Opts(outputLen, opts = {}, keyLen, saltLen, persLen) {
    anumber(keyLen);
    if (outputLen < 0 || outputLen > keyLen)
        throw new Error('outputLen bigger than keyLen');
    const { key, salt, personalization } = opts;
    if (key !== undefined && (key.length < 1 || key.length > keyLen))
        throw new Error('key length must be undefined or 1..' + keyLen);
    if (salt !== undefined && salt.length !== saltLen)
        throw new Error('salt must be undefined or ' + saltLen);
    if (personalization !== undefined && personalization.length !== persLen)
        throw new Error('personalization must be undefined or ' + persLen);
}
/** Class, from which others are subclassed. */
class BLAKE2 extends Hash {
    constructor(blockLen, outputLen) {
        super();
        this.finished = false;
        this.destroyed = false;
        this.length = 0;
        this.pos = 0;
        anumber(blockLen);
        anumber(outputLen);
        this.blockLen = blockLen;
        this.outputLen = outputLen;
        this.buffer = new Uint8Array(blockLen);
        this.buffer32 = u32(this.buffer);
    }
    update(data) {
        aexists(this);
        data = toBytes(data);
        abytes(data);
        // Main difference with other hashes: there is flag for last block,
        // so we cannot process current block before we know that there
        // is the next one. This significantly complicates logic and reduces ability
        // to do zero-copy processing
        const { blockLen, buffer, buffer32 } = this;
        const len = data.length;
        const offset = data.byteOffset;
        const buf = data.buffer;
        for (let pos = 0; pos < len;) {
            // If buffer is full and we still have input (don't process last block, same as blake2s)
            if (this.pos === blockLen) {
                swap32IfBE(buffer32);
                this.compress(buffer32, 0, false);
                swap32IfBE(buffer32);
                this.pos = 0;
            }
            const take = Math.min(blockLen - this.pos, len - pos);
            const dataOffset = offset + pos;
            // full block && aligned to 4 bytes && not last in input
            if (take === blockLen && !(dataOffset % 4) && pos + take < len) {
                const data32 = new Uint32Array(buf, dataOffset, Math.floor((len - pos) / 4));
                swap32IfBE(data32);
                for (let pos32 = 0; pos + blockLen < len; pos32 += buffer32.length, pos += blockLen) {
                    this.length += blockLen;
                    this.compress(data32, pos32, false);
                }
                swap32IfBE(data32);
                continue;
            }
            buffer.set(data.subarray(pos, pos + take), this.pos);
            this.pos += take;
            this.length += take;
            pos += take;
        }
        return this;
    }
    digestInto(out) {
        aexists(this);
        aoutput(out, this);
        const { pos, buffer32 } = this;
        this.finished = true;
        // Padding
        clean(this.buffer.subarray(pos));
        swap32IfBE(buffer32);
        this.compress(buffer32, 0, true);
        swap32IfBE(buffer32);
        const out32 = u32(out);
        this.get().forEach((v, i) => (out32[i] = swap8IfBE(v)));
    }
    digest() {
        const { buffer, outputLen } = this;
        this.digestInto(buffer);
        const res = buffer.slice(0, outputLen);
        this.destroy();
        return res;
    }
    _cloneInto(to) {
        const { buffer, length, finished, destroyed, outputLen, pos } = this;
        to || (to = new this.constructor({ dkLen: outputLen }));
        to.set(...this.get());
        to.buffer.set(buffer);
        to.destroyed = destroyed;
        to.finished = finished;
        to.length = length;
        to.pos = pos;
        // @ts-ignore
        to.outputLen = outputLen;
        return to;
    }
    clone() {
        return this._cloneInto();
    }
}
class BLAKE2b extends BLAKE2 {
    constructor(opts = {}) {
        const olen = opts.dkLen === undefined ? 64 : opts.dkLen;
        super(128, olen);
        // Same as SHA-512, but LE
        this.v0l = B2B_IV[0] | 0;
        this.v0h = B2B_IV[1] | 0;
        this.v1l = B2B_IV[2] | 0;
        this.v1h = B2B_IV[3] | 0;
        this.v2l = B2B_IV[4] | 0;
        this.v2h = B2B_IV[5] | 0;
        this.v3l = B2B_IV[6] | 0;
        this.v3h = B2B_IV[7] | 0;
        this.v4l = B2B_IV[8] | 0;
        this.v4h = B2B_IV[9] | 0;
        this.v5l = B2B_IV[10] | 0;
        this.v5h = B2B_IV[11] | 0;
        this.v6l = B2B_IV[12] | 0;
        this.v6h = B2B_IV[13] | 0;
        this.v7l = B2B_IV[14] | 0;
        this.v7h = B2B_IV[15] | 0;
        checkBlake2Opts(olen, opts, 64, 16, 16);
        let { key, personalization, salt } = opts;
        let keyLength = 0;
        if (key !== undefined) {
            key = toBytes(key);
            keyLength = key.length;
        }
        this.v0l ^= this.outputLen | (keyLength << 8) | (0x01 << 16) | (0x01 << 24);
        if (salt !== undefined) {
            salt = toBytes(salt);
            const slt = u32(salt);
            this.v4l ^= swap8IfBE(slt[0]);
            this.v4h ^= swap8IfBE(slt[1]);
            this.v5l ^= swap8IfBE(slt[2]);
            this.v5h ^= swap8IfBE(slt[3]);
        }
        if (personalization !== undefined) {
            personalization = toBytes(personalization);
            const pers = u32(personalization);
            this.v6l ^= swap8IfBE(pers[0]);
            this.v6h ^= swap8IfBE(pers[1]);
            this.v7l ^= swap8IfBE(pers[2]);
            this.v7h ^= swap8IfBE(pers[3]);
        }
        if (key !== undefined) {
            // Pad to blockLen and update
            const tmp = new Uint8Array(this.blockLen);
            tmp.set(key);
            this.update(tmp);
        }
    }
    // prettier-ignore
    get() {
        let { v0l, v0h, v1l, v1h, v2l, v2h, v3l, v3h, v4l, v4h, v5l, v5h, v6l, v6h, v7l, v7h } = this;
        return [v0l, v0h, v1l, v1h, v2l, v2h, v3l, v3h, v4l, v4h, v5l, v5h, v6l, v6h, v7l, v7h];
    }
    // prettier-ignore
    set(v0l, v0h, v1l, v1h, v2l, v2h, v3l, v3h, v4l, v4h, v5l, v5h, v6l, v6h, v7l, v7h) {
        this.v0l = v0l | 0;
        this.v0h = v0h | 0;
        this.v1l = v1l | 0;
        this.v1h = v1h | 0;
        this.v2l = v2l | 0;
        this.v2h = v2h | 0;
        this.v3l = v3l | 0;
        this.v3h = v3h | 0;
        this.v4l = v4l | 0;
        this.v4h = v4h | 0;
        this.v5l = v5l | 0;
        this.v5h = v5h | 0;
        this.v6l = v6l | 0;
        this.v6h = v6h | 0;
        this.v7l = v7l | 0;
        this.v7h = v7h | 0;
    }
    compress(msg, offset, isLast) {
        this.get().forEach((v, i) => (BBUF[i] = v)); // First half from state.
        BBUF.set(B2B_IV, 16); // Second half from IV.
        let { h, l } = fromBig(BigInt(this.length));
        BBUF[24] = B2B_IV[8] ^ l; // Low word of the offset.
        BBUF[25] = B2B_IV[9] ^ h; // High word.
        // Invert all bits for last block
        if (isLast) {
            BBUF[28] = ~BBUF[28];
            BBUF[29] = ~BBUF[29];
        }
        let j = 0;
        const s = BSIGMA;
        for (let i = 0; i < 12; i++) {
            G1b(0, 4, 8, 12, msg, offset + 2 * s[j++]);
            G2b(0, 4, 8, 12, msg, offset + 2 * s[j++]);
            G1b(1, 5, 9, 13, msg, offset + 2 * s[j++]);
            G2b(1, 5, 9, 13, msg, offset + 2 * s[j++]);
            G1b(2, 6, 10, 14, msg, offset + 2 * s[j++]);
            G2b(2, 6, 10, 14, msg, offset + 2 * s[j++]);
            G1b(3, 7, 11, 15, msg, offset + 2 * s[j++]);
            G2b(3, 7, 11, 15, msg, offset + 2 * s[j++]);
            G1b(0, 5, 10, 15, msg, offset + 2 * s[j++]);
            G2b(0, 5, 10, 15, msg, offset + 2 * s[j++]);
            G1b(1, 6, 11, 12, msg, offset + 2 * s[j++]);
            G2b(1, 6, 11, 12, msg, offset + 2 * s[j++]);
            G1b(2, 7, 8, 13, msg, offset + 2 * s[j++]);
            G2b(2, 7, 8, 13, msg, offset + 2 * s[j++]);
            G1b(3, 4, 9, 14, msg, offset + 2 * s[j++]);
            G2b(3, 4, 9, 14, msg, offset + 2 * s[j++]);
        }
        this.v0l ^= BBUF[0] ^ BBUF[16];
        this.v0h ^= BBUF[1] ^ BBUF[17];
        this.v1l ^= BBUF[2] ^ BBUF[18];
        this.v1h ^= BBUF[3] ^ BBUF[19];
        this.v2l ^= BBUF[4] ^ BBUF[20];
        this.v2h ^= BBUF[5] ^ BBUF[21];
        this.v3l ^= BBUF[6] ^ BBUF[22];
        this.v3h ^= BBUF[7] ^ BBUF[23];
        this.v4l ^= BBUF[8] ^ BBUF[24];
        this.v4h ^= BBUF[9] ^ BBUF[25];
        this.v5l ^= BBUF[10] ^ BBUF[26];
        this.v5h ^= BBUF[11] ^ BBUF[27];
        this.v6l ^= BBUF[12] ^ BBUF[28];
        this.v6h ^= BBUF[13] ^ BBUF[29];
        this.v7l ^= BBUF[14] ^ BBUF[30];
        this.v7h ^= BBUF[15] ^ BBUF[31];
        clean(BBUF);
    }
    destroy() {
        this.destroyed = true;
        clean(this.buffer32);
        this.set(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }
}
/**
 * Blake2b hash function. 64-bit. 1.5x slower than blake2s in JS.
 * @param msg - message that would be hashed
 * @param opts - dkLen output length, key for MAC mode, salt, personalization
 */
const blake2b$1 = /* @__PURE__ */ createOptHasher((opts) => new BLAKE2b(opts));

/**
 * Blake2b hash function. Focuses on 64-bit platforms, but in JS speed different from Blake2s is negligible.
 * @module
 * @deprecated
 */
/** @deprecated Use import from `noble/hashes/blake2` module */
const blake2b = blake2b$1;

const OBJECT_MODULE_NAME = "object";
const ID_STRUCT_NAME = "ID";
const STD_ASCII_MODULE_NAME = "ascii";
const STD_ASCII_STRUCT_NAME = "String";
const STD_UTF8_MODULE_NAME = "string";
const STD_UTF8_STRUCT_NAME = "String";
const STD_OPTION_MODULE_NAME = "option";
const STD_OPTION_STRUCT_NAME = "Option";
function isTxContext(param) {
  const struct = typeof param.body === "object" && "datatype" in param.body ? param.body.datatype : null;
  return !!struct && normalizeSuiAddress(struct.package) === normalizeSuiAddress("0x2") && struct.module === "tx_context" && struct.type === "TxContext";
}
function getPureBcsSchema(typeSignature) {
  if (typeof typeSignature === "string") {
    switch (typeSignature) {
      case "address":
        return suiBcs.Address;
      case "bool":
        return suiBcs.Bool;
      case "u8":
        return suiBcs.U8;
      case "u16":
        return suiBcs.U16;
      case "u32":
        return suiBcs.U32;
      case "u64":
        return suiBcs.U64;
      case "u128":
        return suiBcs.U128;
      case "u256":
        return suiBcs.U256;
      default:
        throw new Error(`Unknown type signature ${typeSignature}`);
    }
  }
  if ("vector" in typeSignature) {
    if (typeSignature.vector === "u8") {
      return suiBcs.byteVector().transform({
        input: (val) => typeof val === "string" ? new TextEncoder().encode(val) : val,
        output: (val) => val
      });
    }
    const type = getPureBcsSchema(typeSignature.vector);
    return type ? suiBcs.vector(type) : null;
  }
  if ("datatype" in typeSignature) {
    const pkg = normalizeSuiAddress(typeSignature.datatype.package);
    if (pkg === normalizeSuiAddress(MOVE_STDLIB_ADDRESS)) {
      if (typeSignature.datatype.module === STD_ASCII_MODULE_NAME && typeSignature.datatype.type === STD_ASCII_STRUCT_NAME) {
        return suiBcs.String;
      }
      if (typeSignature.datatype.module === STD_UTF8_MODULE_NAME && typeSignature.datatype.type === STD_UTF8_STRUCT_NAME) {
        return suiBcs.String;
      }
      if (typeSignature.datatype.module === STD_OPTION_MODULE_NAME && typeSignature.datatype.type === STD_OPTION_STRUCT_NAME) {
        const type = getPureBcsSchema(typeSignature.datatype.typeParameters[0]);
        return type ? suiBcs.vector(type) : null;
      }
    }
    if (pkg === normalizeSuiAddress(SUI_FRAMEWORK_ADDRESS) && typeSignature.datatype.module === OBJECT_MODULE_NAME && typeSignature.datatype.type === ID_STRUCT_NAME) {
      return suiBcs.Address;
    }
  }
  return null;
}
function normalizedTypeToMoveTypeSignature(type) {
  if (typeof type === "object" && "Reference" in type) {
    return {
      ref: "&",
      body: normalizedTypeToMoveTypeSignatureBody(type.Reference)
    };
  }
  if (typeof type === "object" && "MutableReference" in type) {
    return {
      ref: "&mut",
      body: normalizedTypeToMoveTypeSignatureBody(type.MutableReference)
    };
  }
  return {
    ref: null,
    body: normalizedTypeToMoveTypeSignatureBody(type)
  };
}
function normalizedTypeToMoveTypeSignatureBody(type) {
  if (typeof type === "string") {
    switch (type) {
      case "Address":
        return "address";
      case "Bool":
        return "bool";
      case "U8":
        return "u8";
      case "U16":
        return "u16";
      case "U32":
        return "u32";
      case "U64":
        return "u64";
      case "U128":
        return "u128";
      case "U256":
        return "u256";
      default:
        throw new Error(`Unexpected type ${type}`);
    }
  }
  if ("Vector" in type) {
    return { vector: normalizedTypeToMoveTypeSignatureBody(type.Vector) };
  }
  if ("Struct" in type) {
    return {
      datatype: {
        package: type.Struct.address,
        module: type.Struct.module,
        type: type.Struct.name,
        typeParameters: type.Struct.typeArguments.map(normalizedTypeToMoveTypeSignatureBody)
      }
    };
  }
  if ("TypeParameter" in type) {
    return { typeParameter: type.TypeParameter };
  }
  throw new Error(`Unexpected type ${JSON.stringify(type)}`);
}

function Pure(data) {
  return {
    $kind: "Pure",
    Pure: {
      bytes: data instanceof Uint8Array ? toBase64(data) : data.toBase64()
    }
  };
}
const Inputs = {
  Pure,
  ObjectRef({ objectId, digest, version }) {
    return {
      $kind: "Object",
      Object: {
        $kind: "ImmOrOwnedObject",
        ImmOrOwnedObject: {
          digest,
          version,
          objectId: normalizeSuiAddress(objectId)
        }
      }
    };
  },
  SharedObjectRef({
    objectId,
    mutable,
    initialSharedVersion
  }) {
    return {
      $kind: "Object",
      Object: {
        $kind: "SharedObject",
        SharedObject: {
          mutable,
          initialSharedVersion,
          objectId: normalizeSuiAddress(objectId)
        }
      }
    };
  },
  ReceivingRef({ objectId, digest, version }) {
    return {
      $kind: "Object",
      Object: {
        $kind: "Receiving",
        Receiving: {
          digest,
          version,
          objectId: normalizeSuiAddress(objectId)
        }
      }
    };
  }
};

//#region src/storages/globalConfig/globalConfig.ts
let store$4;
/**
* Returns the global configuration.
*
* @param config The config to merge.
*
* @returns The configuration.
*/
/* @__NO_SIDE_EFFECTS__ */
function getGlobalConfig(config$1) {
	return {
		lang: config$1?.lang ?? store$4?.lang,
		message: config$1?.message,
		abortEarly: config$1?.abortEarly ?? store$4?.abortEarly,
		abortPipeEarly: config$1?.abortPipeEarly ?? store$4?.abortPipeEarly
	};
}

//#endregion
//#region src/storages/globalMessage/globalMessage.ts
let store$3;
/**
* Returns a global error message.
*
* @param lang The language of the message.
*
* @returns The error message.
*/
/* @__NO_SIDE_EFFECTS__ */
function getGlobalMessage(lang) {
	return store$3?.get(lang);
}

//#endregion
//#region src/storages/schemaMessage/schemaMessage.ts
let store$2;
/**
* Returns a schema error message.
*
* @param lang The language of the message.
*
* @returns The error message.
*/
/* @__NO_SIDE_EFFECTS__ */
function getSchemaMessage(lang) {
	return store$2?.get(lang);
}

//#endregion
//#region src/storages/specificMessage/specificMessage.ts
let store$1;
/**
* Returns a specific error message.
*
* @param reference The identifier reference.
* @param lang The language of the message.
*
* @returns The error message.
*/
/* @__NO_SIDE_EFFECTS__ */
function getSpecificMessage(reference, lang) {
	return store$1?.get(reference)?.get(lang);
}

//#endregion
//#region src/utils/_stringify/_stringify.ts
/**
* Stringifies an unknown input to a literal or type string.
*
* @param input The unknown input.
*
* @returns A literal or type string.
*
* @internal
*/
/* @__NO_SIDE_EFFECTS__ */
function _stringify(input) {
	const type = typeof input;
	if (type === "string") return `"${input}"`;
	if (type === "number" || type === "bigint" || type === "boolean") return `${input}`;
	if (type === "object" || type === "function") return (input && Object.getPrototypeOf(input)?.constructor?.name) ?? "null";
	return type;
}

//#endregion
//#region src/utils/_addIssue/_addIssue.ts
/**
* Adds an issue to the dataset.
*
* @param context The issue context.
* @param label The issue label.
* @param dataset The input dataset.
* @param config The configuration.
* @param other The optional props.
*
* @internal
*/
function _addIssue(context, label, dataset, config$1, other) {
	const input = other && "input" in other ? other.input : dataset.value;
	const expected = other?.expected ?? context.expects ?? null;
	const received = other?.received ?? /* @__PURE__ */ _stringify(input);
	const issue = {
		kind: context.kind,
		type: context.type,
		input,
		expected,
		received,
		message: `Invalid ${label}: ${expected ? `Expected ${expected} but r` : "R"}eceived ${received}`,
		requirement: context.requirement,
		path: other?.path,
		issues: other?.issues,
		lang: config$1.lang,
		abortEarly: config$1.abortEarly,
		abortPipeEarly: config$1.abortPipeEarly
	};
	const isSchema = context.kind === "schema";
	const message$1 = other?.message ?? context.message ?? /* @__PURE__ */ getSpecificMessage(context.reference, issue.lang) ?? (isSchema ? /* @__PURE__ */ getSchemaMessage(issue.lang) : null) ?? config$1.message ?? /* @__PURE__ */ getGlobalMessage(issue.lang);
	if (message$1 !== void 0) issue.message = typeof message$1 === "function" ? message$1(issue) : message$1;
	if (isSchema) dataset.typed = false;
	if (dataset.issues) dataset.issues.push(issue);
	else dataset.issues = [issue];
}

//#endregion
//#region src/utils/_getStandardProps/_getStandardProps.ts
/**
* Returns the Standard Schema properties.
*
* @param context The schema context.
*
* @returns The Standard Schema properties.
*/
/* @__NO_SIDE_EFFECTS__ */
function _getStandardProps(context) {
	return {
		version: 1,
		vendor: "valibot",
		validate(value$1) {
			return context["~run"]({ value: value$1 }, /* @__PURE__ */ getGlobalConfig());
		}
	};
}

//#endregion
//#region src/utils/_isValidObjectKey/_isValidObjectKey.ts
/**
* Disallows inherited object properties and prevents object prototype
* pollution by disallowing certain keys.
*
* @param object The object to check.
* @param key The key to check.
*
* @returns Whether the key is allowed.
*
* @internal
*/
/* @__NO_SIDE_EFFECTS__ */
function _isValidObjectKey(object$1, key) {
	return Object.hasOwn(object$1, key) && key !== "__proto__" && key !== "prototype" && key !== "constructor";
}

//#endregion
//#region src/utils/_joinExpects/_joinExpects.ts
/**
* Joins multiple `expects` values with the given separator.
*
* @param values The `expects` values.
* @param separator The separator.
*
* @returns The joined `expects` property.
*
* @internal
*/
/* @__NO_SIDE_EFFECTS__ */
function _joinExpects(values$1, separator) {
	const list = [...new Set(values$1)];
	if (list.length > 1) return `(${list.join(` ${separator} `)})`;
	return list[0] ?? "never";
}

//#endregion
//#region src/utils/ValiError/ValiError.ts
/**
* A Valibot error with useful information.
*/
var ValiError = class extends Error {
	/**
	* Creates a Valibot error with useful information.
	*
	* @param issues The error issues.
	*/
	constructor(issues) {
		super(issues[0].message);
		this.name = "ValiError";
		this.issues = issues;
	}
};

//#endregion
//#region src/actions/check/check.ts
/* @__NO_SIDE_EFFECTS__ */
function check(requirement, message$1) {
	return {
		kind: "validation",
		type: "check",
		reference: check,
		async: false,
		expects: null,
		requirement,
		message: message$1,
		"~run"(dataset, config$1) {
			if (dataset.typed && !this.requirement(dataset.value)) _addIssue(this, "input", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/actions/integer/integer.ts
/* @__NO_SIDE_EFFECTS__ */
function integer(message$1) {
	return {
		kind: "validation",
		type: "integer",
		reference: integer,
		async: false,
		expects: null,
		requirement: Number.isInteger,
		message: message$1,
		"~run"(dataset, config$1) {
			if (dataset.typed && !this.requirement(dataset.value)) _addIssue(this, "integer", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/actions/transform/transform.ts
/**
* Creates a custom transformation action.
*
* @param operation The transformation operation.
*
* @returns A transform action.
*/
/* @__NO_SIDE_EFFECTS__ */
function transform(operation) {
	return {
		kind: "transformation",
		type: "transform",
		reference: transform,
		async: false,
		operation,
		"~run"(dataset) {
			dataset.value = this.operation(dataset.value);
			return dataset;
		}
	};
}

//#endregion
//#region src/methods/getFallback/getFallback.ts
/**
* Returns the fallback value of the schema.
*
* @param schema The schema to get it from.
* @param dataset The output dataset if available.
* @param config The config if available.
*
* @returns The fallback value.
*/
/* @__NO_SIDE_EFFECTS__ */
function getFallback(schema, dataset, config$1) {
	return typeof schema.fallback === "function" ? schema.fallback(dataset, config$1) : schema.fallback;
}

//#endregion
//#region src/methods/getDefault/getDefault.ts
/**
* Returns the default value of the schema.
*
* @param schema The schema to get it from.
* @param dataset The input dataset if available.
* @param config The config if available.
*
* @returns The default value.
*/
/* @__NO_SIDE_EFFECTS__ */
function getDefault(schema, dataset, config$1) {
	return typeof schema.default === "function" ? schema.default(dataset, config$1) : schema.default;
}

//#endregion
//#region src/methods/is/is.ts
/**
* Checks if the input matches the schema. By using a type predicate, this
* function can be used as a type guard.
*
* @param schema The schema to be used.
* @param input The input to be tested.
*
* @returns Whether the input matches the schema.
*/
/* @__NO_SIDE_EFFECTS__ */
function is(schema, input) {
	return !schema["~run"]({ value: input }, { abortEarly: true }).issues;
}

//#endregion
//#region src/schemas/array/array.ts
/* @__NO_SIDE_EFFECTS__ */
function array(item, message$1) {
	return {
		kind: "schema",
		type: "array",
		reference: array,
		expects: "Array",
		async: false,
		item,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			const input = dataset.value;
			if (Array.isArray(input)) {
				dataset.typed = true;
				dataset.value = [];
				for (let key = 0; key < input.length; key++) {
					const value$1 = input[key];
					const itemDataset = this.item["~run"]({ value: value$1 }, config$1);
					if (itemDataset.issues) {
						const pathItem = {
							type: "array",
							origin: "value",
							input,
							key,
							value: value$1
						};
						for (const issue of itemDataset.issues) {
							if (issue.path) issue.path.unshift(pathItem);
							else issue.path = [pathItem];
							dataset.issues?.push(issue);
						}
						if (!dataset.issues) dataset.issues = itemDataset.issues;
						if (config$1.abortEarly) {
							dataset.typed = false;
							break;
						}
					}
					if (!itemDataset.typed) dataset.typed = false;
					dataset.value.push(itemDataset.value);
				}
			} else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/bigint/bigint.ts
/* @__NO_SIDE_EFFECTS__ */
function bigint(message$1) {
	return {
		kind: "schema",
		type: "bigint",
		reference: bigint,
		expects: "bigint",
		async: false,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (typeof dataset.value === "bigint") dataset.typed = true;
			else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/boolean/boolean.ts
/* @__NO_SIDE_EFFECTS__ */
function boolean(message$1) {
	return {
		kind: "schema",
		type: "boolean",
		reference: boolean,
		expects: "boolean",
		async: false,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (typeof dataset.value === "boolean") dataset.typed = true;
			else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/lazy/lazy.ts
/**
* Creates a lazy schema.
*
* @param getter The schema getter.
*
* @returns A lazy schema.
*/
/* @__NO_SIDE_EFFECTS__ */
function lazy(getter) {
	return {
		kind: "schema",
		type: "lazy",
		reference: lazy,
		expects: "unknown",
		async: false,
		getter,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			return this.getter(dataset.value)["~run"](dataset, config$1);
		}
	};
}

//#endregion
//#region src/schemas/literal/literal.ts
/* @__NO_SIDE_EFFECTS__ */
function literal(literal_, message$1) {
	return {
		kind: "schema",
		type: "literal",
		reference: literal,
		expects: /* @__PURE__ */ _stringify(literal_),
		async: false,
		literal: literal_,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (dataset.value === this.literal) dataset.typed = true;
			else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/nullable/nullable.ts
/* @__NO_SIDE_EFFECTS__ */
function nullable(wrapped, default_) {
	return {
		kind: "schema",
		type: "nullable",
		reference: nullable,
		expects: `(${wrapped.expects} | null)`,
		async: false,
		wrapped,
		default: default_,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (dataset.value === null) {
				if (this.default !== void 0) dataset.value = /* @__PURE__ */ getDefault(this, dataset, config$1);
				if (dataset.value === null) {
					dataset.typed = true;
					return dataset;
				}
			}
			return this.wrapped["~run"](dataset, config$1);
		}
	};
}

//#endregion
//#region src/schemas/nullish/nullish.ts
/* @__NO_SIDE_EFFECTS__ */
function nullish(wrapped, default_) {
	return {
		kind: "schema",
		type: "nullish",
		reference: nullish,
		expects: `(${wrapped.expects} | null | undefined)`,
		async: false,
		wrapped,
		default: default_,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (dataset.value === null || dataset.value === void 0) {
				if (this.default !== void 0) dataset.value = /* @__PURE__ */ getDefault(this, dataset, config$1);
				if (dataset.value === null || dataset.value === void 0) {
					dataset.typed = true;
					return dataset;
				}
			}
			return this.wrapped["~run"](dataset, config$1);
		}
	};
}

//#endregion
//#region src/schemas/number/number.ts
/* @__NO_SIDE_EFFECTS__ */
function number(message$1) {
	return {
		kind: "schema",
		type: "number",
		reference: number,
		expects: "number",
		async: false,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (typeof dataset.value === "number" && !isNaN(dataset.value)) dataset.typed = true;
			else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/object/object.ts
/* @__NO_SIDE_EFFECTS__ */
function object(entries$1, message$1) {
	return {
		kind: "schema",
		type: "object",
		reference: object,
		expects: "Object",
		async: false,
		entries: entries$1,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			const input = dataset.value;
			if (input && typeof input === "object") {
				dataset.typed = true;
				dataset.value = {};
				for (const key in this.entries) {
					const valueSchema = this.entries[key];
					if (key in input || (valueSchema.type === "exact_optional" || valueSchema.type === "optional" || valueSchema.type === "nullish") && valueSchema.default !== void 0) {
						const value$1 = key in input ? input[key] : /* @__PURE__ */ getDefault(valueSchema);
						const valueDataset = valueSchema["~run"]({ value: value$1 }, config$1);
						if (valueDataset.issues) {
							const pathItem = {
								type: "object",
								origin: "value",
								input,
								key,
								value: value$1
							};
							for (const issue of valueDataset.issues) {
								if (issue.path) issue.path.unshift(pathItem);
								else issue.path = [pathItem];
								dataset.issues?.push(issue);
							}
							if (!dataset.issues) dataset.issues = valueDataset.issues;
							if (config$1.abortEarly) {
								dataset.typed = false;
								break;
							}
						}
						if (!valueDataset.typed) dataset.typed = false;
						dataset.value[key] = valueDataset.value;
					} else if (valueSchema.fallback !== void 0) dataset.value[key] = /* @__PURE__ */ getFallback(valueSchema);
					else if (valueSchema.type !== "exact_optional" && valueSchema.type !== "optional" && valueSchema.type !== "nullish") {
						_addIssue(this, "key", dataset, config$1, {
							input: void 0,
							expected: `"${key}"`,
							path: [{
								type: "object",
								origin: "key",
								input,
								key,
								value: input[key]
							}]
						});
						if (config$1.abortEarly) break;
					}
				}
			} else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/optional/optional.ts
/* @__NO_SIDE_EFFECTS__ */
function optional(wrapped, default_) {
	return {
		kind: "schema",
		type: "optional",
		reference: optional,
		expects: `(${wrapped.expects} | undefined)`,
		async: false,
		wrapped,
		default: default_,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (dataset.value === void 0) {
				if (this.default !== void 0) dataset.value = /* @__PURE__ */ getDefault(this, dataset, config$1);
				if (dataset.value === void 0) {
					dataset.typed = true;
					return dataset;
				}
			}
			return this.wrapped["~run"](dataset, config$1);
		}
	};
}

//#endregion
//#region src/schemas/record/record.ts
/* @__NO_SIDE_EFFECTS__ */
function record(key, value$1, message$1) {
	return {
		kind: "schema",
		type: "record",
		reference: record,
		expects: "Object",
		async: false,
		key,
		value: value$1,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			const input = dataset.value;
			if (input && typeof input === "object") {
				dataset.typed = true;
				dataset.value = {};
				for (const entryKey in input) if (/* @__PURE__ */ _isValidObjectKey(input, entryKey)) {
					const entryValue = input[entryKey];
					const keyDataset = this.key["~run"]({ value: entryKey }, config$1);
					if (keyDataset.issues) {
						const pathItem = {
							type: "object",
							origin: "key",
							input,
							key: entryKey,
							value: entryValue
						};
						for (const issue of keyDataset.issues) {
							issue.path = [pathItem];
							dataset.issues?.push(issue);
						}
						if (!dataset.issues) dataset.issues = keyDataset.issues;
						if (config$1.abortEarly) {
							dataset.typed = false;
							break;
						}
					}
					const valueDataset = this.value["~run"]({ value: entryValue }, config$1);
					if (valueDataset.issues) {
						const pathItem = {
							type: "object",
							origin: "value",
							input,
							key: entryKey,
							value: entryValue
						};
						for (const issue of valueDataset.issues) {
							if (issue.path) issue.path.unshift(pathItem);
							else issue.path = [pathItem];
							dataset.issues?.push(issue);
						}
						if (!dataset.issues) dataset.issues = valueDataset.issues;
						if (config$1.abortEarly) {
							dataset.typed = false;
							break;
						}
					}
					if (!keyDataset.typed || !valueDataset.typed) dataset.typed = false;
					if (keyDataset.typed) dataset.value[keyDataset.value] = valueDataset.value;
				}
			} else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/string/string.ts
/* @__NO_SIDE_EFFECTS__ */
function string(message$1) {
	return {
		kind: "schema",
		type: "string",
		reference: string,
		expects: "string",
		async: false,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			if (typeof dataset.value === "string") dataset.typed = true;
			else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/tuple/tuple.ts
/* @__NO_SIDE_EFFECTS__ */
function tuple(items, message$1) {
	return {
		kind: "schema",
		type: "tuple",
		reference: tuple,
		expects: "Array",
		async: false,
		items,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			const input = dataset.value;
			if (Array.isArray(input)) {
				dataset.typed = true;
				dataset.value = [];
				for (let key = 0; key < this.items.length; key++) {
					const value$1 = input[key];
					const itemDataset = this.items[key]["~run"]({ value: value$1 }, config$1);
					if (itemDataset.issues) {
						const pathItem = {
							type: "array",
							origin: "value",
							input,
							key,
							value: value$1
						};
						for (const issue of itemDataset.issues) {
							if (issue.path) issue.path.unshift(pathItem);
							else issue.path = [pathItem];
							dataset.issues?.push(issue);
						}
						if (!dataset.issues) dataset.issues = itemDataset.issues;
						if (config$1.abortEarly) {
							dataset.typed = false;
							break;
						}
					}
					if (!itemDataset.typed) dataset.typed = false;
					dataset.value.push(itemDataset.value);
				}
			} else _addIssue(this, "type", dataset, config$1);
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/union/utils/_subIssues/_subIssues.ts
/**
* Returns the sub issues of the provided datasets for the union issue.
*
* @param datasets The datasets.
*
* @returns The sub issues.
*
* @internal
*/
/* @__NO_SIDE_EFFECTS__ */
function _subIssues(datasets) {
	let issues;
	if (datasets) for (const dataset of datasets) if (issues) issues.push(...dataset.issues);
	else issues = dataset.issues;
	return issues;
}

//#endregion
//#region src/schemas/union/union.ts
/* @__NO_SIDE_EFFECTS__ */
function union(options, message$1) {
	return {
		kind: "schema",
		type: "union",
		reference: union,
		expects: /* @__PURE__ */ _joinExpects(options.map((option) => option.expects), "|"),
		async: false,
		options,
		message: message$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			let validDataset;
			let typedDatasets;
			let untypedDatasets;
			for (const schema of this.options) {
				const optionDataset = schema["~run"]({ value: dataset.value }, config$1);
				if (optionDataset.typed) if (optionDataset.issues) if (typedDatasets) typedDatasets.push(optionDataset);
				else typedDatasets = [optionDataset];
				else {
					validDataset = optionDataset;
					break;
				}
				else if (untypedDatasets) untypedDatasets.push(optionDataset);
				else untypedDatasets = [optionDataset];
			}
			if (validDataset) return validDataset;
			if (typedDatasets) {
				if (typedDatasets.length === 1) return typedDatasets[0];
				_addIssue(this, "type", dataset, config$1, { issues: /* @__PURE__ */ _subIssues(typedDatasets) });
				dataset.typed = true;
			} else if (untypedDatasets?.length === 1) return untypedDatasets[0];
			else _addIssue(this, "type", dataset, config$1, { issues: /* @__PURE__ */ _subIssues(untypedDatasets) });
			return dataset;
		}
	};
}

//#endregion
//#region src/schemas/unknown/unknown.ts
/**
* Creates a unknown schema.
*
* @returns A unknown schema.
*/
/* @__NO_SIDE_EFFECTS__ */
function unknown() {
	return {
		kind: "schema",
		type: "unknown",
		reference: unknown,
		expects: "unknown",
		async: false,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset) {
			dataset.typed = true;
			return dataset;
		}
	};
}

//#endregion
//#region src/methods/parse/parse.ts
/**
* Parses an unknown input based on a schema.
*
* @param schema The schema to be used.
* @param input The input to be parsed.
* @param config The parse configuration.
*
* @returns The parsed input.
*/
function parse(schema, input, config$1) {
	const dataset = schema["~run"]({ value: input }, /* @__PURE__ */ getGlobalConfig(config$1));
	if (dataset.issues) throw new ValiError(dataset.issues);
	return dataset.value;
}

//#endregion
//#region src/methods/pipe/pipe.ts
/* @__NO_SIDE_EFFECTS__ */
function pipe(...pipe$1) {
	return {
		...pipe$1[0],
		pipe: pipe$1,
		get "~standard"() {
			return /* @__PURE__ */ _getStandardProps(this);
		},
		"~run"(dataset, config$1) {
			for (const item of pipe$1) if (item.kind !== "metadata") {
				if (dataset.issues && (item.kind === "schema" || item.kind === "transformation")) {
					dataset.typed = false;
					break;
				}
				if (!dataset.issues || !config$1.abortEarly && !config$1.abortPipeEarly) dataset = item["~run"](dataset, config$1);
			}
			return dataset;
		}
	};
}

function safeEnum(options) {
  const unionOptions = Object.entries(options).map(([key, value]) => object({ [key]: value }));
  return pipe(
    union(unionOptions),
    transform(
      (value) => ({
        ...value,
        $kind: Object.keys(value)[0]
      })
    )
  );
}
const SuiAddress = pipe(
  string(),
  transform((value) => normalizeSuiAddress(value)),
  check(isValidSuiAddress)
);
const ObjectID = SuiAddress;
const BCSBytes = string();
const JsonU64 = pipe(
  union([string(), pipe(number(), integer())]),
  check((val) => {
    try {
      BigInt(val);
      return BigInt(val) >= 0 && BigInt(val) <= 18446744073709551615n;
    } catch {
      return false;
    }
  }, "Invalid u64")
);
const ObjectRefSchema = object({
  objectId: SuiAddress,
  version: JsonU64,
  digest: string()
});
const ArgumentSchema = pipe(
  union([
    object({ GasCoin: literal(true) }),
    object({ Input: pipe(number(), integer()), type: optional(literal("pure")) }),
    object({ Input: pipe(number(), integer()), type: optional(literal("object")) }),
    object({ Result: pipe(number(), integer()) }),
    object({ NestedResult: tuple([pipe(number(), integer()), pipe(number(), integer())]) })
  ]),
  transform((value) => ({
    ...value,
    $kind: Object.keys(value)[0]
  }))
  // Defined manually to add `type?: 'pure' | 'object'` to Input
);
const GasDataSchema = object({
  budget: nullable(JsonU64),
  price: nullable(JsonU64),
  owner: nullable(SuiAddress),
  payment: nullable(array(ObjectRefSchema))
});
const OpenMoveTypeSignatureBodySchema = union([
  literal("address"),
  literal("bool"),
  literal("u8"),
  literal("u16"),
  literal("u32"),
  literal("u64"),
  literal("u128"),
  literal("u256"),
  object({ vector: lazy(() => OpenMoveTypeSignatureBodySchema) }),
  object({
    datatype: object({
      package: string(),
      module: string(),
      type: string(),
      typeParameters: array(lazy(() => OpenMoveTypeSignatureBodySchema))
    })
  }),
  object({ typeParameter: pipe(number(), integer()) })
]);
const OpenMoveTypeSignatureSchema = object({
  ref: nullable(union([literal("&"), literal("&mut")])),
  body: OpenMoveTypeSignatureBodySchema
});
const ProgrammableMoveCallSchema = object({
  package: ObjectID,
  module: string(),
  function: string(),
  // snake case in rust
  typeArguments: array(string()),
  arguments: array(ArgumentSchema),
  _argumentTypes: optional(nullable(array(OpenMoveTypeSignatureSchema)))
});
const $Intent$1 = object({
  name: string(),
  inputs: record(string(), union([ArgumentSchema, array(ArgumentSchema)])),
  data: record(string(), unknown())
});
const CommandSchema = safeEnum({
  MoveCall: ProgrammableMoveCallSchema,
  TransferObjects: object({
    objects: array(ArgumentSchema),
    address: ArgumentSchema
  }),
  SplitCoins: object({
    coin: ArgumentSchema,
    amounts: array(ArgumentSchema)
  }),
  MergeCoins: object({
    destination: ArgumentSchema,
    sources: array(ArgumentSchema)
  }),
  Publish: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID)
  }),
  MakeMoveVec: object({
    type: nullable(string()),
    elements: array(ArgumentSchema)
  }),
  Upgrade: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID),
    package: ObjectID,
    ticket: ArgumentSchema
  }),
  $Intent: $Intent$1
});
const ObjectArgSchema = safeEnum({
  ImmOrOwnedObject: ObjectRefSchema,
  SharedObject: object({
    objectId: ObjectID,
    // snake case in rust
    initialSharedVersion: JsonU64,
    mutable: boolean()
  }),
  Receiving: ObjectRefSchema
});
const CallArgSchema = safeEnum({
  Object: ObjectArgSchema,
  Pure: object({
    bytes: BCSBytes
  }),
  UnresolvedPure: object({
    value: unknown()
  }),
  UnresolvedObject: object({
    objectId: ObjectID,
    version: optional(nullable(JsonU64)),
    digest: optional(nullable(string())),
    initialSharedVersion: optional(nullable(JsonU64)),
    mutable: optional(nullable(boolean()))
  })
});
const NormalizedCallArg$1 = safeEnum({
  Object: ObjectArgSchema,
  Pure: object({
    bytes: BCSBytes
  })
});
const TransactionExpiration$1 = safeEnum({
  None: literal(true),
  Epoch: JsonU64
});
const TransactionDataSchema = object({
  version: literal(2),
  sender: nullish(SuiAddress),
  expiration: nullish(TransactionExpiration$1),
  gasData: GasDataSchema,
  inputs: array(CallArgSchema),
  commands: array(CommandSchema)
});

const Commands = {
  MoveCall(input) {
    const [pkg, mod = "", fn = ""] = "target" in input ? input.target.split("::") : [input.package, input.module, input.function];
    return {
      $kind: "MoveCall",
      MoveCall: {
        package: pkg,
        module: mod,
        function: fn,
        typeArguments: input.typeArguments ?? [],
        arguments: input.arguments ?? []
      }
    };
  },
  TransferObjects(objects, address) {
    return {
      $kind: "TransferObjects",
      TransferObjects: {
        objects: objects.map((o) => parse(ArgumentSchema, o)),
        address: parse(ArgumentSchema, address)
      }
    };
  },
  SplitCoins(coin, amounts) {
    return {
      $kind: "SplitCoins",
      SplitCoins: {
        coin: parse(ArgumentSchema, coin),
        amounts: amounts.map((o) => parse(ArgumentSchema, o))
      }
    };
  },
  MergeCoins(destination, sources) {
    return {
      $kind: "MergeCoins",
      MergeCoins: {
        destination: parse(ArgumentSchema, destination),
        sources: sources.map((o) => parse(ArgumentSchema, o))
      }
    };
  },
  Publish({
    modules,
    dependencies
  }) {
    return {
      $kind: "Publish",
      Publish: {
        modules: modules.map(
          (module) => typeof module === "string" ? module : toBase64(new Uint8Array(module))
        ),
        dependencies: dependencies.map((dep) => normalizeSuiObjectId(dep))
      }
    };
  },
  Upgrade({
    modules,
    dependencies,
    package: packageId,
    ticket
  }) {
    return {
      $kind: "Upgrade",
      Upgrade: {
        modules: modules.map(
          (module) => typeof module === "string" ? module : toBase64(new Uint8Array(module))
        ),
        dependencies: dependencies.map((dep) => normalizeSuiObjectId(dep)),
        package: packageId,
        ticket: parse(ArgumentSchema, ticket)
      }
    };
  },
  MakeMoveVec({
    type,
    elements
  }) {
    return {
      $kind: "MakeMoveVec",
      MakeMoveVec: {
        type: type ?? null,
        elements: elements.map((o) => parse(ArgumentSchema, o))
      }
    };
  },
  Intent({
    name,
    inputs = {},
    data = {}
  }) {
    return {
      $kind: "$Intent",
      $Intent: {
        name,
        inputs: Object.fromEntries(
          Object.entries(inputs).map(([key, value]) => [
            key,
            Array.isArray(value) ? value.map((o) => parse(ArgumentSchema, o)) : parse(ArgumentSchema, value)
          ])
        ),
        data
      }
    };
  }
};

const ObjectRef = object({
  digest: string(),
  objectId: string(),
  version: union([pipe(number(), integer()), string(), bigint()])
});
const ObjectArg$1 = safeEnum({
  ImmOrOwned: ObjectRef,
  Shared: object({
    objectId: ObjectID,
    initialSharedVersion: JsonU64,
    mutable: boolean()
  }),
  Receiving: ObjectRef
});
const NormalizedCallArg = safeEnum({
  Object: ObjectArg$1,
  Pure: array(pipe(number(), integer()))
});
function serializeV1TransactionData(transactionData) {
  const inputs = transactionData.inputs.map(
    (input, index) => {
      if (input.Object) {
        return {
          kind: "Input",
          index,
          value: {
            Object: input.Object.ImmOrOwnedObject ? {
              ImmOrOwned: input.Object.ImmOrOwnedObject
            } : input.Object.Receiving ? {
              Receiving: {
                digest: input.Object.Receiving.digest,
                version: input.Object.Receiving.version,
                objectId: input.Object.Receiving.objectId
              }
            } : {
              Shared: {
                mutable: input.Object.SharedObject.mutable,
                initialSharedVersion: input.Object.SharedObject.initialSharedVersion,
                objectId: input.Object.SharedObject.objectId
              }
            }
          },
          type: "object"
        };
      }
      if (input.Pure) {
        return {
          kind: "Input",
          index,
          value: {
            Pure: Array.from(fromBase64(input.Pure.bytes))
          },
          type: "pure"
        };
      }
      if (input.UnresolvedPure) {
        return {
          kind: "Input",
          type: "pure",
          index,
          value: input.UnresolvedPure.value
        };
      }
      if (input.UnresolvedObject) {
        return {
          kind: "Input",
          type: "object",
          index,
          value: input.UnresolvedObject.objectId
        };
      }
      throw new Error("Invalid input");
    }
  );
  return {
    version: 1,
    sender: transactionData.sender ?? void 0,
    expiration: transactionData.expiration?.$kind === "Epoch" ? { Epoch: Number(transactionData.expiration.Epoch) } : transactionData.expiration ? { None: true } : null,
    gasConfig: {
      owner: transactionData.gasData.owner ?? void 0,
      budget: transactionData.gasData.budget ?? void 0,
      price: transactionData.gasData.price ?? void 0,
      payment: transactionData.gasData.payment ?? void 0
    },
    inputs,
    transactions: transactionData.commands.map((command) => {
      if (command.MakeMoveVec) {
        return {
          kind: "MakeMoveVec",
          type: command.MakeMoveVec.type === null ? { None: true } : { Some: TypeTagSerializer.parseFromStr(command.MakeMoveVec.type) },
          objects: command.MakeMoveVec.elements.map(
            (arg) => convertTransactionArgument(arg, inputs)
          )
        };
      }
      if (command.MergeCoins) {
        return {
          kind: "MergeCoins",
          destination: convertTransactionArgument(command.MergeCoins.destination, inputs),
          sources: command.MergeCoins.sources.map((arg) => convertTransactionArgument(arg, inputs))
        };
      }
      if (command.MoveCall) {
        return {
          kind: "MoveCall",
          target: `${command.MoveCall.package}::${command.MoveCall.module}::${command.MoveCall.function}`,
          typeArguments: command.MoveCall.typeArguments,
          arguments: command.MoveCall.arguments.map(
            (arg) => convertTransactionArgument(arg, inputs)
          )
        };
      }
      if (command.Publish) {
        return {
          kind: "Publish",
          modules: command.Publish.modules.map((mod) => Array.from(fromBase64(mod))),
          dependencies: command.Publish.dependencies
        };
      }
      if (command.SplitCoins) {
        return {
          kind: "SplitCoins",
          coin: convertTransactionArgument(command.SplitCoins.coin, inputs),
          amounts: command.SplitCoins.amounts.map((arg) => convertTransactionArgument(arg, inputs))
        };
      }
      if (command.TransferObjects) {
        return {
          kind: "TransferObjects",
          objects: command.TransferObjects.objects.map(
            (arg) => convertTransactionArgument(arg, inputs)
          ),
          address: convertTransactionArgument(command.TransferObjects.address, inputs)
        };
      }
      if (command.Upgrade) {
        return {
          kind: "Upgrade",
          modules: command.Upgrade.modules.map((mod) => Array.from(fromBase64(mod))),
          dependencies: command.Upgrade.dependencies,
          packageId: command.Upgrade.package,
          ticket: convertTransactionArgument(command.Upgrade.ticket, inputs)
        };
      }
      throw new Error(`Unknown transaction ${Object.keys(command)}`);
    })
  };
}
function convertTransactionArgument(arg, inputs) {
  if (arg.$kind === "GasCoin") {
    return { kind: "GasCoin" };
  }
  if (arg.$kind === "Result") {
    return { kind: "Result", index: arg.Result };
  }
  if (arg.$kind === "NestedResult") {
    return { kind: "NestedResult", index: arg.NestedResult[0], resultIndex: arg.NestedResult[1] };
  }
  if (arg.$kind === "Input") {
    return inputs[arg.Input];
  }
  throw new Error(`Invalid argument ${Object.keys(arg)}`);
}
function transactionDataFromV1(data) {
  return parse(TransactionDataSchema, {
    version: 2,
    sender: data.sender ?? null,
    expiration: data.expiration ? "Epoch" in data.expiration ? { Epoch: data.expiration.Epoch } : { None: true } : null,
    gasData: {
      owner: data.gasConfig.owner ?? null,
      budget: data.gasConfig.budget?.toString() ?? null,
      price: data.gasConfig.price?.toString() ?? null,
      payment: data.gasConfig.payment?.map((ref) => ({
        digest: ref.digest,
        objectId: ref.objectId,
        version: ref.version.toString()
      })) ?? null
    },
    inputs: data.inputs.map((input) => {
      if (input.kind === "Input") {
        if (is(NormalizedCallArg, input.value)) {
          const value = parse(NormalizedCallArg, input.value);
          if (value.Object) {
            if (value.Object.ImmOrOwned) {
              return {
                Object: {
                  ImmOrOwnedObject: {
                    objectId: value.Object.ImmOrOwned.objectId,
                    version: String(value.Object.ImmOrOwned.version),
                    digest: value.Object.ImmOrOwned.digest
                  }
                }
              };
            }
            if (value.Object.Shared) {
              return {
                Object: {
                  SharedObject: {
                    mutable: value.Object.Shared.mutable ?? null,
                    initialSharedVersion: value.Object.Shared.initialSharedVersion,
                    objectId: value.Object.Shared.objectId
                  }
                }
              };
            }
            if (value.Object.Receiving) {
              return {
                Object: {
                  Receiving: {
                    digest: value.Object.Receiving.digest,
                    version: String(value.Object.Receiving.version),
                    objectId: value.Object.Receiving.objectId
                  }
                }
              };
            }
            throw new Error("Invalid object input");
          }
          return {
            Pure: {
              bytes: toBase64(new Uint8Array(value.Pure))
            }
          };
        }
        if (input.type === "object") {
          return {
            UnresolvedObject: {
              objectId: input.value
            }
          };
        }
        return {
          UnresolvedPure: {
            value: input.value
          }
        };
      }
      throw new Error("Invalid input");
    }),
    commands: data.transactions.map((transaction) => {
      switch (transaction.kind) {
        case "MakeMoveVec":
          return {
            MakeMoveVec: {
              type: "Some" in transaction.type ? TypeTagSerializer.tagToString(transaction.type.Some) : null,
              elements: transaction.objects.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        case "MergeCoins": {
          return {
            MergeCoins: {
              destination: parseV1TransactionArgument(transaction.destination),
              sources: transaction.sources.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        }
        case "MoveCall": {
          const [pkg, mod, fn] = transaction.target.split("::");
          return {
            MoveCall: {
              package: pkg,
              module: mod,
              function: fn,
              typeArguments: transaction.typeArguments,
              arguments: transaction.arguments.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        }
        case "Publish": {
          return {
            Publish: {
              modules: transaction.modules.map((mod) => toBase64(Uint8Array.from(mod))),
              dependencies: transaction.dependencies
            }
          };
        }
        case "SplitCoins": {
          return {
            SplitCoins: {
              coin: parseV1TransactionArgument(transaction.coin),
              amounts: transaction.amounts.map((arg) => parseV1TransactionArgument(arg))
            }
          };
        }
        case "TransferObjects": {
          return {
            TransferObjects: {
              objects: transaction.objects.map((arg) => parseV1TransactionArgument(arg)),
              address: parseV1TransactionArgument(transaction.address)
            }
          };
        }
        case "Upgrade": {
          return {
            Upgrade: {
              modules: transaction.modules.map((mod) => toBase64(Uint8Array.from(mod))),
              dependencies: transaction.dependencies,
              package: transaction.packageId,
              ticket: parseV1TransactionArgument(transaction.ticket)
            }
          };
        }
      }
      throw new Error(`Unknown transaction ${Object.keys(transaction)}`);
    })
  });
}
function parseV1TransactionArgument(arg) {
  switch (arg.kind) {
    case "GasCoin": {
      return { GasCoin: true };
    }
    case "Result":
      return { Result: arg.index };
    case "NestedResult": {
      return { NestedResult: [arg.index, arg.resultIndex] };
    }
    case "Input": {
      return { Input: arg.index };
    }
  }
}

function enumUnion(options) {
  return union(
    Object.entries(options).map(([key, value]) => object({ [key]: value }))
  );
}
const Argument = enumUnion({
  GasCoin: literal(true),
  Input: pipe(number(), integer()),
  Result: pipe(number(), integer()),
  NestedResult: tuple([pipe(number(), integer()), pipe(number(), integer())])
});
const GasData = object({
  budget: nullable(JsonU64),
  price: nullable(JsonU64),
  owner: nullable(SuiAddress),
  payment: nullable(array(ObjectRefSchema))
});
const ProgrammableMoveCall = object({
  package: ObjectID,
  module: string(),
  function: string(),
  // snake case in rust
  typeArguments: array(string()),
  arguments: array(Argument)
});
const $Intent = object({
  name: string(),
  inputs: record(string(), union([Argument, array(Argument)])),
  data: record(string(), unknown())
});
const Command = enumUnion({
  MoveCall: ProgrammableMoveCall,
  TransferObjects: object({
    objects: array(Argument),
    address: Argument
  }),
  SplitCoins: object({
    coin: Argument,
    amounts: array(Argument)
  }),
  MergeCoins: object({
    destination: Argument,
    sources: array(Argument)
  }),
  Publish: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID)
  }),
  MakeMoveVec: object({
    type: nullable(string()),
    elements: array(Argument)
  }),
  Upgrade: object({
    modules: array(BCSBytes),
    dependencies: array(ObjectID),
    package: ObjectID,
    ticket: Argument
  }),
  $Intent
});
const ObjectArg = enumUnion({
  ImmOrOwnedObject: ObjectRefSchema,
  SharedObject: object({
    objectId: ObjectID,
    // snake case in rust
    initialSharedVersion: JsonU64,
    mutable: boolean()
  }),
  Receiving: ObjectRefSchema
});
const CallArg = enumUnion({
  Object: ObjectArg,
  Pure: object({
    bytes: BCSBytes
  }),
  UnresolvedPure: object({
    value: unknown()
  }),
  UnresolvedObject: object({
    objectId: ObjectID,
    version: optional(nullable(JsonU64)),
    digest: optional(nullable(string())),
    initialSharedVersion: optional(nullable(JsonU64)),
    mutable: optional(nullable(boolean()))
  })
});
const TransactionExpiration = enumUnion({
  None: literal(true),
  Epoch: JsonU64
});
const SerializedTransactionDataV2Schema = object({
  version: literal(2),
  sender: nullish(SuiAddress),
  expiration: nullish(TransactionExpiration),
  gasData: GasData,
  inputs: array(CallArg),
  commands: array(Command),
  digest: optional(nullable(string()))
});

const MAX_OBJECTS_PER_FETCH = 50;
const GAS_SAFE_OVERHEAD = 1000n;
const MAX_GAS = 5e10;
function jsonRpcClientResolveTransactionPlugin(client) {
  return async function resolveTransactionData(transactionData, options, next) {
    await normalizeInputs(transactionData, client);
    await resolveObjectReferences(transactionData, client);
    if (!options.onlyTransactionKind) {
      await setGasPrice(transactionData, client);
      await setGasBudget(transactionData, client);
      await setGasPayment(transactionData, client);
    }
    return await next();
  };
}
async function setGasPrice(transactionData, client) {
  if (!transactionData.gasConfig.price) {
    transactionData.gasConfig.price = String(await client.getReferenceGasPrice());
  }
}
async function setGasBudget(transactionData, client) {
  if (transactionData.gasConfig.budget) {
    return;
  }
  const dryRunResult = await client.dryRunTransactionBlock({
    transactionBlock: transactionData.build({
      overrides: {
        gasData: {
          budget: String(MAX_GAS),
          payment: []
        }
      }
    })
  });
  if (dryRunResult.effects.status.status !== "success") {
    throw new Error(
      `Dry run failed, could not automatically determine a budget: ${dryRunResult.effects.status.error}`,
      { cause: dryRunResult }
    );
  }
  const safeOverhead = GAS_SAFE_OVERHEAD * BigInt(transactionData.gasConfig.price || 1n);
  const baseComputationCostWithOverhead = BigInt(dryRunResult.effects.gasUsed.computationCost) + safeOverhead;
  const gasBudget = baseComputationCostWithOverhead + BigInt(dryRunResult.effects.gasUsed.storageCost) - BigInt(dryRunResult.effects.gasUsed.storageRebate);
  transactionData.gasConfig.budget = String(
    gasBudget > baseComputationCostWithOverhead ? gasBudget : baseComputationCostWithOverhead
  );
}
async function setGasPayment(transactionData, client) {
  if (!transactionData.gasConfig.payment) {
    const coins = await client.getCoins({
      owner: transactionData.gasConfig.owner || transactionData.sender,
      coinType: SUI_TYPE_ARG
    });
    const paymentCoins = coins.data.filter((coin) => {
      const matchingInput = transactionData.inputs.find((input) => {
        if (input.Object?.ImmOrOwnedObject) {
          return coin.coinObjectId === input.Object.ImmOrOwnedObject.objectId;
        }
        return false;
      });
      return !matchingInput;
    }).map((coin) => ({
      objectId: coin.coinObjectId,
      digest: coin.digest,
      version: coin.version
    }));
    if (!paymentCoins.length) {
      throw new Error("No valid gas coins found for the transaction.");
    }
    transactionData.gasConfig.payment = paymentCoins.map(
      (payment) => parse(ObjectRefSchema, payment)
    );
  }
}
async function resolveObjectReferences(transactionData, client) {
  const objectsToResolve = transactionData.inputs.filter((input) => {
    return input.UnresolvedObject && !(input.UnresolvedObject.version || input.UnresolvedObject?.initialSharedVersion);
  });
  const dedupedIds = [
    ...new Set(
      objectsToResolve.map((input) => normalizeSuiObjectId(input.UnresolvedObject.objectId))
    )
  ];
  const objectChunks = dedupedIds.length ? chunk(dedupedIds, MAX_OBJECTS_PER_FETCH) : [];
  const resolved = (await Promise.all(
    objectChunks.map(
      (chunk2) => client.multiGetObjects({
        ids: chunk2,
        options: { showOwner: true }
      })
    )
  )).flat();
  const responsesById = new Map(
    dedupedIds.map((id, index) => {
      return [id, resolved[index]];
    })
  );
  const invalidObjects = Array.from(responsesById).filter(([_, obj]) => obj.error).map(([_, obj]) => JSON.stringify(obj.error));
  if (invalidObjects.length) {
    throw new Error(`The following input objects are invalid: ${invalidObjects.join(", ")}`);
  }
  const objects = resolved.map((object) => {
    if (object.error || !object.data) {
      throw new Error(`Failed to fetch object: ${object.error}`);
    }
    const owner = object.data.owner;
    const initialSharedVersion = owner && typeof owner === "object" ? "Shared" in owner ? owner.Shared.initial_shared_version : "ConsensusAddressOwner" in owner ? owner.ConsensusAddressOwner.start_version : null : null;
    return {
      objectId: object.data.objectId,
      digest: object.data.digest,
      version: object.data.version,
      initialSharedVersion
    };
  });
  const objectsById = new Map(
    dedupedIds.map((id, index) => {
      return [id, objects[index]];
    })
  );
  for (const [index, input] of transactionData.inputs.entries()) {
    if (!input.UnresolvedObject) {
      continue;
    }
    let updated;
    const id = normalizeSuiAddress(input.UnresolvedObject.objectId);
    const object = objectsById.get(id);
    if (input.UnresolvedObject.initialSharedVersion ?? object?.initialSharedVersion) {
      updated = Inputs.SharedObjectRef({
        objectId: id,
        initialSharedVersion: input.UnresolvedObject.initialSharedVersion || object?.initialSharedVersion,
        mutable: input.UnresolvedObject.mutable || isUsedAsMutable(transactionData, index)
      });
    } else if (isUsedAsReceiving(transactionData, index)) {
      updated = Inputs.ReceivingRef(
        {
          objectId: id,
          digest: input.UnresolvedObject.digest ?? object?.digest,
          version: input.UnresolvedObject.version ?? object?.version
        }
      );
    }
    transactionData.inputs[transactionData.inputs.indexOf(input)] = updated ?? Inputs.ObjectRef({
      objectId: id,
      digest: input.UnresolvedObject.digest ?? object?.digest,
      version: input.UnresolvedObject.version ?? object?.version
    });
  }
}
async function normalizeInputs(transactionData, client) {
  const { inputs, commands } = transactionData;
  const moveCallsToResolve = [];
  const moveFunctionsToResolve = /* @__PURE__ */ new Set();
  commands.forEach((command) => {
    if (command.MoveCall) {
      if (command.MoveCall._argumentTypes) {
        return;
      }
      const inputs2 = command.MoveCall.arguments.map((arg) => {
        if (arg.$kind === "Input") {
          return transactionData.inputs[arg.Input];
        }
        return null;
      });
      const needsResolution = inputs2.some(
        (input) => input?.UnresolvedPure || input?.UnresolvedObject && typeof input?.UnresolvedObject.mutable !== "boolean"
      );
      if (needsResolution) {
        const functionName = `${command.MoveCall.package}::${command.MoveCall.module}::${command.MoveCall.function}`;
        moveFunctionsToResolve.add(functionName);
        moveCallsToResolve.push(command.MoveCall);
      }
    }
  });
  const moveFunctionParameters = /* @__PURE__ */ new Map();
  if (moveFunctionsToResolve.size > 0) {
    await Promise.all(
      [...moveFunctionsToResolve].map(async (functionName) => {
        const [packageId, moduleId, functionId] = functionName.split("::");
        const def = await client.getNormalizedMoveFunction({
          package: packageId,
          module: moduleId,
          function: functionId
        });
        moveFunctionParameters.set(
          functionName,
          def.parameters.map((param) => normalizedTypeToMoveTypeSignature(param))
        );
      })
    );
  }
  if (moveCallsToResolve.length) {
    await Promise.all(
      moveCallsToResolve.map(async (moveCall) => {
        const parameters = moveFunctionParameters.get(
          `${moveCall.package}::${moveCall.module}::${moveCall.function}`
        );
        if (!parameters) {
          return;
        }
        const hasTxContext = parameters.length > 0 && isTxContext(parameters.at(-1));
        const params = hasTxContext ? parameters.slice(0, parameters.length - 1) : parameters;
        moveCall._argumentTypes = params;
      })
    );
  }
  commands.forEach((command) => {
    if (!command.MoveCall) {
      return;
    }
    const moveCall = command.MoveCall;
    const fnName = `${moveCall.package}::${moveCall.module}::${moveCall.function}`;
    const params = moveCall._argumentTypes;
    if (!params) {
      return;
    }
    if (params.length !== command.MoveCall.arguments.length) {
      throw new Error(`Incorrect number of arguments for ${fnName}`);
    }
    params.forEach((param, i) => {
      const arg = moveCall.arguments[i];
      if (arg.$kind !== "Input") return;
      const input = inputs[arg.Input];
      if (!input.UnresolvedPure && !input.UnresolvedObject) {
        return;
      }
      const inputValue = input.UnresolvedPure?.value ?? input.UnresolvedObject?.objectId;
      const schema = getPureBcsSchema(param.body);
      if (schema) {
        arg.type = "pure";
        inputs[inputs.indexOf(input)] = Inputs.Pure(schema.serialize(inputValue));
        return;
      }
      if (typeof inputValue !== "string") {
        throw new Error(
          `Expect the argument to be an object id string, got ${JSON.stringify(
            inputValue,
            null,
            2
          )}`
        );
      }
      arg.type = "object";
      const unresolvedObject = input.UnresolvedPure ? {
        $kind: "UnresolvedObject",
        UnresolvedObject: {
          objectId: inputValue
        }
      } : input;
      inputs[arg.Input] = unresolvedObject;
    });
  });
}
function isUsedAsMutable(transactionData, index) {
  let usedAsMutable = false;
  transactionData.getInputUses(index, (arg, tx) => {
    if (tx.MoveCall && tx.MoveCall._argumentTypes) {
      const argIndex = tx.MoveCall.arguments.indexOf(arg);
      usedAsMutable = tx.MoveCall._argumentTypes[argIndex].ref !== "&" || usedAsMutable;
    }
    if (tx.$kind === "MakeMoveVec" || tx.$kind === "MergeCoins" || tx.$kind === "SplitCoins" || tx.$kind === "TransferObjects") {
      usedAsMutable = true;
    }
  });
  return usedAsMutable;
}
function isUsedAsReceiving(transactionData, index) {
  let usedAsReceiving = false;
  transactionData.getInputUses(index, (arg, tx) => {
    if (tx.MoveCall && tx.MoveCall._argumentTypes) {
      const argIndex = tx.MoveCall.arguments.indexOf(arg);
      usedAsReceiving = isReceivingType(tx.MoveCall._argumentTypes[argIndex]) || usedAsReceiving;
    }
  });
  return usedAsReceiving;
}
function isReceivingType(type) {
  if (typeof type.body !== "object" || !("datatype" in type.body)) {
    return false;
  }
  return type.body.datatype.package === "0x2" && type.body.datatype.module === "transfer" && type.body.datatype.type === "Receiving";
}

function needsTransactionResolution(data, options) {
  if (data.inputs.some((input) => {
    return input.UnresolvedObject || input.UnresolvedPure;
  })) {
    return true;
  }
  if (!options.onlyTransactionKind) {
    if (!data.gasConfig.price || !data.gasConfig.budget || !data.gasConfig.payment) {
      return true;
    }
  }
  return false;
}
async function resolveTransactionPlugin(transactionData, options, next) {
  normalizeRawArguments(transactionData);
  if (!needsTransactionResolution(transactionData, options)) {
    await validate(transactionData);
    return next();
  }
  const client = getClient$1(options);
  const plugin = client.core?.resolveTransactionPlugin() ?? jsonRpcClientResolveTransactionPlugin(client);
  return plugin(transactionData, options, async () => {
    await validate(transactionData);
    await next();
  });
}
function validate(transactionData) {
  transactionData.inputs.forEach((input, index) => {
    if (input.$kind !== "Object" && input.$kind !== "Pure") {
      throw new Error(
        `Input at index ${index} has not been resolved.  Expected a Pure or Object input, but found ${JSON.stringify(
          input
        )}`
      );
    }
  });
}
function getClient$1(options) {
  if (!options.client) {
    throw new Error(
      `No sui client passed to Transaction#build, but transaction data was not sufficient to build offline.`
    );
  }
  return options.client;
}
function normalizeRawArguments(transactionData) {
  for (const command of transactionData.commands) {
    switch (command.$kind) {
      case "SplitCoins":
        command.SplitCoins.amounts.forEach((amount) => {
          normalizeRawArgument(amount, suiBcs.U64, transactionData);
        });
        break;
      case "TransferObjects":
        normalizeRawArgument(command.TransferObjects.address, suiBcs.Address, transactionData);
        break;
    }
  }
}
function normalizeRawArgument(arg, schema, transactionData) {
  if (arg.$kind !== "Input") {
    return;
  }
  const input = transactionData.inputs[arg.Input];
  if (input.$kind !== "UnresolvedPure") {
    return;
  }
  transactionData.inputs[arg.Input] = Inputs.Pure(schema.serialize(input.UnresolvedPure.value));
}

function createObjectMethods(makeObject) {
  function object(value) {
    return makeObject(value);
  }
  object.system = (options) => {
    const mutable = options?.mutable;
    if (mutable !== void 0) {
      return object(
        Inputs.SharedObjectRef({
          objectId: "0x5",
          initialSharedVersion: 1,
          mutable
        })
      );
    }
    return object({
      $kind: "UnresolvedObject",
      UnresolvedObject: {
        objectId: "0x5",
        initialSharedVersion: 1
      }
    });
  };
  object.clock = () => object(
    Inputs.SharedObjectRef({
      objectId: "0x6",
      initialSharedVersion: 1,
      mutable: false
    })
  );
  object.random = () => object({
    $kind: "UnresolvedObject",
    UnresolvedObject: {
      objectId: "0x8",
      mutable: false
    }
  });
  object.denyList = (options) => {
    return object({
      $kind: "UnresolvedObject",
      UnresolvedObject: {
        objectId: "0x403",
        mutable: options?.mutable
      }
    });
  };
  object.option = ({ type, value }) => (tx) => tx.moveCall({
    typeArguments: [type],
    target: `0x1::option::${value === null ? "none" : "some"}`,
    arguments: value === null ? [] : [tx.object(value)]
  });
  return object;
}

function createPure(makePure) {
  function pure(typeOrSerializedValue, value) {
    if (typeof typeOrSerializedValue === "string") {
      return makePure(pureBcsSchemaFromTypeName(typeOrSerializedValue).serialize(value));
    }
    if (typeOrSerializedValue instanceof Uint8Array || isSerializedBcs(typeOrSerializedValue)) {
      return makePure(typeOrSerializedValue);
    }
    throw new Error("tx.pure must be called either a bcs type name, or a serialized bcs value");
  }
  pure.u8 = (value) => makePure(suiBcs.U8.serialize(value));
  pure.u16 = (value) => makePure(suiBcs.U16.serialize(value));
  pure.u32 = (value) => makePure(suiBcs.U32.serialize(value));
  pure.u64 = (value) => makePure(suiBcs.U64.serialize(value));
  pure.u128 = (value) => makePure(suiBcs.U128.serialize(value));
  pure.u256 = (value) => makePure(suiBcs.U256.serialize(value));
  pure.bool = (value) => makePure(suiBcs.Bool.serialize(value));
  pure.string = (value) => makePure(suiBcs.String.serialize(value));
  pure.address = (value) => makePure(suiBcs.Address.serialize(value));
  pure.id = pure.address;
  pure.vector = (type, value) => {
    return makePure(
      suiBcs.vector(pureBcsSchemaFromTypeName(type)).serialize(value)
    );
  };
  pure.option = (type, value) => {
    return makePure(suiBcs.option(pureBcsSchemaFromTypeName(type)).serialize(value));
  };
  return pure;
}

function hashTypedData(typeTag, data) {
  const typeTagBytes = Array.from(`${typeTag}::`).map((e) => e.charCodeAt(0));
  const dataWithTag = new Uint8Array(typeTagBytes.length + data.length);
  dataWithTag.set(typeTagBytes);
  dataWithTag.set(data, typeTagBytes.length);
  return blake2b(dataWithTag, { dkLen: 32 });
}

function getIdFromCallArg(arg) {
  if (typeof arg === "string") {
    return normalizeSuiAddress(arg);
  }
  if (arg.Object) {
    if (arg.Object.ImmOrOwnedObject) {
      return normalizeSuiAddress(arg.Object.ImmOrOwnedObject.objectId);
    }
    if (arg.Object.Receiving) {
      return normalizeSuiAddress(arg.Object.Receiving.objectId);
    }
    return normalizeSuiAddress(arg.Object.SharedObject.objectId);
  }
  if (arg.UnresolvedObject) {
    return normalizeSuiAddress(arg.UnresolvedObject.objectId);
  }
  return void 0;
}
function remapCommandArguments(command, inputMapping, commandMapping) {
  const remapArg = (arg) => {
    switch (arg.$kind) {
      case "Input": {
        const newInputIndex = inputMapping.get(arg.Input);
        if (newInputIndex === void 0) {
          throw new Error(`Input ${arg.Input} not found in input mapping`);
        }
        return { ...arg, Input: newInputIndex };
      }
      case "Result": {
        const newCommandIndex = commandMapping.get(arg.Result);
        if (newCommandIndex !== void 0) {
          return { ...arg, Result: newCommandIndex };
        }
        return arg;
      }
      case "NestedResult": {
        const newCommandIndex = commandMapping.get(arg.NestedResult[0]);
        if (newCommandIndex !== void 0) {
          return { ...arg, NestedResult: [newCommandIndex, arg.NestedResult[1]] };
        }
        return arg;
      }
      default:
        return arg;
    }
  };
  switch (command.$kind) {
    case "MoveCall":
      command.MoveCall.arguments = command.MoveCall.arguments.map(remapArg);
      break;
    case "TransferObjects":
      command.TransferObjects.objects = command.TransferObjects.objects.map(remapArg);
      command.TransferObjects.address = remapArg(command.TransferObjects.address);
      break;
    case "SplitCoins":
      command.SplitCoins.coin = remapArg(command.SplitCoins.coin);
      command.SplitCoins.amounts = command.SplitCoins.amounts.map(remapArg);
      break;
    case "MergeCoins":
      command.MergeCoins.destination = remapArg(command.MergeCoins.destination);
      command.MergeCoins.sources = command.MergeCoins.sources.map(remapArg);
      break;
    case "MakeMoveVec":
      command.MakeMoveVec.elements = command.MakeMoveVec.elements.map(remapArg);
      break;
    case "Upgrade":
      command.Upgrade.ticket = remapArg(command.Upgrade.ticket);
      break;
    case "$Intent": {
      const inputs = command.$Intent.inputs;
      command.$Intent.inputs = {};
      for (const [key, value] of Object.entries(inputs)) {
        command.$Intent.inputs[key] = Array.isArray(value) ? value.map(remapArg) : remapArg(value);
      }
      break;
    }
  }
}

function prepareSuiAddress(address) {
  return normalizeSuiAddress(address).replace("0x", "");
}
class TransactionDataBuilder {
  constructor(clone) {
    this.version = 2;
    this.sender = clone?.sender ?? null;
    this.expiration = clone?.expiration ?? null;
    this.inputs = clone?.inputs ?? [];
    this.commands = clone?.commands ?? [];
    this.gasData = clone?.gasData ?? {
      budget: null,
      price: null,
      owner: null,
      payment: null
    };
  }
  static fromKindBytes(bytes) {
    const kind = suiBcs.TransactionKind.parse(bytes);
    const programmableTx = kind.ProgrammableTransaction;
    if (!programmableTx) {
      throw new Error("Unable to deserialize from bytes.");
    }
    return TransactionDataBuilder.restore({
      version: 2,
      sender: null,
      expiration: null,
      gasData: {
        budget: null,
        owner: null,
        payment: null,
        price: null
      },
      inputs: programmableTx.inputs,
      commands: programmableTx.commands
    });
  }
  static fromBytes(bytes) {
    const rawData = suiBcs.TransactionData.parse(bytes);
    const data = rawData?.V1;
    const programmableTx = data.kind.ProgrammableTransaction;
    if (!data || !programmableTx) {
      throw new Error("Unable to deserialize from bytes.");
    }
    return TransactionDataBuilder.restore({
      version: 2,
      sender: data.sender,
      expiration: data.expiration,
      gasData: data.gasData,
      inputs: programmableTx.inputs,
      commands: programmableTx.commands
    });
  }
  static restore(data) {
    if (data.version === 2) {
      return new TransactionDataBuilder(parse(TransactionDataSchema, data));
    } else {
      return new TransactionDataBuilder(parse(TransactionDataSchema, transactionDataFromV1(data)));
    }
  }
  /**
   * Generate transaction digest.
   *
   * @param bytes BCS serialized transaction data
   * @returns transaction digest.
   */
  static getDigestFromBytes(bytes) {
    const hash = hashTypedData("TransactionData", bytes);
    return toBase58(hash);
  }
  // @deprecated use gasData instead
  get gasConfig() {
    return this.gasData;
  }
  // @deprecated use gasData instead
  set gasConfig(value) {
    this.gasData = value;
  }
  build({
    maxSizeBytes = Infinity,
    overrides,
    onlyTransactionKind
  } = {}) {
    const inputs = this.inputs;
    const commands = this.commands;
    const kind = {
      ProgrammableTransaction: {
        inputs,
        commands
      }
    };
    if (onlyTransactionKind) {
      return suiBcs.TransactionKind.serialize(kind, { maxSize: maxSizeBytes }).toBytes();
    }
    const expiration = overrides?.expiration ?? this.expiration;
    const sender = overrides?.sender ?? this.sender;
    const gasData = { ...this.gasData, ...overrides?.gasConfig, ...overrides?.gasData };
    if (!sender) {
      throw new Error("Missing transaction sender");
    }
    if (!gasData.budget) {
      throw new Error("Missing gas budget");
    }
    if (!gasData.payment) {
      throw new Error("Missing gas payment");
    }
    if (!gasData.price) {
      throw new Error("Missing gas price");
    }
    const transactionData = {
      sender: prepareSuiAddress(sender),
      expiration: expiration ? expiration : { None: true },
      gasData: {
        payment: gasData.payment,
        owner: prepareSuiAddress(this.gasData.owner ?? sender),
        price: BigInt(gasData.price),
        budget: BigInt(gasData.budget)
      },
      kind: {
        ProgrammableTransaction: {
          inputs,
          commands
        }
      }
    };
    return suiBcs.TransactionData.serialize(
      { V1: transactionData },
      { maxSize: maxSizeBytes }
    ).toBytes();
  }
  addInput(type, arg) {
    const index = this.inputs.length;
    this.inputs.push(arg);
    return { Input: index, type, $kind: "Input" };
  }
  getInputUses(index, fn) {
    this.mapArguments((arg, command) => {
      if (arg.$kind === "Input" && arg.Input === index) {
        fn(arg, command);
      }
      return arg;
    });
  }
  mapCommandArguments(index, fn) {
    const command = this.commands[index];
    switch (command.$kind) {
      case "MoveCall":
        command.MoveCall.arguments = command.MoveCall.arguments.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "TransferObjects":
        command.TransferObjects.objects = command.TransferObjects.objects.map(
          (arg) => fn(arg, command, index)
        );
        command.TransferObjects.address = fn(command.TransferObjects.address, command, index);
        break;
      case "SplitCoins":
        command.SplitCoins.coin = fn(command.SplitCoins.coin, command, index);
        command.SplitCoins.amounts = command.SplitCoins.amounts.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "MergeCoins":
        command.MergeCoins.destination = fn(command.MergeCoins.destination, command, index);
        command.MergeCoins.sources = command.MergeCoins.sources.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "MakeMoveVec":
        command.MakeMoveVec.elements = command.MakeMoveVec.elements.map(
          (arg) => fn(arg, command, index)
        );
        break;
      case "Upgrade":
        command.Upgrade.ticket = fn(command.Upgrade.ticket, command, index);
        break;
      case "$Intent":
        const inputs = command.$Intent.inputs;
        command.$Intent.inputs = {};
        for (const [key, value] of Object.entries(inputs)) {
          command.$Intent.inputs[key] = Array.isArray(value) ? value.map((arg) => fn(arg, command, index)) : fn(value, command, index);
        }
        break;
      case "Publish":
        break;
      default:
        throw new Error(`Unexpected transaction kind: ${command.$kind}`);
    }
  }
  mapArguments(fn) {
    for (const commandIndex of this.commands.keys()) {
      this.mapCommandArguments(commandIndex, fn);
    }
  }
  replaceCommand(index, replacement, resultIndex = index) {
    if (!Array.isArray(replacement)) {
      this.commands[index] = replacement;
      return;
    }
    const sizeDiff = replacement.length - 1;
    this.commands.splice(index, 1, ...structuredClone(replacement));
    this.mapArguments((arg, _command, commandIndex) => {
      if (commandIndex < index + replacement.length) {
        return arg;
      }
      if (typeof resultIndex !== "number") {
        if (arg.$kind === "Result" && arg.Result === index || arg.$kind === "NestedResult" && arg.NestedResult[0] === index) {
          if (!("NestedResult" in arg) || arg.NestedResult[1] === 0) {
            return parse(ArgumentSchema, structuredClone(resultIndex));
          } else {
            throw new Error(
              `Cannot replace command ${index} with a specific result type: NestedResult[${index}, ${arg.NestedResult[1]}] references a nested element that cannot be mapped to the replacement result`
            );
          }
        }
      }
      switch (arg.$kind) {
        case "Result":
          if (arg.Result === index && typeof resultIndex === "number") {
            arg.Result = resultIndex;
          }
          if (arg.Result > index) {
            arg.Result += sizeDiff;
          }
          break;
        case "NestedResult":
          if (arg.NestedResult[0] === index && typeof resultIndex === "number") {
            return {
              $kind: "NestedResult",
              NestedResult: [resultIndex, arg.NestedResult[1]]
            };
          }
          if (arg.NestedResult[0] > index) {
            arg.NestedResult[0] += sizeDiff;
          }
          break;
      }
      return arg;
    });
  }
  replaceCommandWithTransaction(index, otherTransaction, result) {
    if (result.$kind !== "Result" && result.$kind !== "NestedResult") {
      throw new Error("Result must be of kind Result or NestedResult");
    }
    this.insertTransaction(index, otherTransaction);
    this.replaceCommand(
      index + otherTransaction.commands.length,
      [],
      "Result" in result ? { NestedResult: [result.Result + index, 0] } : {
        NestedResult: [
          result.NestedResult[0] + index,
          result.NestedResult[1]
        ]
      }
    );
  }
  insertTransaction(atCommandIndex, otherTransaction) {
    const inputMapping = /* @__PURE__ */ new Map();
    const commandMapping = /* @__PURE__ */ new Map();
    for (let i = 0; i < otherTransaction.inputs.length; i++) {
      const otherInput = otherTransaction.inputs[i];
      const id = getIdFromCallArg(otherInput);
      let existingIndex = -1;
      if (id !== void 0) {
        existingIndex = this.inputs.findIndex((input) => getIdFromCallArg(input) === id);
        if (existingIndex !== -1 && this.inputs[existingIndex].Object?.SharedObject && otherInput.Object?.SharedObject) {
          this.inputs[existingIndex].Object.SharedObject.mutable = this.inputs[existingIndex].Object.SharedObject.mutable || otherInput.Object.SharedObject.mutable;
        }
      }
      if (existingIndex !== -1) {
        inputMapping.set(i, existingIndex);
      } else {
        const newIndex = this.inputs.length;
        this.inputs.push(otherInput);
        inputMapping.set(i, newIndex);
      }
    }
    for (let i = 0; i < otherTransaction.commands.length; i++) {
      commandMapping.set(i, atCommandIndex + i);
    }
    const remappedCommands = [];
    for (let i = 0; i < otherTransaction.commands.length; i++) {
      const command = structuredClone(otherTransaction.commands[i]);
      remapCommandArguments(command, inputMapping, commandMapping);
      remappedCommands.push(command);
    }
    this.commands.splice(atCommandIndex, 0, ...remappedCommands);
    const sizeDiff = remappedCommands.length;
    if (sizeDiff > 0) {
      this.mapArguments((arg, _command, commandIndex) => {
        if (commandIndex >= atCommandIndex && commandIndex < atCommandIndex + remappedCommands.length) {
          return arg;
        }
        switch (arg.$kind) {
          case "Result":
            if (arg.Result >= atCommandIndex) {
              arg.Result += sizeDiff;
            }
            break;
          case "NestedResult":
            if (arg.NestedResult[0] >= atCommandIndex) {
              arg.NestedResult[0] += sizeDiff;
            }
            break;
        }
        return arg;
      });
    }
  }
  getDigest() {
    const bytes = this.build({ onlyTransactionKind: false });
    return TransactionDataBuilder.getDigestFromBytes(bytes);
  }
  snapshot() {
    return parse(TransactionDataSchema, this);
  }
  shallowClone() {
    return new TransactionDataBuilder({
      version: this.version,
      sender: this.sender,
      expiration: this.expiration,
      gasData: {
        ...this.gasData
      },
      inputs: [...this.inputs],
      commands: [...this.commands]
    });
  }
  applyResolvedData(resolved) {
    if (!this.sender) {
      this.sender = resolved.sender ?? null;
    }
    if (!this.expiration) {
      this.expiration = resolved.expiration ?? null;
    }
    if (!this.gasData.budget) {
      this.gasData.budget = resolved.gasData.budget;
    }
    if (!this.gasData.owner) {
      this.gasData.owner = resolved.gasData.owner ?? null;
    }
    if (!this.gasData.payment) {
      this.gasData.payment = resolved.gasData.payment;
    }
    if (!this.gasData.price) {
      this.gasData.price = resolved.gasData.price;
    }
    for (let i = 0; i < this.inputs.length; i++) {
      const input = this.inputs[i];
      const resolvedInput = resolved.inputs[i];
      switch (input.$kind) {
        case "UnresolvedPure":
          if (resolvedInput.$kind !== "Pure") {
            throw new Error(
              `Expected input at index ${i} to resolve to a Pure argument, but got ${JSON.stringify(
                resolvedInput
              )}`
            );
          }
          this.inputs[i] = resolvedInput;
          break;
        case "UnresolvedObject":
          if (resolvedInput.$kind !== "Object") {
            throw new Error(
              `Expected input at index ${i} to resolve to an Object argument, but got ${JSON.stringify(
                resolvedInput
              )}`
            );
          }
          if (resolvedInput.Object.$kind === "ImmOrOwnedObject" || resolvedInput.Object.$kind === "Receiving") {
            const original = input.UnresolvedObject;
            const resolved2 = resolvedInput.Object.ImmOrOwnedObject ?? resolvedInput.Object.Receiving;
            if (normalizeSuiAddress(original.objectId) !== normalizeSuiAddress(resolved2.objectId) || original.version != null && original.version !== resolved2.version || original.digest != null && original.digest !== resolved2.digest || // Objects with shared object properties should not resolve to owned objects
            original.mutable != null || original.initialSharedVersion != null) {
              throw new Error(
                `Input at index ${i} did not match unresolved object. ${JSON.stringify(original)} is not compatible with ${JSON.stringify(resolved2)}`
              );
            }
          } else if (resolvedInput.Object.$kind === "SharedObject") {
            const original = input.UnresolvedObject;
            const resolved2 = resolvedInput.Object.SharedObject;
            if (normalizeSuiAddress(original.objectId) !== normalizeSuiAddress(resolved2.objectId) || original.initialSharedVersion != null && original.initialSharedVersion !== resolved2.initialSharedVersion || original.mutable != null && original.mutable !== resolved2.mutable || // Objects with owned object properties should not resolve to shared objects
            original.version != null || original.digest != null) {
              throw new Error(
                `Input at index ${i} did not match unresolved object. ${JSON.stringify(original)} is not compatible with ${JSON.stringify(resolved2)}`
              );
            }
          } else {
            throw new Error(
              `Input at index ${i} resolved to an unexpected Object kind: ${JSON.stringify(
                resolvedInput.Object
              )}`
            );
          }
          this.inputs[i] = resolvedInput;
          break;
      }
    }
  }
}

const NAME_SEPARATOR = "/";
function hasMvrName(nameOrType) {
  return nameOrType.includes(NAME_SEPARATOR) || nameOrType.includes("@") || nameOrType.includes(".sui");
}
function findNamesInTransaction(builder) {
  const packages = /* @__PURE__ */ new Set();
  const types = /* @__PURE__ */ new Set();
  for (const command of builder.commands) {
    switch (command.$kind) {
      case "MakeMoveVec":
        if (command.MakeMoveVec.type) {
          getNamesFromTypeList([command.MakeMoveVec.type]).forEach((type) => {
            types.add(type);
          });
        }
        break;
      case "MoveCall":
        const moveCall = command.MoveCall;
        const pkg = moveCall.package.split("::")[0];
        if (hasMvrName(pkg)) {
          if (!isValidNamedPackage(pkg)) throw new Error(`Invalid package name: ${pkg}`);
          packages.add(pkg);
        }
        getNamesFromTypeList(moveCall.typeArguments ?? []).forEach((type) => {
          types.add(type);
        });
        break;
    }
  }
  return {
    packages: [...packages],
    types: [...types]
  };
}
function replaceNames(builder, resolved) {
  for (const command of builder.commands) {
    if (command.MakeMoveVec?.type) {
      if (!hasMvrName(command.MakeMoveVec.type)) continue;
      if (!resolved.types[command.MakeMoveVec.type])
        throw new Error(`No resolution found for type: ${command.MakeMoveVec.type}`);
      command.MakeMoveVec.type = resolved.types[command.MakeMoveVec.type].type;
    }
    const tx = command.MoveCall;
    if (!tx) continue;
    const nameParts = tx.package.split("::");
    const name = nameParts[0];
    if (hasMvrName(name) && !resolved.packages[name])
      throw new Error(`No address found for package: ${name}`);
    if (hasMvrName(name)) {
      nameParts[0] = resolved.packages[name].package;
      tx.package = nameParts.join("::");
    }
    const types = tx.typeArguments;
    if (!types) continue;
    for (let i = 0; i < types.length; i++) {
      if (!hasMvrName(types[i])) continue;
      if (!resolved.types[types[i]]) throw new Error(`No resolution found for type: ${types[i]}`);
      types[i] = resolved.types[types[i]].type;
    }
    tx.typeArguments = types;
  }
}
function getNamesFromTypeList(types) {
  const names = /* @__PURE__ */ new Set();
  for (const type of types) {
    if (hasMvrName(type)) {
      if (!isValidNamedType(type)) throw new Error(`Invalid type with names: ${type}`);
      names.add(type);
    }
  }
  return names;
}

const namedPackagesPlugin = (options) => {
  return async (transactionData, buildOptions, next) => {
    const names = findNamesInTransaction(transactionData);
    if (names.types.length === 0 && names.packages.length === 0) {
      return next();
    }
    const resolved = await (getClient(buildOptions).core.mvr).resolve({
      types: names.types,
      packages: names.packages
    });
    replaceNames(transactionData, resolved);
    await next();
  };
};
function getClient(options) {
  if (!options.client) {
    throw new Error(
      `No sui client passed to Transaction#build, but transaction data was not sufficient to build offline.`
    );
  }
  return options.client;
}

var __typeError = (msg) => {
  throw TypeError(msg);
};
var __accessCheck = (obj, member, msg) => member.has(obj) || __typeError("Cannot " + msg);
var __privateGet = (obj, member, getter) => (__accessCheck(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd = (obj, member, value) => member.has(obj) ? __typeError("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet = (obj, member, value, setter) => (__accessCheck(obj, member, "write to private field"), member.set(obj, value), value);
var __privateMethod = (obj, member, method) => (__accessCheck(obj, member, "access private method"), method);
var _serializationPlugins, _buildPlugins, _intentResolvers, _inputSection, _commandSection, _availableResults, _pendingPromises, _added, _data, _Transaction_instances, fork_fn, addCommand_fn, addInput_fn, normalizeTransactionArgument_fn, resolveArgument_fn, prepareBuild_fn, runPlugins_fn, waitForPendingTasks_fn, sortCommandsAndInputs_fn;
function createTransactionResult(index, length = Infinity) {
  const baseResult = {
    $kind: "Result",
    get Result() {
      return typeof index === "function" ? index() : index;
    }
  };
  const nestedResults = [];
  const nestedResultFor = (resultIndex) => nestedResults[resultIndex] ?? (nestedResults[resultIndex] = {
    $kind: "NestedResult",
    get NestedResult() {
      return [typeof index === "function" ? index() : index, resultIndex];
    }
  });
  return new Proxy(baseResult, {
    set() {
      throw new Error(
        "The transaction result is a proxy, and does not support setting properties directly"
      );
    },
    // TODO: Instead of making this return a concrete argument, we should ideally
    // make it reference-based (so that this gets resolved at build-time), which
    // allows re-ordering transactions.
    get(target, property) {
      if (property in target) {
        return Reflect.get(target, property);
      }
      if (property === Symbol.iterator) {
        return function* () {
          let i = 0;
          while (i < length) {
            yield nestedResultFor(i);
            i++;
          }
        };
      }
      if (typeof property === "symbol") return;
      const resultIndex = parseInt(property, 10);
      if (Number.isNaN(resultIndex) || resultIndex < 0) return;
      return nestedResultFor(resultIndex);
    }
  });
}
const TRANSACTION_BRAND = Symbol.for("@mysten/transaction");
function isTransaction(obj) {
  return !!obj && typeof obj === "object" && obj[TRANSACTION_BRAND] === true;
}
const modulePluginRegistry = {
  buildPlugins: /* @__PURE__ */ new Map(),
  serializationPlugins: /* @__PURE__ */ new Map()
};
const TRANSACTION_REGISTRY_KEY = Symbol.for("@mysten/transaction/registry");
function getGlobalPluginRegistry() {
  try {
    const target = globalThis;
    if (!target[TRANSACTION_REGISTRY_KEY]) {
      target[TRANSACTION_REGISTRY_KEY] = modulePluginRegistry;
    }
    return target[TRANSACTION_REGISTRY_KEY];
  } catch {
    return modulePluginRegistry;
  }
}
const _Transaction = class _Transaction {
  constructor() {
    __privateAdd(this, _Transaction_instances);
    __privateAdd(this, _serializationPlugins);
    __privateAdd(this, _buildPlugins);
    __privateAdd(this, _intentResolvers, /* @__PURE__ */ new Map());
    __privateAdd(this, _inputSection, []);
    __privateAdd(this, _commandSection, []);
    __privateAdd(this, _availableResults, /* @__PURE__ */ new Set());
    __privateAdd(this, _pendingPromises, /* @__PURE__ */ new Set());
    __privateAdd(this, _added, /* @__PURE__ */ new Map());
    __privateAdd(this, _data);
    /**
     * Add a new object input to the transaction.
     */
    this.object = createObjectMethods(
      (value) => {
        if (typeof value === "function") {
          return this.object(this.add(value));
        }
        if (typeof value === "object" && is(ArgumentSchema, value)) {
          return value;
        }
        const id = getIdFromCallArg(value);
        const inserted = __privateGet(this, _data).inputs.find((i) => id === getIdFromCallArg(i));
        if (inserted?.Object?.SharedObject && typeof value === "object" && value.Object?.SharedObject) {
          inserted.Object.SharedObject.mutable = inserted.Object.SharedObject.mutable || value.Object.SharedObject.mutable;
        }
        return inserted ? { $kind: "Input", Input: __privateGet(this, _data).inputs.indexOf(inserted), type: "object" } : __privateMethod(this, _Transaction_instances, addInput_fn).call(this, "object", typeof value === "string" ? {
          $kind: "UnresolvedObject",
          UnresolvedObject: { objectId: normalizeSuiAddress(value) }
        } : value);
      }
    );
    const globalPlugins = getGlobalPluginRegistry();
    __privateSet(this, _data, new TransactionDataBuilder());
    __privateSet(this, _buildPlugins, [...globalPlugins.buildPlugins.values()]);
    __privateSet(this, _serializationPlugins, [...globalPlugins.serializationPlugins.values()]);
  }
  /**
   * Converts from a serialize transaction kind (built with `build({ onlyTransactionKind: true })`) to a `Transaction` class.
   * Supports either a byte array, or base64-encoded bytes.
   */
  static fromKind(serialized) {
    const tx = new _Transaction();
    __privateSet(tx, _data, TransactionDataBuilder.fromKindBytes(
      typeof serialized === "string" ? fromBase64(serialized) : serialized
    ));
    __privateSet(tx, _inputSection, __privateGet(tx, _data).inputs.slice());
    __privateSet(tx, _commandSection, __privateGet(tx, _data).commands.slice());
    __privateSet(tx, _availableResults, new Set(__privateGet(tx, _commandSection).map((_, i) => i)));
    return tx;
  }
  /**
   * Converts from a serialized transaction format to a `Transaction` class.
   * There are two supported serialized formats:
   * - A string returned from `Transaction#serialize`. The serialized format must be compatible, or it will throw an error.
   * - A byte array (or base64-encoded bytes) containing BCS transaction data.
   */
  static from(transaction) {
    const newTransaction = new _Transaction();
    if (isTransaction(transaction)) {
      __privateSet(newTransaction, _data, TransactionDataBuilder.restore(
        transaction.getData()
      ));
    } else if (typeof transaction !== "string" || !transaction.startsWith("{")) {
      __privateSet(newTransaction, _data, TransactionDataBuilder.fromBytes(
        typeof transaction === "string" ? fromBase64(transaction) : transaction
      ));
    } else {
      __privateSet(newTransaction, _data, TransactionDataBuilder.restore(JSON.parse(transaction)));
    }
    __privateSet(newTransaction, _inputSection, __privateGet(newTransaction, _data).inputs.slice());
    __privateSet(newTransaction, _commandSection, __privateGet(newTransaction, _data).commands.slice());
    __privateSet(newTransaction, _availableResults, new Set(__privateGet(newTransaction, _commandSection).map((_, i) => i)));
    return newTransaction;
  }
  static registerGlobalSerializationPlugin(stepOrStep, step) {
    getGlobalPluginRegistry().serializationPlugins.set(
      stepOrStep,
      step ?? stepOrStep
    );
  }
  static unregisterGlobalSerializationPlugin(name) {
    getGlobalPluginRegistry().serializationPlugins.delete(name);
  }
  static registerGlobalBuildPlugin(stepOrStep, step) {
    getGlobalPluginRegistry().buildPlugins.set(
      stepOrStep,
      step ?? stepOrStep
    );
  }
  static unregisterGlobalBuildPlugin(name) {
    getGlobalPluginRegistry().buildPlugins.delete(name);
  }
  addSerializationPlugin(step) {
    __privateGet(this, _serializationPlugins).push(step);
  }
  addBuildPlugin(step) {
    __privateGet(this, _buildPlugins).push(step);
  }
  addIntentResolver(intent, resolver) {
    if (__privateGet(this, _intentResolvers).has(intent) && __privateGet(this, _intentResolvers).get(intent) !== resolver) {
      throw new Error(`Intent resolver for ${intent} already exists`);
    }
    __privateGet(this, _intentResolvers).set(intent, resolver);
  }
  setSender(sender) {
    __privateGet(this, _data).sender = sender;
  }
  /**
   * Sets the sender only if it has not already been set.
   * This is useful for sponsored transaction flows where the sender may not be the same as the signer address.
   */
  setSenderIfNotSet(sender) {
    if (!__privateGet(this, _data).sender) {
      __privateGet(this, _data).sender = sender;
    }
  }
  setExpiration(expiration) {
    __privateGet(this, _data).expiration = expiration ? parse(TransactionExpiration$1, expiration) : null;
  }
  setGasPrice(price) {
    __privateGet(this, _data).gasConfig.price = String(price);
  }
  setGasBudget(budget) {
    __privateGet(this, _data).gasConfig.budget = String(budget);
  }
  setGasBudgetIfNotSet(budget) {
    if (__privateGet(this, _data).gasData.budget == null) {
      __privateGet(this, _data).gasConfig.budget = String(budget);
    }
  }
  setGasOwner(owner) {
    __privateGet(this, _data).gasConfig.owner = owner;
  }
  setGasPayment(payments) {
    __privateGet(this, _data).gasConfig.payment = payments.map((payment) => parse(ObjectRefSchema, payment));
  }
  /** @deprecated Use `getData()` instead. */
  get blockData() {
    return serializeV1TransactionData(__privateGet(this, _data).snapshot());
  }
  /** Get a snapshot of the transaction data, in JSON form: */
  getData() {
    return __privateGet(this, _data).snapshot();
  }
  // Used to brand transaction classes so that they can be identified, even between multiple copies
  // of the builder.
  get [TRANSACTION_BRAND]() {
    return true;
  }
  // Temporary workaround for the wallet interface accidentally serializing transactions via postMessage
  get pure() {
    Object.defineProperty(this, "pure", {
      enumerable: false,
      value: createPure((value) => {
        if (isSerializedBcs(value)) {
          return __privateMethod(this, _Transaction_instances, addInput_fn).call(this, "pure", {
            $kind: "Pure",
            Pure: {
              bytes: value.toBase64()
            }
          });
        }
        return __privateMethod(this, _Transaction_instances, addInput_fn).call(this, "pure", is(NormalizedCallArg$1, value) ? parse(NormalizedCallArg$1, value) : value instanceof Uint8Array ? Inputs.Pure(value) : { $kind: "UnresolvedPure", UnresolvedPure: { value } });
      })
    });
    return this.pure;
  }
  /** Returns an argument for the gas coin, to be used in a transaction. */
  get gas() {
    return { $kind: "GasCoin", GasCoin: true };
  }
  /**
   * Add a new object input to the transaction using the fully-resolved object reference.
   * If you only have an object ID, use `builder.object(id)` instead.
   */
  objectRef(...args) {
    return this.object(Inputs.ObjectRef(...args));
  }
  /**
   * Add a new receiving input to the transaction using the fully-resolved object reference.
   * If you only have an object ID, use `builder.object(id)` instead.
   */
  receivingRef(...args) {
    return this.object(Inputs.ReceivingRef(...args));
  }
  /**
   * Add a new shared object input to the transaction using the fully-resolved shared object reference.
   * If you only have an object ID, use `builder.object(id)` instead.
   */
  sharedObjectRef(...args) {
    return this.object(Inputs.SharedObjectRef(...args));
  }
  add(command) {
    if (typeof command === "function") {
      if (__privateGet(this, _added).has(command)) {
        return __privateGet(this, _added).get(command);
      }
      const fork = __privateMethod(this, _Transaction_instances, fork_fn).call(this);
      const result = command(fork);
      if (!(result && typeof result === "object" && "then" in result)) {
        __privateSet(this, _availableResults, __privateGet(fork, _availableResults));
        __privateGet(this, _added).set(command, result);
        return result;
      }
      const placeholder = __privateMethod(this, _Transaction_instances, addCommand_fn).call(this, {
        $kind: "$Intent",
        $Intent: {
          name: "AsyncTransactionThunk",
          inputs: {},
          data: {
            resultIndex: __privateGet(this, _data).commands.length,
            result: null
          }
        }
      });
      __privateGet(this, _pendingPromises).add(
        Promise.resolve(result).then((result2) => {
          placeholder.$Intent.data.result = result2;
        })
      );
      const txResult = createTransactionResult(() => placeholder.$Intent.data.resultIndex);
      __privateGet(this, _added).set(command, txResult);
      return txResult;
    } else {
      __privateMethod(this, _Transaction_instances, addCommand_fn).call(this, command);
    }
    return createTransactionResult(__privateGet(this, _data).commands.length - 1);
  }
  // Method shorthands:
  splitCoins(coin, amounts) {
    const command = Commands.SplitCoins(
      typeof coin === "string" ? this.object(coin) : __privateMethod(this, _Transaction_instances, resolveArgument_fn).call(this, coin),
      amounts.map(
        (amount) => typeof amount === "number" || typeof amount === "bigint" || typeof amount === "string" ? this.pure.u64(amount) : __privateMethod(this, _Transaction_instances, normalizeTransactionArgument_fn).call(this, amount)
      )
    );
    __privateMethod(this, _Transaction_instances, addCommand_fn).call(this, command);
    return createTransactionResult(__privateGet(this, _data).commands.length - 1, amounts.length);
  }
  mergeCoins(destination, sources) {
    return this.add(
      Commands.MergeCoins(
        this.object(destination),
        sources.map((src) => this.object(src))
      )
    );
  }
  publish({ modules, dependencies }) {
    return this.add(
      Commands.Publish({
        modules,
        dependencies
      })
    );
  }
  upgrade({
    modules,
    dependencies,
    package: packageId,
    ticket
  }) {
    return this.add(
      Commands.Upgrade({
        modules,
        dependencies,
        package: packageId,
        ticket: this.object(ticket)
      })
    );
  }
  moveCall({
    arguments: args,
    ...input
  }) {
    return this.add(
      Commands.MoveCall({
        ...input,
        arguments: args?.map((arg) => __privateMethod(this, _Transaction_instances, normalizeTransactionArgument_fn).call(this, arg))
      })
    );
  }
  transferObjects(objects, address) {
    return this.add(
      Commands.TransferObjects(
        objects.map((obj) => this.object(obj)),
        typeof address === "string" ? this.pure.address(address) : __privateMethod(this, _Transaction_instances, normalizeTransactionArgument_fn).call(this, address)
      )
    );
  }
  makeMoveVec({
    type,
    elements
  }) {
    return this.add(
      Commands.MakeMoveVec({
        type,
        elements: elements.map((obj) => this.object(obj))
      })
    );
  }
  /**
   * @deprecated Use toJSON instead.
   * For synchronous serialization, you can use `getData()`
   * */
  serialize() {
    return JSON.stringify(serializeV1TransactionData(__privateGet(this, _data).snapshot()));
  }
  async toJSON(options = {}) {
    await this.prepareForSerialization(options);
    const fullyResolved = this.isFullyResolved();
    return JSON.stringify(
      parse(
        SerializedTransactionDataV2Schema,
        fullyResolved ? {
          ...__privateGet(this, _data).snapshot(),
          digest: __privateGet(this, _data).getDigest()
        } : __privateGet(this, _data).snapshot()
      ),
      (_key, value) => typeof value === "bigint" ? value.toString() : value,
      2
    );
  }
  /** Build the transaction to BCS bytes, and sign it with the provided keypair. */
  async sign(options) {
    const { signer, ...buildOptions } = options;
    const bytes = await this.build(buildOptions);
    return signer.signTransaction(bytes);
  }
  /**
   *  Ensures that:
   *  - All objects have been fully resolved to a specific version
   *  - All pure inputs have been serialized to bytes
   *  - All async thunks have been fully resolved
   *  - All transaction intents have been resolved
   * 	- The gas payment, budget, and price have been set
   *  - The transaction sender has been set
   *
   *  When true, the transaction will always be built to the same bytes and digest (unless the transaction is mutated)
   */
  isFullyResolved() {
    if (!__privateGet(this, _data).sender) {
      return false;
    }
    if (__privateGet(this, _pendingPromises).size > 0) {
      return false;
    }
    if (__privateGet(this, _data).commands.some((cmd) => cmd.$Intent)) {
      return false;
    }
    if (needsTransactionResolution(__privateGet(this, _data), {})) {
      return false;
    }
    return true;
  }
  /** Build the transaction to BCS bytes. */
  async build(options = {}) {
    await this.prepareForSerialization(options);
    await __privateMethod(this, _Transaction_instances, prepareBuild_fn).call(this, options);
    return __privateGet(this, _data).build({
      onlyTransactionKind: options.onlyTransactionKind
    });
  }
  /** Derive transaction digest */
  async getDigest(options = {}) {
    await this.prepareForSerialization(options);
    await __privateMethod(this, _Transaction_instances, prepareBuild_fn).call(this, options);
    return __privateGet(this, _data).getDigest();
  }
  async prepareForSerialization(options) {
    await __privateMethod(this, _Transaction_instances, waitForPendingTasks_fn).call(this);
    __privateMethod(this, _Transaction_instances, sortCommandsAndInputs_fn).call(this);
    const intents = /* @__PURE__ */ new Set();
    for (const command of __privateGet(this, _data).commands) {
      if (command.$Intent) {
        intents.add(command.$Intent.name);
      }
    }
    const steps = [...__privateGet(this, _serializationPlugins)];
    for (const intent of intents) {
      if (options.supportedIntents?.includes(intent)) {
        continue;
      }
      if (!__privateGet(this, _intentResolvers).has(intent)) {
        throw new Error(`Missing intent resolver for ${intent}`);
      }
      steps.push(__privateGet(this, _intentResolvers).get(intent));
    }
    steps.push(namedPackagesPlugin());
    await __privateMethod(this, _Transaction_instances, runPlugins_fn).call(this, steps, options);
  }
};
_serializationPlugins = new WeakMap();
_buildPlugins = new WeakMap();
_intentResolvers = new WeakMap();
_inputSection = new WeakMap();
_commandSection = new WeakMap();
_availableResults = new WeakMap();
_pendingPromises = new WeakMap();
_added = new WeakMap();
_data = new WeakMap();
_Transaction_instances = new WeakSet();
fork_fn = function() {
  const fork = new _Transaction();
  __privateSet(fork, _data, __privateGet(this, _data));
  __privateSet(fork, _serializationPlugins, __privateGet(this, _serializationPlugins));
  __privateSet(fork, _buildPlugins, __privateGet(this, _buildPlugins));
  __privateSet(fork, _intentResolvers, __privateGet(this, _intentResolvers));
  __privateSet(fork, _pendingPromises, __privateGet(this, _pendingPromises));
  __privateSet(fork, _availableResults, new Set(__privateGet(this, _availableResults)));
  __privateSet(fork, _added, __privateGet(this, _added));
  __privateGet(this, _inputSection).push(__privateGet(fork, _inputSection));
  __privateGet(this, _commandSection).push(__privateGet(fork, _commandSection));
  return fork;
};
addCommand_fn = function(command) {
  const resultIndex = __privateGet(this, _data).commands.length;
  __privateGet(this, _commandSection).push(command);
  __privateGet(this, _availableResults).add(resultIndex);
  __privateGet(this, _data).commands.push(command);
  __privateGet(this, _data).mapCommandArguments(resultIndex, (arg) => {
    if (arg.$kind === "Result" && !__privateGet(this, _availableResults).has(arg.Result)) {
      throw new Error(
        `Result { Result: ${arg.Result} } is not available to use in the current transaction`
      );
    }
    if (arg.$kind === "NestedResult" && !__privateGet(this, _availableResults).has(arg.NestedResult[0])) {
      throw new Error(
        `Result { NestedResult: [${arg.NestedResult[0]}, ${arg.NestedResult[1]}] } is not available to use in the current transaction`
      );
    }
    if (arg.$kind === "Input" && arg.Input >= __privateGet(this, _data).inputs.length) {
      throw new Error(
        `Input { Input: ${arg.Input} } references an input that does not exist in the current transaction`
      );
    }
    return arg;
  });
  return command;
};
addInput_fn = function(type, input) {
  __privateGet(this, _inputSection).push(input);
  return __privateGet(this, _data).addInput(type, input);
};
normalizeTransactionArgument_fn = function(arg) {
  if (isSerializedBcs(arg)) {
    return this.pure(arg);
  }
  return __privateMethod(this, _Transaction_instances, resolveArgument_fn).call(this, arg);
};
resolveArgument_fn = function(arg) {
  if (typeof arg === "function") {
    const resolved = this.add(arg);
    if (typeof resolved === "function") {
      return __privateMethod(this, _Transaction_instances, resolveArgument_fn).call(this, resolved);
    }
    return parse(ArgumentSchema, resolved);
  }
  return parse(ArgumentSchema, arg);
};
prepareBuild_fn = async function(options) {
  if (!options.onlyTransactionKind && !__privateGet(this, _data).sender) {
    throw new Error("Missing transaction sender");
  }
  await __privateMethod(this, _Transaction_instances, runPlugins_fn).call(this, [...__privateGet(this, _buildPlugins), resolveTransactionPlugin], options);
};
runPlugins_fn = async function(plugins, options) {
  try {
    const createNext = (i) => {
      if (i >= plugins.length) {
        return () => {
        };
      }
      const plugin = plugins[i];
      return async () => {
        const next = createNext(i + 1);
        let calledNext = false;
        let nextResolved = false;
        await plugin(__privateGet(this, _data), options, async () => {
          if (calledNext) {
            throw new Error(`next() was call multiple times in TransactionPlugin ${i}`);
          }
          calledNext = true;
          await next();
          nextResolved = true;
        });
        if (!calledNext) {
          throw new Error(`next() was not called in TransactionPlugin ${i}`);
        }
        if (!nextResolved) {
          throw new Error(`next() was not awaited in TransactionPlugin ${i}`);
        }
      };
    };
    await createNext(0)();
  } finally {
    __privateSet(this, _inputSection, __privateGet(this, _data).inputs.slice());
    __privateSet(this, _commandSection, __privateGet(this, _data).commands.slice());
    __privateSet(this, _availableResults, new Set(__privateGet(this, _commandSection).map((_, i) => i)));
  }
};
waitForPendingTasks_fn = async function() {
  while (__privateGet(this, _pendingPromises).size > 0) {
    const newPromise = Promise.all(__privateGet(this, _pendingPromises));
    __privateGet(this, _pendingPromises).clear();
    __privateGet(this, _pendingPromises).add(newPromise);
    await newPromise;
    __privateGet(this, _pendingPromises).delete(newPromise);
  }
};
sortCommandsAndInputs_fn = function() {
  const unorderedCommands = __privateGet(this, _data).commands;
  const unorderedInputs = __privateGet(this, _data).inputs;
  const orderedCommands = __privateGet(this, _commandSection).flat(Infinity);
  const orderedInputs = __privateGet(this, _inputSection).flat(Infinity);
  if (orderedCommands.length !== unorderedCommands.length) {
    throw new Error("Unexpected number of commands found in transaction data");
  }
  if (orderedInputs.length !== unorderedInputs.length) {
    throw new Error("Unexpected number of inputs found in transaction data");
  }
  const filteredCommands = orderedCommands.filter(
    (cmd) => cmd.$Intent?.name !== "AsyncTransactionThunk"
  );
  __privateGet(this, _data).commands = filteredCommands;
  __privateGet(this, _data).inputs = orderedInputs;
  __privateSet(this, _commandSection, filteredCommands);
  __privateSet(this, _inputSection, orderedInputs);
  __privateSet(this, _availableResults, new Set(filteredCommands.map((_, i) => i)));
  function getOriginalIndex(index) {
    const command = unorderedCommands[index];
    if (command.$Intent?.name === "AsyncTransactionThunk") {
      const result = command.$Intent.data.result;
      if (result == null) {
        throw new Error("AsyncTransactionThunk has not been resolved");
      }
      return getOriginalIndex(result.Result);
    }
    const updated = filteredCommands.indexOf(command);
    if (updated === -1) {
      throw new Error("Unable to find original index for command");
    }
    return updated;
  }
  __privateGet(this, _data).mapArguments((arg) => {
    if (arg.$kind === "Input") {
      const updated = orderedInputs.indexOf(unorderedInputs[arg.Input]);
      if (updated === -1) {
        throw new Error("Input has not been resolved");
      }
      return { ...arg, Input: updated };
    } else if (arg.$kind === "Result") {
      const updated = getOriginalIndex(arg.Result);
      return { ...arg, Result: updated };
    } else if (arg.$kind === "NestedResult") {
      const updated = getOriginalIndex(arg.NestedResult[0]);
      return { ...arg, NestedResult: [updated, arg.NestedResult[1]] };
    }
    return arg;
  });
  for (const [i, cmd] of unorderedCommands.entries()) {
    if (cmd.$Intent?.name === "AsyncTransactionThunk") {
      try {
        cmd.$Intent.data.resultIndex = getOriginalIndex(i);
      } catch {
      }
    }
  }
};
let Transaction = _Transaction;

const smashCoins = defineLoggedTask({
  meta: {
    name: "execute:smash-coins",
    description: "Smash all sui coins into a single gas coin"
  },
  async run() {
    const { digest } = await queue.runTask(async () => {
      await serialExecutor.waitForLastTransaction();
      const coins = await loadAllCoins();
      const firstCoin = coins.shift();
      let digest2;
      let gasCoin = {
        objectId: firstCoin.coinObjectId,
        digest: firstCoin.digest,
        version: firstCoin.version
      };
      while (coins.length) {
        console.log(`Smashing coins: ${coins.length} remaining`);
        const transaction = new Transaction();
        transaction.setGasPayment([
          gasCoin,
          ...coins.splice(0, 254).map((coin) => ({
            objectId: coin.coinObjectId,
            digest: coin.digest,
            version: coin.version
          }))
        ]);
        const result = await suiClient.signAndExecuteTransaction({
          transaction,
          signer: keypair
        });
        digest2 = result.digest;
        const { effects } = await suiClient.waitForTransaction({
          digest: digest2,
          options: {
            showEffects: true
          }
        });
        gasCoin = effects.gasObject.reference;
      }
      await serialExecutor.resetCache();
      return { digest: digest2 };
    });
    return {
      result: {
        digest
      }
    };
  },
  logResult: (result) => ({
    digest: result.digest
  })
});
async function loadAllCoins() {
  const coins = [];
  let hasMore = true;
  let cursor = null;
  while (hasMore) {
    const page = await suiClient.getCoins({
      cursor,
      owner: keypair.toSuiAddress()
    });
    coins.push(...page.data);
    hasMore = page.hasNextPage;
    cursor = page.nextCursor;
  }
  coins.sort((a, b) => Number(BigInt(b.balance) - BigInt(a.balance)));
  return coins;
}

export { smashCoins as default };
//# sourceMappingURL=smash-coins.mjs.map
