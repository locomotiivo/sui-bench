import { n as normalizeSuiAddress, S as SUI_ADDRESS_LENGTH, i as isValidSuiAddress } from './sui-types.mjs';

/*! scure-base - MIT License (c) 2022 Paul Miller (paulmillr.com) */
function isBytes(a) {
    return a instanceof Uint8Array || (ArrayBuffer.isView(a) && a.constructor.name === 'Uint8Array');
}
function isArrayOf(isString, arr) {
    if (!Array.isArray(arr))
        return false;
    if (arr.length === 0)
        return true;
    if (isString) {
        return arr.every((item) => typeof item === 'string');
    }
    else {
        return arr.every((item) => Number.isSafeInteger(item));
    }
}
function astr(label, input) {
    if (typeof input !== 'string')
        throw new Error(`${label}: string expected`);
    return true;
}
function anumber(n) {
    if (!Number.isSafeInteger(n))
        throw new Error(`invalid integer: ${n}`);
}
function aArr(input) {
    if (!Array.isArray(input))
        throw new Error('array expected');
}
function astrArr(label, input) {
    if (!isArrayOf(true, input))
        throw new Error(`${label}: array of strings expected`);
}
function anumArr(label, input) {
    if (!isArrayOf(false, input))
        throw new Error(`${label}: array of numbers expected`);
}
/**
 * @__NO_SIDE_EFFECTS__
 */
function chain(...args) {
    const id = (a) => a;
    // Wrap call in closure so JIT can inline calls
    const wrap = (a, b) => (c) => a(b(c));
    // Construct chain of args[-1].encode(args[-2].encode([...]))
    const encode = args.map((x) => x.encode).reduceRight(wrap, id);
    // Construct chain of args[0].decode(args[1].decode(...))
    const decode = args.map((x) => x.decode).reduce(wrap, id);
    return { encode, decode };
}
/**
 * Encodes integer radix representation to array of strings using alphabet and back.
 * Could also be array of strings.
 * @__NO_SIDE_EFFECTS__
 */
function alphabet(letters) {
    // mapping 1 to "b"
    const lettersA = typeof letters === 'string' ? letters.split('') : letters;
    const len = lettersA.length;
    astrArr('alphabet', lettersA);
    // mapping "b" to 1
    const indexes = new Map(lettersA.map((l, i) => [l, i]));
    return {
        encode: (digits) => {
            aArr(digits);
            return digits.map((i) => {
                if (!Number.isSafeInteger(i) || i < 0 || i >= len)
                    throw new Error(`alphabet.encode: digit index outside alphabet "${i}". Allowed: ${letters}`);
                return lettersA[i];
            });
        },
        decode: (input) => {
            aArr(input);
            return input.map((letter) => {
                astr('alphabet.decode', letter);
                const i = indexes.get(letter);
                if (i === undefined)
                    throw new Error(`Unknown letter: "${letter}". Allowed: ${letters}`);
                return i;
            });
        },
    };
}
/**
 * @__NO_SIDE_EFFECTS__
 */
function join(separator = '') {
    astr('join', separator);
    return {
        encode: (from) => {
            astrArr('join.decode', from);
            return from.join(separator);
        },
        decode: (to) => {
            astr('join.decode', to);
            return to.split(separator);
        },
    };
}
/**
 * Slow: O(n^2) time complexity
 */
function convertRadix(data, from, to) {
    // base 1 is impossible
    if (from < 2)
        throw new Error(`convertRadix: invalid from=${from}, base cannot be less than 2`);
    if (to < 2)
        throw new Error(`convertRadix: invalid to=${to}, base cannot be less than 2`);
    aArr(data);
    if (!data.length)
        return [];
    let pos = 0;
    const res = [];
    const digits = Array.from(data, (d) => {
        anumber(d);
        if (d < 0 || d >= from)
            throw new Error(`invalid integer: ${d}`);
        return d;
    });
    const dlen = digits.length;
    while (true) {
        let carry = 0;
        let done = true;
        for (let i = pos; i < dlen; i++) {
            const digit = digits[i];
            const fromCarry = from * carry;
            const digitBase = fromCarry + digit;
            if (!Number.isSafeInteger(digitBase) ||
                fromCarry / from !== carry ||
                digitBase - digit !== fromCarry) {
                throw new Error('convertRadix: carry overflow');
            }
            const div = digitBase / to;
            carry = digitBase % to;
            const rounded = Math.floor(div);
            digits[i] = rounded;
            if (!Number.isSafeInteger(rounded) || rounded * to + carry !== digitBase)
                throw new Error('convertRadix: carry overflow');
            if (!done)
                continue;
            else if (!rounded)
                pos = i;
            else
                done = false;
        }
        res.push(carry);
        if (done)
            break;
    }
    for (let i = 0; i < data.length - 1 && data[i] === 0; i++)
        res.push(0);
    return res.reverse();
}
/**
 * @__NO_SIDE_EFFECTS__
 */
function radix(num) {
    anumber(num);
    const _256 = 2 ** 8;
    return {
        encode: (bytes) => {
            if (!isBytes(bytes))
                throw new Error('radix.encode input should be Uint8Array');
            return convertRadix(Array.from(bytes), _256, num);
        },
        decode: (digits) => {
            anumArr('radix.decode', digits);
            return Uint8Array.from(convertRadix(digits, num, _256));
        },
    };
}
// base58 code
// -----------
const genBase58 = /* @__NO_SIDE_EFFECTS__ */ (abc) => chain(radix(58), alphabet(abc), join(''));
/**
 * base58: base64 without ambigous characters +, /, 0, O, I, l.
 * Quadratic (O(n^2)) - so, can't be used on large inputs.
 * @example
 * ```js
 * base58.decode('01abcdef');
 * // => '3UhJW'
 * ```
 */
const base58 = genBase58('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz');

const toBase58 = (buffer) => base58.encode(buffer);
const fromBase58 = (str) => base58.decode(str);

function fromBase64(base64String) {
  return Uint8Array.from(atob(base64String), (char) => char.charCodeAt(0));
}
const CHUNK_SIZE = 8192;
function toBase64(bytes) {
  if (bytes.length < CHUNK_SIZE) {
    return btoa(String.fromCharCode(...bytes));
  }
  let output = "";
  for (var i = 0; i < bytes.length; i += CHUNK_SIZE) {
    const chunk = bytes.slice(i, i + CHUNK_SIZE);
    output += String.fromCharCode(...chunk);
  }
  return btoa(output);
}

function fromHex(hexStr) {
  const normalized = hexStr.startsWith("0x") ? hexStr.slice(2) : hexStr;
  const padded = normalized.length % 2 === 0 ? normalized : `0${normalized}`;
  const intArr = padded.match(/[0-9a-fA-F]{2}/g)?.map((byte) => parseInt(byte, 16)) ?? [];
  if (intArr.length !== padded.length / 2) {
    throw new Error(`Invalid hex string ${hexStr}`);
  }
  return Uint8Array.from(intArr);
}
function toHex(bytes) {
  return bytes.reduce((str, byte) => str + byte.toString(16).padStart(2, "0"), "");
}

function ulebEncode(num) {
  let bigNum = BigInt(num);
  const arr = [];
  let len = 0;
  if (bigNum === 0n) {
    return [0];
  }
  while (bigNum > 0) {
    arr[len] = Number(bigNum & 0x7fn);
    bigNum >>= 7n;
    if (bigNum > 0n) {
      arr[len] |= 128;
    }
    len += 1;
  }
  return arr;
}
function ulebDecode(arr) {
  let total = 0n;
  let shift = 0n;
  let len = 0;
  while (true) {
    if (len >= arr.length) {
      throw new Error("ULEB decode error: buffer overflow");
    }
    const byte = arr[len];
    len += 1;
    total += BigInt(byte & 127) << shift;
    if ((byte & 128) === 0) {
      break;
    }
    shift += 7n;
  }
  if (total > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error("ULEB decode error: value exceeds MAX_SAFE_INTEGER");
  }
  return {
    value: Number(total),
    length: len
  };
}

class BcsReader {
  /**
   * @param {Uint8Array} data Data to use as a buffer.
   */
  constructor(data) {
    this.bytePosition = 0;
    this.dataView = new DataView(data.buffer, data.byteOffset, data.byteLength);
  }
  /**
   * Shift current cursor position by `bytes`.
   *
   * @param {Number} bytes Number of bytes to
   * @returns {this} Self for possible chaining.
   */
  shift(bytes) {
    this.bytePosition += bytes;
    return this;
  }
  /**
   * Read U8 value from the buffer and shift cursor by 1.
   * @returns
   */
  read8() {
    const value = this.dataView.getUint8(this.bytePosition);
    this.shift(1);
    return value;
  }
  /**
   * Read U16 value from the buffer and shift cursor by 2.
   * @returns
   */
  read16() {
    const value = this.dataView.getUint16(this.bytePosition, true);
    this.shift(2);
    return value;
  }
  /**
   * Read U32 value from the buffer and shift cursor by 4.
   * @returns
   */
  read32() {
    const value = this.dataView.getUint32(this.bytePosition, true);
    this.shift(4);
    return value;
  }
  /**
   * Read U64 value from the buffer and shift cursor by 8.
   * @returns
   */
  read64() {
    const value1 = this.read32();
    const value2 = this.read32();
    const result = value2.toString(16) + value1.toString(16).padStart(8, "0");
    return BigInt("0x" + result).toString(10);
  }
  /**
   * Read U128 value from the buffer and shift cursor by 16.
   */
  read128() {
    const value1 = BigInt(this.read64());
    const value2 = BigInt(this.read64());
    const result = value2.toString(16) + value1.toString(16).padStart(16, "0");
    return BigInt("0x" + result).toString(10);
  }
  /**
   * Read U128 value from the buffer and shift cursor by 32.
   * @returns
   */
  read256() {
    const value1 = BigInt(this.read128());
    const value2 = BigInt(this.read128());
    const result = value2.toString(16) + value1.toString(16).padStart(32, "0");
    return BigInt("0x" + result).toString(10);
  }
  /**
   * Read `num` number of bytes from the buffer and shift cursor by `num`.
   * @param num Number of bytes to read.
   */
  readBytes(num) {
    const start = this.bytePosition + this.dataView.byteOffset;
    const value = new Uint8Array(this.dataView.buffer, start, num);
    this.shift(num);
    return value;
  }
  /**
   * Read ULEB value - an integer of varying size. Used for enum indexes and
   * vector lengths.
   * @returns {Number} The ULEB value.
   */
  readULEB() {
    const start = this.bytePosition + this.dataView.byteOffset;
    const buffer = new Uint8Array(this.dataView.buffer, start);
    const { value, length } = ulebDecode(buffer);
    this.shift(length);
    return value;
  }
  /**
   * Read a BCS vector: read a length and then apply function `cb` X times
   * where X is the length of the vector, defined as ULEB in BCS bytes.
   * @param cb Callback to process elements of vector.
   * @returns {Array<Any>} Array of the resulting values, returned by callback.
   */
  readVec(cb) {
    const length = this.readULEB();
    const result = [];
    for (let i = 0; i < length; i++) {
      result.push(cb(this, i, length));
    }
    return result;
  }
}

function encodeStr(data, encoding) {
  switch (encoding) {
    case "base58":
      return toBase58(data);
    case "base64":
      return toBase64(data);
    case "hex":
      return toHex(data);
    default:
      throw new Error("Unsupported encoding, supported values are: base64, hex");
  }
}
function splitGenericParameters(str, genericSeparators = ["<", ">"]) {
  const [left, right] = genericSeparators;
  const tok = [];
  let word = "";
  let nestedAngleBrackets = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    if (char === left) {
      nestedAngleBrackets++;
    }
    if (char === right) {
      nestedAngleBrackets--;
    }
    if (nestedAngleBrackets === 0 && char === ",") {
      tok.push(word.trim());
      word = "";
      continue;
    }
    word += char;
  }
  tok.push(word.trim());
  return tok;
}

class BcsWriter {
  constructor({
    initialSize = 1024,
    maxSize = Infinity,
    allocateSize = 1024
  } = {}) {
    this.bytePosition = 0;
    this.size = initialSize;
    this.maxSize = maxSize;
    this.allocateSize = allocateSize;
    this.dataView = new DataView(new ArrayBuffer(initialSize));
  }
  ensureSizeOrGrow(bytes) {
    const requiredSize = this.bytePosition + bytes;
    if (requiredSize > this.size) {
      const nextSize = Math.min(
        this.maxSize,
        Math.max(this.size + requiredSize, this.size + this.allocateSize)
      );
      if (requiredSize > nextSize) {
        throw new Error(
          `Attempting to serialize to BCS, but buffer does not have enough size. Allocated size: ${this.size}, Max size: ${this.maxSize}, Required size: ${requiredSize}`
        );
      }
      this.size = nextSize;
      const nextBuffer = new ArrayBuffer(this.size);
      new Uint8Array(nextBuffer).set(new Uint8Array(this.dataView.buffer));
      this.dataView = new DataView(nextBuffer);
    }
  }
  /**
   * Shift current cursor position by `bytes`.
   *
   * @param {Number} bytes Number of bytes to
   * @returns {this} Self for possible chaining.
   */
  shift(bytes) {
    this.bytePosition += bytes;
    return this;
  }
  /**
   * Write a U8 value into a buffer and shift cursor position by 1.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  write8(value) {
    this.ensureSizeOrGrow(1);
    this.dataView.setUint8(this.bytePosition, Number(value));
    return this.shift(1);
  }
  /**
   * Write a U8 value into a buffer and shift cursor position by 1.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  writeBytes(bytes) {
    this.ensureSizeOrGrow(bytes.length);
    for (let i = 0; i < bytes.length; i++) {
      this.dataView.setUint8(this.bytePosition + i, bytes[i]);
    }
    return this.shift(bytes.length);
  }
  /**
   * Write a U16 value into a buffer and shift cursor position by 2.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  write16(value) {
    this.ensureSizeOrGrow(2);
    this.dataView.setUint16(this.bytePosition, Number(value), true);
    return this.shift(2);
  }
  /**
   * Write a U32 value into a buffer and shift cursor position by 4.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  write32(value) {
    this.ensureSizeOrGrow(4);
    this.dataView.setUint32(this.bytePosition, Number(value), true);
    return this.shift(4);
  }
  /**
   * Write a U64 value into a buffer and shift cursor position by 8.
   * @param {bigint} value Value to write.
   * @returns {this}
   */
  write64(value) {
    toLittleEndian(BigInt(value), 8).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a U128 value into a buffer and shift cursor position by 16.
   *
   * @param {bigint} value Value to write.
   * @returns {this}
   */
  write128(value) {
    toLittleEndian(BigInt(value), 16).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a U256 value into a buffer and shift cursor position by 16.
   *
   * @param {bigint} value Value to write.
   * @returns {this}
   */
  write256(value) {
    toLittleEndian(BigInt(value), 32).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a ULEB value into a buffer and shift cursor position by number of bytes
   * written.
   * @param {Number} value Value to write.
   * @returns {this}
   */
  writeULEB(value) {
    ulebEncode(value).forEach((el) => this.write8(el));
    return this;
  }
  /**
   * Write a vector into a buffer by first writing the vector length and then calling
   * a callback on each passed value.
   *
   * @param {Array<Any>} vector Array of elements to write.
   * @param {WriteVecCb} cb Callback to call on each element of the vector.
   * @returns {this}
   */
  writeVec(vector, cb) {
    this.writeULEB(vector.length);
    Array.from(vector).forEach((el, i) => cb(this, el, i, vector.length));
    return this;
  }
  /**
   * Adds support for iterations over the object.
   * @returns {Uint8Array}
   */
  // oxlint-disable-next-line require-yields
  *[Symbol.iterator]() {
    for (let i = 0; i < this.bytePosition; i++) {
      yield this.dataView.getUint8(i);
    }
    return this.toBytes();
  }
  /**
   * Get underlying buffer taking only value bytes (in case initial buffer size was bigger).
   * @returns {Uint8Array} Resulting bcs.
   */
  toBytes() {
    return new Uint8Array(this.dataView.buffer.slice(0, this.bytePosition));
  }
  /**
   * Represent data as 'hex' or 'base64'
   * @param encoding Encoding to use: 'base64' or 'hex'
   */
  toString(encoding) {
    return encodeStr(this.toBytes(), encoding);
  }
}
function toLittleEndian(bigint, size) {
  const result = new Uint8Array(size);
  let i = 0;
  while (bigint > 0) {
    result[i] = Number(bigint % BigInt(256));
    bigint = bigint / BigInt(256);
    i += 1;
  }
  return result;
}

var __typeError = (msg) => {
  throw TypeError(msg);
};
var __accessCheck = (obj, member, msg) => member.has(obj) || __typeError("Cannot " + msg);
var __privateGet = (obj, member, getter) => (__accessCheck(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd = (obj, member, value) => member.has(obj) ? __typeError("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet = (obj, member, value, setter) => (__accessCheck(obj, member, "write to private field"), member.set(obj, value), value);
var _write, _serialize, _schema, _bytes;
const _BcsType = class _BcsType {
  constructor(options) {
    __privateAdd(this, _write);
    __privateAdd(this, _serialize);
    this.name = options.name;
    this.read = options.read;
    this.serializedSize = options.serializedSize ?? (() => null);
    __privateSet(this, _write, options.write);
    __privateSet(this, _serialize, options.serialize ?? ((value, options2) => {
      const writer = new BcsWriter({
        initialSize: this.serializedSize(value) ?? void 0,
        ...options2
      });
      __privateGet(this, _write).call(this, value, writer);
      return writer.toBytes();
    }));
    this.validate = options.validate ?? (() => {
    });
  }
  write(value, writer) {
    this.validate(value);
    __privateGet(this, _write).call(this, value, writer);
  }
  serialize(value, options) {
    this.validate(value);
    return new SerializedBcs(this, __privateGet(this, _serialize).call(this, value, options));
  }
  parse(bytes) {
    const reader = new BcsReader(bytes);
    return this.read(reader);
  }
  fromHex(hex) {
    return this.parse(fromHex(hex));
  }
  fromBase58(b64) {
    return this.parse(fromBase58(b64));
  }
  fromBase64(b64) {
    return this.parse(fromBase64(b64));
  }
  transform({
    name,
    input,
    output,
    validate
  }) {
    return new _BcsType({
      name: name ?? this.name,
      read: (reader) => output ? output(this.read(reader)) : this.read(reader),
      write: (value, writer) => __privateGet(this, _write).call(this, input ? input(value) : value, writer),
      serializedSize: (value) => this.serializedSize(input ? input(value) : value),
      serialize: (value, options) => __privateGet(this, _serialize).call(this, input ? input(value) : value, options),
      validate: (value) => {
        validate?.(value);
        this.validate(input ? input(value) : value);
      }
    });
  }
};
_write = new WeakMap();
_serialize = new WeakMap();
let BcsType = _BcsType;
const SERIALIZED_BCS_BRAND = Symbol.for("@mysten/serialized-bcs");
function isSerializedBcs(obj) {
  return !!obj && typeof obj === "object" && obj[SERIALIZED_BCS_BRAND] === true;
}
class SerializedBcs {
  constructor(schema, bytes) {
    __privateAdd(this, _schema);
    __privateAdd(this, _bytes);
    __privateSet(this, _schema, schema);
    __privateSet(this, _bytes, bytes);
  }
  // Used to brand SerializedBcs so that they can be identified, even between multiple copies
  // of the @mysten/bcs package are installed
  get [SERIALIZED_BCS_BRAND]() {
    return true;
  }
  toBytes() {
    return __privateGet(this, _bytes);
  }
  toHex() {
    return toHex(__privateGet(this, _bytes));
  }
  toBase64() {
    return toBase64(__privateGet(this, _bytes));
  }
  toBase58() {
    return toBase58(__privateGet(this, _bytes));
  }
  parse() {
    return __privateGet(this, _schema).parse(__privateGet(this, _bytes));
  }
}
_schema = new WeakMap();
_bytes = new WeakMap();
function fixedSizeBcsType({
  size,
  ...options
}) {
  return new BcsType({
    ...options,
    serializedSize: () => size
  });
}
function uIntBcsType({
  readMethod,
  writeMethod,
  ...options
}) {
  return fixedSizeBcsType({
    ...options,
    read: (reader) => reader[readMethod](),
    write: (value, writer) => writer[writeMethod](value),
    validate: (value) => {
      if (value < 0 || value > options.maxValue) {
        throw new TypeError(
          `Invalid ${options.name} value: ${value}. Expected value in range 0-${options.maxValue}`
        );
      }
      options.validate?.(value);
    }
  });
}
function bigUIntBcsType({
  readMethod,
  writeMethod,
  ...options
}) {
  return fixedSizeBcsType({
    ...options,
    read: (reader) => reader[readMethod](),
    write: (value, writer) => writer[writeMethod](BigInt(value)),
    validate: (val) => {
      const value = BigInt(val);
      if (value < 0 || value > options.maxValue) {
        throw new TypeError(
          `Invalid ${options.name} value: ${value}. Expected value in range 0-${options.maxValue}`
        );
      }
      options.validate?.(value);
    }
  });
}
function dynamicSizeBcsType({
  serialize,
  ...options
}) {
  const type = new BcsType({
    ...options,
    serialize,
    write: (value, writer) => {
      for (const byte of type.serialize(value).toBytes()) {
        writer.write8(byte);
      }
    }
  });
  return type;
}
function stringLikeBcsType({
  toBytes,
  fromBytes,
  ...options
}) {
  return new BcsType({
    ...options,
    read: (reader) => {
      const length = reader.readULEB();
      const bytes = reader.readBytes(length);
      return fromBytes(bytes);
    },
    write: (hex, writer) => {
      const bytes = toBytes(hex);
      writer.writeULEB(bytes.length);
      for (let i = 0; i < bytes.length; i++) {
        writer.write8(bytes[i]);
      }
    },
    serialize: (value) => {
      const bytes = toBytes(value);
      const size = ulebEncode(bytes.length);
      const result = new Uint8Array(size.length + bytes.length);
      result.set(size, 0);
      result.set(bytes, size.length);
      return result;
    },
    validate: (value) => {
      if (typeof value !== "string") {
        throw new TypeError(`Invalid ${options.name} value: ${value}. Expected string`);
      }
      options.validate?.(value);
    }
  });
}
function lazyBcsType(cb) {
  let lazyType = null;
  function getType() {
    if (!lazyType) {
      lazyType = cb();
    }
    return lazyType;
  }
  return new BcsType({
    name: "lazy",
    read: (data) => getType().read(data),
    serializedSize: (value) => getType().serializedSize(value),
    write: (value, writer) => getType().write(value, writer),
    serialize: (value, options) => getType().serialize(value, options).toBytes()
  });
}
class BcsStruct extends BcsType {
  constructor({ name, fields, ...options }) {
    const canonicalOrder = Object.entries(fields);
    super({
      name,
      serializedSize: (values) => {
        let total = 0;
        for (const [field, type] of canonicalOrder) {
          const size = type.serializedSize(values[field]);
          if (size == null) {
            return null;
          }
          total += size;
        }
        return total;
      },
      read: (reader) => {
        const result = {};
        for (const [field, type] of canonicalOrder) {
          result[field] = type.read(reader);
        }
        return result;
      },
      write: (value, writer) => {
        for (const [field, type] of canonicalOrder) {
          type.write(value[field], writer);
        }
      },
      ...options,
      validate: (value) => {
        options?.validate?.(value);
        if (typeof value !== "object" || value == null) {
          throw new TypeError(`Expected object, found ${typeof value}`);
        }
      }
    });
  }
}
class BcsEnum extends BcsType {
  constructor({ fields, ...options }) {
    const canonicalOrder = Object.entries(fields);
    super({
      read: (reader) => {
        const index = reader.readULEB();
        const enumEntry = canonicalOrder[index];
        if (!enumEntry) {
          throw new TypeError(`Unknown value ${index} for enum ${options.name}`);
        }
        const [kind, type] = enumEntry;
        return {
          [kind]: type?.read(reader) ?? true,
          $kind: kind
        };
      },
      write: (value, writer) => {
        const [name, val] = Object.entries(value).filter(
          ([name2]) => Object.hasOwn(fields, name2)
        )[0];
        for (let i = 0; i < canonicalOrder.length; i++) {
          const [optionName, optionType] = canonicalOrder[i];
          if (optionName === name) {
            writer.writeULEB(i);
            optionType?.write(val, writer);
            return;
          }
        }
      },
      ...options,
      validate: (value) => {
        options?.validate?.(value);
        if (typeof value !== "object" || value == null) {
          throw new TypeError(`Expected object, found ${typeof value}`);
        }
        const keys = Object.keys(value).filter(
          (k) => value[k] !== void 0 && Object.hasOwn(fields, k)
        );
        if (keys.length !== 1) {
          throw new TypeError(
            `Expected object with one key, but found ${keys.length} for type ${options.name}}`
          );
        }
        const [variant] = keys;
        if (!Object.hasOwn(fields, variant)) {
          throw new TypeError(`Invalid enum variant ${variant}`);
        }
      }
    });
  }
}
class BcsTuple extends BcsType {
  constructor({ fields, name, ...options }) {
    super({
      name: name ?? `(${fields.map((t) => t.name).join(", ")})`,
      serializedSize: (values) => {
        let total = 0;
        for (let i = 0; i < fields.length; i++) {
          const size = fields[i].serializedSize(values[i]);
          if (size == null) {
            return null;
          }
          total += size;
        }
        return total;
      },
      read: (reader) => {
        const result = [];
        for (const field of fields) {
          result.push(field.read(reader));
        }
        return result;
      },
      write: (value, writer) => {
        for (let i = 0; i < fields.length; i++) {
          fields[i].write(value[i], writer);
        }
      },
      ...options,
      validate: (value) => {
        options?.validate?.(value);
        if (!Array.isArray(value)) {
          throw new TypeError(`Expected array, found ${typeof value}`);
        }
        if (value.length !== fields.length) {
          throw new TypeError(`Expected array of length ${fields.length}, found ${value.length}`);
        }
      }
    });
  }
}

function fixedArray(size, type, options) {
  return new BcsType({
    read: (reader) => {
      const result = new Array(size);
      for (let i = 0; i < size; i++) {
        result[i] = type.read(reader);
      }
      return result;
    },
    write: (value, writer) => {
      for (const item of value) {
        type.write(item, writer);
      }
    },
    ...options,
    name: options?.name ?? `${type.name}[${size}]`,
    validate: (value) => {
      options?.validate?.(value);
      if (!value || typeof value !== "object" || !("length" in value)) {
        throw new TypeError(`Expected array, found ${typeof value}`);
      }
      if (value.length !== size) {
        throw new TypeError(`Expected array of length ${size}, found ${value.length}`);
      }
    }
  });
}
function option(type) {
  return bcs.enum(`Option<${type.name}>`, {
    None: null,
    Some: type
  }).transform({
    input: (value) => {
      if (value == null) {
        return { None: true };
      }
      return { Some: value };
    },
    output: (value) => {
      if (value.$kind === "Some") {
        return value.Some;
      }
      return null;
    }
  });
}
function vector(type, options) {
  return new BcsType({
    read: (reader) => {
      const length = reader.readULEB();
      const result = new Array(length);
      for (let i = 0; i < length; i++) {
        result[i] = type.read(reader);
      }
      return result;
    },
    write: (value, writer) => {
      writer.writeULEB(value.length);
      for (const item of value) {
        type.write(item, writer);
      }
    },
    ...options,
    name: options?.name ?? `vector<${type.name}>`,
    validate: (value) => {
      options?.validate?.(value);
      if (!value || typeof value !== "object" || !("length" in value)) {
        throw new TypeError(`Expected array, found ${typeof value}`);
      }
    }
  });
}
function map(keyType, valueType) {
  return bcs.vector(bcs.tuple([keyType, valueType])).transform({
    name: `Map<${keyType.name}, ${valueType.name}>`,
    input: (value) => {
      return [...value.entries()];
    },
    output: (value) => {
      const result = /* @__PURE__ */ new Map();
      for (const [key, val] of value) {
        result.set(key, val);
      }
      return result;
    }
  });
}
const bcs = {
  /**
   * Creates a BcsType that can be used to read and write an 8-bit unsigned integer.
   * @example
   * bcs.u8().serialize(255).toBytes() // Uint8Array [ 255 ]
   */
  u8(options) {
    return uIntBcsType({
      readMethod: "read8",
      writeMethod: "write8",
      size: 1,
      maxValue: 2 ** 8 - 1,
      ...options,
      name: options?.name ?? "u8"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 16-bit unsigned integer.
   * @example
   * bcs.u16().serialize(65535).toBytes() // Uint8Array [ 255, 255 ]
   */
  u16(options) {
    return uIntBcsType({
      readMethod: "read16",
      writeMethod: "write16",
      size: 2,
      maxValue: 2 ** 16 - 1,
      ...options,
      name: options?.name ?? "u16"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 32-bit unsigned integer.
   * @example
   * bcs.u32().serialize(4294967295).toBytes() // Uint8Array [ 255, 255, 255, 255 ]
   */
  u32(options) {
    return uIntBcsType({
      readMethod: "read32",
      writeMethod: "write32",
      size: 4,
      maxValue: 2 ** 32 - 1,
      ...options,
      name: options?.name ?? "u32"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 64-bit unsigned integer.
   * @example
   * bcs.u64().serialize(1).toBytes() // Uint8Array [ 1, 0, 0, 0, 0, 0, 0, 0 ]
   */
  u64(options) {
    return bigUIntBcsType({
      readMethod: "read64",
      writeMethod: "write64",
      size: 8,
      maxValue: 2n ** 64n - 1n,
      ...options,
      name: options?.name ?? "u64"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 128-bit unsigned integer.
   * @example
   * bcs.u128().serialize(1).toBytes() // Uint8Array [ 1, ..., 0 ]
   */
  u128(options) {
    return bigUIntBcsType({
      readMethod: "read128",
      writeMethod: "write128",
      size: 16,
      maxValue: 2n ** 128n - 1n,
      ...options,
      name: options?.name ?? "u128"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write a 256-bit unsigned integer.
   * @example
   * bcs.u256().serialize(1).toBytes() // Uint8Array [ 1, ..., 0 ]
   */
  u256(options) {
    return bigUIntBcsType({
      readMethod: "read256",
      writeMethod: "write256",
      size: 32,
      maxValue: 2n ** 256n - 1n,
      ...options,
      name: options?.name ?? "u256"
    });
  },
  /**
   * Creates a BcsType that can be used to read and write boolean values.
   * @example
   * bcs.bool().serialize(true).toBytes() // Uint8Array [ 1 ]
   */
  bool(options) {
    return fixedSizeBcsType({
      size: 1,
      read: (reader) => reader.read8() === 1,
      write: (value, writer) => writer.write8(value ? 1 : 0),
      ...options,
      name: options?.name ?? "bool",
      validate: (value) => {
        options?.validate?.(value);
        if (typeof value !== "boolean") {
          throw new TypeError(`Expected boolean, found ${typeof value}`);
        }
      }
    });
  },
  /**
   * Creates a BcsType that can be used to read and write unsigned LEB encoded integers
   * @example
   *
   */
  uleb128(options) {
    return dynamicSizeBcsType({
      read: (reader) => reader.readULEB(),
      serialize: (value) => {
        return Uint8Array.from(ulebEncode(value));
      },
      ...options,
      name: options?.name ?? "uleb128"
    });
  },
  /**
   * Creates a BcsType representing a fixed length byte array
   * @param size The number of bytes this types represents
   * @example
   * bcs.bytes(3).serialize(new Uint8Array([1, 2, 3])).toBytes() // Uint8Array [1, 2, 3]
   */
  bytes(size, options) {
    return fixedSizeBcsType({
      size,
      read: (reader) => reader.readBytes(size),
      write: (value, writer) => {
        writer.writeBytes(new Uint8Array(value));
      },
      ...options,
      name: options?.name ?? `bytes[${size}]`,
      validate: (value) => {
        options?.validate?.(value);
        if (!value || typeof value !== "object" || !("length" in value)) {
          throw new TypeError(`Expected array, found ${typeof value}`);
        }
        if (value.length !== size) {
          throw new TypeError(`Expected array of length ${size}, found ${value.length}`);
        }
      }
    });
  },
  /**
   * Creates a BcsType representing a variable length byte array
   *
   * @example
   * bcs.byteVector().serialize([1, 2, 3]).toBytes() // Uint8Array [3, 1, 2, 3]
   */
  byteVector(options) {
    return new BcsType({
      read: (reader) => {
        const length = reader.readULEB();
        return reader.readBytes(length);
      },
      write: (value, writer) => {
        const array = new Uint8Array(value);
        writer.writeULEB(array.length);
        writer.writeBytes(array);
      },
      ...options,
      name: options?.name ?? "vector<u8>",
      serializedSize: (value) => {
        const length = "length" in value ? value.length : null;
        return length == null ? null : ulebEncode(length).length + length;
      },
      validate: (value) => {
        options?.validate?.(value);
        if (!value || typeof value !== "object" || !("length" in value)) {
          throw new TypeError(`Expected array, found ${typeof value}`);
        }
      }
    });
  },
  /**
   * Creates a BcsType that can ser/de string values.  Strings will be UTF-8 encoded
   * @example
   * bcs.string().serialize('a').toBytes() // Uint8Array [ 1, 97 ]
   */
  string(options) {
    return stringLikeBcsType({
      toBytes: (value) => new TextEncoder().encode(value),
      fromBytes: (bytes) => new TextDecoder().decode(bytes),
      ...options,
      name: options?.name ?? "string"
    });
  },
  /**
   * Creates a BcsType that represents a fixed length array of a given type
   * @param size The number of elements in the array
   * @param type The BcsType of each element in the array
   * @example
   * bcs.fixedArray(3, bcs.u8()).serialize([1, 2, 3]).toBytes() // Uint8Array [ 1, 2, 3 ]
   */
  fixedArray,
  /**
   * Creates a BcsType representing an optional value
   * @param type The BcsType of the optional value
   * @example
   * bcs.option(bcs.u8()).serialize(null).toBytes() // Uint8Array [ 0 ]
   * bcs.option(bcs.u8()).serialize(1).toBytes() // Uint8Array [ 1, 1 ]
   */
  option,
  /**
   * Creates a BcsType representing a variable length vector of a given type
   * @param type The BcsType of each element in the vector
   *
   * @example
   * bcs.vector(bcs.u8()).toBytes([1, 2, 3]) // Uint8Array [ 3, 1, 2, 3 ]
   */
  vector,
  /**
   * Creates a BcsType representing a tuple of a given set of types
   * @param types The BcsTypes for each element in the tuple
   *
   * @example
   * const tuple = bcs.tuple([bcs.u8(), bcs.string(), bcs.bool()])
   * tuple.serialize([1, 'a', true]).toBytes() // Uint8Array [ 1, 1, 97, 1 ]
   */
  tuple(fields, options) {
    return new BcsTuple({
      fields,
      ...options
    });
  },
  /**
   * Creates a BcsType representing a struct of a given set of fields
   * @param name The name of the struct
   * @param fields The fields of the struct. The order of the fields affects how data is serialized and deserialized
   *
   * @example
   * const struct = bcs.struct('MyStruct', {
   *  a: bcs.u8(),
   *  b: bcs.string(),
   * })
   * struct.serialize({ a: 1, b: 'a' }).toBytes() // Uint8Array [ 1, 1, 97 ]
   */
  struct(name, fields, options) {
    return new BcsStruct({
      name,
      fields,
      ...options
    });
  },
  /**
   * Creates a BcsType representing an enum of a given set of options
   * @param name The name of the enum
   * @param values The values of the enum. The order of the values affects how data is serialized and deserialized.
   * null can be used to represent a variant with no data.
   *
   * @example
   * const enum = bcs.enum('MyEnum', {
   *   A: bcs.u8(),
   *   B: bcs.string(),
   *   C: null,
   * })
   * enum.serialize({ A: 1 }).toBytes() // Uint8Array [ 0, 1 ]
   * enum.serialize({ B: 'a' }).toBytes() // Uint8Array [ 1, 1, 97 ]
   * enum.serialize({ C: true }).toBytes() // Uint8Array [ 2 ]
   */
  enum(name, fields, options) {
    return new BcsEnum({
      name,
      fields,
      ...options
    });
  },
  /**
   * Creates a BcsType representing a map of a given key and value type
   * @param keyType The BcsType of the key
   * @param valueType The BcsType of the value
   * @example
   * const map = bcs.map(bcs.u8(), bcs.string())
   * map.serialize(new Map([[2, 'a']])).toBytes() // Uint8Array [ 1, 2, 1, 97 ]
   */
  map,
  /**
   * Creates a BcsType that wraps another BcsType which is lazily evaluated. This is useful for creating recursive types.
   * @param cb A callback that returns the BcsType
   */
  lazy(cb) {
    return lazyBcsType(cb);
  }
};

const VECTOR_REGEX = /^vector<(.+)>$/;
const STRUCT_REGEX = /^([^:]+)::([^:]+)::([^<]+)(<(.+)>)?/;
class TypeTagSerializer {
  static parseFromStr(str, normalizeAddress = false) {
    if (str === "address") {
      return { address: null };
    } else if (str === "bool") {
      return { bool: null };
    } else if (str === "u8") {
      return { u8: null };
    } else if (str === "u16") {
      return { u16: null };
    } else if (str === "u32") {
      return { u32: null };
    } else if (str === "u64") {
      return { u64: null };
    } else if (str === "u128") {
      return { u128: null };
    } else if (str === "u256") {
      return { u256: null };
    } else if (str === "signer") {
      return { signer: null };
    }
    const vectorMatch = str.match(VECTOR_REGEX);
    if (vectorMatch) {
      return {
        vector: TypeTagSerializer.parseFromStr(vectorMatch[1], normalizeAddress)
      };
    }
    const structMatch = str.match(STRUCT_REGEX);
    if (structMatch) {
      const address = normalizeAddress ? normalizeSuiAddress(structMatch[1]) : structMatch[1];
      return {
        struct: {
          address,
          module: structMatch[2],
          name: structMatch[3],
          typeParams: structMatch[5] === void 0 ? [] : TypeTagSerializer.parseStructTypeArgs(structMatch[5], normalizeAddress)
        }
      };
    }
    throw new Error(`Encountered unexpected token when parsing type args for ${str}`);
  }
  static parseStructTypeArgs(str, normalizeAddress = false) {
    return splitGenericParameters(str).map(
      (tok) => TypeTagSerializer.parseFromStr(tok, normalizeAddress)
    );
  }
  static tagToString(tag) {
    if ("bool" in tag) {
      return "bool";
    }
    if ("u8" in tag) {
      return "u8";
    }
    if ("u16" in tag) {
      return "u16";
    }
    if ("u32" in tag) {
      return "u32";
    }
    if ("u64" in tag) {
      return "u64";
    }
    if ("u128" in tag) {
      return "u128";
    }
    if ("u256" in tag) {
      return "u256";
    }
    if ("address" in tag) {
      return "address";
    }
    if ("signer" in tag) {
      return "signer";
    }
    if ("vector" in tag) {
      return `vector<${TypeTagSerializer.tagToString(tag.vector)}>`;
    }
    if ("struct" in tag) {
      const struct = tag.struct;
      const typeParams = struct.typeParams.map(TypeTagSerializer.tagToString).join(", ");
      return `${struct.address}::${struct.module}::${struct.name}${typeParams ? `<${typeParams}>` : ""}`;
    }
    throw new Error("Invalid TypeTag");
  }
}

function unsafe_u64(options) {
  return bcs.u64({
    name: "unsafe_u64",
    ...options
  }).transform({
    input: (val) => val,
    output: (val) => Number(val)
  });
}
function optionEnum(type) {
  return bcs.enum("Option", {
    None: null,
    Some: type
  });
}
const Address = bcs.bytes(SUI_ADDRESS_LENGTH).transform({
  validate: (val) => {
    const address = typeof val === "string" ? val : toHex(val);
    if (!address || !isValidSuiAddress(normalizeSuiAddress(address))) {
      throw new Error(`Invalid Sui address ${address}`);
    }
  },
  input: (val) => typeof val === "string" ? fromHex(normalizeSuiAddress(val)) : val,
  output: (val) => normalizeSuiAddress(toHex(val))
});
const ObjectDigest = bcs.byteVector().transform({
  name: "ObjectDigest",
  input: (value) => fromBase58(value),
  output: (value) => toBase58(new Uint8Array(value)),
  validate: (value) => {
    if (fromBase58(value).length !== 32) {
      throw new Error("ObjectDigest must be 32 bytes");
    }
  }
});
const SuiObjectRef = bcs.struct("SuiObjectRef", {
  objectId: Address,
  version: bcs.u64(),
  digest: ObjectDigest
});
const SharedObjectRef = bcs.struct("SharedObjectRef", {
  objectId: Address,
  initialSharedVersion: bcs.u64(),
  mutable: bcs.bool()
});
const ObjectArg = bcs.enum("ObjectArg", {
  ImmOrOwnedObject: SuiObjectRef,
  SharedObject: SharedObjectRef,
  Receiving: SuiObjectRef
});
const Owner = bcs.enum("Owner", {
  AddressOwner: Address,
  ObjectOwner: Address,
  Shared: bcs.struct("Shared", {
    initialSharedVersion: bcs.u64()
  }),
  Immutable: null,
  ConsensusAddressOwner: bcs.struct("ConsensusAddressOwner", {
    owner: Address,
    startVersion: bcs.u64()
  })
});
const CallArg = bcs.enum("CallArg", {
  Pure: bcs.struct("Pure", {
    bytes: bcs.byteVector().transform({
      input: (val) => typeof val === "string" ? fromBase64(val) : val,
      output: (val) => toBase64(new Uint8Array(val))
    })
  }),
  Object: ObjectArg
});
const InnerTypeTag = bcs.enum("TypeTag", {
  bool: null,
  u8: null,
  u64: null,
  u128: null,
  address: null,
  signer: null,
  vector: bcs.lazy(() => InnerTypeTag),
  struct: bcs.lazy(() => StructTag),
  u16: null,
  u32: null,
  u256: null
});
const TypeTag = InnerTypeTag.transform({
  input: (typeTag) => typeof typeTag === "string" ? TypeTagSerializer.parseFromStr(typeTag, true) : typeTag,
  output: (typeTag) => TypeTagSerializer.tagToString(typeTag)
});
const Argument = bcs.enum("Argument", {
  GasCoin: null,
  Input: bcs.u16(),
  Result: bcs.u16(),
  NestedResult: bcs.tuple([bcs.u16(), bcs.u16()])
});
const ProgrammableMoveCall = bcs.struct("ProgrammableMoveCall", {
  package: Address,
  module: bcs.string(),
  function: bcs.string(),
  typeArguments: bcs.vector(TypeTag),
  arguments: bcs.vector(Argument)
});
const Command = bcs.enum("Command", {
  /**
   * A Move Call - any public Move function can be called via
   * this transaction. The results can be used that instant to pass
   * into the next transaction.
   */
  MoveCall: ProgrammableMoveCall,
  /**
   * Transfer vector of objects to a receiver.
   */
  TransferObjects: bcs.struct("TransferObjects", {
    objects: bcs.vector(Argument),
    address: Argument
  }),
  // /**
  //  * Split `amount` from a `coin`.
  //  */
  SplitCoins: bcs.struct("SplitCoins", {
    coin: Argument,
    amounts: bcs.vector(Argument)
  }),
  // /**
  //  * Merge Vector of Coins (`sources`) into a `destination`.
  //  */
  MergeCoins: bcs.struct("MergeCoins", {
    destination: Argument,
    sources: bcs.vector(Argument)
  }),
  // /**
  //  * Publish a Move module.
  //  */
  Publish: bcs.struct("Publish", {
    modules: bcs.vector(
      bcs.byteVector().transform({
        input: (val) => typeof val === "string" ? fromBase64(val) : val,
        output: (val) => toBase64(new Uint8Array(val))
      })
    ),
    dependencies: bcs.vector(Address)
  }),
  // /**
  //  * Build a vector of objects using the input arguments.
  //  * It is impossible to export construct a `vector<T: key>` otherwise,
  //  * so this call serves a utility function.
  //  */
  MakeMoveVec: bcs.struct("MakeMoveVec", {
    type: optionEnum(TypeTag).transform({
      input: (val) => val === null ? {
        None: true
      } : {
        Some: val
      },
      output: (val) => val.Some ?? null
    }),
    elements: bcs.vector(Argument)
  }),
  Upgrade: bcs.struct("Upgrade", {
    modules: bcs.vector(
      bcs.byteVector().transform({
        input: (val) => typeof val === "string" ? fromBase64(val) : val,
        output: (val) => toBase64(new Uint8Array(val))
      })
    ),
    dependencies: bcs.vector(Address),
    package: Address,
    ticket: Argument
  })
});
const ProgrammableTransaction = bcs.struct("ProgrammableTransaction", {
  inputs: bcs.vector(CallArg),
  commands: bcs.vector(Command)
});
const TransactionKind = bcs.enum("TransactionKind", {
  ProgrammableTransaction,
  ChangeEpoch: null,
  Genesis: null,
  ConsensusCommitPrologue: null
});
const TransactionExpiration = bcs.enum("TransactionExpiration", {
  None: null,
  Epoch: unsafe_u64()
});
const StructTag = bcs.struct("StructTag", {
  address: Address,
  module: bcs.string(),
  name: bcs.string(),
  typeParams: bcs.vector(InnerTypeTag)
});
const GasData = bcs.struct("GasData", {
  payment: bcs.vector(SuiObjectRef),
  owner: Address,
  price: bcs.u64(),
  budget: bcs.u64()
});
const TransactionDataV1 = bcs.struct("TransactionDataV1", {
  kind: TransactionKind,
  sender: Address,
  gasData: GasData,
  expiration: TransactionExpiration
});
const TransactionData = bcs.enum("TransactionData", {
  V1: TransactionDataV1
});
const IntentScope = bcs.enum("IntentScope", {
  TransactionData: null,
  TransactionEffects: null,
  CheckpointSummary: null,
  PersonalMessage: null
});
const IntentVersion = bcs.enum("IntentVersion", {
  V0: null
});
const AppId = bcs.enum("AppId", {
  Sui: null
});
const Intent = bcs.struct("Intent", {
  scope: IntentScope,
  version: IntentVersion,
  appId: AppId
});
function IntentMessage(T) {
  return bcs.struct(`IntentMessage<${T.name}>`, {
    intent: Intent,
    value: T
  });
}
const CompressedSignature = bcs.enum("CompressedSignature", {
  ED25519: bcs.bytes(64),
  Secp256k1: bcs.bytes(64),
  Secp256r1: bcs.bytes(64),
  ZkLogin: bcs.byteVector(),
  Passkey: bcs.byteVector()
});
const PublicKey = bcs.enum("PublicKey", {
  ED25519: bcs.bytes(32),
  Secp256k1: bcs.bytes(33),
  Secp256r1: bcs.bytes(33),
  ZkLogin: bcs.byteVector(),
  Passkey: bcs.bytes(33)
});
const MultiSigPkMap = bcs.struct("MultiSigPkMap", {
  pubKey: PublicKey,
  weight: bcs.u8()
});
const MultiSigPublicKey = bcs.struct("MultiSigPublicKey", {
  pk_map: bcs.vector(MultiSigPkMap),
  threshold: bcs.u16()
});
const MultiSig = bcs.struct("MultiSig", {
  sigs: bcs.vector(CompressedSignature),
  bitmap: bcs.u16(),
  multisig_pk: MultiSigPublicKey
});
const base64String = bcs.byteVector().transform({
  input: (val) => typeof val === "string" ? fromBase64(val) : val,
  output: (val) => toBase64(new Uint8Array(val))
});
const SenderSignedTransaction = bcs.struct("SenderSignedTransaction", {
  intentMessage: IntentMessage(TransactionData),
  txSignatures: bcs.vector(base64String)
});
const SenderSignedData = bcs.vector(SenderSignedTransaction, {
  name: "SenderSignedData"
});
const PasskeyAuthenticator = bcs.struct("PasskeyAuthenticator", {
  authenticatorData: bcs.byteVector(),
  clientDataJson: bcs.string(),
  userSignature: bcs.byteVector()
});

const PackageUpgradeError = bcs.enum("PackageUpgradeError", {
  UnableToFetchPackage: bcs.struct("UnableToFetchPackage", { packageId: Address }),
  NotAPackage: bcs.struct("NotAPackage", { objectId: Address }),
  IncompatibleUpgrade: null,
  DigestDoesNotMatch: bcs.struct("DigestDoesNotMatch", { digest: bcs.byteVector() }),
  UnknownUpgradePolicy: bcs.struct("UnknownUpgradePolicy", { policy: bcs.u8() }),
  PackageIDDoesNotMatch: bcs.struct("PackageIDDoesNotMatch", {
    packageId: Address,
    ticketId: Address
  })
});
const ModuleId = bcs.struct("ModuleId", {
  address: Address,
  name: bcs.string()
});
const MoveLocation = bcs.struct("MoveLocation", {
  module: ModuleId,
  function: bcs.u16(),
  instruction: bcs.u16(),
  functionName: bcs.option(bcs.string())
});
const CommandArgumentError = bcs.enum("CommandArgumentError", {
  TypeMismatch: null,
  InvalidBCSBytes: null,
  InvalidUsageOfPureArg: null,
  InvalidArgumentToPrivateEntryFunction: null,
  IndexOutOfBounds: bcs.struct("IndexOutOfBounds", { idx: bcs.u16() }),
  SecondaryIndexOutOfBounds: bcs.struct("SecondaryIndexOutOfBounds", {
    resultIdx: bcs.u16(),
    secondaryIdx: bcs.u16()
  }),
  InvalidResultArity: bcs.struct("InvalidResultArity", { resultIdx: bcs.u16() }),
  InvalidGasCoinUsage: null,
  InvalidValueUsage: null,
  InvalidObjectByValue: null,
  InvalidObjectByMutRef: null,
  SharedObjectOperationNotAllowed: null
});
const TypeArgumentError = bcs.enum("TypeArgumentError", {
  TypeNotFound: null,
  ConstraintNotSatisfied: null
});
const ExecutionFailureStatus = bcs.enum("ExecutionFailureStatus", {
  InsufficientGas: null,
  InvalidGasObject: null,
  InvariantViolation: null,
  FeatureNotYetSupported: null,
  MoveObjectTooBig: bcs.struct("MoveObjectTooBig", {
    objectSize: bcs.u64(),
    maxObjectSize: bcs.u64()
  }),
  MovePackageTooBig: bcs.struct("MovePackageTooBig", {
    objectSize: bcs.u64(),
    maxObjectSize: bcs.u64()
  }),
  CircularObjectOwnership: bcs.struct("CircularObjectOwnership", { object: Address }),
  InsufficientCoinBalance: null,
  CoinBalanceOverflow: null,
  PublishErrorNonZeroAddress: null,
  SuiMoveVerificationError: null,
  MovePrimitiveRuntimeError: bcs.option(MoveLocation),
  MoveAbort: bcs.tuple([MoveLocation, bcs.u64()]),
  VMVerificationOrDeserializationError: null,
  VMInvariantViolation: null,
  FunctionNotFound: null,
  ArityMismatch: null,
  TypeArityMismatch: null,
  NonEntryFunctionInvoked: null,
  CommandArgumentError: bcs.struct("CommandArgumentError", {
    argIdx: bcs.u16(),
    kind: CommandArgumentError
  }),
  TypeArgumentError: bcs.struct("TypeArgumentError", {
    argumentIdx: bcs.u16(),
    kind: TypeArgumentError
  }),
  UnusedValueWithoutDrop: bcs.struct("UnusedValueWithoutDrop", {
    resultIdx: bcs.u16(),
    secondaryIdx: bcs.u16()
  }),
  InvalidPublicFunctionReturnType: bcs.struct("InvalidPublicFunctionReturnType", {
    idx: bcs.u16()
  }),
  InvalidTransferObject: null,
  EffectsTooLarge: bcs.struct("EffectsTooLarge", { currentSize: bcs.u64(), maxSize: bcs.u64() }),
  PublishUpgradeMissingDependency: null,
  PublishUpgradeDependencyDowngrade: null,
  PackageUpgradeError: bcs.struct("PackageUpgradeError", { upgradeError: PackageUpgradeError }),
  WrittenObjectsTooLarge: bcs.struct("WrittenObjectsTooLarge", {
    currentSize: bcs.u64(),
    maxSize: bcs.u64()
  }),
  CertificateDenied: null,
  SuiMoveVerificationTimedout: null,
  SharedObjectOperationNotAllowed: null,
  InputObjectDeleted: null,
  ExecutionCancelledDueToSharedObjectCongestion: bcs.struct(
    "ExecutionCancelledDueToSharedObjectCongestion",
    {
      congestedObjects: bcs.vector(Address)
    }
  ),
  AddressDeniedForCoin: bcs.struct("AddressDeniedForCoin", {
    address: Address,
    coinType: bcs.string()
  }),
  CoinTypeGlobalPause: bcs.struct("CoinTypeGlobalPause", { coinType: bcs.string() }),
  ExecutionCancelledDueToRandomnessUnavailable: null
});
const ExecutionStatus = bcs.enum("ExecutionStatus", {
  Success: null,
  Failed: bcs.struct("ExecutionFailed", {
    error: ExecutionFailureStatus,
    command: bcs.option(bcs.u64())
  })
});
const GasCostSummary = bcs.struct("GasCostSummary", {
  computationCost: bcs.u64(),
  storageCost: bcs.u64(),
  storageRebate: bcs.u64(),
  nonRefundableStorageFee: bcs.u64()
});
const TransactionEffectsV1 = bcs.struct("TransactionEffectsV1", {
  status: ExecutionStatus,
  executedEpoch: bcs.u64(),
  gasUsed: GasCostSummary,
  modifiedAtVersions: bcs.vector(bcs.tuple([Address, bcs.u64()])),
  sharedObjects: bcs.vector(SuiObjectRef),
  transactionDigest: ObjectDigest,
  created: bcs.vector(bcs.tuple([SuiObjectRef, Owner])),
  mutated: bcs.vector(bcs.tuple([SuiObjectRef, Owner])),
  unwrapped: bcs.vector(bcs.tuple([SuiObjectRef, Owner])),
  deleted: bcs.vector(SuiObjectRef),
  unwrappedThenDeleted: bcs.vector(SuiObjectRef),
  wrapped: bcs.vector(SuiObjectRef),
  gasObject: bcs.tuple([SuiObjectRef, Owner]),
  eventsDigest: bcs.option(ObjectDigest),
  dependencies: bcs.vector(ObjectDigest)
});
const VersionDigest = bcs.tuple([bcs.u64(), ObjectDigest]);
const ObjectIn = bcs.enum("ObjectIn", {
  NotExist: null,
  Exist: bcs.tuple([VersionDigest, Owner])
});
const ObjectOut = bcs.enum("ObjectOut", {
  NotExist: null,
  ObjectWrite: bcs.tuple([ObjectDigest, Owner]),
  PackageWrite: VersionDigest
});
const IDOperation = bcs.enum("IDOperation", {
  None: null,
  Created: null,
  Deleted: null
});
const EffectsObjectChange = bcs.struct("EffectsObjectChange", {
  inputState: ObjectIn,
  outputState: ObjectOut,
  idOperation: IDOperation
});
const UnchangedSharedKind = bcs.enum("UnchangedSharedKind", {
  ReadOnlyRoot: VersionDigest,
  // TODO: these have been renamed to MutateConsensusStreamEnded and ReadConsensusStreamEnded
  MutateDeleted: bcs.u64(),
  ReadDeleted: bcs.u64(),
  Cancelled: bcs.u64(),
  PerEpochConfig: null
});
const TransactionEffectsV2 = bcs.struct("TransactionEffectsV2", {
  status: ExecutionStatus,
  executedEpoch: bcs.u64(),
  gasUsed: GasCostSummary,
  transactionDigest: ObjectDigest,
  gasObjectIndex: bcs.option(bcs.u32()),
  eventsDigest: bcs.option(ObjectDigest),
  dependencies: bcs.vector(ObjectDigest),
  lamportVersion: bcs.u64(),
  changedObjects: bcs.vector(bcs.tuple([Address, EffectsObjectChange])),
  unchangedSharedObjects: bcs.vector(bcs.tuple([Address, UnchangedSharedKind])),
  auxDataDigest: bcs.option(ObjectDigest)
});
const TransactionEffects = bcs.enum("TransactionEffects", {
  V1: TransactionEffectsV1,
  V2: TransactionEffectsV2
});

const suiBcs = {
  ...bcs,
  U8: bcs.u8(),
  U16: bcs.u16(),
  U32: bcs.u32(),
  U64: bcs.u64(),
  U128: bcs.u128(),
  U256: bcs.u256(),
  ULEB128: bcs.uleb128(),
  Bool: bcs.bool(),
  String: bcs.string(),
  Address,
  AppId,
  Argument,
  CallArg,
  Command,
  CompressedSignature,
  GasData,
  Intent,
  IntentMessage,
  IntentScope,
  IntentVersion,
  MultiSig,
  MultiSigPkMap,
  MultiSigPublicKey,
  ObjectArg,
  ObjectDigest,
  Owner,
  PasskeyAuthenticator,
  ProgrammableMoveCall,
  ProgrammableTransaction,
  PublicKey,
  SenderSignedData,
  SenderSignedTransaction,
  SharedObjectRef,
  StructTag,
  SuiObjectRef,
  TransactionData,
  TransactionDataV1,
  TransactionEffects,
  TransactionExpiration,
  TransactionKind,
  TypeTag
};

export { Address as A, TypeTagSerializer as T, toBase58 as a, bcs as b, fromBase64 as f, isSerializedBcs as i, suiBcs as s, toBase64 as t };
//# sourceMappingURL=index.mjs.map
