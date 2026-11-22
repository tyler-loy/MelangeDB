/**
 * The MessagePack subset MelangeDB framing uses, hand-rolled so the package has no dependencies.
 * The writer emits the shortest encoding of every value; the reader accepts every valid width
 * (CLIENT-CONFORMANCE C2.1) and returns 64-bit integers as `bigint` (C2.2).
 */

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

/** Thrown when bytes are not well-formed MessagePack or not a frame shape this client knows. */
export class ProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtocolError";
  }
}

export class MsgPackWriter {
  private buffer: Uint8Array;
  private view: DataView;
  private position = 0;

  constructor(sizeHint = 64) {
    this.buffer = new Uint8Array(Math.max(sizeHint, 32));
    this.view = new DataView(this.buffer.buffer);
  }

  nil(): void {
    this.byte(0xc0);
  }

  bool(value: boolean): void {
    this.byte(value ? 0xc3 : 0xc2);
  }

  /** An integer in its shortest form. A `number` must be a safe integer. */
  int(value: number | bigint): void {
    let big: bigint;
    if (typeof value === "number") {
      if (!Number.isSafeInteger(value)) throw new RangeError(`${value} is not a safe integer.`);
      big = BigInt(value);
    } else {
      big = value;
    }

    if (big >= 0n) {
      this.uint(big);
      return;
    }

    if (big >= -32n) {
      this.byte(Number(big) & 0xff);
    } else if (big >= -128n) {
      this.byte(0xd0);
      this.ensure(1);
      this.view.setInt8(this.position++, Number(big));
    } else if (big >= -32768n) {
      this.byte(0xd1);
      this.ensure(2);
      this.view.setInt16(this.position, Number(big));
      this.position += 2;
    } else if (big >= -2147483648n) {
      this.byte(0xd2);
      this.ensure(4);
      this.view.setInt32(this.position, Number(big));
      this.position += 4;
    } else {
      if (big < -(1n << 63n)) throw new RangeError(`${big} is below Int64.`);
      this.byte(0xd3);
      this.ensure(8);
      this.view.setBigInt64(this.position, big);
      this.position += 8;
    }
  }

  private uint(value: bigint): void {
    if (value <= 0x7fn) {
      this.byte(Number(value));
    } else if (value <= 0xffn) {
      this.byte(0xcc);
      this.byte(Number(value));
    } else if (value <= 0xffffn) {
      this.byte(0xcd);
      this.ensure(2);
      this.view.setUint16(this.position, Number(value));
      this.position += 2;
    } else if (value <= 0xffffffffn) {
      this.byte(0xce);
      this.ensure(4);
      this.view.setUint32(this.position, Number(value));
      this.position += 4;
    } else {
      if (value >= 1n << 64n) throw new RangeError(`${value} is above UInt64.`);
      this.byte(0xcf);
      this.ensure(8);
      this.view.setBigUint64(this.position, value);
      this.position += 8;
    }
  }

  float64(value: number): void {
    this.byte(0xcb);
    this.ensure(8);
    this.view.setFloat64(this.position, value);
    this.position += 8;
  }

  str(value: string | null | undefined): void {
    if (value === null || value === undefined) {
      this.nil();
      return;
    }

    const bytes = textEncoder.encode(value);
    const length = bytes.length;
    if (length <= 31) {
      this.byte(0xa0 | length);
    } else if (length <= 0xff) {
      this.byte(0xd9);
      this.byte(length);
    } else if (length <= 0xffff) {
      this.byte(0xda);
      this.u16(length);
    } else {
      this.byte(0xdb);
      this.u32(length);
    }

    this.raw(bytes);
  }

  bin(value: Uint8Array): void {
    const length = value.length;
    if (length <= 0xff) {
      this.byte(0xc4);
      this.byte(length);
    } else if (length <= 0xffff) {
      this.byte(0xc5);
      this.u16(length);
    } else {
      this.byte(0xc6);
      this.u32(length);
    }

    this.raw(value);
  }

  arrayHeader(count: number): void {
    if (count <= 15) {
      this.byte(0x90 | count);
    } else if (count <= 0xffff) {
      this.byte(0xdc);
      this.u16(count);
    } else {
      this.byte(0xdd);
      this.u32(count);
    }
  }

  mapHeader(count: number): void {
    if (count <= 15) {
      this.byte(0x80 | count);
    } else if (count <= 0xffff) {
      this.byte(0xde);
      this.u16(count);
    } else {
      this.byte(0xdf);
      this.u32(count);
    }
  }

  toBytes(): Uint8Array {
    return this.buffer.slice(0, this.position);
  }

  private byte(value: number): void {
    this.ensure(1);
    this.buffer[this.position++] = value;
  }

  private u16(value: number): void {
    this.ensure(2);
    this.view.setUint16(this.position, value);
    this.position += 2;
  }

  private u32(value: number): void {
    this.ensure(4);
    this.view.setUint32(this.position, value);
    this.position += 4;
  }

  private raw(bytes: Uint8Array): void {
    this.ensure(bytes.length);
    this.buffer.set(bytes, this.position);
    this.position += bytes.length;
  }

  private ensure(count: number): void {
    if (this.position + count <= this.buffer.length) return;
    const grown = new Uint8Array(Math.max(this.buffer.length * 2, this.position + count));
    grown.set(this.buffer.subarray(0, this.position));
    this.buffer = grown;
    this.view = new DataView(grown.buffer);
  }
}

export class MsgPackReader {
  private position = 0;

  constructor(private readonly bytes: Uint8Array) {}

  get end(): boolean {
    return this.position >= this.bytes.length;
  }

  peek(): number {
    if (this.position >= this.bytes.length) throw new ProtocolError("Frame is truncated.");
    return this.bytes[this.position]!;
  }

  tryNil(): boolean {
    if (this.peek() !== 0xc0) return false;
    this.position++;
    return true;
  }

  bool(): boolean {
    const code = this.next();
    if (code === 0xc2) return false;
    if (code === 0xc3) return true;
    throw unexpected(code, "bool");
  }

  /** Any MessagePack integer, as a bigint. */
  bigint(): bigint {
    const code = this.next();
    if (code <= 0x7f) return BigInt(code);
    if (code >= 0xe0) return BigInt(code - 0x100);
    switch (code) {
      case 0xcc: return BigInt(this.take(1).getUint8(0));
      case 0xcd: return BigInt(this.take(2).getUint16(0));
      case 0xce: return BigInt(this.take(4).getUint32(0));
      case 0xcf: return this.take(8).getBigUint64(0);
      case 0xd0: return BigInt(this.take(1).getInt8(0));
      case 0xd1: return BigInt(this.take(2).getInt16(0));
      case 0xd2: return BigInt(this.take(4).getInt32(0));
      case 0xd3: return this.take(8).getBigInt64(0);
      default: throw unexpected(code, "integer");
    }
  }

  /** An integer that fits a `number` exactly (ids, counts, versions, channels). */
  number(): number {
    const value = this.bigint();
    if (value > BigInt(Number.MAX_SAFE_INTEGER) || value < BigInt(Number.MIN_SAFE_INTEGER))
      throw new ProtocolError(`Integer ${value} does not fit this field.`);
    return Number(value);
  }

  float(): number {
    const code = this.next();
    if (code === 0xca) return this.take(4).getFloat32(0);
    if (code === 0xcb) return this.take(8).getFloat64(0);
    throw unexpected(code, "float");
  }

  str(): string | null {
    if (this.tryNil()) return null;
    const code = this.next();
    let length: number;
    if ((code & 0xe0) === 0xa0) length = code & 0x1f;
    else if (code === 0xd9) length = this.take(1).getUint8(0);
    else if (code === 0xda) length = this.take(2).getUint16(0);
    else if (code === 0xdb) length = this.take(4).getUint32(0);
    else throw unexpected(code, "string");
    return textDecoder.decode(this.slice(length));
  }

  /** A string that may not be nil. */
  text(what: string): string {
    const value = this.str();
    if (value === null) throw new ProtocolError(`${what} is required.`);
    return value;
  }

  bin(): Uint8Array {
    const code = this.next();
    let length: number;
    if (code === 0xc4) length = this.take(1).getUint8(0);
    else if (code === 0xc5) length = this.take(2).getUint16(0);
    else if (code === 0xc6) length = this.take(4).getUint32(0);
    else throw unexpected(code, "binary");
    return this.slice(length).slice();
  }

  arrayHeader(): number {
    const code = this.next();
    if ((code & 0xf0) === 0x90) return code & 0x0f;
    if (code === 0xdc) return this.take(2).getUint16(0);
    if (code === 0xdd) return this.take(4).getUint32(0);
    throw unexpected(code, "array");
  }

  mapHeader(): number {
    const code = this.next();
    if ((code & 0xf0) === 0x80) return code & 0x0f;
    if (code === 0xde) return this.take(2).getUint16(0);
    if (code === 0xdf) return this.take(4).getUint32(0);
    throw unexpected(code, "map");
  }

  /** Skips one value of any type: how a reader ignores trailing fields it does not know (C2.4). */
  skip(): void {
    const code = this.peek();
    if (code <= 0x7f || code >= 0xe0 || code === 0xc0 || code === 0xc2 || code === 0xc3) {
      this.position++;
    } else if ((code & 0xe0) === 0xa0 || code === 0xd9 || code === 0xda || code === 0xdb) {
      this.str();
    } else if (code >= 0xc4 && code <= 0xc6) {
      this.bin();
    } else if ((code & 0xf0) === 0x90 || code === 0xdc || code === 0xdd) {
      const count = this.arrayHeader();
      for (let i = 0; i < count; i++) this.skip();
    } else if ((code & 0xf0) === 0x80 || code === 0xde || code === 0xdf) {
      const count = this.mapHeader();
      for (let i = 0; i < count * 2; i++) this.skip();
    } else if (code === 0xca || code === 0xcb) {
      this.float();
    } else if ((code >= 0xcc && code <= 0xcf) || (code >= 0xd0 && code <= 0xd3)) {
      this.bigint();
    } else {
      const sizes: Record<number, number> = { 0xd4: 2, 0xd5: 3, 0xd6: 5, 0xd7: 9, 0xd8: 17 };
      const fixed = sizes[code];
      if (fixed !== undefined) {
        this.position += fixed;
      } else if (code === 0xc7 || code === 0xc8 || code === 0xc9) {
        this.position++;
        const length = code === 0xc7 ? this.take(1).getUint8(0) : code === 0xc8 ? this.take(2).getUint16(0) : this.take(4).getUint32(0);
        this.position += 1 + length;
      } else {
        throw unexpected(code, "a value");
      }
    }
  }

  private next(): number {
    const code = this.peek();
    this.position++;
    return code;
  }

  private take(count: number): DataView {
    if (this.position + count > this.bytes.length) throw new ProtocolError("Frame is truncated.");
    const view = new DataView(this.bytes.buffer, this.bytes.byteOffset + this.position, count);
    this.position += count;
    return view;
  }

  private slice(count: number): Uint8Array {
    if (count < 0 || this.position + count > this.bytes.length) throw new ProtocolError("Frame is truncated.");
    const slice = this.bytes.subarray(this.position, this.position + count);
    this.position += count;
    return slice;
  }
}

function unexpected(code: number, expected: string): ProtocolError {
  return new ProtocolError(`Unexpected MessagePack code 0x${code.toString(16).padStart(2, "0")}; expected ${expected}.`);
}
