import { ProtocolError } from "./msgpack.js";
import { ColumnKind, Identity } from "./values.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Encodes a primary-key value as the server indexes it (C5.2): big-endian, signed kinds with the
 * sign bit flipped, strings as UTF-8, identities and bytes raw. Rows are found by these bytes.
 */
export function encodeKey(kind: number, value: unknown): Uint8Array {
  const fixed = (width: number, write: (view: DataView) => void): Uint8Array => {
    const bytes = new Uint8Array(width);
    write(new DataView(bytes.buffer));
    return bytes;
  };
  switch (kind) {
    case ColumnKind.Bool: return Uint8Array.of(value ? 1 : 0);
    case ColumnKind.Int8: return Uint8Array.of((Number(value) ^ 0x80) & 0xff);
    case ColumnKind.UInt8: return Uint8Array.of(Number(value) & 0xff);
    case ColumnKind.Int16: return fixed(2, (v) => v.setUint16(0, (Number(value) ^ 0x8000) & 0xffff));
    case ColumnKind.UInt16: return fixed(2, (v) => v.setUint16(0, Number(value)));
    case ColumnKind.Int32: return fixed(4, (v) => v.setUint32(0, (Number(value) ^ 0x80000000) >>> 0));
    case ColumnKind.UInt32: return fixed(4, (v) => v.setUint32(0, Number(value)));
    case ColumnKind.Int64:
    case ColumnKind.Timestamp:
      return fixed(8, (v) => v.setBigUint64(0, BigInt.asUintN(64, BigInt(value as bigint) ^ (1n << 63n))));
    case ColumnKind.UInt64: return fixed(8, (v) => v.setBigUint64(0, BigInt(value as bigint)));
    case ColumnKind.String: return textEncoder.encode(String(value));
    case ColumnKind.Bytes: return (value as Uint8Array).slice();
    case ColumnKind.Identity: return (value as Identity).bytes.slice();
    default: throw new RangeError(`Column kind ${kind} is not key-encodable.`);
  }
}

/** The argument tags of the reducer-argument encoding (C9.2). */
export const ArgTag = {
  Null: 0,
  Bool: 1,
  Int64: 2,
  UInt64: 3,
  Float64: 4,
  String: 5,
  Bytes: 6,
  Identity: 7,
  Timestamp: 8,
  Array: 9,
} as const;

/**
 * One argument with its tag chosen explicitly. A JavaScript `number` does not say whether it is
 * an `Int64`, a `UInt64`, or a `Float64`, so the untyped call API takes these; generated stubs
 * build them from the manifest's parameter kinds.
 */
export type Arg =
  | { readonly tag: "Null" }
  | { readonly tag: "Bool"; readonly value: boolean }
  | { readonly tag: "Int64"; readonly value: bigint }
  | { readonly tag: "UInt64"; readonly value: bigint }
  | { readonly tag: "Float64"; readonly value: number }
  | { readonly tag: "String"; readonly value: string }
  | { readonly tag: "Bytes"; readonly value: Uint8Array }
  | { readonly tag: "Identity"; readonly value: Identity }
  | { readonly tag: "Timestamp"; readonly value: bigint }
  | { readonly tag: "Array"; readonly items: readonly Arg[] };

/** Constructors for {@link Arg}. Integers accept a `number` and widen it exactly. */
export const arg = {
  null: (): Arg => ({ tag: "Null" }),
  bool: (value: boolean): Arg => ({ tag: "Bool", value }),
  int64: (value: number | bigint): Arg => ({ tag: "Int64", value: toBigInt(value) }),
  uint64: (value: number | bigint): Arg => ({ tag: "UInt64", value: toBigInt(value) }),
  float64: (value: number): Arg => ({ tag: "Float64", value }),
  string: (value: string): Arg => ({ tag: "String", value }),
  bytes: (value: Uint8Array): Arg => ({ tag: "Bytes", value }),
  identity: (value: Identity): Arg => ({ tag: "Identity", value }),
  timestamp: (micros: number | bigint): Arg => ({ tag: "Timestamp", value: toBigInt(micros) }),
  array: (items: readonly Arg[]): Arg => ({ tag: "Array", items }),
  /** `Null` for a null value; otherwise `encode(value)`. For strings, bytes, and lists. */
  optional: <T>(value: T | null, encode: (value: T) => Arg): Arg => (value === null ? { tag: "Null" } : encode(value)),
} as const;

function toBigInt(value: number | bigint): bigint {
  if (typeof value === "bigint") return value;
  if (!Number.isSafeInteger(value)) throw new RangeError(`${value} is not a safe integer.`);
  return BigInt(value);
}

/**
 * Drops trailing arguments for defaulted parameters the caller left `undefined`, so the server
 * applies its own defaults (C9.2). `optional` are those parameters' values, in order, and
 * `required` how many arguments come before them. Generated stubs call this.
 */
export function trimDefaults(args: readonly Arg[], optional: readonly unknown[], required: number): Arg[] {
  let end = args.length;
  while (end > required && optional[end - required - 1] === undefined) end--;
  return args.slice(0, end);
}

/**
 * Encodes reducer or producer arguments: a little-endian UInt16 count, then each tagged value.
 * No arguments is the empty payload, not a zero count.
 */
export function encodeArguments(args: readonly Arg[]): Uint8Array {
  if (args.length === 0) return new Uint8Array(0);
  const writer = new ByteWriter();
  writer.u16(args.length);
  for (const value of args) writeArg(writer, value);
  return writer.toBytes();
}

function writeArg(writer: ByteWriter, value: Arg): void {
  writer.u8(ArgTag[value.tag]);
  switch (value.tag) {
    case "Null": return;
    case "Bool": writer.u8(value.value ? 1 : 0); return;
    case "Int64":
    case "Timestamp":
      writer.i64(value.value);
      return;
    case "UInt64": writer.u64(value.value); return;
    case "Float64": writer.f64(value.value); return;
    case "String": {
      const bytes = textEncoder.encode(value.value);
      writer.i32(bytes.length);
      writer.raw(bytes);
      return;
    }
    case "Bytes":
      writer.i32(value.value.length);
      writer.raw(value.value);
      return;
    case "Identity": writer.raw(value.value.bytes); return;
    case "Array":
      writer.i32(value.items.length);
      for (const item of value.items) writeArg(writer, item);
      return;
  }
}

/** Decodes exactly one tagged value — a producer's result — failing if bytes are left over. */
export function decodeTagged(payload: Uint8Array): Arg {
  const reader = new TaggedReader(payload);
  const value = reader.any();
  if (!reader.end) throw new ProtocolError(`A tagged value left ${payload.length - reader.position} byte(s) unread.`);
  return value;
}

/** Decodes a whole argument payload. */
export function decodeArguments(payload: Uint8Array): Arg[] {
  if (payload.length === 0) return [];
  const reader = new TaggedReader(payload);
  const count = reader.u16();
  const values: Arg[] = [];
  for (let i = 0; i < count; i++) values.push(reader.any());
  if (!reader.end) throw new ProtocolError(`Arguments left ${payload.length - reader.position} byte(s) unread.`);
  return values;
}

/**
 * Reads tagged values in place, by expected type: what generated producer decoders use. Integer
 * readers accept either integer tag (a server-side field widened from signed to unsigned stays
 * readable), and every reader reports the tag it found when it is the wrong one.
 */
export class TaggedReader {
  private readonly view: DataView;
  position = 0;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get end(): boolean {
    return this.position >= this.bytes.length;
  }

  /** Consumes a `Null` tag if one is next. */
  tryNull(): boolean {
    if (this.peek() !== ArgTag.Null) return false;
    this.position++;
    return true;
  }

  bool(): boolean {
    this.expect(ArgTag.Bool, "Bool");
    return this.view.getUint8(this.advance(1)) !== 0;
  }

  int(): bigint {
    const tag = this.tag();
    if (tag === ArgTag.Int64) return this.view.getBigInt64(this.advance(8), true);
    if (tag === ArgTag.UInt64) return this.view.getBigUint64(this.advance(8), true);
    throw this.wrong(tag, "an integer");
  }

  float(): number {
    this.expect(ArgTag.Float64, "Float64");
    return this.view.getFloat64(this.advance(8), true);
  }

  string(): string {
    this.expect(ArgTag.String, "String");
    const length = this.view.getInt32(this.advance(4), true);
    return textDecoder.decode(this.bytes.subarray(this.advance(length), this.position));
  }

  bytesValue(): Uint8Array {
    this.expect(ArgTag.Bytes, "Bytes");
    const length = this.view.getInt32(this.advance(4), true);
    return this.bytes.slice(this.advance(length), this.position);
  }

  identity(): Identity {
    this.expect(ArgTag.Identity, "Identity");
    return new Identity(this.bytes.subarray(this.advance(32), this.position));
  }

  timestamp(): bigint {
    this.expect(ArgTag.Timestamp, "Timestamp");
    return this.view.getBigInt64(this.advance(8), true);
  }

  /** Reads an `Array` header and returns its element count. */
  array(): number {
    this.expect(ArgTag.Array, "Array");
    return this.view.getInt32(this.advance(4), true);
  }

  /** Skips one value of any tag: how a decoder reads past fields appended after it was generated. */
  skip(): void {
    this.any();
  }

  any(): Arg {
    const tag = this.tag();
    switch (tag) {
      case ArgTag.Null: return { tag: "Null" };
      case ArgTag.Bool: return { tag: "Bool", value: this.view.getUint8(this.advance(1)) !== 0 };
      case ArgTag.Int64: return { tag: "Int64", value: this.view.getBigInt64(this.advance(8), true) };
      case ArgTag.UInt64: return { tag: "UInt64", value: this.view.getBigUint64(this.advance(8), true) };
      case ArgTag.Float64: return { tag: "Float64", value: this.view.getFloat64(this.advance(8), true) };
      case ArgTag.String: {
        const length = this.view.getInt32(this.advance(4), true);
        return { tag: "String", value: textDecoder.decode(this.bytes.subarray(this.advance(length), this.position)) };
      }
      case ArgTag.Bytes: {
        const length = this.view.getInt32(this.advance(4), true);
        return { tag: "Bytes", value: this.bytes.slice(this.advance(length), this.position) };
      }
      case ArgTag.Identity: return { tag: "Identity", value: new Identity(this.bytes.subarray(this.advance(32), this.position)) };
      case ArgTag.Timestamp: return { tag: "Timestamp", value: this.view.getBigInt64(this.advance(8), true) };
      case ArgTag.Array: {
        const count = this.view.getInt32(this.advance(4), true);
        const items: Arg[] = [];
        for (let i = 0; i < count; i++) items.push(this.any());
        return { tag: "Array", items };
      }
      default:
        throw new ProtocolError(`Unknown argument tag ${tag}.`);
    }
  }

  u16(): number {
    return this.view.getUint16(this.advance(2), true);
  }

  private peek(): number {
    if (this.position >= this.bytes.length) throw new ProtocolError("A tagged value is truncated.");
    return this.bytes[this.position]!;
  }

  private tag(): number {
    const tag = this.peek();
    this.position++;
    return tag;
  }

  private expect(tag: number, name: string): void {
    const found = this.tag();
    if (found !== tag) throw this.wrong(found, name);
  }

  private wrong(found: number, expected: string): ProtocolError {
    const name = Object.entries(ArgTag).find(([, value]) => value === found)?.[0] ?? `tag ${found}`;
    return new ProtocolError(`Expected ${expected} in a tagged value, found ${name}.`);
  }

  private advance(count: number): number {
    const start = this.position;
    if (count < 0 || start + count > this.bytes.length) throw new ProtocolError("A tagged value is truncated.");
    this.position = start + count;
    return start;
  }
}

class ByteWriter {
  private buffer = new Uint8Array(64);
  private view = new DataView(this.buffer.buffer);
  private position = 0;

  u8(value: number): void {
    this.ensure(1);
    this.view.setUint8(this.position++, value);
  }

  u16(value: number): void {
    this.ensure(2);
    this.view.setUint16(this.position, value, true);
    this.position += 2;
  }

  i32(value: number): void {
    this.ensure(4);
    this.view.setInt32(this.position, value, true);
    this.position += 4;
  }

  i64(value: bigint): void {
    this.ensure(8);
    this.view.setBigInt64(this.position, value, true);
    this.position += 8;
  }

  u64(value: bigint): void {
    this.ensure(8);
    this.view.setBigUint64(this.position, value, true);
    this.position += 8;
  }

  f64(value: number): void {
    this.ensure(8);
    this.view.setFloat64(this.position, value, true);
    this.position += 8;
  }

  raw(bytes: Uint8Array): void {
    this.ensure(bytes.length);
    this.buffer.set(bytes, this.position);
    this.position += bytes.length;
  }

  toBytes(): Uint8Array {
    return this.buffer.slice(0, this.position);
  }

  private ensure(count: number): void {
    if (this.position + count <= this.buffer.length) return;
    const grown = new Uint8Array(Math.max(this.buffer.length * 2, this.position + count));
    grown.set(this.buffer.subarray(0, this.position));
    this.buffer = grown;
    this.view = new DataView(grown.buffer);
  }
}
