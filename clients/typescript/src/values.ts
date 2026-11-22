import { ProtocolError } from "./msgpack.js";

/** The wire kind of a column value (CLIENT-CONFORMANCE C4.2). Values are contract. */
export const ColumnKind = {
  Bool: 1,
  Int8: 2,
  UInt8: 3,
  Int16: 4,
  UInt16: 5,
  Int32: 6,
  UInt32: 7,
  Int64: 8,
  UInt64: 9,
  Float32: 10,
  Float64: 11,
  String: 12,
  Bytes: 13,
  Identity: 14,
  Timestamp: 15,
  ScheduleAt: 16,
} as const;
export type ColumnKind = (typeof ColumnKind)[keyof typeof ColumnKind];

/** The kind of a row op in a delta. */
export const RowOpKind = { Insert: 1, Update: 2, Delete: 3 } as const;
export type RowOpKind = (typeof RowOpKind)[keyof typeof RowOpKind];

const hexDigits = "0123456789abcdef";

export function toHex(bytes: Uint8Array): string {
  let out = "";
  for (const byte of bytes) out += hexDigits[byte >> 4]! + hexDigits[byte & 15]!;
  return out;
}

export function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || /[^0-9a-fA-F]/.test(hex)) throw new RangeError(`'${hex}' is not hex.`);
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i++) bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  return bytes;
}

/** Orders byte strings the way encoded keys sort on the server: lexicographically, shorter first on a tie. */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const length = Math.min(a.length, b.length);
  for (let i = 0; i < length; i++) if (a[i] !== b[i]) return a[i]! - b[i]!;
  return a.length - b.length;
}

export function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * Who a connection is: 32 bytes the server derives from the token's issuer and subject. Read it
 * from the connection (`client.identity`), never derive it (C16.1).
 */
export class Identity {
  static readonly size = 32;
  static readonly none = new Identity(new Uint8Array(32));

  readonly bytes: Uint8Array;

  constructor(bytes: Uint8Array) {
    if (bytes.length !== Identity.size) throw new RangeError(`An identity is ${Identity.size} bytes, not ${bytes.length}.`);
    this.bytes = bytes.slice();
  }

  static fromHex(hex: string): Identity {
    return new Identity(fromHex(hex));
  }

  equals(other: Identity | null | undefined): boolean {
    return other instanceof Identity && bytesEqual(this.bytes, other.bytes);
  }

  /** 64 lowercase hex characters. */
  toString(): string {
    return toHex(this.bytes);
  }

  toJSON(): string {
    return this.toString();
  }
}

/** A `ScheduleAt` value: an instant (microseconds since the Unix epoch) or a repeat interval. */
export interface ScheduleAt {
  readonly interval: boolean;
  readonly micros: bigint;
}

/** A decoded column value, by kind: see the table in CLIENT-BINDINGS.md's TypeScript section. */
export type ColumnValue = boolean | number | bigint | string | Uint8Array | Identity | ScheduleAt | null;

/** Converts a microsecond timestamp to a `Date` (millisecond precision). */
export function timestampToDate(micros: bigint): Date {
  return new Date(Number(micros / 1000n));
}

/** Converts a `Date` to a microsecond timestamp. */
export function dateToTimestamp(date: Date): bigint {
  return BigInt(date.getTime()) * 1000n;
}

const textDecoder = new TextDecoder("utf-8", { fatal: true });

/** Reads row format v1: little-endian, positional, no padding (C4.2). */
export class RowReader {
  private readonly view: DataView;
  position = 0;

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get length(): number {
    return this.bytes.length;
  }

  column(kind: number): ColumnValue {
    switch (kind) {
      case ColumnKind.Bool: return this.u8() !== 0;
      case ColumnKind.Int8: return this.view.getInt8(this.advance(1));
      case ColumnKind.UInt8: return this.u8();
      case ColumnKind.Int16: return this.view.getInt16(this.advance(2), true);
      case ColumnKind.UInt16: return this.view.getUint16(this.advance(2), true);
      case ColumnKind.Int32: return this.view.getInt32(this.advance(4), true);
      case ColumnKind.UInt32: return this.view.getUint32(this.advance(4), true);
      case ColumnKind.Int64: return this.view.getBigInt64(this.advance(8), true);
      case ColumnKind.UInt64: return this.view.getBigUint64(this.advance(8), true);
      case ColumnKind.Float32: return this.view.getFloat32(this.advance(4), true);
      case ColumnKind.Float64: return this.view.getFloat64(this.advance(8), true);
      case ColumnKind.String: {
        if (this.u8() === 0) return null;
        const length = this.view.getInt32(this.advance(4), true);
        return textDecoder.decode(this.bytes.subarray(this.advance(length), this.position));
      }
      case ColumnKind.Bytes: {
        if (this.u8() === 0) return null;
        const length = this.view.getInt32(this.advance(4), true);
        return this.bytes.slice(this.advance(length), this.position);
      }
      case ColumnKind.Identity: return new Identity(this.bytes.subarray(this.advance(32), this.position));
      case ColumnKind.Timestamp: return this.view.getBigInt64(this.advance(8), true);
      case ColumnKind.ScheduleAt: {
        const interval = this.u8() !== 0;
        return { interval, micros: this.view.getBigInt64(this.advance(8), true) };
      }
      default:
        throw new ProtocolError(`Unknown column kind ${kind}.`);
    }
  }

  private u8(): number {
    return this.view.getUint8(this.advance(1));
  }

  /** Returns the start of the next `count` bytes and moves past them. */
  private advance(count: number): number {
    const start = this.position;
    if (count < 0 || start + count > this.bytes.length) throw new ProtocolError("Row bytes are truncated.");
    this.position = start + count;
    return start;
  }
}

/** One column a subscription's rows carry: its name and kind, in row-byte order. */
export interface WireColumn {
  readonly name: string;
  readonly kind: number;
}

/** The shape of a subscription's rows, sent once on chunk 0 of its initial set (C4.1). */
export interface WireDescriptor {
  readonly table: string;
  readonly columns: readonly WireColumn[];
}

/** Whether descriptor column `ordinal` is present under `mask`; an empty mask means all (C4.3). */
export function isPresent(mask: Uint8Array, ordinal: number): boolean {
  return mask.length === 0 || ((mask[ordinal >> 3] ?? 0) & (1 << (ordinal & 7))) !== 0;
}

/**
 * A row decoded against its descriptor: each present column's value and the byte range it
 * occupied, by descriptor ordinal. A masked-out column is `undefined` in both.
 */
export interface DecodedRow {
  readonly values: readonly (ColumnValue | undefined)[];
  readonly ranges: readonly ([number, number] | undefined)[];
}

export function decodeRow(descriptor: WireDescriptor, row: Uint8Array, mask: Uint8Array): DecodedRow {
  const reader = new RowReader(row);
  const values: (ColumnValue | undefined)[] = [];
  const ranges: ([number, number] | undefined)[] = [];
  descriptor.columns.forEach((column, ordinal) => {
    if (!isPresent(mask, ordinal)) {
      values.push(undefined);
      ranges.push(undefined);
      return;
    }

    const start = reader.position;
    values.push(reader.column(column.kind));
    ranges.push([start, reader.position]);
  });
  if (reader.position !== row.length)
    throw new ProtocolError(`A '${descriptor.table}' row left ${row.length - reader.position} byte(s) undecoded: the descriptor does not describe it.`);
  return { values, ranges };
}
