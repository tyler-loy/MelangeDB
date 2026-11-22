// The corpus's canonical decoded form (docs/CLIENT-CONFORMANCE.md, "The corpus"), produced from
// this runtime's decoders. Shared by the unit tests and the conformance adapter.
import {
  ColumnKind,
  Identity,
  decodeRow,
  toHex,
  type Arg,
  type ColumnValue,
  type ServerFrame,
  type WireDescriptor,
} from "../src/index.js";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

export function f32Hex(value: number): string {
  const view = new DataView(new ArrayBuffer(4));
  view.setFloat32(0, value);
  return view.getUint32(0).toString(16).padStart(8, "0");
}

export function f64Hex(value: number): string {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, value);
  return view.getBigUint64(0).toString(16).padStart(16, "0");
}

export function renderValue(kind: number, value: ColumnValue): Json {
  if (value === null) return null;
  switch (kind) {
    case ColumnKind.Bool: return value as boolean;
    case ColumnKind.Int64:
    case ColumnKind.UInt64:
    case ColumnKind.Timestamp:
      return (value as bigint).toString();
    case ColumnKind.Float32: return f32Hex(value as number);
    case ColumnKind.Float64: return f64Hex(value as number);
    case ColumnKind.String: return value as string;
    case ColumnKind.Bytes: return toHex(value as Uint8Array);
    case ColumnKind.Identity: return (value as Identity).toString();
    case ColumnKind.ScheduleAt: {
      const schedule = value as { interval: boolean; micros: bigint };
      return { interval: schedule.interval, micros: schedule.micros.toString() };
    }
    default: return value as number;
  }
}

export function renderRow(key: Uint8Array, row: Uint8Array, mask: Uint8Array, descriptor: WireDescriptor): { [key: string]: Json } {
  const { values } = decodeRow(descriptor, row, mask);
  const columns: { [key: string]: Json } = {};
  descriptor.columns.forEach((column, i) => {
    const value = values[i];
    if (value !== undefined) columns[column.name] = renderValue(column.kind, value);
  });
  return { key: toHex(key), mask: toHex(mask), columns };
}

export function renderTagged(value: Arg): { [key: string]: Json } {
  switch (value.tag) {
    case "Null": return { tag: "Null" };
    case "Bool": return { tag: "Bool", value: value.value };
    case "Int64":
    case "UInt64":
    case "Timestamp":
      return { tag: value.tag, value: value.value.toString() };
    case "Float64": return { tag: "Float64", value: f64Hex(value.value) };
    case "String": return { tag: "String", value: value.value };
    case "Bytes": return { tag: "Bytes", value: toHex(value.value) };
    case "Identity": return { tag: "Identity", value: value.value.toString() };
    case "Array": return { tag: "Array", items: value.items.map(renderTagged) };
  }
}

export function renderFrame(frame: ServerFrame, descriptors: Map<number, WireDescriptor>, decodePayload: (payload: Uint8Array) => Arg): { [key: string]: Json } {
  const base = { type: frame.type, channel: frame.channel };
  switch (frame.type) {
    case "Welcome":
      return { ...base, version: frame.version, connectionId: toHex(frame.connectionId), epochId: toHex(frame.epochId), headLsn: frame.headLsn.toString(), httpProtocol: frame.httpProtocol, identity: frame.identity.toString() };
    case "ReducerResult":
      return { ...base, requestId: frame.requestId, ok: frame.ok, lsn: frame.lsn.toString(), errorCode: frame.errorCode, message: frame.message };
    case "ProducerResult":
      return { ...base, requestId: frame.requestId, ok: frame.ok, lsn: frame.lsn.toString(), payload: toHex(frame.payload), value: frame.payload.length === 0 ? null : renderTagged(decodePayload(frame.payload)), errorCode: frame.errorCode, message: frame.message };
    case "Unsubscribed":
      return { ...base, subscriptionId: frame.subscriptionId };
    case "SubscriptionApplied": {
      if (frame.descriptor !== null) descriptors.set(frame.subscriptionId, frame.descriptor);
      const shape = descriptors.get(frame.subscriptionId)!;
      return {
        ...base,
        subscriptionId: frame.subscriptionId,
        anchorLsn: frame.anchorLsn.toString(),
        chunkIndex: frame.chunkIndex,
        isLast: frame.isLast,
        descriptor: frame.descriptor === null ? null : { table: frame.descriptor.table, columns: frame.descriptor.columns.map((c) => ({ name: c.name, kind: c.kind })) },
        rows: frame.rows.map((r) => renderRow(r.key, r.row, r.mask, shape)),
      };
    }
    case "TransactionUpdate":
      return {
        ...base,
        lsn: frame.lsn.toString(),
        updates: frame.updates.map((u) => ({
          subscriptionId: u.subscriptionId,
          ops: u.ops.map((op): Json => op.kind === 3
            ? { kind: op.kind, key: toHex(op.key), mask: toHex(op.mask), columns: null }
            : { kind: op.kind, ...renderRow(op.key, op.row, op.mask, descriptors.get(u.subscriptionId)!) }),
        })),
      };
    case "Error":
      return { ...base, code: frame.code, message: frame.message, requestId: frame.requestId, subscriptionId: frame.subscriptionId };
    case "Ping":
    case "Pong":
      return { ...base, id: frame.id };
    case "ResumeResult":
      return { ...base, accepted: frame.accepted, reason: frame.reason };
    case "ReauthenticateResult":
      return { ...base, accepted: frame.accepted, message: frame.message };
  }
}

/** JSON text with object keys sorted, so equal values compare equal as strings. */
export function canonical(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>).filter(([, v]) => v !== undefined).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(",")}}`;
}

/** A corpus tagged value back into an {@link Arg}. */
export function parseTagged(json: { tag: string; value?: unknown; items?: unknown[] }): Arg {
  switch (json.tag) {
    case "Null": return { tag: "Null" };
    case "Bool": return { tag: "Bool", value: json.value as boolean };
    case "Int64": return { tag: "Int64", value: BigInt(json.value as string) };
    case "UInt64": return { tag: "UInt64", value: BigInt(json.value as string) };
    case "Timestamp": return { tag: "Timestamp", value: BigInt(json.value as string) };
    case "Float64": {
      const view = new DataView(new ArrayBuffer(8));
      view.setBigUint64(0, BigInt(`0x${json.value as string}`));
      return { tag: "Float64", value: view.getFloat64(0) };
    }
    case "String": return { tag: "String", value: json.value as string };
    case "Bytes": return { tag: "Bytes", value: Uint8Array.from((json.value as string).match(/../g) ?? [], (h) => parseInt(h, 16)) };
    case "Identity": return { tag: "Identity", value: Identity.fromHex(json.value as string) };
    case "Array": return { tag: "Array", items: (json.items ?? []).map((item) => parseTagged(item as { tag: string })) };
    default: throw new Error(`Unknown tag ${json.tag}`);
  }
}

/** A corpus canonical value back into the runtime's value type, for key encoding. */
export function parseValue(kind: number, json: Json): unknown {
  switch (kind) {
    case ColumnKind.Int64:
    case ColumnKind.UInt64:
    case ColumnKind.Timestamp:
      return BigInt(json as string);
    case ColumnKind.Bytes: return Uint8Array.from((json as string).match(/../g) ?? [], (h) => parseInt(h, 16));
    case ColumnKind.Identity: return Identity.fromHex(json as string);
    default: return json;
  }
}
