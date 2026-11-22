import { MsgPackReader, MsgPackWriter, ProtocolError } from "./msgpack.js";
import { Identity, type WireDescriptor } from "./values.js";

/** Frame type numbers (protocol version 3). Values are contract. */
export const FrameType = {
  Hello: 1,
  Welcome: 2,
  CallReducer: 3,
  ReducerResult: 4,
  Subscribe: 5,
  Unsubscribe: 6,
  Unsubscribed: 7,
  SubscriptionApplied: 8,
  TransactionUpdate: 9,
  Error: 10,
  Ping: 11,
  Pong: 12,
  Resume: 13,
  ResumeResult: 14,
  Reauthenticate: 15,
  ReauthenticateResult: 16,
  CallProducer: 17,
  ProducerResult: 18,
} as const;

/** Channels: ordering is promised only within one (C2.6). */
export const Channel = { Control: 0, Calls: 1, Data: 2, BulkBase: 16 } as const;

export const MIN_PROTOCOL_VERSION = 2;
export const PROTOCOL_VERSION = 3;

/** A subscription parameter (C15.2): integers, booleans, strings, bytes, identities. */
export type ParameterValue = boolean | number | bigint | string | Uint8Array | Identity;
export type Parameters = Readonly<Record<string, ParameterValue>>;

export interface WireRow {
  readonly key: Uint8Array;
  readonly row: Uint8Array;
  readonly mask: Uint8Array;
}

export interface WireRowOp extends WireRow {
  readonly kind: number;
}

export interface SubscriptionUpdate {
  readonly subscriptionId: number;
  readonly ops: readonly WireRowOp[];
}

export type ServerFrame =
  | { readonly type: "Welcome"; readonly channel: number; readonly version: number; readonly connectionId: Uint8Array; readonly epochId: Uint8Array; readonly headLsn: bigint; readonly httpProtocol: string; readonly identity: Identity }
  | { readonly type: "ReducerResult"; readonly channel: number; readonly requestId: number; readonly ok: boolean; readonly lsn: bigint; readonly errorCode: string | null; readonly message: string | null }
  | { readonly type: "ProducerResult"; readonly channel: number; readonly requestId: number; readonly ok: boolean; readonly lsn: bigint; readonly payload: Uint8Array; readonly errorCode: string | null; readonly message: string | null }
  | { readonly type: "Unsubscribed"; readonly channel: number; readonly subscriptionId: number }
  | { readonly type: "SubscriptionApplied"; readonly channel: number; readonly subscriptionId: number; readonly anchorLsn: bigint; readonly chunkIndex: number; readonly isLast: boolean; readonly descriptor: WireDescriptor | null; readonly rows: readonly WireRow[] }
  | { readonly type: "TransactionUpdate"; readonly channel: number; readonly lsn: bigint; readonly updates: readonly SubscriptionUpdate[] }
  | { readonly type: "Error"; readonly channel: number; readonly code: string; readonly message: string; readonly requestId: number; readonly subscriptionId: number }
  | { readonly type: "Ping"; readonly channel: number; readonly id: number }
  | { readonly type: "Pong"; readonly channel: number; readonly id: number }
  | { readonly type: "ResumeResult"; readonly channel: number; readonly accepted: boolean; readonly reason: string | null }
  | { readonly type: "ReauthenticateResult"; readonly channel: number; readonly accepted: boolean; readonly message: string | null };

/** Decodes one server-to-client frame. Fields past the ones this version knows are skipped (C2.4). */
export function decodeServerFrame(bytes: Uint8Array): ServerFrame {
  const r = new MsgPackReader(bytes);
  const count = r.arrayHeader();
  if (count < 2) throw new ProtocolError("A frame carries a type and a channel.");
  const type = r.number();
  const channel = r.number();
  let read = 2;
  const field = <T>(value: T): T => {
    read++;
    return value;
  };
  let frame: ServerFrame;
  switch (type) {
    case FrameType.Welcome:
      frame = {
        type: "Welcome",
        channel,
        version: field(r.number()),
        connectionId: field(r.bin()),
        epochId: field(r.bin()),
        headLsn: field(r.bigint()),
        httpProtocol: field(r.str()) ?? "",
        identity: field(new Identity(r.bin())),
      };
      break;
    case FrameType.ReducerResult:
      frame = {
        type: "ReducerResult",
        channel,
        requestId: field(r.number()),
        ok: field(r.bool()),
        lsn: field(r.bigint()),
        errorCode: field(r.str()),
        message: field(r.str()),
      };
      break;
    case FrameType.ProducerResult:
      frame = {
        type: "ProducerResult",
        channel,
        requestId: field(r.number()),
        ok: field(r.bool()),
        lsn: field(r.bigint()),
        payload: field(r.bin()),
        errorCode: field(r.str()),
        message: field(r.str()),
      };
      break;
    case FrameType.Unsubscribed:
      frame = { type: "Unsubscribed", channel, subscriptionId: field(r.number()) };
      break;
    case FrameType.SubscriptionApplied: {
      const subscriptionId = field(r.number());
      const anchorLsn = field(r.bigint());
      const chunkIndex = field(r.number());
      const isLast = field(r.bool());
      const descriptor = field(readDescriptor(r));
      const rowCount = field(r.arrayHeader());
      const rows: WireRow[] = [];
      for (let i = 0; i < rowCount; i++) {
        const parts = r.arrayHeader();
        if (parts < 3) throw new ProtocolError("A wire row is [key, row, mask].");
        rows.push({ key: r.bin(), row: r.bin(), mask: r.bin() });
        for (let extra = 3; extra < parts; extra++) r.skip();
      }

      frame = { type: "SubscriptionApplied", channel, subscriptionId, anchorLsn, chunkIndex, isLast, descriptor, rows };
      break;
    }
    case FrameType.TransactionUpdate: {
      const lsn = field(r.bigint());
      const updateCount = field(r.arrayHeader());
      const updates: SubscriptionUpdate[] = [];
      for (let i = 0; i < updateCount; i++) {
        const parts = r.arrayHeader();
        if (parts < 2) throw new ProtocolError("A subscription update is [subscriptionId, ops].");
        const subscriptionId = r.number();
        const opCount = r.arrayHeader();
        const ops: WireRowOp[] = [];
        for (let j = 0; j < opCount; j++) {
          const opParts = r.arrayHeader();
          if (opParts < 4) throw new ProtocolError("A row op is [kind, key, row, mask].");
          ops.push({ kind: r.number(), key: r.bin(), row: r.bin(), mask: r.bin() });
          for (let extra = 4; extra < opParts; extra++) r.skip();
        }

        for (let extra = 2; extra < parts; extra++) r.skip();
        updates.push({ subscriptionId, ops });
      }

      frame = { type: "TransactionUpdate", channel, lsn, updates };
      break;
    }
    case FrameType.Error:
      frame = {
        type: "Error",
        channel,
        code: field(r.str()) ?? "internal",
        message: field(r.str()) ?? "",
        requestId: field(r.number()),
        subscriptionId: field(r.number()),
      };
      break;
    case FrameType.Ping:
      frame = { type: "Ping", channel, id: field(r.number()) };
      break;
    case FrameType.Pong:
      frame = { type: "Pong", channel, id: field(r.number()) };
      break;
    case FrameType.ResumeResult:
      frame = { type: "ResumeResult", channel, accepted: field(r.bool()), reason: field(r.str()) };
      break;
    case FrameType.ReauthenticateResult:
      frame = { type: "ReauthenticateResult", channel, accepted: field(r.bool()), message: field(r.str()) };
      break;
    default:
      throw new ProtocolError(`Frame type ${type} is not a server-to-client frame this client knows.`);
  }

  for (; read < count; read++) r.skip();
  return frame;
}

function readDescriptor(r: MsgPackReader): WireDescriptor | null {
  if (r.tryNil()) return null;
  const parts = r.arrayHeader();
  if (parts < 2) throw new ProtocolError("A wire descriptor is [table, columns].");
  const table = r.text("A descriptor's table name");
  const count = r.arrayHeader();
  const columns = [];
  for (let i = 0; i < count; i++) {
    const columnParts = r.arrayHeader();
    if (columnParts < 2) throw new ProtocolError("A descriptor column is [name, kind].");
    columns.push({ name: r.text("A descriptor column's name"), kind: r.number() });
    for (let extra = 2; extra < columnParts; extra++) r.skip();
  }

  for (let extra = 2; extra < parts; extra++) r.skip();
  return { table, columns };
}

function header(w: MsgPackWriter, type: number, channel: number, fields: number): MsgPackWriter {
  w.arrayHeader(fields + 2);
  w.int(type);
  w.int(channel);
  return w;
}

export function encodeHello(token: string | null): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Hello, Channel.Control, 3);
  w.int(MIN_PROTOCOL_VERSION);
  w.int(PROTOCOL_VERSION);
  w.str(token);
  return w.toBytes();
}

export function encodeCallReducer(requestId: number, reducer: string, args: Uint8Array, traceParent: string | null = null): Uint8Array {
  const w = header(new MsgPackWriter(64 + args.length), FrameType.CallReducer, Channel.Calls, 4);
  w.int(requestId);
  w.str(reducer);
  w.bin(args);
  w.str(traceParent);
  return w.toBytes();
}

export function encodeCallProducer(requestId: number, producer: string, args: Uint8Array, traceParent: string | null = null): Uint8Array {
  const w = header(new MsgPackWriter(64 + args.length), FrameType.CallProducer, Channel.Calls, 4);
  w.int(requestId);
  w.str(producer);
  w.bin(args);
  w.str(traceParent);
  return w.toBytes();
}

export function encodeSubscribe(subscriptionId: number, query: string, parameters: Parameters | null): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Subscribe, Channel.Data, 3);
  w.int(subscriptionId);
  w.str(query);
  writeParameters(w, parameters);
  return w.toBytes();
}

export function encodeUnsubscribe(subscriptionId: number): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Unsubscribe, Channel.Data, 1);
  w.int(subscriptionId);
  return w.toBytes();
}

export function encodePing(id: number): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Ping, Channel.Control, 1);
  w.int(id);
  return w.toBytes();
}

export function encodePong(id: number): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Pong, Channel.Control, 1);
  w.int(id);
  return w.toBytes();
}

export interface ResumeSubscription {
  readonly subscriptionId: number;
  readonly query: string;
  readonly parameters: Parameters | null;
}

export function encodeResume(epochId: Uint8Array, lastAckedLsn: bigint, subscriptions: readonly ResumeSubscription[]): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Resume, Channel.Control, 3);
  w.bin(epochId);
  w.int(lastAckedLsn);
  w.arrayHeader(subscriptions.length);
  for (const subscription of subscriptions) {
    w.arrayHeader(3);
    w.int(subscription.subscriptionId);
    w.str(subscription.query);
    writeParameters(w, subscription.parameters);
  }

  return w.toBytes();
}

export function encodeReauthenticate(token: string): Uint8Array {
  const w = header(new MsgPackWriter(), FrameType.Reauthenticate, Channel.Control, 1);
  w.str(token);
  return w.toBytes();
}

/** The parameter map, pinned by the contract to a closed set of value shapes (C15). */
function writeParameters(w: MsgPackWriter, parameters: Parameters | null): void {
  if (parameters === null) {
    w.nil();
    return;
  }

  const entries = Object.entries(parameters);
  w.mapHeader(entries.length);
  for (const [name, value] of entries) {
    w.str(name);
    if (typeof value === "boolean") w.bool(value);
    else if (typeof value === "bigint") w.int(value);
    else if (typeof value === "number") {
      if (!Number.isSafeInteger(value))
        throw new RangeError(`Parameter :${name} is ${value}; a parameter is an integer, boolean, string, bytes, or identity (C15.2).`);
      w.int(value);
    } else if (typeof value === "string") w.str(value);
    else if (value instanceof Uint8Array) w.bin(value);
    else if (value instanceof Identity) w.bin(value.bytes);
    else throw new RangeError(`Parameter :${name} has a value no column kind accepts (C15.2).`);
  }
}
