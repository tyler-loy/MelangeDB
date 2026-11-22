// The TypeScript client's conformance adapter: the adapter protocol (docs/CLIENT-CONFORMANCE.md,
// "The adapter protocol") on stdin and stdout, driving @melangedb/client through bindings
// generated from the conformance manifest. Run by the suite, never by hand:
//
//   dotnet run --project tools/MelangeDB.Conformance -c Release -- run -- node clients/typescript/build-test/conformance/adapter.js
import { createInterface } from "node:readline";
import {
  MelangeClient,
  MelangeError,
  RowReader,
  TableCaches,
  decodeServerFrame,
  decodeTagged,
  encodeArguments,
  encodeKey,
  fromHex,
  toHex,
  type ParameterValue,
  type Parameters,
  type TableCache,
  type TableCodec,
  type TypedSubscription,
  type WireDescriptor,
} from "@melangedb/client";
import { SampleCodec, SecretCodec } from "./melange-bindings.js";
import { parseTagged, parseValue, renderFrame, renderTagged, renderValue, type Json } from "./canonical.js";

type Request = { id: number; op: string; [field: string]: unknown };

const codecs: TableCodec<any>[] = [SampleCodec, SecretCodec];

let client: MelangeClient | null = null;
let caches: TableCaches | null = null;
const subscriptions = new Map<number, TypedSubscription<unknown>>();
let nextSubscription = 0;

function write(message: Record<string, Json>): void {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function current(): MelangeClient {
  if (client === null) throw new Error("Not connected.");
  return client;
}

function renderTyped(codec: TableCodec<any>, row: Record<string, unknown>): Json {
  const columns: Record<string, Json> = {};
  for (const [name, kind] of codec.columns) columns[name] = renderValue(kind, row[name] as never);
  return columns;
}

function parameters(json: unknown): Parameters | null {
  if (json === null || json === undefined) return null;
  const map: Record<string, ParameterValue> = {};
  for (const [name, tagged] of Object.entries(json as Record<string, { tag: string }>)) {
    const value = parseTagged(tagged);
    switch (value.tag) {
      case "Bool":
      case "Int64":
      case "UInt64":
      case "Timestamp":
      case "String":
      case "Bytes":
      case "Identity":
        map[name] = value.value;
        break;
      default:
        throw new Error(`A ${value.tag} is not a parameter value.`);
    }
  }

  return map;
}

async function handle(request: Request): Promise<Record<string, Json>> {
  switch (request.op) {
    case "hello":
      return { adapter: "@melangedb/client", language: "TypeScript" };

    case "decodeFrames": {
      const descriptors = new Map<number, WireDescriptor>();
      return { decoded: (request.frames as string[]).map((hex) => renderFrame(decodeServerFrame(fromHex(hex)), descriptors, decodeTagged)) };
    }

    case "decodeColumn": {
      const reader = new RowReader(fromHex(request.bytes as string));
      const value = reader.column(request.kind as number);
      return { value: renderValue(request.kind as number, value), consumed: reader.position };
    }

    case "encodeKey":
      return { key: toHex(encodeKey(request.kind as number, parseValue(request.kind as number, request.value as Json))) };

    case "encodeArguments":
      return { bytes: toHex(encodeArguments((request.arguments as { tag: string }[]).map(parseTagged))) };

    case "connect": {
      client?.close();
      subscriptions.clear();
      const connecting = new MelangeClient({ url: request.url as string, token: request.token as string });
      connecting.onError((error) => write({ event: "error", code: error.code, message: error.message }));
      connecting.onDisconnected(() => write({ event: "disconnected" }));
      await connecting.connect();
      caches = new TableCaches(connecting);
      for (const codec of codecs) {
        const cache: TableCache<any> = caches.getOrAdd(codec);
        cache.onInsert((row) => write({ event: "insert", table: codec.tableName, row: renderTyped(codec, row) }));
        cache.onUpdate((old, now) => write({ event: "update", table: codec.tableName, old: renderTyped(codec, old), new: renderTyped(codec, now) }));
        cache.onDelete((row) => write({ event: "delete", table: codec.tableName, row: renderTyped(codec, row) }));
      }

      client = connecting;
      return { identity: connecting.identity!.toString(), protocolVersion: connecting.protocolVersion };
    }

    case "subscribe": {
      const codec = codecs.find((c) => c.tableName === request.table);
      if (codec === undefined || caches === null) throw new Error(`No bindings for table '${String(request.table)}'.`);
      const subscription = await caches.subscribe(codec, request.query as string, parameters(request.parameters));
      const id = ++nextSubscription;
      subscriptions.set(id, subscription);
      return { subscription: id };
    }

    case "rescope":
      await subscription(request).rescope(parameters(request.parameters)!);
      return {};

    case "unsubscribe":
      await subscription(request).unsubscribe();
      subscriptions.delete(request.subscription as number);
      return {};

    case "call": {
      const args = (request.arguments as { tag: string }[]).map(parseTagged);
      return { lsn: (await current().callReducer(request.reducer as string, encodeArguments(args))).toString() };
    }

    case "callProducer": {
      const args = (request.arguments as { tag: string }[]).map(parseTagged);
      const reply = await current().callProducer(request.producer as string, encodeArguments(args));
      return { lsn: reply.lsn.toString(), value: renderTagged(decodeTagged(reply.payload)) };
    }

    case "sync": {
      const target = BigInt(request.lsn as string);
      const deadline = Date.now() + 20_000;
      while (current().lastAckedLsn < target) {
        if (Date.now() > deadline) throw new Error(`The cursor stayed at ${current().lastAckedLsn}, below ${target}.`);
        await new Promise((resolve) => setTimeout(resolve, 5));
      }

      return { cursor: current().lastAckedLsn.toString() };
    }

    case "cache": {
      const codec = codecs.find((c) => c.tableName === request.table)!;
      return { rows: caches!.getOrAdd(codec).rows().map((row) => renderTyped(codec, row as Record<string, unknown>)) };
    }

    case "drop":
      current().drop();
      return {};

    case "reconnect":
      return { resumed: await current().reconnect() };

    case "reauthenticate":
      await current().reauthenticate(request.token as string);
      return {};

    case "close":
      client?.close();
      client = null;
      caches = null;
      subscriptions.clear();
      return {};

    default:
      throw new Error(`Unknown op '${request.op}'.`);
  }
}

function subscription(request: Request): TypedSubscription<unknown> {
  const found = subscriptions.get(request.subscription as number);
  if (found === undefined) throw new Error(`No subscription ${String(request.subscription)}.`);
  return found;
}

async function serve(request: Request): Promise<void> {
  try {
    write({ id: request.id, ok: true, ...(await handle(request)) });
  } catch (error) {
    const code = error instanceof MelangeError ? error.code : "adapter";
    const message = error instanceof Error ? error.message : String(error);
    write({ id: request.id, ok: false, error: { code, message } });
  }
}

const pending: Promise<void>[] = [];
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on("line", (line) => {
  if (line.trim().length > 0) pending.push(serve(JSON.parse(line) as Request));
});
lines.on("close", () => {
  void Promise.allSettled(pending).then(() => {
    client?.close();
    process.exit(0);
  });
});

