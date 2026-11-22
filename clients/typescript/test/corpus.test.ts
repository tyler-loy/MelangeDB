import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import {
  MsgPackReader,
  MsgPackWriter,
  RowReader,
  decodeServerFrame,
  decodeTagged,
  encodeArguments,
  encodeKey,
  fromHex,
  toHex,
  type WireDescriptor,
} from "../src/index.js";
import { canonical, parseTagged, parseValue, renderFrame, renderTagged, renderValue, type Json } from "../conformance/canonical.js";

// build-test/test/corpus.test.js -> repo root/conformance/corpus.json
const corpusPath = fileURLToPath(new URL("../../../../conformance/corpus.json", import.meta.url));
const corpus = JSON.parse(readFileSync(corpusPath, "utf8")) as {
  sessions: { name: string; descriptorsFrom?: string | null; steps: { receive: { hex: string; expect: Json }[] }[] }[];
  columnVectors: { kind: number; kindName: string; value: Json; bytes: string }[];
  keyVectors: { kind: number; kindName: string; value: Json; key: string }[];
  argumentVectors: { about: string; arguments: { tag: string }[]; bytes: string }[];
};

const frames = (name: string) => corpus.sessions.find((s) => s.name === name)!.steps.flatMap((step) => step.receive);

for (const session of corpus.sessions) {
  test(`corpus session '${session.name}' decodes to every expectation`, () => {
    const descriptors = new Map<number, WireDescriptor>();
    if (session.descriptorsFrom) for (const frame of frames(session.descriptorsFrom)) renderFrame(decodeServerFrame(fromHex(frame.hex)), descriptors, decodeTagged);
    for (const frame of frames(session.name)) {
      const decoded = renderFrame(decodeServerFrame(fromHex(frame.hex)), descriptors, decodeTagged);
      assert.equal(canonical(decoded), canonical(frame.expect), `frame ${frame.hex.slice(0, 32)}...`);
    }
  });
}

test("column vectors decode exactly, consuming every byte", () => {
  for (const vector of corpus.columnVectors) {
    const bytes = fromHex(vector.bytes);
    const reader = new RowReader(bytes);
    const value = reader.column(vector.kind);
    assert.equal(canonical(renderValue(vector.kind, value)), canonical(vector.value), `${vector.kindName} ${vector.bytes}`);
    assert.equal(reader.position, bytes.length, `${vector.kindName} ${vector.bytes} consumed`);
  }
});

test("key vectors encode byte for byte", () => {
  for (const vector of corpus.keyVectors)
    assert.equal(toHex(encodeKey(vector.kind, parseValue(vector.kind, vector.value))), vector.key, `${vector.kindName} ${JSON.stringify(vector.value)}`);
});

test("argument vectors encode byte for byte and decode back", () => {
  for (const vector of corpus.argumentVectors) {
    const args = vector.arguments.map(parseTagged);
    assert.equal(toHex(encodeArguments(args)), vector.bytes, vector.about);
    assert.equal(canonical(args.map(renderTagged)), canonical(vector.arguments), `${vector.about} (round trip)`);
  }
});

test("a decoder ignores trailing fields it does not know (C2.4)", () => {
  // A Pong with an extra trailing field, and a Ping with a nested one.
  const w = new MsgPackWriter();
  w.arrayHeader(4);
  w.int(12);
  w.int(0);
  w.int(7);
  w.str("a future field");
  assert.deepEqual(decodeServerFrame(w.toBytes()), { type: "Pong", channel: 0, id: 7 });
});

test("integers decode from every MessagePack width (C2.1) and stay exact past 2^53 (C2.2)", () => {
  const widths: [number[], bigint][] = [
    [[0x05], 5n],
    [[0xcc, 0xff], 255n],
    [[0xcd, 0x01, 0x00], 256n],
    [[0xce, 0x00, 0x01, 0x00, 0x00], 65536n],
    [[0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff], 18446744073709551615n],
    [[0xd0, 0x80], -128n],
    [[0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0], -9223372036854775808n],
    [[0xe0], -32n],
  ];
  for (const [bytes, expected] of widths) assert.equal(new MsgPackReader(Uint8Array.from(bytes)).bigint(), expected);
  const w = new MsgPackWriter();
  w.int(9007199254740993n);
  assert.equal(new MsgPackReader(w.toBytes()).bigint(), 9007199254740993n);
});
