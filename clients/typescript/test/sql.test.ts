import assert from "node:assert/strict";
import { test } from "node:test";
import { ColumnKind, MelangeSql, type TableCodec } from "../src/index.js";

// The SQL endpoint writes 64-bit integers and timestamps as decimal strings (0.4). Past 2^53 a
// JSON number would have been rounded by JSON.parse; a string cannot be.
const big = "9007199254740993"; // 2^53 + 1: the first integer a double cannot hold.

function respondWith(body: unknown): void {
  globalThis.fetch = (async () => new Response(JSON.stringify(body), { status: 200 })) as typeof fetch;
}

interface Account {
  id: bigint;
  level: number;
  at: bigint;
}

const accounts: TableCodec<Account> = {
  tableName: "Account",
  columns: [
    ["Id", ColumnKind.Int64],
    ["Level", ColumnKind.Int32],
    ["At", ColumnKind.Timestamp],
  ],
  create: (values) => ({ id: values["Id"] as bigint, level: values["Level"] as number, at: values["At"] as bigint }),
  encodeKey: () => new Uint8Array(),
};

test("query returns 64-bit values as the exact strings the endpoint sent", async () => {
  respondWith({ columns: ["Id", "Level"], rows: [[big, 7]] });
  const result = await new MelangeSql({ url: "http://localhost/melange" }).query("SELECT Id, Level FROM Account");
  assert.deepEqual(result.rows, [[big, 7]]);
});

test("queryTable reads 64-bit and timestamp strings as exact bigints", async () => {
  respondWith({ columns: ["Id", "Level", "At"], rows: [[big, 7, "1700000000000000"]] });
  const rows = await new MelangeSql({ url: "http://localhost/melange" }).queryTable(accounts, "SELECT * FROM Account");
  assert.deepEqual(rows, [{ id: 9007199254740993n, level: 7, at: 1700000000000000n }]);
});

test("queryTable still reads the numbers a pre-0.4 server sent", async () => {
  respondWith({ columns: ["Id", "Level", "At"], rows: [[42, 7, 1700000000000000]] });
  const rows = await new MelangeSql({ url: "http://localhost/melange" }).queryTable(accounts, "SELECT * FROM Account");
  assert.deepEqual(rows, [{ id: 42n, level: 7, at: 1700000000000000n }]);
});
