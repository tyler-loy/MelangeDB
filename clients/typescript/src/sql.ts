import type { Parameters } from "./frames.js";
import type { TableCodec } from "./cache.js";
import { MelangeError } from "./subscription.js";
import { ColumnKind, Identity, toHex, type ColumnValue } from "./values.js";

/**
 * A value in a SQL result row, as the endpoint writes it: 64-bit integers, timestamps (Unix
 * microseconds) and `COUNT`s are decimal strings, so they arrive exact; integers of 32 bits and
 * less are numbers; bytes are base64 and identities hex strings. A `numeric` aggregate (a `SUM`
 * over a 64-bit column) is a JSON number, kept exact as a `bigint` past 2^53.
 */
export type SqlValue = string | number | bigint | boolean | null;

export interface SqlResult {
  readonly columns: readonly string[];
  readonly rows: readonly (readonly SqlValue[])[];
}

export interface MelangeSqlOptions {
  /** The transport's HTTP base, e.g. `https://game.example.com/melange`; `/sql` is appended. */
  readonly url: string | URL;
  readonly token?: string;
  readonly tokenProvider?: () => string | Promise<string>;
}

/**
 * One-shot queries against `{path}/sql` (ad-hoc SQL, including aggregates; owner mode needs the
 * server's `Sql:OwnerRole` claim). Subscriptions cannot express an aggregate, so this is the
 * admin console's half of the client. `query` returns untyped rows for any query; `queryTable`
 * returns typed rows for a `SELECT *` over one table, the only shape whose result the manifest
 * describes.
 */
export class MelangeSql {
  constructor(private readonly options: MelangeSqlOptions) {}

  async query(query: string, parameters: Parameters | null = null): Promise<SqlResult> {
    const token = this.options.tokenProvider !== undefined ? await this.options.tokenProvider() : this.options.token;
    const base = this.options.url.toString().replace(/\/$/, "");
    const response = await fetch(`${base}/sql`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token === undefined ? {} : { Authorization: `Bearer ${token}` }),
      },
      body: `{"query":${JSON.stringify(query)},"params":${parameters === null ? "{}" : encodeParameters(parameters)}}`,
    });
    const text = await response.text();
    const body = parseExact(text) as { columns?: string[]; rows?: SqlValue[][]; error?: string; message?: string } | null;
    if (!response.ok) {
      throw new MelangeError(body?.error ?? "internal", body?.message ?? `The SQL endpoint answered ${response.status}.`);
    }

    return { columns: body?.columns ?? [], rows: body?.rows ?? [] };
  }

  /**
   * Runs a `SELECT *` over `codec`'s table and returns typed rows. Columns are matched by name,
   * so a server column the bindings predate is ignored and a missing one is an error.
   */
  async queryTable<T>(codec: TableCodec<T>, query: string, parameters: Parameters | null = null): Promise<T[]> {
    const result = await this.query(query, parameters);
    const ordinals = codec.columns.map(([name]) => {
      const ordinal = result.columns.indexOf(name);
      if (ordinal < 0)
        throw new MelangeError("schema_mismatch", `The result has no column '${name}', which ${codec.tableName} rows need; queryTable is for SELECT * over one table.`);
      return ordinal;
    });
    return result.rows.map((row) => {
      const values: Record<string, ColumnValue> = {};
      codec.columns.forEach(([name, kind], i) => {
        values[name] = fromJson(kind, row[ordinals[i]!] ?? null, `${codec.tableName}.${name}`);
      });
      return codec.create(values);
    });
  }
}

/** The SQL endpoint's JSON form of each column kind, back to the runtime's value type. */
function fromJson(kind: number, value: SqlValue, where: string): ColumnValue {
  if (value === null) return null;
  switch (kind) {
    case ColumnKind.Int64:
    case ColumnKind.UInt64:
    case ColumnKind.Timestamp:
      // Decimal strings from the endpoint; BigInt also takes the number a pre-0.4 server sent.
      return BigInt(value as string | number | bigint);
    case ColumnKind.Bytes:
      return Uint8Array.from(atob(String(value)), (c) => c.charCodeAt(0));
    case ColumnKind.Identity:
      return Identity.fromHex(String(value));
    case ColumnKind.Bool:
    case ColumnKind.String:
      return value as boolean | string;
    case ColumnKind.ScheduleAt:
      throw new MelangeError("schema_mismatch", `${where}: ScheduleAt has no JSON form on the SQL endpoint.`);
    default:
      return Number(value);
  }
}

/**
 * Parses JSON keeping integers exact: past `Number.MAX_SAFE_INTEGER` a number is read from its
 * source text as a `bigint`. Needs `JSON.parse`'s source-text access (Node 21+, current
 * browsers); without it, an unsafe integer is an error rather than a silently rounded value.
 */
function parseExact(text: string): unknown {
  if (text.length === 0) return null;
  return JSON.parse(text, function (this: unknown, _key: string, value: unknown, context?: { source?: string }) {
    if (typeof value !== "number" || Number.isSafeInteger(value) || !Number.isInteger(value)) return value;
    const source = context?.source;
    if (source === undefined)
      throw new MelangeError("internal", "A SQL result integer is past 2^53, and this runtime's JSON.parse cannot read it exactly.");
    return BigInt(source);
  } as (key: string, value: unknown) => unknown);
}

/** Parameters in the endpoint's JSON conventions: bigints as exact numbers, `$identity`, `$bytes`. */
function encodeParameters(parameters: Parameters): string {
  const members = Object.entries(parameters).map(([name, value]) => {
    let json: string;
    if (typeof value === "bigint") json = value.toString();
    else if (value instanceof Identity) json = `{"$identity":"${toHex(value.bytes)}"}`;
    else if (value instanceof Uint8Array) json = `{"$bytes":"${btoa(String.fromCharCode(...value))}"}`;
    else json = JSON.stringify(value);
    return `${JSON.stringify(name)}:${json}`;
  });
  return `{${members.join(",")}}`;
}
