import type { Parameters } from "./frames.js";
import type { MelangeClient } from "./client.js";
import { CachedRow, SchemaMismatchError, Subscription, type SubscriptionSink } from "./subscription.js";
import { RowOpKind, bytesEqual, decodeRow, toHex, type ColumnValue, type WireDescriptor } from "./values.js";

/**
 * What generated bindings declare per table: the wire name, the columns they read (name and kind,
 * in manifest order), how to build a row from values by column name, and how to encode its key.
 */
export interface TableCodec<T> {
  readonly tableName: string;
  readonly columns: readonly (readonly [name: string, kind: number])[];
  create(values: Readonly<Record<string, ColumnValue>>): T;
  encodeKey(row: T): Uint8Array;
}

type Listener<T extends unknown[]> = (...args: T) => void;

interface Entry<T> {
  row: T;
  fingerprint: Uint8Array;
  refCount: number;
}

/**
 * One table's merged cache: rows from every subscription over the table, keyed by encoded primary
 * key and refcounted by covering subscription (C8.1). Events come from transitions, not wire ops:
 * the first covering subscription inserts, a changed value updates once, the last uncovering
 * deletes. A new initial set reconciles by diff (C8.2).
 */
export class TableCache<T> {
  private readonly entries = new Map<string, Entry<T>>();
  private readonly insertListeners = new Set<Listener<[T]>>();
  private readonly updateListeners = new Set<Listener<[T, T]>>();
  private readonly deleteListeners = new Set<Listener<[T]>>();

  constructor(readonly codec: TableCodec<T>) {}

  get tableName(): string {
    return this.codec.tableName;
  }

  get count(): number {
    return this.entries.size;
  }

  /** A snapshot of the cached rows. */
  rows(): T[] {
    return [...this.entries.values()].map((entry) => entry.row);
  }

  /** Finds a row by its encoded primary key. */
  findByKey(key: Uint8Array): T | undefined {
    return this.entries.get(toHex(key))?.row;
  }

  onInsert(listener: Listener<[T]>): () => void {
    this.insertListeners.add(listener);
    return () => this.insertListeners.delete(listener);
  }

  onUpdate(listener: Listener<[T, T]>): () => void {
    this.updateListeners.add(listener);
    return () => this.updateListeners.delete(listener);
  }

  onDelete(listener: Listener<[T]>): () => void {
    this.deleteListeners.add(listener);
    return () => this.deleteListeners.delete(listener);
  }

  /** @internal */
  createBinding(): CacheBinding<T> {
    return new CacheBinding(this);
  }

  /** @internal */
  cover(binding: CacheBinding<T>, row: CachedRow, fire: (() => void)[]): void {
    const { value, fingerprint } = binding.read(row);
    const key = row.keyHex;
    const newlyCovered = !binding.covered.has(key);
    binding.covered.add(key);
    const entry = this.entries.get(key);
    if (entry !== undefined) {
      if (newlyCovered) entry.refCount++;
      if (!bytesEqual(entry.fingerprint, fingerprint)) {
        const previous = entry.row;
        entry.row = value;
        entry.fingerprint = fingerprint;
        for (const listener of this.updateListeners) fire.push(() => listener(previous, value));
      }

      return;
    }

    this.entries.set(key, { row: value, fingerprint, refCount: 1 });
    for (const listener of this.insertListeners) fire.push(() => listener(value));
  }

  /** @internal */
  uncover(binding: CacheBinding<T>, key: string, fire: (() => void)[]): void {
    if (!binding.covered.delete(key)) return;
    const entry = this.entries.get(key);
    if (entry === undefined) return;
    entry.refCount--;
    if (entry.refCount > 0) return;
    this.entries.delete(key);
    const removed = entry.row;
    for (const listener of this.deleteListeners) fire.push(() => listener(removed));
  }
}

/**
 * One subscription's view into a {@link TableCache}: the keys it covers, and the plan that maps
 * its descriptor's columns to the codec's by name (C4.5), made once per descriptor.
 */
export class CacheBinding<T> implements SubscriptionSink {
  /** @internal */
  readonly covered = new Set<string>();
  private plannedFor: WireDescriptor | null = null;
  private plan: number[] = [];

  constructor(private readonly cache: TableCache<T>) {}

  onSnapshot(rows: readonly CachedRow[]): void {
    const fire: (() => void)[] = [];
    const incoming = new Set(rows.map((row) => row.keyHex));
    for (const key of [...this.covered]) if (!incoming.has(key)) this.cache.uncover(this, key, fire);
    for (const row of rows) this.cache.cover(this, row, fire);
    for (const event of fire) event();
  }

  onRowOp(kind: number, previous: CachedRow | undefined, current: CachedRow | undefined): void {
    const fire: (() => void)[] = [];
    if (kind === RowOpKind.Delete) {
      if (previous !== undefined) this.cache.uncover(this, previous.keyHex, fire);
    } else if (current !== undefined) {
      this.cache.cover(this, current, fire);
    }

    for (const event of fire) event();
  }

  onReset(): void {
    // Nothing: a reset is always followed by a fresh initial set, and the snapshot diff against
    // the coverage still held is what turns a resync into precise events instead of a flush.
  }

  /** Detaches the subscription: rows only it covered leave with a delete. */
  detach(): void {
    const fire: (() => void)[] = [];
    for (const key of [...this.covered]) this.cache.uncover(this, key, fire);
    for (const event of fire) event();
  }

  /**
   * The row as the codec's type, plus a fingerprint of exactly the columns the codec reads: an
   * update that touches only a column these bindings cannot see is not a change they could show,
   * so it raises nothing.
   */
  read(row: CachedRow): { value: T; fingerprint: Uint8Array } {
    if (this.plannedFor !== row.descriptor) {
      this.plan = planColumns(this.cache.codec, row.descriptor);
      this.plannedFor = row.descriptor;
    }

    const { values, ranges } = decodeRow(row.descriptor, row.row, row.mask);
    const named: Record<string, ColumnValue> = {};
    const slices: Uint8Array[] = [];
    let length = 0;
    this.cache.codec.columns.forEach(([name], i) => {
      const ordinal = this.plan[i]!;
      const value = values[ordinal];
      const range = ranges[ordinal];
      if (value === undefined || range === undefined) {
        throw new SchemaMismatchError(
          `Table '${this.cache.tableName}': a column policy masked '${name}' out of this row, so it cannot fill the generated row type. `
            + "Read partially visible rows through the untyped subscription API, which reports exactly which columns arrived.");
      }

      named[name] = value;
      const slice = row.row.subarray(range[0], range[1]);
      slices.push(slice);
      length += slice.length;
    });

    const fingerprint = new Uint8Array(length);
    let offset = 0;
    for (const slice of slices) {
      fingerprint.set(slice, offset);
      offset += slice.length;
    }

    return { value: this.cache.codec.create(named), fingerprint };
  }
}

function planColumns<T>(codec: TableCodec<T>, descriptor: WireDescriptor): number[] {
  return codec.columns.map(([name, kind]) => {
    const ordinal = descriptor.columns.findIndex((column) => column.name === name);
    if (ordinal < 0) {
      throw new SchemaMismatchError(
        `Table '${codec.tableName}': the bindings read column '${name}', which the server's table no longer has. Regenerate the bindings.`);
    }

    const found = descriptor.columns[ordinal]!.kind;
    if (found !== kind) {
      throw new SchemaMismatchError(
        `Table '${codec.tableName}': column '${name}' is kind ${found} on the server but ${kind} in the bindings. Regenerate the bindings.`);
    }

    return ordinal;
  });
}

/** A typed subscription: the raw subscription, and the merged cache it feeds. */
export class TypedSubscription<T> {
  constructor(
    private readonly client: MelangeClient,
    readonly raw: Subscription,
    readonly cache: TableCache<T>,
    private readonly binding: CacheBinding<T>,
  ) {}

  /** Moves the subscription to new parameters: the server answers with a diff, not a new set (C7.2). */
  rescope(parameters: Parameters): Promise<void> {
    return this.client.rescope(this.raw, parameters);
  }

  /** Unsubscribes; rows no other subscription covers leave the cache with a delete. */
  async unsubscribe(): Promise<void> {
    await this.client.unsubscribe(this.raw);
    this.binding.detach();
  }
}

/** The merged caches of one client, one per table, and the typed subscribe that feeds them. */
export class TableCaches {
  private readonly caches = new Map<string, TableCache<unknown>>();

  constructor(readonly client: MelangeClient) {}

  getOrAdd<T>(codec: TableCodec<T>): TableCache<T> {
    let cache = this.caches.get(codec.tableName) as TableCache<T> | undefined;
    if (cache === undefined) {
      cache = new TableCache(codec);
      this.caches.set(codec.tableName, cache as TableCache<unknown>);
    }

    return cache;
  }

  /** Subscribes `query` (over `codec`'s table) into the table's merged cache. */
  async subscribe<T>(codec: TableCodec<T>, query: string, parameters: Parameters | null = null): Promise<TypedSubscription<T>> {
    const cache = this.getOrAdd(codec);
    const binding = cache.createBinding();
    const subscription = await this.client.subscribe(query, parameters, binding);
    return new TypedSubscription(this.client, subscription, cache, binding);
  }
}
