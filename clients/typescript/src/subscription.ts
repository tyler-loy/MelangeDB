import type { Parameters, ServerFrame, WireRow, WireRowOp } from "./frames.js";
import { ProtocolError } from "./msgpack.js";
import { RowOpKind, decodeRow, toHex, type ColumnValue, type WireDescriptor } from "./values.js";

/** A failure the server reported, or one the client raises under a MelangeDB error code. */
export class MelangeError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "MelangeError";
  }
}

/**
 * Bindings that cannot read the rows a subscription delivers: a column they read was removed,
 * renamed, or re-kinded on the server, or a column policy masked a column out of a row the
 * generated row type needs whole (C4.4, C4.5).
 */
export class SchemaMismatchError extends MelangeError {
  constructor(message: string) {
    super("schema_mismatch", message);
    this.name = "SchemaMismatchError";
  }
}

/**
 * One row in a subscription's cache: its encoded primary key and its row bytes in the
 * subscription's descriptor order. `columns` decodes on first read; columns a policy masked out
 * are absent, never defaulted.
 */
export class CachedRow {
  private decoded: Readonly<Record<string, ColumnValue>> | undefined;

  constructor(
    readonly key: Uint8Array,
    readonly row: Uint8Array,
    readonly mask: Uint8Array,
    readonly descriptor: WireDescriptor,
  ) {}

  /** The key as hex: the identity of the row in every cache (C5.1). */
  get keyHex(): string {
    return toHex(this.key);
  }

  get columns(): Readonly<Record<string, ColumnValue>> {
    if (this.decoded === undefined) {
      const { values } = decodeRow(this.descriptor, this.row, this.mask);
      const columns: Record<string, ColumnValue> = {};
      this.descriptor.columns.forEach((column, i) => {
        const value = values[i];
        if (value !== undefined) columns[column.name] = value;
      });
      this.decoded = columns;
    }

    return this.decoded;
  }
}

/**
 * Where a typed cache hears a subscription: the completed initial set (first subscribe,
 * re-establishment, and server-driven re-issue all land here), each applied op with its
 * self-healed kind, and resync resets. Called in apply order, before the subscription's own events.
 */
export interface SubscriptionSink {
  onSnapshot(rows: readonly CachedRow[]): void;
  onRowOp(kind: number, previous: CachedRow | undefined, current: CachedRow | undefined): void;
  onReset(): void;
}

type Listener<T extends unknown[]> = (...args: T) => void;

/**
 * A live subscription and its own row cache, loaded from the initial set and advanced by deltas.
 * Deltas that arrive before the set completes are buffered and replayed against its anchor,
 * which is what keeps the boundary gap-free and duplicate-free (C6.3).
 */
export class Subscription {
  readonly rows = new Map<string, CachedRow>();
  descriptor: WireDescriptor | null = null;
  anchorLsn = 0n;

  /**
   * Deltas that contradicted the cache (an insert for a held key, an update for a missing one).
   * Zero unless the anchor boundary leaked; deletes for missing keys are legitimate on replay.
   */
  inconsistencies = 0;

  private live = false;
  private initialRows: WireRow[] = [];
  private pending: { lsn: bigint; ops: readonly WireRowOp[] }[] = [];
  private appliedPromise!: Promise<void>;
  private settle!: { resolve: () => void; reject: (error: Error) => void; done: boolean };
  private readonly insertListeners = new Set<Listener<[CachedRow]>>();
  private readonly updateListeners = new Set<Listener<[CachedRow, CachedRow]>>();
  private readonly deleteListeners = new Set<Listener<[CachedRow]>>();

  constructor(
    readonly id: number,
    readonly query: string,
    public parameters: Parameters | null,
    private readonly sink: SubscriptionSink | null = null,
  ) {
    this.renewApplied();
  }

  /** Settles when the initial set has been applied; rejects if the server refused the subscription. */
  get applied(): Promise<void> {
    return this.appliedPromise;
  }

  get isLive(): boolean {
    return this.live;
  }

  get count(): number {
    return this.rows.size;
  }

  onInsert(listener: Listener<[CachedRow]>): () => void {
    this.insertListeners.add(listener);
    return () => this.insertListeners.delete(listener);
  }

  onUpdate(listener: Listener<[CachedRow, CachedRow]>): () => void {
    this.updateListeners.add(listener);
    return () => this.updateListeners.delete(listener);
  }

  onDelete(listener: Listener<[CachedRow]>): () => void {
    this.deleteListeners.add(listener);
    return () => this.deleteListeners.delete(listener);
  }

  /** @internal */
  acceptChunk(chunk: Extract<ServerFrame, { type: "SubscriptionApplied" }>): void {
    // A set arriving for a live subscription replaces it (a re-establishment or a server-side
    // re-issue): buffer deltas until it completes, exactly as on the first subscribe.
    this.live = false;
    if (chunk.descriptor !== null) this.descriptor = chunk.descriptor;
    if (chunk.chunkIndex === 0) this.initialRows = [];
    this.initialRows.push(...chunk.rows);
    if (!chunk.isLast) return;

    const shape = this.descriptor;
    if (shape === null) throw new ProtocolError(`Subscription ${this.id} completed an initial set without a descriptor.`);
    this.anchorLsn = chunk.anchorLsn;
    this.rows.clear();
    for (const row of this.initialRows) {
      const cached = new CachedRow(row.key, row.row, row.mask, shape);
      this.rows.set(cached.keyHex, cached);
    }

    this.initialRows = [];
    this.live = true;
    const replay = this.pending;
    this.pending = [];

    if (this.sink !== null) {
      try {
        this.sink.onSnapshot([...this.rows.values()]);
      } catch (error) {
        if (error instanceof SchemaMismatchError && this.fail(error)) return;
        throw error;
      }
    }

    for (const { lsn, ops } of replay) this.apply(lsn, ops);
    this.settle.done = true;
    this.settle.resolve();
  }

  /** @internal Applies one commit's ops; LSN 0 is a re-scope diff and skips the anchor test (C7.2). */
  apply(lsn: bigint, ops: readonly WireRowOp[]): void {
    if (!this.live) {
      this.pending.push({ lsn, ops });
      return;
    }

    if (lsn !== 0n && lsn <= this.anchorLsn) return;
    for (const op of ops) this.applyOp(op);
  }

  /** @internal */
  resetForResync(): void {
    this.live = false;
    this.rows.clear();
    this.initialRows = [];
    this.pending = [];
    if (this.settle.done) this.renewApplied();
    this.sink?.onReset();
  }

  /** @internal Fails a pending subscribe; false once the subscription was already applied. */
  fail(error: Error): boolean {
    if (this.settle.done) return false;
    this.settle.done = true;
    this.settle.reject(error);
    return true;
  }

  private applyOp(op: WireRowOp): void {
    const shape = this.descriptor!;
    const keyHex = toHex(op.key);
    let kind = op.kind;
    let previous: CachedRow | undefined;
    let current: CachedRow | undefined;
    switch (op.kind) {
      case RowOpKind.Insert:
        previous = this.rows.get(keyHex);
        if (previous !== undefined) {
          this.inconsistencies++;
          kind = RowOpKind.Update;
        }

        current = new CachedRow(op.key, op.row, op.mask, shape);
        this.rows.set(keyHex, current);
        break;
      case RowOpKind.Update:
        previous = this.rows.get(keyHex);
        if (previous === undefined) {
          this.inconsistencies++;
          kind = RowOpKind.Insert;
        }

        current = new CachedRow(op.key, op.row, op.mask, shape);
        this.rows.set(keyHex, current);
        break;
      case RowOpKind.Delete:
        previous = this.rows.get(keyHex);
        this.rows.delete(keyHex);
        if (previous === undefined) return;
        break;
      default:
        throw new ProtocolError(`Unknown row op kind ${op.kind}.`);
    }

    this.sink?.onRowOp(kind, previous, current);
    if (kind === RowOpKind.Insert) for (const listener of this.insertListeners) listener(current!);
    else if (kind === RowOpKind.Update) for (const listener of this.updateListeners) listener(previous ?? current!, current!);
    else for (const listener of this.deleteListeners) listener(previous!);
  }

  private renewApplied(): void {
    let resolve!: () => void;
    let reject!: (error: Error) => void;
    this.appliedPromise = new Promise<void>((res, rej) => {
      resolve = res;
      reject = rej;
    });

    // An unobserved refusal must not surface as an unhandled rejection; awaiting it still throws.
    this.appliedPromise.catch(() => undefined);
    this.settle = { resolve, reject, done: false };
  }
}
