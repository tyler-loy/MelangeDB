import {
  decodeServerFrame,
  encodeCallProducer,
  encodeCallReducer,
  encodeHello,
  encodePong,
  encodeReauthenticate,
  encodeResume,
  encodeSubscribe,
  encodeUnsubscribe,
  type Parameters,
  type ServerFrame,
} from "./frames.js";
import { MelangeError, Subscription, type SubscriptionSink } from "./subscription.js";
import { Identity, bytesEqual } from "./values.js";

/** When frames apply: as they arrive, or only inside {@link MelangeClient.frameTick}. */
export type DispatchMode = "immediate" | "manual";

/** Automatic reconnection after an unexpected drop: exponential backoff with jitter. */
export interface ReconnectPolicy {
  readonly initialDelayMs?: number;
  readonly maxDelayMs?: number;
  readonly multiplier?: number;
  readonly jitter?: number;
  readonly maxAttempts?: number;
}

/** The subset of the WebSocket API the client uses; the browser's and Node's both satisfy it. */
export interface WebSocketLike {
  binaryType: string;
  readonly readyState: number;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  onclose: ((event: { code?: number; reason?: string }) => void) | null;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export interface MelangeClientOptions {
  /** The server's socket endpoint, e.g. `wss://game.example.com/melange`. */
  readonly url: string | URL;
  /** A bearer token presented in `Hello` (or exchanged for a connect ticket). */
  readonly token?: string;
  /** Supplies a fresh token for each connect and reconnect; takes precedence over `token`. */
  readonly tokenProvider?: () => string | Promise<string>;
  /**
   * Exchange the token for a single-use ticket over HTTP and put only the ticket on the socket
   * URL (C1.2). Optional: browsers can send the token in `Hello` instead.
   */
  readonly useTicket?: boolean;
  readonly ticketUrl?: string | URL;
  /** `immediate` (the default) or `manual`: frames apply only inside `frameTick()`. */
  readonly dispatch?: DispatchMode;
  /** Manual dispatch only: queued frames past this fail the connection loudly. Default 65536. */
  readonly dispatchQueueLimit?: number;
  /** Reconnect automatically after an unexpected drop. Off unless given. */
  readonly reconnect?: ReconnectPolicy;
  /** Creates sockets; defaults to the global `WebSocket`. */
  readonly webSocket?: WebSocketFactory;
}

interface Pending<T> {
  resolve(value: T): void;
  reject(error: Error): void;
}

type DataFrame = Extract<ServerFrame, { type: "SubscriptionApplied" | "TransactionUpdate" }>;

interface QueueEntry {
  readonly era: number;
  readonly run: () => void;
}

const OPEN = 1;

/**
 * A connection to a MelangeDB server: subscriptions, reducer and producer calls, resume, and the
 * conformance contract's obligations (docs/CLIENT-CONFORMANCE.md). Browser-first: it needs only
 * `WebSocket`, `fetch` (for connect tickets), and `TextEncoder`.
 */
export class MelangeClient {
  /** Who the server says this connection is (C16.1). */
  identity: Identity | null = null;
  protocolVersion = 0;
  connectionId: Uint8Array | null = null;
  /** The log incarnation the resume cursor counts against (C2.5): opaque bytes, echoed exactly. */
  epochId: Uint8Array | null = null;
  httpProtocol = "";

  private cursor = 0n;
  private socket: WebSocketLike | null = null;
  private era = 0;
  private eraFloor = 0;
  private closing = false;
  private reconnecting = false;
  private nextRequestId = 0;
  private nextSubscriptionId = 0;
  private readonly subscriptions = new Map<number, Subscription>();
  private readonly calls = new Map<number, Pending<{ ok: boolean; lsn: bigint; payload: Uint8Array; code: string | null; message: string | null }>>();
  private readonly unsubscribes = new Map<number, Pending<void>>();
  private welcome: Pending<Extract<ServerFrame, { type: "Welcome" }>> | null = null;
  private resume: Pending<Extract<ServerFrame, { type: "ResumeResult" }>> | null = null;
  private reauth: Pending<Extract<ServerFrame, { type: "ReauthenticateResult" }>> | null = null;
  private readonly queue: QueueEntry[] = [];
  private readonly errorListeners = new Set<(error: MelangeError) => void>();
  private readonly disconnectListeners = new Set<() => void>();
  private readonly reconnectListeners = new Set<(resumed: boolean) => void>();

  constructor(private readonly options: MelangeClientOptions) {}

  /** The resume cursor: the highest LSN fully applied, or retained to apply (C10.1). */
  get lastAckedLsn(): bigint {
    return this.cursor;
  }

  get isConnected(): boolean {
    return this.socket?.readyState === OPEN;
  }

  /** Connection-scoped errors, and errors no pending request claimed. */
  onError(listener: (error: MelangeError) => void): () => void {
    this.errorListeners.add(listener);
    return () => this.errorListeners.delete(listener);
  }

  onDisconnected(listener: () => void): () => void {
    this.disconnectListeners.add(listener);
    return () => this.disconnectListeners.delete(listener);
  }

  /** After an automatic reconnect: `true` if it resumed, `false` if it fully resynced. */
  onReconnected(listener: (resumed: boolean) => void): () => void {
    this.reconnectListeners.add(listener);
    return () => this.reconnectListeners.delete(listener);
  }

  async connect(): Promise<void> {
    this.closing = false;
    await this.dial();
  }

  /** Closes the connection; no reconnect follows. */
  close(): void {
    this.closing = true;
    const socket = this.socket;
    this.socket = null;
    this.era++;
    this.failPending(new MelangeError("internal", "The client was closed."));
    socket?.close(1000);
  }

  /**
   * Subscribes to `query`. Resolves once the initial set is applied; rejects with the server's
   * refusal code. `sink` is how a typed cache attaches before the first frame can arrive.
   */
  async subscribe(query: string, parameters: Parameters | null = null, sink: SubscriptionSink | null = null): Promise<Subscription> {
    const id = ++this.nextSubscriptionId;
    const subscription = new Subscription(id, query, parameters, sink);
    this.subscriptions.set(id, subscription);
    this.send(encodeSubscribe(id, query, parameters));
    try {
      await subscription.applied;
    } catch (error) {
      this.subscriptions.delete(id);
      throw error;
    }

    return subscription;
  }

  /** Re-scopes a live subscription's parameters in place: the server answers with an LSN-0 diff. */
  async rescope(subscription: Subscription, parameters: Parameters): Promise<void> {
    subscription.parameters = parameters;
    this.send(encodeSubscribe(subscription.id, subscription.query, parameters));
  }

  /** Removes a subscription and waits for the server's acknowledgement. */
  async unsubscribe(subscription: Subscription): Promise<void> {
    const done = new Promise<void>((resolve, reject) => this.unsubscribes.set(subscription.id, { resolve, reject }));
    this.send(encodeUnsubscribe(subscription.id));
    await done;
    this.subscriptions.delete(subscription.id);
  }

  /** Calls a reducer with an encoded argument payload; resolves to the commit's LSN. */
  async callReducer(reducer: string, args: Uint8Array = new Uint8Array(0)): Promise<bigint> {
    const id = ++this.nextRequestId;
    const result = this.pendingCall(id);
    this.send(encodeCallReducer(id, reducer, args));
    const reply = await result;
    if (!reply.ok) throw new MelangeError(reply.code ?? "internal", reply.message ?? `Calling ${reducer} failed.`);
    return reply.lsn;
  }

  /** Calls a producer (protocol 3); resolves to its result payload (one tagged value) and LSN. */
  async callProducer(producer: string, args: Uint8Array = new Uint8Array(0)): Promise<{ payload: Uint8Array; lsn: bigint }> {
    if (this.protocolVersion < 3)
      throw new MelangeError("unsupported_version", `Producers need protocol 3; this connection speaks ${this.protocolVersion}.`);
    const id = ++this.nextRequestId;
    const result = this.pendingCall(id);
    this.send(encodeCallProducer(id, producer, args));
    const reply = await result;
    if (!reply.ok) throw new MelangeError(reply.code ?? "internal", reply.message ?? `Calling ${producer} failed.`);
    return { payload: reply.payload, lsn: reply.lsn };
  }

  /** Presents a fresh token before the current one expires (C13.1). */
  async reauthenticate(token: string): Promise<void> {
    const result = new Promise<Extract<ServerFrame, { type: "ReauthenticateResult" }>>((resolve, reject) => {
      this.reauth = { resolve, reject };
    });
    this.send(encodeReauthenticate(token));
    const reply = await result;
    if (!reply.accepted) throw new MelangeError("unauthorized", reply.message ?? "Reauthentication was rejected.");
  }

  /**
   * Reconnects after a drop. Resumes from the cursor when the server's log is the same epoch and
   * accepts the gap; otherwise re-establishes every subscription and reconciles by diff (C10).
   * Resolves `true` if it resumed. Refuses a server that welcomes a different identity (C10.5).
   */
  async reconnect(): Promise<boolean> {
    const previousEpoch = this.epochId;
    const previousIdentity = this.identity;
    this.closing = false;
    await this.dial();
    if (previousIdentity !== null && !previousIdentity.equals(this.identity)) {
      const welcomed = this.identity;
      this.identity = previousIdentity;
      this.socket?.close(1000);
      throw new MelangeError("identity_changed", `The reconnect was welcomed as ${welcomed}, but this client is ${previousIdentity}.`);
    }

    const live = [...this.subscriptions.values()].filter((s) => s.isLive);
    if (live.length > 0 && previousEpoch !== null && this.epochId !== null && bytesEqual(previousEpoch, this.epochId)) {
      const result = new Promise<Extract<ServerFrame, { type: "ResumeResult" }>>((resolve, reject) => {
        this.resume = { resolve, reject };
      });
      this.send(encodeResume(previousEpoch, this.cursor, live.map((s) => ({ subscriptionId: s.id, query: s.query, parameters: s.parameters }))));
      if ((await result).accepted) {
        await this.reestablish([...this.subscriptions.values()].filter((s) => !s.isLive));
        return true;
      }
    }

    // Full resync: nothing queued from the dead era may apply to the rebuilt caches.
    this.cursor = 0n;
    this.eraFloor = this.era;
    this.queue.length = 0;
    for (const subscription of this.subscriptions.values()) subscription.resetForResync();
    await this.reestablish([...this.subscriptions.values()]);
    return false;
  }

  /**
   * Manual dispatch: applies up to `maxFrames` queued frames, in arrival order, on the caller's
   * stack. Cache state and events advance together (C8.3). Returns how many applied.
   */
  frameTick(maxFrames = Number.MAX_SAFE_INTEGER): number {
    if (this.options.dispatch !== "manual") throw new Error("frameTick() requires dispatch: 'manual'.");
    let applied = 0;
    while (applied < maxFrames && this.queue.length > 0) {
      const entry = this.queue.shift()!;
      if (entry.era < this.eraFloor) continue;
      entry.run();
      applied++;
    }

    return applied;
  }

  /** Aborts the socket without the client's consent: for tests of drop and resume. */
  drop(): void {
    this.socket?.close(4000, "dropped");
  }

  private async reestablish(subscriptions: Subscription[]): Promise<void> {
    for (const subscription of subscriptions) this.send(encodeSubscribe(subscription.id, subscription.query, subscription.parameters));
    await Promise.all(subscriptions.map((subscription) => subscription.applied));
  }

  private pendingCall(id: number): Promise<{ ok: boolean; lsn: bigint; payload: Uint8Array; code: string | null; message: string | null }> {
    return new Promise((resolve, reject) => this.calls.set(id, { resolve, reject }));
  }

  private async dial(): Promise<void> {
    const token = this.options.tokenProvider !== undefined ? await this.options.tokenProvider() : this.options.token ?? null;
    let url = new URL(this.options.url.toString());
    let helloToken = token;
    if (this.options.useTicket) {
      const ticket = await this.mintTicket(token);
      url.searchParams.set("ticket", ticket);
      helloToken = null;
    }

    this.failPending(new MelangeError("internal", "The connection was replaced."));
    const era = ++this.era;
    const factory = this.options.webSocket ?? ((target: string) => new WebSocket(target) as unknown as WebSocketLike);
    const socket = factory(url.toString());
    socket.binaryType = "arraybuffer";
    this.socket = socket;

    await new Promise<void>((resolve, reject) => {
      socket.onopen = () => resolve();
      socket.onerror = () => reject(new MelangeError("internal", `Could not connect to ${url.origin}.`));
      socket.onclose = (event) => reject(new MelangeError("internal", `The server closed the connection during the handshake (${event.code ?? "no code"}).`));
    });

    const welcome = new Promise<Extract<ServerFrame, { type: "Welcome" }>>((resolve, reject) => {
      this.welcome = { resolve, reject };
    });
    socket.onmessage = (event) => this.receive(event.data, socket, era);
    socket.onerror = () => undefined;
    socket.onclose = () => this.closed(era);
    this.send(encodeHello(helloToken));
    const frame = await welcome;
    this.protocolVersion = frame.version;
    this.connectionId = frame.connectionId;
    this.epochId = frame.epochId;
    this.httpProtocol = frame.httpProtocol;
    this.identity = frame.identity;
  }

  private async mintTicket(token: string | null): Promise<string> {
    const socketUrl = new URL(this.options.url.toString());
    const endpoint = this.options.ticketUrl !== undefined
      ? new URL(this.options.ticketUrl.toString())
      : new URL(`${socketUrl.protocol === "wss:" ? "https:" : "http:"}//${socketUrl.host}${socketUrl.pathname.replace(/\/$/, "")}/ticket`);
    const response = await fetch(endpoint, { method: "POST", headers: token === null ? {} : { Authorization: `Bearer ${token}` } });
    const body: unknown = await response.json().catch(() => null);
    if (!response.ok) throw new MelangeError("unauthorized", `The ticket endpoint answered ${response.status}.`);
    const ticket = (body as { ticket?: unknown } | null)?.ticket;
    if (typeof ticket !== "string" || ticket.length === 0) throw new MelangeError("internal", "The ticket endpoint returned no ticket.");
    return ticket;
  }

  private send(bytes: Uint8Array): void {
    const socket = this.socket;
    if (socket === null || socket.readyState !== OPEN) throw new MelangeError("internal", "Not connected.");
    socket.send(bytes);
  }

  private receive(data: unknown, socket: WebSocketLike, era: number): void {
    if (era !== this.era) return;
    let frame: ServerFrame;
    try {
      const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : (data as Uint8Array);
      frame = decodeServerFrame(bytes);
    } catch (error) {
      this.raise(new MelangeError("protocol", error instanceof Error ? error.message : String(error)));
      socket.close(1002);
      return;
    }

    this.handle(frame, socket, era);
  }

  private handle(frame: ServerFrame, socket: WebSocketLike, era: number): void {
    switch (frame.type) {
      case "Welcome":
        this.welcome?.resolve(frame);
        this.welcome = null;
        return;
      case "ReducerResult":
      case "ProducerResult": {
        const call = this.calls.get(frame.requestId);
        if (call === undefined) return;
        this.calls.delete(frame.requestId);
        call.resolve({
          ok: frame.ok,
          lsn: frame.lsn,
          payload: frame.type === "ProducerResult" ? frame.payload : new Uint8Array(0),
          code: frame.errorCode,
          message: frame.message,
        });
        return;
      }
      case "SubscriptionApplied":
      case "TransactionUpdate":
        this.dispatchData(frame, socket, era);
        return;
      case "Unsubscribed": {
        const pending = this.unsubscribes.get(frame.subscriptionId);
        this.unsubscribes.delete(frame.subscriptionId);
        pending?.resolve();
        return;
      }
      case "ResumeResult":
        this.resume?.resolve(frame);
        this.resume = null;
        return;
      case "ReauthenticateResult":
        this.reauth?.resolve(frame);
        this.reauth = null;
        return;
      case "Ping":
        if (socket.readyState === OPEN) socket.send(encodePong(frame.id));
        return;
      case "Pong":
        return;
      case "Error":
        this.handleError(frame, era);
        return;
    }
  }

  private dispatchData(frame: DataFrame, socket: WebSocketLike, era: number): void {
    if (this.options.dispatch !== "manual") {
      this.applyData(frame);
      return;
    }

    if (this.queue.length >= (this.options.dispatchQueueLimit ?? 65536)) {
      // Never drop silently and never block the receive path (C11.2): fail loudly, cursor unmoved.
      this.raise(new MelangeError("dispatch_overflow", "The dispatch queue reached its limit without a frameTick; the connection was closed. Reconnect to recover."));
      socket.close(4001, "dispatch overflow");
      return;
    }

    this.queue.push({ era, run: () => this.applyData(frame) });
    this.advanceCursor(frame);
  }

  private applyData(frame: DataFrame): void {
    if (frame.type === "SubscriptionApplied") {
      const subscription = this.subscriptions.get(frame.subscriptionId);
      if (subscription === undefined) return;
      subscription.acceptChunk(frame);
    } else {
      for (const update of frame.updates) this.subscriptions.get(update.subscriptionId)?.apply(frame.lsn, update.ops);
    }

    this.advanceCursor(frame);
  }

  /** The cursor moves when a whole frame is applied or retained to apply (C10.1); LSN 0 never moves it. */
  private advanceCursor(frame: DataFrame): void {
    if (frame.type === "SubscriptionApplied") {
      if (frame.isLast && this.subscriptions.has(frame.subscriptionId) && frame.anchorLsn > this.cursor) this.cursor = frame.anchorLsn;
    } else if (frame.lsn > this.cursor) {
      this.cursor = frame.lsn;
    }
  }

  private handleError(frame: Extract<ServerFrame, { type: "Error" }>, era: number): void {
    const error = new MelangeError(frame.code, frame.message);
    if (frame.subscriptionId !== 0) {
      const subscription = this.subscriptions.get(frame.subscriptionId);
      if (subscription?.fail(error)) {
        this.subscriptions.delete(frame.subscriptionId);
        return;
      }
    }

    if (frame.requestId !== 0) {
      const call = this.calls.get(frame.requestId);
      if (call !== undefined) {
        this.calls.delete(frame.requestId);
        call.reject(error);
        return;
      }
    }

    if (this.welcome !== null) {
      this.welcome.reject(error);
      this.welcome = null;
    }

    // The server forgot every subscription (C11.1): re-establish them all, at once.
    if (frame.code === "overflow_resync" && frame.subscriptionId === 0 && era === this.era) {
      for (const subscription of this.subscriptions.values()) {
        try {
          this.send(encodeSubscribe(subscription.id, subscription.query, subscription.parameters));
        } catch {
          break;
        }
      }
    }

    this.raise(error);
  }

  private raise(error: MelangeError): void {
    for (const listener of this.errorListeners) listener(error);
  }

  private closed(era: number): void {
    if (era !== this.era) return;
    this.socket = null;
    this.failPending(new MelangeError("internal", "The connection closed."));
    for (const listener of this.disconnectListeners) listener();
    if (!this.closing && this.options.reconnect !== undefined && !this.reconnecting) void this.reconnectLoop(this.options.reconnect);
  }

  private async reconnectLoop(policy: ReconnectPolicy): Promise<void> {
    this.reconnecting = true;
    let delay = policy.initialDelayMs ?? 250;
    try {
      for (let attempt = 1; policy.maxAttempts === undefined || attempt <= policy.maxAttempts; attempt++) {
        const jitter = 1 + (Math.random() * 2 - 1) * (policy.jitter ?? 0.2);
        await new Promise((resolve) => setTimeout(resolve, delay * jitter));
        if (this.closing) return;
        try {
          const resumed = await this.reconnect();
          for (const listener of this.reconnectListeners) listener(resumed);
          return;
        } catch (error) {
          if (error instanceof MelangeError && error.code === "identity_changed") {
            this.raise(error);
            return;
          }
        }

        delay = Math.min(delay * (policy.multiplier ?? 2), policy.maxDelayMs ?? 30_000);
      }

      this.raise(new MelangeError("internal", "Reconnecting gave up."));
    } finally {
      this.reconnecting = false;
    }
  }

  private failPending(error: Error): void {
    for (const call of this.calls.values()) call.reject(error);
    this.calls.clear();
    for (const pending of this.unsubscribes.values()) pending.reject(error);
    this.unsubscribes.clear();
    this.welcome?.reject(error);
    this.welcome = null;
    this.resume?.reject(error);
    this.resume = null;
    this.reauth?.reject(error);
    this.reauth = null;
  }
}
