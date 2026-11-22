// @melangedb/client: the MelangeDB client for browsers. See docs/CLIENT-BINDINGS.md (TypeScript)
// and docs/CLIENT-CONFORMANCE.md, the contract this package is held to.
export { MelangeClient, type MelangeClientOptions, type DispatchMode, type ReconnectPolicy, type WebSocketFactory, type WebSocketLike } from "./client.js";
export { Subscription, CachedRow, MelangeError, SchemaMismatchError, type SubscriptionSink } from "./subscription.js";
export { TableCache, TableCaches, TypedSubscription, CacheBinding, type TableCodec } from "./cache.js";
export { MelangeSql, type MelangeSqlOptions, type SqlResult, type SqlValue } from "./sql.js";
export { arg, trimDefaults, encodeArguments, decodeArguments, decodeTagged, encodeKey, TaggedReader, ArgTag, type Arg } from "./encoding.js";
export {
  ColumnKind,
  RowOpKind,
  Identity,
  RowReader,
  decodeRow,
  isPresent,
  toHex,
  fromHex,
  bytesEqual,
  compareBytes,
  timestampToDate,
  dateToTimestamp,
  type ColumnValue,
  type ScheduleAt,
  type WireColumn,
  type WireDescriptor,
  type DecodedRow,
} from "./values.js";
export {
  FrameType,
  Channel,
  PROTOCOL_VERSION,
  MIN_PROTOCOL_VERSION,
  decodeServerFrame,
  encodeHello,
  encodeSubscribe,
  encodeCallReducer,
  type ServerFrame,
  type Parameters,
  type ParameterValue,
} from "./frames.js";
export { MsgPackReader, MsgPackWriter, ProtocolError } from "./msgpack.js";
