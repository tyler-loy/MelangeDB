namespace MelangeDB.Protocol;

/// <summary>
/// Protocol version 2 framing: every frame is one MessagePack array of
/// <c>[type, channel, ...fields]</c>. Parameter maps use native MessagePack types;
/// <see cref="Identity"/> travels as 32-byte binary and <see cref="Timestamp"/> as its microsecond
/// integer.
/// <para>
/// Rows do not. Version 1 sent every row as a MessagePack map of column name to boxed value, which
/// re-sent the schema with every row and cost the encoder a dictionary build per subscriber per
/// row. Version 2 sends the schema-ordered v1 row bytes the store already holds, shaped once per
/// subscription by a <see cref="WireDescriptor"/>. Measured against the map shape: 1.18–1.40x the
/// bytes, 4.6–12.4x the encode time, 2.4–2.9x the decode time. The bytes were never the headline —
/// the CPU was, and it is spent on the fan-out path under the engine's write lock.
/// </para>
/// <para>
/// The break is hard: there is no version-1 encoder left, and a version-1 peer is turned away at
/// the handshake with <see cref="MelangeErrorCodes.UnsupportedVersion"/> rather than allowed to
/// fail later on a row it cannot read.
/// </para>
/// </summary>
public sealed class MessagePackFrameSerializer : IMelangeSerializer
{
    /// <summary>The protocol version this serializer implements.</summary>
    public const int ProtocolVersion = 2;

    public string Name => "MessagePack";

    public byte[] Serialize(Frame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var writer = new MsgPackWriter(64);
        Write(ref writer, frame);
        return writer.ToArray();
    }

    public int Measure(Frame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var writer = MsgPackWriter.Counting();
        Write(ref writer, frame);
        return writer.Length;
    }

    private static void Write(ref MsgPackWriter writer, Frame frame)
    {
        switch (frame)
        {
            case HelloFrame f:
                Header(ref writer, f, 3);
                writer.WriteInt64(f.MinVersion);
                writer.WriteInt64(f.MaxVersion);
                writer.WriteString(f.Token);
                break;
            case WelcomeFrame f:
                Header(ref writer, f, 5);
                writer.WriteInt64(f.Version);
                writer.WriteBinary(f.ConnectionId.ToByteArray());
                writer.WriteBinary(f.EpochId.ToByteArray());
                writer.WriteUInt64(f.HeadLsn);
                writer.WriteString(f.HttpProtocol);
                break;
            case CallReducerFrame f:
                Header(ref writer, f, 4);
                writer.WriteUInt64(f.RequestId);
                writer.WriteString(f.Reducer);
                writer.WriteBinary(f.Arguments);
                writer.WriteString(f.TraceParent);
                break;
            case ReducerResultFrame f:
                Header(ref writer, f, 5);
                writer.WriteUInt64(f.RequestId);
                writer.WriteBool(f.Ok);
                writer.WriteUInt64(f.Lsn);
                writer.WriteString(f.ErrorCode);
                writer.WriteString(f.Message);
                break;
            case SubscribeFrame f:
                Header(ref writer, f, 3);
                writer.WriteUInt64(f.SubscriptionId);
                writer.WriteString(f.Query);
                WriteValueMap(ref writer, f.Parameters);
                break;
            case UnsubscribeFrame f:
                Header(ref writer, f, 1);
                writer.WriteUInt64(f.SubscriptionId);
                break;
            case UnsubscribedFrame f:
                Header(ref writer, f, 1);
                writer.WriteUInt64(f.SubscriptionId);
                break;
            case SubscriptionAppliedFrame f:
                Header(ref writer, f, 6);
                writer.WriteUInt64(f.SubscriptionId);
                writer.WriteUInt64(f.AnchorLsn);
                writer.WriteUInt64(f.ChunkIndex);
                writer.WriteBool(f.IsLast);
                WriteDescriptor(ref writer, f.Descriptor);
                writer.WriteArrayHeader(f.Rows.Count);
                foreach (var row in f.Rows)
                {
                    writer.WriteArrayHeader(3);
                    writer.WriteBinary(row.Key);
                    writer.WriteBinary(row.Row.Span);
                    writer.WriteBinary(row.ColumnMask.Span);
                }

                break;
            case TransactionUpdateFrame f:
                Header(ref writer, f, 2);
                writer.WriteUInt64(f.Lsn);
                writer.WriteArrayHeader(f.Updates.Count);
                foreach (var update in f.Updates)
                {
                    writer.WriteArrayHeader(2);
                    writer.WriteUInt64(update.SubscriptionId);
                    writer.WriteArrayHeader(update.Ops.Count);
                    foreach (var op in update.Ops)
                    {
                        writer.WriteArrayHeader(4);
                        writer.WriteInt64((byte)op.Kind);
                        writer.WriteBinary(op.Key);
                        writer.WriteBinary(op.Row.Span);
                        writer.WriteBinary(op.ColumnMask.Span);
                    }
                }

                break;
            case ErrorFrame f:
                Header(ref writer, f, 4);
                writer.WriteString(f.Code);
                writer.WriteString(f.Message);
                writer.WriteUInt64(f.RequestId);
                writer.WriteUInt64(f.SubscriptionId);
                break;
            case PingFrame f:
                Header(ref writer, f, 1);
                writer.WriteUInt64(f.Id);
                break;
            case PongFrame f:
                Header(ref writer, f, 1);
                writer.WriteUInt64(f.Id);
                break;
            case ResumeFrame f:
                Header(ref writer, f, 3);
                writer.WriteBinary(f.EpochId.ToByteArray());
                writer.WriteUInt64(f.LastAckedLsn);
                writer.WriteArrayHeader(f.Subscriptions.Count);
                foreach (var subscription in f.Subscriptions)
                {
                    writer.WriteArrayHeader(3);
                    writer.WriteUInt64(subscription.SubscriptionId);
                    writer.WriteString(subscription.Query);
                    WriteValueMap(ref writer, subscription.Parameters);
                }

                break;
            case ResumeResultFrame f:
                Header(ref writer, f, 2);
                writer.WriteBool(f.Accepted);
                writer.WriteString(f.Reason);
                break;
            case ReauthenticateFrame f:
                Header(ref writer, f, 1);
                writer.WriteString(f.Token);
                break;
            case ReauthenticateResultFrame f:
                Header(ref writer, f, 2);
                writer.WriteBool(f.Accepted);
                writer.WriteString(f.Message);
                break;
            default:
                throw new NotSupportedException($"Unknown frame type {frame.GetType()}.");
        }
    }

    public Frame Deserialize(ReadOnlySpan<byte> message)
    {
        var reader = new MsgPackReader(message);
        var count = reader.ReadArrayHeader();
        if (count < 2)
            throw new MelangeProtocolException("Frame envelope must carry a type and a channel.");
        var type = (FrameType)reader.ReadInt64();
        var channel = (int)reader.ReadInt64();
        Frame frame = type switch
        {
            FrameType.Hello => new HelloFrame(
                (int)reader.ReadInt64(),
                (int)reader.ReadInt64(),
                reader.ReadString()),
            FrameType.Welcome => new WelcomeFrame(
                (int)reader.ReadInt64(),
                new Guid(reader.ReadBinary()),
                new Guid(reader.ReadBinary()),
                reader.ReadUInt64(),
                reader.ReadString() ?? string.Empty),
            FrameType.CallReducer => new CallReducerFrame(
                (uint)reader.ReadUInt64(),
                reader.ReadString() ?? throw new MelangeProtocolException("CallReducer requires a reducer name."),
                reader.ReadBinary(),
                reader.ReadString()),
            FrameType.ReducerResult => new ReducerResultFrame(
                (uint)reader.ReadUInt64(),
                reader.ReadBool(),
                reader.ReadUInt64(),
                reader.ReadString(),
                reader.ReadString()),
            FrameType.Subscribe => new SubscribeFrame(
                (uint)reader.ReadUInt64(),
                reader.ReadString() ?? throw new MelangeProtocolException("Subscribe requires a query."),
                ReadValueMap(ref reader)),
            FrameType.Unsubscribe => new UnsubscribeFrame((uint)reader.ReadUInt64()),
            FrameType.Unsubscribed => new UnsubscribedFrame((uint)reader.ReadUInt64()),
            FrameType.SubscriptionApplied => ReadSubscriptionApplied(ref reader),
            FrameType.TransactionUpdate => ReadTransactionUpdate(ref reader),
            FrameType.Error => new ErrorFrame(
                reader.ReadString() ?? MelangeErrorCodes.Internal,
                reader.ReadString() ?? string.Empty,
                (uint)reader.ReadUInt64(),
                (uint)reader.ReadUInt64()),
            FrameType.Ping => new PingFrame((uint)reader.ReadUInt64()),
            FrameType.Pong => new PongFrame((uint)reader.ReadUInt64()),
            FrameType.Resume => ReadResume(ref reader),
            FrameType.ResumeResult => new ResumeResultFrame(reader.ReadBool(), reader.ReadString()),
            FrameType.Reauthenticate => new ReauthenticateFrame(
                reader.ReadString() ?? throw new MelangeProtocolException("Reauthenticate requires a token.")),
            FrameType.ReauthenticateResult => new ReauthenticateResultFrame(reader.ReadBool(), reader.ReadString()),
            _ => throw new MelangeProtocolException($"Unknown frame type {(byte)type}."),
        };
        return frame with { Channel = channel };
    }

    private static void Header(ref MsgPackWriter writer, Frame frame, int fieldCount)
    {
        writer.WriteArrayHeader(fieldCount + 2);
        writer.WriteInt64((byte)frame.Type);
        writer.WriteInt64(frame.Channel);
    }

    private static SubscriptionAppliedFrame ReadSubscriptionApplied(ref MsgPackReader reader)
    {
        var subscriptionId = (uint)reader.ReadUInt64();
        var anchor = reader.ReadUInt64();
        var chunkIndex = (uint)reader.ReadUInt64();
        var isLast = reader.ReadBool();
        var descriptor = ReadDescriptor(ref reader);
        var rowCount = reader.ReadArrayHeader();
        var rows = new List<WireRow>(rowCount);
        for (var i = 0; i < rowCount; i++)
        {
            var parts = reader.ReadArrayHeader();
            if (parts != 3)
                throw new MelangeProtocolException("A wire row is [key, row, mask].");
            var key = reader.ReadBinary();
            var row = reader.ReadBinary();
            var mask = reader.ReadBinary();
            rows.Add(new WireRow(key, row, mask));
        }

        return new SubscriptionAppliedFrame(subscriptionId, anchor, chunkIndex, isLast, rows, descriptor);
    }

    private static void WriteDescriptor(ref MsgPackWriter writer, WireDescriptor? descriptor)
    {
        if (descriptor is null)
        {
            writer.WriteNil();
            return;
        }

        writer.WriteArrayHeader(2);
        writer.WriteString(descriptor.Table);
        writer.WriteArrayHeader(descriptor.Columns.Count);
        foreach (var column in descriptor.Columns)
        {
            writer.WriteArrayHeader(2);
            writer.WriteString(column.Name);
            writer.WriteInt64((byte)column.Kind);
        }
    }

    private static WireDescriptor? ReadDescriptor(ref MsgPackReader reader)
    {
        if (reader.TryReadNil())
            return null;
        if (reader.ReadArrayHeader() != 2)
            throw new MelangeProtocolException("A wire descriptor is [table, columns].");
        var table = reader.ReadString() ?? throw new MelangeProtocolException("A wire descriptor requires a table name.");
        var count = reader.ReadArrayHeader();
        var columns = new WireColumn[count];
        for (var i = 0; i < count; i++)
        {
            if (reader.ReadArrayHeader() != 2)
                throw new MelangeProtocolException("A wire descriptor column is [name, kind].");
            var name = reader.ReadString() ?? throw new MelangeProtocolException("A wire descriptor column requires a name.");
            columns[i] = new WireColumn(name, (ColumnKind)reader.ReadInt64());
        }

        return new WireDescriptor(table, columns);
    }

    private static TransactionUpdateFrame ReadTransactionUpdate(ref MsgPackReader reader)
    {
        var lsn = reader.ReadUInt64();
        var updateCount = reader.ReadArrayHeader();
        var updates = new List<SubscriptionUpdate>(updateCount);
        for (var i = 0; i < updateCount; i++)
        {
            var parts = reader.ReadArrayHeader();
            if (parts != 2)
                throw new MelangeProtocolException("A subscription update is [subscriptionId, ops].");
            var subscriptionId = (uint)reader.ReadUInt64();
            var opCount = reader.ReadArrayHeader();
            var ops = new List<WireRowOp>(opCount);
            for (var j = 0; j < opCount; j++)
            {
                var opParts = reader.ReadArrayHeader();
                if (opParts != 4)
                    throw new MelangeProtocolException("A row op is [kind, key, row, mask].");
                var kind = (RowOpKind)reader.ReadInt64();
                var key = reader.ReadBinary();
                var row = reader.ReadBinary();
                var mask = reader.ReadBinary();
                ops.Add(new WireRowOp(kind, key, row, mask));
            }

            updates.Add(new SubscriptionUpdate(subscriptionId, ops));
        }

        return new TransactionUpdateFrame(lsn, updates);
    }

    private static ResumeFrame ReadResume(ref MsgPackReader reader)
    {
        var epoch = new Guid(reader.ReadBinary());
        var lastAcked = reader.ReadUInt64();
        var count = reader.ReadArrayHeader();
        var subscriptions = new List<ResumeSubscription>(count);
        for (var i = 0; i < count; i++)
        {
            var parts = reader.ReadArrayHeader();
            if (parts != 3)
                throw new MelangeProtocolException("A resume subscription is [id, query, params].");
            subscriptions.Add(new ResumeSubscription(
                (uint)reader.ReadUInt64(),
                reader.ReadString() ?? throw new MelangeProtocolException("A resume subscription requires a query."),
                ReadValueMap(ref reader)));
        }

        return new ResumeFrame(epoch, lastAcked, subscriptions);
    }

    /// <summary>Writes a column/parameter value using native MessagePack types.</summary>
    public static void WriteValue(ref MsgPackWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNil();
                break;
            case bool b:
                writer.WriteBool(b);
                break;
            case sbyte or short or int or long:
                writer.WriteInt64(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case byte or ushort or uint or ulong:
                writer.WriteUInt64(Convert.ToUInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            case float f:
                writer.WriteFloat32(f);
                break;
            case double d:
                writer.WriteFloat64(d);
                break;
            case string s:
                writer.WriteString(s);
                break;
            case byte[] bytes:
                writer.WriteBinary(bytes);
                break;
            case Identity identity:
                writer.WriteBinary(identity.ToByteArray());
                break;
            case Timestamp timestamp:
                writer.WriteInt64(timestamp.UnixTimeMicroseconds);
                break;
            default:
                throw new NotSupportedException($"Value of type {value.GetType()} is not wire-encodable.");
        }
    }

    /// <summary>Reads one value written by <see cref="WriteValue"/>. Integers surface as long, or ulong beyond long range.</summary>
    public static object? ReadValue(ref MsgPackReader reader)
    {
        var code = reader.PeekCode();
        if (code == 0xc0)
        {
            reader.TryReadNil();
            return null;
        }

        if (code is 0xc2 or 0xc3)
            return reader.ReadBool();
        if (code <= 0x7f || code is >= 0xcc and <= 0xcf)
        {
            var unsigned = reader.ReadUInt64();
            return unsigned <= long.MaxValue ? (long)unsigned : unsigned;
        }

        if (code >= 0xe0 || code is >= 0xd0 and <= 0xd3)
            return reader.ReadInt64();
        if (code == 0xca)
            return (float)reader.ReadFloat64();
        if (code == 0xcb)
            return reader.ReadFloat64();
        if ((code & 0xe0) == 0xa0 || code is 0xd9 or 0xda or 0xdb)
            return reader.ReadString();
        if (code is 0xc4 or 0xc5 or 0xc6)
            return reader.ReadBinary();
        throw new MelangeProtocolException($"MessagePack code 0x{code:x2} is not a wire value.");
    }

    private static void WriteValueMap(ref MsgPackWriter writer, IReadOnlyDictionary<string, object?>? map)
    {
        if (map is null)
        {
            writer.WriteNil();
            return;
        }

        writer.WriteMapHeader(map.Count);
        foreach (var (key, value) in map)
        {
            writer.WriteString(key);
            WriteValue(ref writer, value);
        }
    }

    private static Dictionary<string, object?>? ReadValueMap(ref MsgPackReader reader)
    {
        if (reader.TryReadNil())
            return null;
        var count = reader.ReadMapHeader();
        var map = new Dictionary<string, object?>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var key = reader.ReadString() ?? throw new MelangeProtocolException("Map keys must be strings.");
            map[key] = ReadValue(ref reader);
        }

        return map;
    }
}
