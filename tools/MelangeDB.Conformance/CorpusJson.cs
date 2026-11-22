using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using MelangeDB.Protocol;

namespace MelangeDB.Conformance;

/// <summary>
/// The corpus's canonical decoded form: how every frame field, column value, and tagged value is
/// written as JSON so a client in any language can compare its own decode against it exactly.
/// docs/CLIENT-CONFORMANCE.md states the same rules; this class is their reference.
/// <list type="bullet">
/// <item>64-bit integers (LSNs, <c>Int64</c>, <c>UInt64</c>, timestamps, microseconds) are decimal
/// strings, because a JSON number loses precision past 2^53 in most readers.</item>
/// <item>Smaller integers are JSON numbers.</item>
/// <item>Floats are the lowercase hex of their IEEE-754 bits, big-endian (8 digits for
/// <c>Float32</c>, 16 for <c>Float64</c>), so NaN payloads, negative zero, and infinities
/// compare exactly.</item>
/// <item>Bytes, identities, keys, masks, and the opaque 16-byte ids are lowercase hex.</item>
/// <item>A column a mask excludes is absent from <c>columns</c>, not null.</item>
/// </list>
/// </summary>
internal static class CorpusJson
{
    /// <summary>
    /// Renders one server-to-client frame. <paramref name="descriptors"/> is the session's
    /// descriptor state, keyed by subscription id: a <c>SubscriptionApplied</c> chunk that
    /// carries a descriptor updates it, and every row decodes against it.
    /// </summary>
    public static JsonObject Frame(Frame frame, Dictionary<uint, WireDescriptor> descriptors)
    {
        var json = new JsonObject
        {
            ["type"] = frame.Type.ToString(),
            ["channel"] = frame.Channel,
        };
        switch (frame)
        {
            case WelcomeFrame f:
                json["version"] = f.Version;
                json["connectionId"] = Hex(f.ConnectionId.ToByteArray());
                json["epochId"] = Hex(f.EpochId.ToByteArray());
                json["headLsn"] = U64(f.HeadLsn);
                json["httpProtocol"] = f.HttpProtocol;
                json["identity"] = Hex(f.Identity.ToByteArray());
                break;
            case ReducerResultFrame f:
                json["requestId"] = f.RequestId;
                json["ok"] = f.Ok;
                json["lsn"] = U64(f.Lsn);
                json["errorCode"] = f.ErrorCode;
                json["message"] = f.Message;
                break;
            case ProducerResultFrame f:
                json["requestId"] = f.RequestId;
                json["ok"] = f.Ok;
                json["lsn"] = U64(f.Lsn);
                json["payload"] = Hex(f.Payload.Span);
                json["value"] = f.Payload.IsEmpty ? null : TaggedValue(f.Payload.Span);
                json["errorCode"] = f.ErrorCode;
                json["message"] = f.Message;
                break;
            case UnsubscribedFrame f:
                json["subscriptionId"] = f.SubscriptionId;
                break;
            case SubscriptionAppliedFrame f:
                if (f.Descriptor is { } descriptor)
                    descriptors[f.SubscriptionId] = descriptor;
                json["subscriptionId"] = f.SubscriptionId;
                json["anchorLsn"] = U64(f.AnchorLsn);
                json["chunkIndex"] = f.ChunkIndex;
                json["isLast"] = f.IsLast;
                json["descriptor"] = f.Descriptor is null ? null : Descriptor(f.Descriptor);
                var rows = new JsonArray();
                foreach (var row in f.Rows)
                    rows.Add(Row(row.Key, row.Row.Span, row.ColumnMask.Span, descriptors[f.SubscriptionId]));
                json["rows"] = rows;
                break;
            case TransactionUpdateFrame f:
                json["lsn"] = U64(f.Lsn);
                var updates = new JsonArray();
                foreach (var update in f.Updates)
                {
                    var ops = new JsonArray();
                    foreach (var op in update.Ops)
                    {
                        var rendered = op.Kind == RowOpKind.Delete
                            ? new JsonObject { ["key"] = Hex(op.Key), ["mask"] = Hex(op.ColumnMask.Span), ["columns"] = null }
                            : Row(op.Key, op.Row.Span, op.ColumnMask.Span, descriptors[update.SubscriptionId]);
                        rendered.Insert(0, "kind", (int)op.Kind);
                        ops.Add(rendered);
                    }

                    updates.Add(new JsonObject { ["subscriptionId"] = update.SubscriptionId, ["ops"] = ops });
                }

                json["updates"] = updates;
                break;
            case ErrorFrame f:
                json["code"] = f.Code;
                json["message"] = f.Message;
                json["requestId"] = f.RequestId;
                json["subscriptionId"] = f.SubscriptionId;
                break;
            case PingFrame f:
                json["id"] = f.Id;
                break;
            case PongFrame f:
                json["id"] = f.Id;
                break;
            case ResumeResultFrame f:
                json["accepted"] = f.Accepted;
                json["reason"] = f.Reason;
                break;
            case ReauthenticateResultFrame f:
                json["accepted"] = f.Accepted;
                json["message"] = f.Message;
                break;
            default:
                throw new InvalidOperationException($"{frame.Type} is not a server-to-client frame.");
        }

        return json;
    }

    public static JsonObject Descriptor(WireDescriptor descriptor)
    {
        var columns = new JsonArray();
        foreach (var column in descriptor.Columns)
            columns.Add(new JsonObject { ["name"] = column.Name, ["kind"] = (int)column.Kind });
        return new JsonObject { ["table"] = descriptor.Table, ["columns"] = columns };
    }

    /// <summary>One row: its key, its mask, and its present columns decoded against the descriptor.</summary>
    public static JsonObject Row(ReadOnlySpan<byte> key, ReadOnlySpan<byte> row, ReadOnlySpan<byte> mask, WireDescriptor descriptor)
    {
        var columns = new JsonObject();
        var reader = new RowReader(row);
        for (var i = 0; i < descriptor.Columns.Count; i++)
        {
            if (!WireRowValues.IsPresent(mask, i))
                continue;
            columns[descriptor.Columns[i].Name] = Column(ref reader, descriptor.Columns[i].Kind);
        }

        if (reader.Position != row.Length)
            throw new InvalidOperationException($"Row decode for '{descriptor.Table}' left {row.Length - reader.Position} byte(s) unread.");
        return new JsonObject { ["key"] = Hex(key), ["mask"] = Hex(mask), ["columns"] = columns };
    }

    /// <summary>One column value in its canonical form.</summary>
    public static JsonNode? Column(ref RowReader reader, ColumnKind kind) => kind switch
    {
        ColumnKind.Bool => reader.ReadBool(),
        ColumnKind.Int8 => reader.ReadInt8(),
        ColumnKind.UInt8 => reader.ReadUInt8(),
        ColumnKind.Int16 => reader.ReadInt16(),
        ColumnKind.UInt16 => reader.ReadUInt16(),
        ColumnKind.Int32 => reader.ReadInt32(),
        ColumnKind.UInt32 => reader.ReadUInt32(),
        ColumnKind.Int64 => I64(reader.ReadInt64()),
        ColumnKind.UInt64 => U64(reader.ReadUInt64()),
        ColumnKind.Float32 => F32(reader.ReadFloat32()),
        ColumnKind.Float64 => F64(reader.ReadFloat64()),
        ColumnKind.String => reader.ReadString(),
        ColumnKind.Bytes => reader.ReadBytes() is { } bytes ? Hex(bytes) : null,
        ColumnKind.Identity => Hex(reader.ReadIdentity().ToByteArray()),
        ColumnKind.Timestamp => I64(reader.ReadTimestamp().UnixTimeMicroseconds),
        ColumnKind.ScheduleAt => ScheduleAtValue(reader.ReadScheduleAt()),
        _ => throw new InvalidOperationException($"Unknown column kind {kind}."),
    };

    public static JsonObject ScheduleAtValue(ScheduleAt value) => new()
    {
        ["interval"] = value.IsInterval,
        ["micros"] = I64(value.Microseconds),
    };

    /// <summary>
    /// Decodes exactly one value in the reducer-argument encoding (a producer's result, or one
    /// argument) and fails if bytes are left over.
    /// </summary>
    public static JsonObject TaggedValue(ReadOnlySpan<byte> payload)
    {
        var position = 0;
        var value = ReadTagged(payload, ref position);
        if (position != payload.Length)
            throw new InvalidOperationException($"Tagged value left {payload.Length - position} byte(s) unread.");
        return value;
    }

    /// <summary>Decodes a reducer-argument payload: a little-endian UInt16 count, then that many tagged values.</summary>
    public static JsonArray Arguments(ReadOnlySpan<byte> payload)
    {
        var values = new JsonArray();
        if (payload.IsEmpty)
            return values;
        var count = BinaryPrimitives.ReadUInt16LittleEndian(payload);
        var position = 2;
        for (var i = 0; i < count; i++)
            values.Add(ReadTagged(payload, ref position));
        if (position != payload.Length)
            throw new InvalidOperationException($"Arguments left {payload.Length - position} byte(s) unread.");
        return values;
    }

    private static JsonObject ReadTagged(ReadOnlySpan<byte> data, ref int position)
    {
        var tag = data[position++];
        switch (tag)
        {
            case 0:
                return new JsonObject { ["tag"] = "Null" };
            case 1:
                return new JsonObject { ["tag"] = "Bool", ["value"] = data[position++] != 0 };
            case 2:
                {
                    var value = BinaryPrimitives.ReadInt64LittleEndian(data[position..]);
                    position += 8;
                    return new JsonObject { ["tag"] = "Int64", ["value"] = I64(value) };
                }

            case 3:
                {
                    var value = BinaryPrimitives.ReadUInt64LittleEndian(data[position..]);
                    position += 8;
                    return new JsonObject { ["tag"] = "UInt64", ["value"] = U64(value) };
                }

            case 4:
                {
                    var value = BinaryPrimitives.ReadDoubleLittleEndian(data[position..]);
                    position += 8;
                    return new JsonObject { ["tag"] = "Float64", ["value"] = F64(value) };
                }

            case 5:
                {
                    var length = BinaryPrimitives.ReadInt32LittleEndian(data[position..]);
                    position += 4;
                    var text = Encoding.UTF8.GetString(data.Slice(position, length));
                    position += length;
                    return new JsonObject { ["tag"] = "String", ["value"] = text };
                }

            case 6:
                {
                    var length = BinaryPrimitives.ReadInt32LittleEndian(data[position..]);
                    position += 4;
                    var bytes = Hex(data.Slice(position, length));
                    position += length;
                    return new JsonObject { ["tag"] = "Bytes", ["value"] = bytes };
                }

            case 7:
                {
                    var identity = Hex(data.Slice(position, Identity.Size));
                    position += Identity.Size;
                    return new JsonObject { ["tag"] = "Identity", ["value"] = identity };
                }

            case 8:
                {
                    var value = BinaryPrimitives.ReadInt64LittleEndian(data[position..]);
                    position += 8;
                    return new JsonObject { ["tag"] = "Timestamp", ["value"] = I64(value) };
                }

            case 9:
                {
                    var count = BinaryPrimitives.ReadInt32LittleEndian(data[position..]);
                    position += 4;
                    var items = new JsonArray();
                    for (var i = 0; i < count; i++)
                        items.Add(ReadTagged(data, ref position));
                    return new JsonObject { ["tag"] = "Array", ["items"] = items };
                }

            default:
                throw new InvalidOperationException($"Unknown argument tag {tag}.");
        }
    }

    public static string Hex(ReadOnlySpan<byte> bytes) => Convert.ToHexStringLower(bytes);

    public static string I64(long value) => value.ToString(CultureInfo.InvariantCulture);

    public static string U64(ulong value) => value.ToString(CultureInfo.InvariantCulture);

    public static string F32(float value) => BitConverter.SingleToUInt32Bits(value).ToString("x8", CultureInfo.InvariantCulture);

    public static string F64(double value) => BitConverter.DoubleToUInt64Bits(value).ToString("x16", CultureInfo.InvariantCulture);
}
