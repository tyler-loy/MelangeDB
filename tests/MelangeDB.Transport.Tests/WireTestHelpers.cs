using MelangeDB.Protocol;
using Xunit;

namespace MelangeDB.Transport.Tests;

/// <summary>
/// One subscription's initial set as a raw socket sees it. Protocol v2 sends rows as bytes and
/// their shape once, so a test that wants column names has to hold both — which is the point of
/// carrying them together here rather than passing a descriptor into every assertion.
/// </summary>
internal sealed class WireInitialSet(WireDescriptor descriptor, IReadOnlyList<WireRow> rows)
{
    public WireDescriptor Descriptor => descriptor;

    public IReadOnlyList<WireRow> Rows => rows;

    /// <summary>The descriptor's column names, in row-byte order — the subscription's static shape.</summary>
    public IReadOnlyList<string> ColumnNames => [.. descriptor.Columns.Select(c => c.Name)];

    public Dictionary<string, object?> Columns(WireRow row) =>
        WireRowValues.ToColumns(descriptor, row.Row.Span, row.ColumnMask.Span);

    /// <summary>
    /// A delta op's columns, read against this subscription's descriptor — which is exactly what a
    /// client does: the descriptor arrives once with the initial set and shapes every op after it.
    /// </summary>
    public Dictionary<string, object?> Columns(WireRowOp op) =>
        WireRowValues.ToColumns(descriptor, op.Row.Span, op.ColumnMask.Span);
}

internal static class WireTestHelpers
{
    /// <summary>Subscribes and drains every chunk, keeping the descriptor chunk 0 carried.</summary>
    public static async Task<WireInitialSet> InitialSetAsync(this RawSocketClient raw, uint id, string query)
    {
        await raw.SendAsync(new SubscribeFrame(id, query, null), TestContext.Current.CancellationToken);
        var rows = new List<WireRow>();
        WireDescriptor? descriptor = null;
        while (true)
        {
            var chunk = await raw.ReceiveUntilAsync<SubscriptionAppliedFrame>(
                TestContext.Current.CancellationToken, f => f.SubscriptionId == id);
            descriptor ??= chunk.Descriptor;
            rows.AddRange(chunk.Rows);
            if (!chunk.IsLast)
                continue;

            Assert.NotNull(descriptor);
            return new WireInitialSet(descriptor, rows);
        }
    }
}
