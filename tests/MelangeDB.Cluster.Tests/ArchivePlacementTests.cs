using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// Archive tables in a cluster (road-to-0.3 phase 25): shard nodes run no Postgres applier, so a
/// shard-side archive write would sit in the shard's log until truncation dropped it. Shard and
/// node-local engines refuse the write; the hub, which runs the applier, accepts it.
/// </summary>
public class ArchivePlacementTests
{
    public struct Archived
    {
        public long Id;
        public long Delta;
    }

    private static TableSchema ArchiveTable(Placement placement)
    {
        var columns = typeof(Archived).GetFields()
            .OrderBy(f => f.MetadataToken)
            .Select(f => new ColumnSchema
            {
                Name = f.Name,
                ClrType = f.FieldType,
                Kind = ColumnKind.Int64,
                IsPrimaryKey = f.Name == "Id",
                GetValue = row => f.GetValue(row),
                SetValue = (row, value) => f.SetValue(row, value),
            })
            .ToList();
        return new TableSchema(typeof(Archived), "Archived", columns, tier: StorageTier.Archive, placement: placement);
    }

    [Theory]
    [InlineData(Placement.Partitioned)]
    [InlineData(Placement.Local)]
    public void A_shard_engine_refuses_archive_writes(Placement placement)
    {
        var guard = PlacementGuards.ShardAccess(new ShardKey(9), "node-x");
        var refused = Assert.Throws<InvalidOperationException>(() => guard(ArchiveTable(placement), TableAccess.Write));
        Assert.Contains("runs no Postgres applier", refused.Message, StringComparison.Ordinal);
        Assert.Contains("Placement.Global", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_shard_nodes_local_engine_refuses_archive_writes()
    {
        var guard = PlacementGuards.NodeLocalAccess("node-x");
        Assert.Throws<InvalidOperationException>(() => guard(ArchiveTable(Placement.Local), TableAccess.Write));
    }

    [Fact]
    public void The_hub_accepts_a_global_archive_write()
    {
        var guard = PlacementGuards.HubAccess();
        guard(ArchiveTable(Placement.Global), TableAccess.Write);
    }
}
