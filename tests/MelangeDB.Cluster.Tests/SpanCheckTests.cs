using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Cluster.Tests;

/// <summary>
/// The shard-span debug check outside a cluster: a single-node deployment with a registered
/// strategy gets the same loud failure in development, before a second node exists, and the
/// off switch really is off.
/// </summary>
public class SpanCheckTests
{
    private static (MelangeEngine Engine, string Root) CreateEngine()
    {
        var root = Directory.CreateTempSubdirectory("melange-span-").FullName;
        var registry = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables());
        var engine = new MelangeEngine(
            new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(root, "hot") },
                CommitLog = { Path = Path.Combine(root, "log") },
            },
            registry);
        return (engine, root);
    }

    [Fact]
    public void A_single_node_deployment_with_a_strategy_trips_the_span_check()
    {
        var (engine, root) = CreateEngine();
        try
        {
            var strategy = new InstancingShardStrategy(engine.Schema, static _ => default);
            engine.AddCommitGuard(new SingleNodeSpanGuard(engine.Schema, () => engine.HotStore, strategy, static () => true));

            var failure = Assert.Throws<ShardSpanException>(() => engine.Invoke("SpanBoth", ClusterFixture.Caller, ctx =>
            {
                ctx.Db.Insert(new Mob { InstanceId = 3, Hp = 1 });
                ctx.Db.Insert(new Mob { InstanceId = 4, Hp = 1 });
            }));

            Assert.Contains("must resolve to the same shard", failure.Message);
            Assert.Equal(0UL, engine.Log.HeadLsn);

            // Same-shard transactions pass, deletes included (their shard is the pre-image's).
            engine.Invoke("SameShard", ClusterFixture.Caller, ctx =>
            {
                ctx.Db.Insert(new Mob { InstanceId = 3, Hp = 1 });
                ctx.Db.Insert(new Mob { InstanceId = 3, Hp = 2 });
            });
            Assert.Equal(1UL, engine.Log.HeadLsn);
        }
        finally
        {
            engine.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void The_span_check_honors_its_off_switch()
    {
        var (engine, root) = CreateEngine();
        try
        {
            var strategy = new InstancingShardStrategy(engine.Schema, static _ => default);
            engine.AddCommitGuard(new SingleNodeSpanGuard(engine.Schema, () => engine.HotStore, strategy, static () => false));

            engine.Invoke("SpanBoth", ClusterFixture.Caller, ctx =>
            {
                ctx.Db.Insert(new Mob { InstanceId = 3, Hp = 1 });
                ctx.Db.Insert(new Mob { InstanceId = 4, Hp = 1 });
            });
            Assert.Equal(1UL, engine.Log.HeadLsn);
        }
        finally
        {
            engine.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void The_mode_resolution_is_explicit_for_always_and_off_and_probes_the_build_for_debug_only()
    {
        Assert.True(ShardSpanCheck.IsEnabled(ShardSpanCheckMode.Always));
        Assert.False(ShardSpanCheck.IsEnabled(ShardSpanCheckMode.Off));
        Assert.Equal(ShardSpanCheck.EntryAssemblyIsDebugBuild(), ShardSpanCheck.IsEnabled(ShardSpanCheckMode.DebugOnly));
    }

    [Fact]
    public void The_instancing_strategy_requires_a_shard_by_column_on_every_partitioned_table()
    {
        // A hand-built schema, so the generator never sees this deliberately broken table.
        var registry = new SchemaRegistry(
        [
            new TableSchema(
                typeof(NoShardBy),
                "NoShardBy",
                [
                    new ColumnSchema
                    {
                        Name = "Id",
                        ClrType = typeof(long),
                        Kind = ColumnKind.Int64,
                        IsPrimaryKey = true,
                        GetValue = static row => ((NoShardBy)row).Id,
                        SetValue = static (_, _) => { },
                    },
                ],
                placement: Placement.Partitioned),
        ]);
        var failure = Assert.Throws<InvalidOperationException>(
            () => new InstancingShardStrategy(registry, static _ => default));
        Assert.Contains("ShardBy", failure.Message);
    }

    private struct NoShardBy
    {
        public long Id { get; set; }
    }
}

/// <summary>Placement guard behavior on the engines a cluster composes.</summary>
public class PlacementGuardTests
{
    [Fact]
    public void A_shard_engine_refuses_global_reads_and_replicated_writes_with_the_fix_in_the_message()
    {
        var root = Directory.CreateTempSubdirectory("melange-placement-").FullName;
        var registry = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables());
        var engine = new MelangeEngine(
            new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(root, "hot") },
                CommitLog = { Path = Path.Combine(root, "log") },
            },
            registry);
        try
        {
            engine.SetTableAccessGuard(PlacementGuards.ShardAccess(new ShardKey(9), "node-x"));

            var globalRead = Assert.Throws<InvalidOperationException>(() =>
                engine.Invoke("readGlobal", ClusterFixture.Caller, ctx => ctx.Db.Find<GlobalCounter>(1L)));
            Assert.Contains("lives on the hub", globalRead.Message);
            Assert.Contains("ReducerSite.Hub", globalRead.Message);

            var replicatedWrite = Assert.Throws<InvalidOperationException>(() =>
                engine.Invoke("writeReplicated", ClusterFixture.Caller, ctx => ctx.Db.Insert(new ItemDef { Id = 1, Name = "x" })));
            Assert.Contains("only the hub", replicatedWrite.Message);

            // Replicated reads and partitioned writes are the shard's bread and butter.
            engine.Invoke("fine", ClusterFixture.Caller, ctx =>
            {
                _ = ctx.Db.Find<ItemDef>(1L);
                ctx.Db.Insert(new Mob { InstanceId = 9, Hp = 1 });
            });
            Assert.Equal(1UL, engine.Log.HeadLsn);
        }
        finally
        {
            engine.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void The_hub_engine_refuses_partitioned_touches_with_the_placement_rule_in_the_message()
    {
        var root = Directory.CreateTempSubdirectory("melange-placement-hub-").FullName;
        var registry = new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables());
        var engine = new MelangeEngine(
            new MelangeDbOptions
            {
                HotStore = { Path = Path.Combine(root, "hot") },
                CommitLog = { Path = Path.Combine(root, "log") },
            },
            registry);
        try
        {
            engine.SetTableAccessGuard(PlacementGuards.HubAccess());

            var failure = Assert.Throws<InvalidOperationException>(() =>
                engine.Invoke("writePartitioned", ClusterFixture.Caller, ctx => ctx.Db.Insert(new Mob { InstanceId = 1, Hp = 1 })));
            Assert.Contains("lives on shard nodes", failure.Message);

            engine.Invoke("fine", ClusterFixture.Caller, ctx => ctx.Db.Insert(new GlobalCounter { Id = 1, Value = 1 }));
            Assert.Equal(1UL, engine.Log.HeadLsn);
        }
        finally
        {
            engine.Dispose();
            Directory.Delete(root, recursive: true);
        }
    }
}
