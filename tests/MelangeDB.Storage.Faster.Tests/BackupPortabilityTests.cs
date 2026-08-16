using MelangeDB.Core;
using Xunit;

namespace MelangeDB.Storage.Faster.Tests;

/// <summary>
/// The archive's store-agnostic property, as a test rather than a claim (road-to-0.2 phase 15):
/// a <c>.mbak</c> carries the truth — snapshot rows and log records — never store files, so a
/// backup taken from a FASTER deployment restores under the in-memory engine and vice versa. The
/// archive predates the projection choice by construction.
/// </summary>
public class BackupPortabilityTests
{
    [Fact]
    public void A_faster_written_archive_restores_under_the_in_memory_engine() =>
        RoundTripAcrossEngines(source: StoreKind.Faster, destination: StoreKind.InMemory);

    [Fact]
    public void An_in_memory_written_archive_restores_under_the_faster_engine() =>
        RoundTripAcrossEngines(source: StoreKind.InMemory, destination: StoreKind.Faster);

    private static void RoundTripAcrossEngines(StoreKind source, StoreKind destination)
    {
        using var harness = new StoreHarness(source);

        // Resident and paged tiers both, with out-of-line blob payloads, an update, and a delete;
        // a snapshot that truncates; then a live tail — the parts of a world an archive must carry.
        for (var i = 0; i < 40; i++)
        {
            var id = i;
            harness.Invoke("seed", ctx =>
            {
                ctx.Db.Insert(new Creature { ChunkId = id % 5, Name = $"c{id}", X = id });
                ctx.Db.Insert(new TerrainBlob { ChunkId = id, Region = id % 4, Data = StoreContractTests.MakeBlob(id, 700) });
            });
        }

        harness.Options.Resume.RetentionWindowSeconds = 0;
        Assert.NotNull(harness.Engine.TakeSnapshot());
        Assert.True(harness.Engine.Log.BaseLsn > 0);

        harness.Invoke("tail", ctx =>
        {
            var moved = ctx.Db.Find<TerrainBlob>(3L)!.Value;
            ctx.Db.Update(moved with { Region = 99, Data = StoreContractTests.MakeBlob(9000, 900) });
            ctx.Db.Delete<TerrainBlob>(11L);
            ctx.Db.Insert(new Creature { ChunkId = 1, Name = "late", X = 40 });
        });

        // Order-insensitive on purpose: byte-identical state, not scan order, is the property.
        var dumpBefore = harness.Dump().Order().ToList();
        harness.Engine.Dispose();

        var scratch = Directory.CreateTempSubdirectory("melange-portability-").FullName;
        try
        {
            var archive = Path.Combine(scratch, "world.mbak");
            MelangeBackup.Create(harness.Options.CommitLog.Path, archive);
            MelangeBackup.Verify(archive);

            var restoredDir = Path.Combine(scratch, "restored");
            MelangeBackup.Restore(archive, restoredDir);

            var options = new MelangeDbOptions
            {
                HotStore =
                {
                    Path = Path.Combine(scratch, "hot"),
                    Engine = destination == StoreKind.Faster ? HotStoreEngine.Faster : HotStoreEngine.InMemory,
                    MemoryBudgetBytes = 8 * 1024 * 1024,
                },
                CommitLog = { Path = restoredDir },
            };
            using var rebooted = new MelangeEngine(
                options,
                new SchemaRegistry(new MelangeDB.Generated.MelangeModel().Tables()
                    .Where(t => t.RowType == typeof(Creature) || t.RowType == typeof(TerrainBlob)
                        || t.RowType == typeof(ItemDefinition) || t.RowType == typeof(AutoSized) || t.RowType == typeof(NamedThing))),
                loggerFactory: null,
                timeProvider: null,
                destination == StoreKind.Faster ? new FasterHotStoreProvider() : null);

            Assert.Equal(dumpBefore, DumpSorted(rebooted));
        }
        finally
        {
            try
            {
                Directory.Delete(scratch, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    private static List<string> DumpSorted(MelangeEngine engine)
    {
        var dump = new List<string>();
        foreach (var table in engine.Schema.Tables)
        {
            foreach (var pair in engine.HotStore.Scan(table.Id))
                dump.Add($"{table.Name}|{pair.Key}|{Convert.ToHexStringLower(pair.Value.Span)}");
        }

        return [.. dump.Order()];
    }
}
