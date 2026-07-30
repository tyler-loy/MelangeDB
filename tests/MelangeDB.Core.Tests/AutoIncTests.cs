using Xunit;

namespace MelangeDB.Core.Tests;

public class AutoIncTests : IDisposable
{
    private readonly EngineHarness _harness = new();

    public void Dispose() => _harness.Dispose();

    [Fact]
    public void Ids_are_assigned_into_the_write_set_before_append_and_returned()
    {
        _harness.Invoke("Insert", ctx =>
        {
            var first = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "a", Quantity = 1 });
            var second = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "b", Quantity = 1 });
            Assert.Equal(1UL, first.Id);
            Assert.Equal(2UL, second.Id);
        });

        // The allocated ids are in the logged write set — replay never reassigns.
        var record = _harness.Engine.Log.ReadFrom(1).Single();
        Assert.Equal(2, record.WriteSet.Count);
    }

    [Fact]
    public void Sequences_recover_on_restart_and_never_reuse()
    {
        _harness.Invoke("Seed", ctx =>
        {
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "a", Quantity = 1 });
            ctx.Db.Insert(new Registration { Email = "a@example.com", CreatedAt = ctx.Timestamp });
        });
        _harness.Restart();
        _harness.Invoke("More", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "b", Quantity = 1 });
            var registration = ctx.Db.Insert(new Registration { Email = "b@example.com", CreatedAt = ctx.Timestamp });
            Assert.Equal(2UL, item.Id);
            Assert.Equal(2L, registration.Id);
        });
    }

    [Fact]
    public void Deleted_ids_are_not_reused_after_restart()
    {
        _harness.Invoke("Seed", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "a", Quantity = 1 });
            Assert.Equal(1UL, item.Id);
        });
        _harness.Invoke("Delete", ctx => Assert.True(ctx.Db.Delete<InventoryItem>(1UL)));
        _harness.Restart();
        _harness.Invoke("More", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "b", Quantity = 1 });
            Assert.Equal(2UL, item.Id);
        });
    }

    [Fact]
    public void Explicit_nonzero_id_is_respected_and_skipped_past()
    {
        _harness.Invoke("Explicit", ctx =>
        {
            var explicitRow = ctx.Db.Insert(new InventoryItem { Id = 100, Owner = Identity.Hash("o"), ItemName = "a", Quantity = 1 });
            Assert.Equal(100UL, explicitRow.Id);
            var allocated = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "b", Quantity = 1 });
            Assert.Equal(101UL, allocated.Id);
        });
        _harness.Restart();
        _harness.Invoke("Next", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "c", Quantity = 1 });
            Assert.Equal(102UL, item.Id);
        });
    }

    [Fact]
    public void Aborted_transaction_consumes_no_value_across_restart()
    {
        Assert.Throws<InvalidOperationException>(() => _harness.Invoke("Fail", ctx =>
        {
            ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "doomed", Quantity = 1 });
            throw new InvalidOperationException("boom");
        }));
        _harness.Restart();
        _harness.Invoke("Next", ctx =>
        {
            var item = ctx.Db.Insert(new InventoryItem { Owner = Identity.Hash("o"), ItemName = "a", Quantity = 1 });
            Assert.Equal(1UL, item.Id);
        });
    }

    [Fact]
    public void Long_columns_allocate_within_63_bits()
    {
        _harness.Invoke("Insert", ctx =>
        {
            var registration = ctx.Db.Insert(new Registration { Email = "x@example.com", CreatedAt = ctx.Timestamp });
            Assert.True(registration.Id > 0);
        });
    }

    [Fact]
    public void Id_layout_is_originator_prefixed_with_sign_bit_clear()
    {
        var sequencer = new AutoIncSequencer(originator: 5);
        var stage = sequencer.BeginStage();
        var table = TableId.FromName("Whatever");

        var first = stage.Allocate(table);
        var second = stage.Allocate(table);

        Assert.Equal((5UL << 47) | 1UL, first);
        Assert.Equal((5UL << 47) | 2UL, second);
        Assert.Equal(0UL, first >> 63); // Sign bit clear: round-trips through Postgres bigint.
        Assert.True((long)first > 0);

        // Ids from another originator never advance this node's sequence.
        stage.ObserveExplicit(table, (9UL << 47) | 500UL);
        stage.Commit();
        Assert.Equal(3UL, sequencer.PeekNextSequence(table));

        // An abandoned stage consumes nothing.
        var abandoned = sequencer.BeginStage();
        abandoned.Allocate(table);
        Assert.Equal(3UL, sequencer.PeekNextSequence(table));

        // The max-originator, max-sequence id still has the top bit clear.
        Assert.Equal(0UL, AutoIncSequencer.Compose(ushort.MaxValue, (1UL << 47) - 1) >> 63);
    }
}
