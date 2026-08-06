using Xunit;

namespace MelangeDB.Core.Tests;

/// <summary>
/// Index maintenance reads a row once per put, not once per index. A three-index table used to
/// deserialize the whole row three times — re-allocating its string and byte columns each time —
/// because the store asked the codec for one column at a time.
/// </summary>
public class IndexEncodeTests
{
    [Fact]
    public void Encoding_every_indexed_column_deserializes_the_row_once()
    {
        var codec = new CountingCodec();
        var destination = new RowKey[3];

        codec.EncodeColumnsFromBytes([1, 2, 3], ["RoomId", "Level", "Name"], destination);

        Assert.Equal(1, codec.Deserializes);
        Assert.Equal(3, codec.ColumnEncodes);
    }

    [Fact]
    public void A_null_column_lands_as_a_zero_length_key_rather_than_being_skipped()
    {
        // Positional alignment with the schema's index list is the contract; a null has to hold its
        // slot, and zero length is how every caller spells "not indexed".
        var codec = new CountingCodec();
        var destination = new RowKey[3];

        codec.EncodeColumnsFromBytes([1, 2, 3], ["RoomId", "Missing", "Name"], destination);

        Assert.NotEqual(0, destination[0].Length);
        Assert.Equal(0, destination[1].Length);
        Assert.NotEqual(0, destination[2].Length);
    }

    [Fact]
    public void The_one_pass_encode_agrees_with_the_column_at_a_time_one()
    {
        var codec = new CountingCodec();
        var columns = new[] { "RoomId", "Level", "Name" };
        var destination = new RowKey[columns.Length];

        codec.EncodeColumnsFromBytes([4, 5, 6], columns, destination);

        for (var i = 0; i < columns.Length; i++)
            Assert.Equal(codec.EncodeColumnFromBytes(columns[i], [4, 5, 6]) ?? default, destination[i]);
    }

    /// <summary>
    /// A hand-written codec over a throwaway row, counting the calls the generated ones make. The
    /// point under test is <see cref="RowCodec{TRow}"/>'s own bridging, which every generated codec
    /// inherits sealed, so the row shape here is irrelevant beyond having distinguishable columns.
    /// </summary>
    private sealed class CountingCodec : RowCodec<CountingCodec.Row>
    {
        public int Deserializes { get; private set; }

        public int ColumnEncodes { get; private set; }

        public override byte[] Serialize(in Row row) => [row.First, row.Second, row.Third];

        public override Row Deserialize(ReadOnlySpan<byte> data)
        {
            Deserializes++;
            return new Row { First = data[0], Second = data[1], Third = data[2] };
        }

        public override RowKey EncodePrimaryKey(in Row row) => new([row.First]);

        public override RowKey? EncodeColumn(string column, in Row row)
        {
            ColumnEncodes++;
            return column switch
            {
                "RoomId" => new RowKey([row.First]),
                "Level" => new RowKey([row.Second]),
                "Name" => new RowKey([row.Third]),
                _ => null,
            };
        }

        public override void AssignAutoInc(ref Row row, AutoIncStage stage, TableId table)
        {
        }

        internal struct Row
        {
            public byte First;
            public byte Second;
            public byte Third;
        }
    }
}
