namespace MelangeDB.Core;

/// <summary>CRC-32 (IEEE 802.3, polynomial 0xEDB88320), table-driven. Guards every log record.</summary>
internal static class Crc32
{
    private static readonly uint[] Table = BuildTable();

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        var crc = 0xFFFFFFFFu;
        foreach (var b in data)
            crc = (crc >> 8) ^ Table[(crc ^ b) & 0xFF];
        return crc ^ 0xFFFFFFFFu;
    }

    /// <summary>Incremental form for streamed payloads too large to buffer: seed with <see cref="Begin"/>, fold chunks, finish with <see cref="End"/>.</summary>
    public static uint Begin() => 0xFFFFFFFFu;

    public static uint Append(uint state, ReadOnlySpan<byte> data)
    {
        foreach (var b in data)
            state = (state >> 8) ^ Table[(state ^ b) & 0xFF];
        return state;
    }

    public static uint End(uint state) => state ^ 0xFFFFFFFFu;

    private static uint[] BuildTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            var entry = i;
            for (var bit = 0; bit < 8; bit++)
                entry = (entry & 1) != 0 ? (entry >> 1) ^ 0xEDB88320u : entry >> 1;
            table[i] = entry;
        }

        return table;
    }
}
