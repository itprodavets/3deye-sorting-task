using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

/// <summary>
/// Guarantee that all three <see cref="IFileSorter"/> strategies produce byte-identical
/// output on the same input. This is the cross-strategy determinism contract: swapping
/// <c>--strategy</c> must not change the bytes on disk.
///
/// Before this was enforced, <see cref="ExternalSorter"/> sorted via <see cref="LineEntry"/>
/// with <see cref="StringComparison.Ordinal"/> (UTF-16 <i>code-unit</i> order), while
/// <see cref="MmfSorter"/> and <see cref="ShardSorter"/> sorted by raw UTF-8 bytes
/// (Unicode <i>code-point</i> order). The two orderings AGREE for code points below
/// <c>U+D800</c>, but DISAGREE for BMP code points in <c>[U+E000, U+FFFF]</c> compared
/// against supplementary-plane code points (<c>U+10000+</c>):
/// <code>
///   UTF-16 Ordinal : 𐀀 (surrogate 0xD800)  &lt;  ﾀ (0xFF80)        → stream puts 𐀀 first
///   UTF-8  / code-pt: 𐀀 (0xF0 90 80 80)    &gt;  ﾀ (0xEF BE 80)    → mmf / shard put ﾀ first
/// </code>
/// The divergence is reproducible with two records — not just on 100 GB inputs — so the
/// regression catches any future reintroduction immediately.
/// </summary>
public class CrossStrategyParityTests : IDisposable
{
    private readonly string _tempDir;

    public CrossStrategyParityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"parity_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Minimum reproduction of the UTF-16 Ordinal vs UTF-8 byte-order divergence.
    /// Two lines: one with a supplementary-plane char (surrogate pair in UTF-16) and
    /// one with a BMP char above the surrogate range. All three strategies must agree.
    /// </summary>
    [Fact]
    public async Task AllStrategies_ProduceIdenticalOutput_OnSupplementaryPlaneChars()
    {
        var input = Path.Combine(_tempDir, "input.txt");
        // U+10000 "𐀀" → UTF-8 {0xF0, 0x90, 0x80, 0x80}, UTF-16 surrogate pair {0xD800, 0xDC00}
        // U+FF80 "ﾀ" → UTF-8 {0xEF, 0xBE, 0x80}        , UTF-16 single code unit 0xFF80
        // UTF-16 Ordinal: 0xD800 < 0xFF80 → "𐀀" < "ﾀ"
        // UTF-8  bytes:   0xEF   < 0xF0   → "ﾀ"  < "𐀀"
        await File.WriteAllLinesAsync(input, new[]
        {
            "1. 𐀀",
            "2. ﾀ"
        });

        var streamOut = await SortWith(new ExternalSorter(NewOptions()), input, "stream.txt");
        var mmfOut    = await SortWith(new MmfSorter(NewOptions()),     input, "mmf.txt");
        var shardOut  = await SortWith(new ShardSorter(NewOptions()),   input, "shard.txt");

        mmfOut.Should().Equal(streamOut,
            "mmf and stream must agree on sort order even for supplementary-plane chars");
        shardOut.Should().Equal(streamOut,
            "shard and stream must agree on sort order even for supplementary-plane chars");
    }

    /// <summary>
    /// Same parity guarantee, but with enough records and a tight chunk budget to force
    /// multi-chunk merge. Exercises <see cref="ChunkMerger.MergeAll"/>'s intermediate
    /// binary merges (<see cref="LineEntry"/>-based) AND its final text merge
    /// (<see cref="RawLineEntry"/>-based) — if those two comparators disagree, multi-level
    /// merges drift silently from single-level ones.
    /// </summary>
    [Fact]
    public async Task AllStrategies_ProduceIdenticalOutput_AcrossManyChunks()
    {
        var input = Path.Combine(_tempDir, "multi.txt");
        var lines = new List<string>();
        // Mix ASCII, Cyrillic, CJK, and supplementary plane so every byte-range path runs.
        for (var i = 1; i <= 2_000; i++)
        {
            var text = (i % 7) switch
            {
                0 => "Apple",
                1 => "Банан",        // Cyrillic — 2-byte UTF-8
                2 => "日本語",         // CJK — 3-byte UTF-8
                3 => "𐀀𐀁𐀂",        // supplementary plane — 4-byte UTF-8
                4 => $"ﾀ{i}",        // BMP above surrogate range (U+FF80)
                5 => "Café",
                _ => $"Z{i}"
            };
            lines.Add($"{i}. {text}");
        }
        await File.WriteAllLinesAsync(input, lines);

        // 32 KB chunks → ~4-8 chunks on this input → forces multi-chunk merge path.
        var opts = NewOptions() with { MaxMemoryPerChunk = 32 * 1024 };

        var streamOut = await SortWith(new ExternalSorter(opts), input, "stream.txt");
        var mmfOut    = await SortWith(new MmfSorter(opts),     input, "mmf.txt");
        var shardOut  = await SortWith(new ShardSorter(opts),   input, "shard.txt");

        mmfOut.Should().Equal(streamOut,
            "mmf and stream must agree on sort order after multi-chunk merge");
        shardOut.Should().Equal(streamOut,
            "shard and stream must agree on sort order after multi-chunk merge");
    }

    private SortOptions NewOptions() => new()
    {
        TempDirectory = _tempDir,
        MaxDegreeOfParallelism = 4
    };

    private async Task<byte[]> SortWith(IFileSorter sorter, string input, string outName)
    {
        var output = Path.Combine(_tempDir, outName);
        await sorter.SortAsync(input, output);
        return await File.ReadAllBytesAsync(output);
    }
}
