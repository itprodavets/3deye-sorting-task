using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

/// <summary>
/// Integration tests for <see cref="ShardSorter"/>. Mirror the
/// <see cref="ExternalSorterTests"/> coverage so any drift between the two
/// merge strategies is caught immediately, and add a parity test that asserts
/// shard output is byte-identical to the stream-merge baseline — the
/// shard partition is deterministic, so identical bytes are the right bar.
/// </summary>
public class ShardSorterTests : IDisposable
{
    private readonly string _tempDir;

    public ShardSorterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"shard_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task SortAsync_ExampleFromSpec_ProducesCorrectOutput()
    {
        var input = Path.Combine(_tempDir, "input.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        await File.WriteAllLinesAsync(input, new[]
        {
            "415. Apple",
            "30432. Something something something",
            "1. Apple",
            "32. Cherry is the best",
            "2. Banana is yellow"
        });

        // Tiny chunk budget forces multi-chunk → exercises the actual shard merge path.
        // Without this the ShardSorter would hit its single-chunk fast path and skip the
        // partition logic we actually want under test.
        var sorter = new ShardSorter(new SortOptions
        {
            MaxMemoryPerChunk = 128, // bytes — every line is its own chunk
            TempDirectory = _tempDir,
            MaxDegreeOfParallelism = 4
        });
        await sorter.SortAsync(input, output);

        var result = await File.ReadAllLinesAsync(output);
        result.Should().Equal(
            "1. Apple",
            "415. Apple",
            "2. Banana is yellow",
            "32. Cherry is the best",
            "30432. Something something something"
        );
    }

    [Fact]
    public async Task SortAsync_EmptyFile_CreatesEmptyOutput()
    {
        var input = Path.Combine(_tempDir, "empty.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        await File.WriteAllTextAsync(input, "");

        var sorter = new ShardSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(input, output);

        var result = await File.ReadAllTextAsync(output);
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task SortAsync_SingleLine_CopiedAsIs()
    {
        var input = Path.Combine(_tempDir, "single.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        await File.WriteAllLinesAsync(input, new[] { "42. Only line" });

        // Single chunk → ShardSorter takes its early-exit path (no shard split).
        var sorter = new ShardSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(input, output);

        var result = await File.ReadAllLinesAsync(output);
        result.Should().Equal("42. Only line");
    }

    [Fact]
    public async Task SortAsync_ForcesMultipleChunks_StillSortsCorrectly()
    {
        var input = Path.Combine(_tempDir, "multi_chunk.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        var generator = new FileGenerator(new GeneratorOptions { Seed = 42, UniquePhraseCount = 50 });
        await generator.GenerateAsync(input, 100 * 1024); // 100 KB

        var sorter = new ShardSorter(new SortOptions
        {
            MaxMemoryPerChunk = 8 * 1024,
            TempDirectory = _tempDir,
            MaxDegreeOfParallelism = 4
        });
        await sorter.SortAsync(input, output);

        var lines = await File.ReadAllLinesAsync(output);
        lines.Should().NotBeEmpty();

        var entries = lines.Select(LineParser.Parse).ToArray();
        for (var i = 1; i < entries.Length; i++)
        {
            entries[i].CompareTo(entries[i - 1]).Should().BeGreaterThanOrEqualTo(0,
                $"line {i} should be >= line {i - 1}");
        }
    }

    [Fact]
    public async Task SortAsync_ForcesMultiLevelMerge_StillSortsCorrectly()
    {
        var input = Path.Combine(_tempDir, "multi_merge.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        var generator = new FileGenerator(new GeneratorOptions { Seed = 77, UniquePhraseCount = 30 });
        await generator.GenerateAsync(input, 200 * 1024); // 200 KB

        var sorter = new ShardSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024,
            MergeWidth = 4,
            TempDirectory = _tempDir,
            MaxDegreeOfParallelism = 4
        });
        await sorter.SortAsync(input, output);

        var lines = await File.ReadAllLinesAsync(output);
        var entries = lines.Select(LineParser.Parse).ToArray();

        for (var i = 1; i < entries.Length; i++)
        {
            entries[i].CompareTo(entries[i - 1]).Should().BeGreaterThanOrEqualTo(0,
                $"line {i} should be >= line {i - 1}");
        }
    }

    [Fact]
    public async Task SortAsync_Cancellation_ThrowsOperationCanceledException()
    {
        var input = Path.Combine(_tempDir, "cancel.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        var generator = new FileGenerator(new GeneratorOptions { Seed = 1 });
        await generator.GenerateAsync(input, 50 * 1024);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var sorter = new ShardSorter(new SortOptions { TempDirectory = _tempDir });
        var act = () => sorter.SortAsync(input, output, ct: cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SortAsync_MissingInputFile_ThrowsFileNotFoundException()
    {
        var sorter = new ShardSorter();
        var act = () => sorter.SortAsync("/nonexistent/file.txt", "/dev/null");
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task SortAsync_PreservesLineCount()
    {
        var input = Path.Combine(_tempDir, "count.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        var generator = new FileGenerator(new GeneratorOptions { Seed = 55 });
        await generator.GenerateAsync(input, 50 * 1024);

        var inputLines = await File.ReadAllLinesAsync(input);

        var sorter = new ShardSorter(new SortOptions
        {
            MaxMemoryPerChunk = 10 * 1024,
            TempDirectory = _tempDir,
            MaxDegreeOfParallelism = 4
        });
        await sorter.SortAsync(input, output);

        var outputLines = await File.ReadAllLinesAsync(output);
        outputLines.Length.Should().Be(inputLines.Length);
    }

    /// <summary>
    /// The cross-strategy oracle: stream merge and shard merge must produce identical
    /// output bytes given the same input + same SortOptions. Both strategies use the
    /// same Phase 1 (deterministic chunk sort), and shard's partition + concat is
    /// purely a reordering of the same merge that stream does in one pass — so equality
    /// at the byte level catches any subtle drift in record ordering, line endings, or
    /// trailing newline handling.
    /// </summary>
    [Fact]
    public async Task SortAsync_OutputMatchesExternalSorterByteForByte()
    {
        var input = Path.Combine(_tempDir, "parity.txt");
        var streamOut = Path.Combine(_tempDir, "stream.txt");
        var shardOut = Path.Combine(_tempDir, "shard.txt");

        // 500 KB with broad phrase variety so the partition actually distributes work
        // across multiple shards (a 5-phrase file would land 90% in one shard).
        var generator = new FileGenerator(new GeneratorOptions { Seed = 12345, UniquePhraseCount = 500 });
        await generator.GenerateAsync(input, 500 * 1024);

        var sharedOptions = new SortOptions
        {
            MaxMemoryPerChunk = 32 * 1024, // 32 KB → ~16 chunks at 500 KB
            TempDirectory = _tempDir,
            MaxDegreeOfParallelism = 8
        };

        await new ExternalSorter(sharedOptions).SortAsync(input, streamOut);
        await new ShardSorter(sharedOptions).SortAsync(input, shardOut);

        var streamBytes = await File.ReadAllBytesAsync(streamOut);
        var shardBytes = await File.ReadAllBytesAsync(shardOut);
        shardBytes.Should().Equal(streamBytes,
            "shard merge must preserve byte-identical sort order vs stream merge");
    }
}
