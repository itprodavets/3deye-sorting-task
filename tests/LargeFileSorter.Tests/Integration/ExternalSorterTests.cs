using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

public class ExternalSorterTests : IDisposable
{
    private readonly string _tempDir;

    public ExternalSorterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sort_test_{Guid.NewGuid():N}");
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

        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
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

        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
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

        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(input, output);

        var result = await File.ReadAllLinesAsync(output);
        result.Should().Equal("42. Only line");
    }

    [Fact]
    public async Task SortAsync_ForcesMultipleChunks_StillSortsCorrectly()
    {
        var input = Path.Combine(_tempDir, "multi_chunk.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        // Generate enough data that a tiny chunk size forces multiple chunks
        var generator = new FileGenerator(new GeneratorOptions { Seed = 42, UniquePhraseCount = 50 });
        await generator.GenerateAsync(input, 100 * 1024); // 100 KB

        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 8 * 1024, // 8 KB per chunk — forces many chunks
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(input, output);

        // Verify output is sorted
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

        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024, // tiny chunks
            MergeWidth = 4,               // narrow merge → forces multi-level
            TempDirectory = _tempDir
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
        cts.Cancel(); // cancel immediately

        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
        var act = () => sorter.SortAsync(input, output, ct: cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SortAsync_MissingInputFile_ThrowsFileNotFoundException()
    {
        var sorter = new ExternalSorter();
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

        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 10 * 1024,
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(input, output);

        var outputLines = await File.ReadAllLinesAsync(output);
        outputLines.Length.Should().Be(inputLines.Length);
    }

    /// <summary>
    /// Regression: when the Phase 1 producer (<c>ReadInputAsync</c>) throws on a malformed
    /// line, it must propagate the exception to the consumer workers so they stop waiting
    /// on <see cref="System.Threading.Channels.ChannelReader{T}.ReadAllAsync"/>.
    ///
    /// <b>The bug</b>: previously the reader task was <c>await ReadInputAsync(...);
    /// channel.Writer.Complete();</c> with no try/catch. If the producer threw, the
    /// <c>Complete()</c> call was skipped and the worker <c>await foreach</c> loops sat
    /// forever on a never-completed channel. The fix wraps the producer in try/catch and
    /// calls <see cref="System.Threading.Channels.ChannelWriter{T}.Complete(Exception?)"/>
    /// — the consumers observe the exception via <c>ReadAllAsync</c> and unwind cleanly.
    ///
    /// <b>Why the hard 30 s timeout matters</b>: pre-fix the top-level <c>SortAsync</c>
    /// still appeared to return promptly because its <c>await Task.WhenAll</c> (now over
    /// reader + workers) would never complete — the test would hang indefinitely under
    /// xUnit, not just leak a task. The bounded CTS guarantees the test fails loudly
    /// rather than wedging CI.
    /// </summary>
    [Fact]
    public async Task SortAsync_ProducerFailsOnMalformedLine_DoesNotHangWorkers()
    {
        var input = Path.Combine(_tempDir, "bad.txt");
        var output = Path.Combine(_tempDir, "out.txt");

        // Many valid lines → the reader primes the channel and workers pick up at least
        // one chunk before the malformed line fires. Small chunk budget forces multiple
        // chunks to flow so sortTasks are actively consuming when the producer faults.
        var lines = Enumerable.Range(1, 5000)
            .Select(i => $"{i}. Line{i}")
            .Append("not-a-valid-line")   // no '.' separator → FormatException
            .ToList();
        await File.WriteAllLinesAsync(input, lines);

        var sorter = new ExternalSorter(new SortOptions
        {
            TempDirectory = _tempDir,
            MaxMemoryPerChunk = 4096,
            SortWorkers = 4
        });

        using var hardTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        Func<Task> act = () => sorter.SortAsync(input, output, ct: hardTimeout.Token);

        await act.Should().ThrowAsync<FormatException>();
        hardTimeout.IsCancellationRequested.Should().BeFalse(
            "SortAsync must surface the producer's FormatException without hanging on " +
            "worker tasks that are still awaiting channel closure");
    }
}
