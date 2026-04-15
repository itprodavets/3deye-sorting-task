using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

public class MmfSorterTests : IDisposable
{
    private readonly string _tempDir;

    public MmfSorterTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"mmf_sort_test_{Guid.NewGuid():N}");
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

        var sorter = new MmfSorter(new SortOptions { TempDirectory = _tempDir });
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

        var sorter = new MmfSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(input, output);

        var result = await File.ReadAllTextAsync(output);
        result.Should().BeEmpty();
    }

    [Fact]
    public async Task SortAsync_SingleLine_CopiedAsIs()
    {
        var input = Path.Combine(_tempDir, "single.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        await File.WriteAllTextAsync(input, "42. Hello World\n");

        var sorter = new MmfSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(input, output);

        var result = (await File.ReadAllLinesAsync(output))
            .Where(l => l.Length > 0).ToArray();
        result.Should().Equal("42. Hello World");
    }

    [Fact]
    public async Task SortAsync_MissingInputFile_ThrowsFileNotFoundException()
    {
        var sorter = new MmfSorter(new SortOptions { TempDirectory = _tempDir });
        var act = () => sorter.SortAsync("nonexistent.txt", "output.txt");
        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task SortAsync_PreservesLineCount()
    {
        var input = Path.Combine(_tempDir, "input.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        var lines = Enumerable.Range(1, 1000)
            .Select(i => $"{i % 100}. Text{i % 20}")
            .ToArray();

        await File.WriteAllLinesAsync(input, lines);

        var sorter = new MmfSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(input, output);

        var result = await File.ReadAllLinesAsync(output);
        result.Where(l => l.Length > 0).Should().HaveCount(1000);
    }

    [Fact]
    public async Task SortAsync_ForcesMultipleChunks_StillSortsCorrectly()
    {
        var input = Path.Combine(_tempDir, "input.txt");
        var output = Path.Combine(_tempDir, "output.txt");

        var lines = Enumerable.Range(1, 5000)
            .Select(i => $"{i}. Line{i % 10}")
            .ToArray();

        await File.WriteAllLinesAsync(input, lines);

        // Tiny chunk budget forces multiple chunks
        var options = new SortOptions
        {
            MaxMemoryPerChunk = 1024, // 1 KB → many small chunks
            TempDirectory = _tempDir,
            BufferSize = 4096
        };

        var sorter = new MmfSorter(options);
        await sorter.SortAsync(input, output);

        var result = (await File.ReadAllLinesAsync(output))
            .Where(l => l.Length > 0).ToArray();
        result.Should().HaveCount(5000);

        // Verify sort order: text in UTF-8 lexicographic byte order (the actual sort
        // contract — see RawLineEntry / EntryIndex), then number ascending.
        // UTF-8 byte order is equivalent to Unicode code-point order and is NOT the
        // same as StringComparison.Ordinal (which is UTF-16 code-unit order and
        // diverges on supplementary-plane code points — see CrossStrategyParityTests).
        // For this test's ASCII-only generator (`$"{i}. Line{i % 10}"`) the two would
        // coincide, but the assertion uses UTF-8 bytes directly so the contract is
        // self-evident from the test and survives future data changes.
        for (var i = 1; i < result.Length; i++)
        {
            var prev = LineParser.Parse(result[i - 1]);
            var curr = LineParser.Parse(result[i]);
            var prevUtf8 = System.Text.Encoding.UTF8.GetBytes(prev.Text);
            var currUtf8 = System.Text.Encoding.UTF8.GetBytes(curr.Text);
            var cmp = prevUtf8.AsSpan().SequenceCompareTo(currUtf8);
            cmp.Should().BeLessThanOrEqualTo(0,
                $"line {i - 1} '{result[i - 1]}' should come before '{result[i]}' in UTF-8 byte order");

            if (cmp == 0)
                prev.Number.Should().BeLessThanOrEqualTo(curr.Number);
        }
    }

    /// <summary>
    /// Regression: passing a <c>--memory</c> budget smaller than <c>sizeof(EntryIndex)</c>
    /// (32 bytes) used to crash the MMF strategy with <c>ArgumentOutOfRangeException</c>
    /// from <c>NativeBuffer&lt;EntryIndex&gt;</c>'s <c>initialCapacity &gt;= 1</c> check.
    ///
    /// The bug was in <see cref="MmfSorter"/>'s budget calculation: <c>MaxMemoryPerChunk /
    /// sizeof(EntryIndex)</c> rounded to <c>0</c> for budgets under 32 bytes, and the 0 then
    /// flowed into the NativeBuffer constructor. A floor of 1 entry per chunk makes the
    /// strategy degrade gracefully (many tiny chunks, merged in multiple passes) instead of
    /// crashing.
    ///
    /// We exercise this with a budget of 16 bytes — half of one EntryIndex — which would
    /// have raised before the fix. The test asserts both no-crash and correct sort order
    /// so we don't regress to silent data loss either.
    /// </summary>
    [Fact]
    public async Task SortAsync_BudgetSmallerThanOneEntry_DoesNotCrash()
    {
        var input = Path.Combine(_tempDir, "tiny_budget.txt");
        var output = Path.Combine(_tempDir, "tiny_budget_out.txt");

        var lines = Enumerable.Range(1, 100)
            .Select(i => $"{i}. Line{i % 5}")
            .ToArray();
        await File.WriteAllLinesAsync(input, lines);

        // 16 bytes is strictly less than sizeof(EntryIndex) = 32; pre-fix this crashed.
        // MergeWidth kept high so we don't blow the FD budget with ~100 one-entry chunks.
        var options = new SortOptions
        {
            MaxMemoryPerChunk = 16,
            MergeWidth = 128,
            TempDirectory = _tempDir
        };

        var sorter = new MmfSorter(options);
        await sorter.SortAsync(input, output);

        var result = (await File.ReadAllLinesAsync(output))
            .Where(l => l.Length > 0).ToArray();
        result.Should().HaveCount(100, "tiny budget must not drop lines silently");

        // Cross-check: same input sorted under a sane budget must produce byte-identical output.
        var reference = Path.Combine(_tempDir, "tiny_budget_ref.txt");
        await new MmfSorter(new SortOptions { TempDirectory = _tempDir })
            .SortAsync(input, reference);
        (await File.ReadAllBytesAsync(output)).Should()
            .Equal(await File.ReadAllBytesAsync(reference),
                "tiny-budget output must equal normal-budget output byte-for-byte");
    }

    [Fact]
    public async Task SortAsync_MatchesStreamStrategy()
    {
        var input = Path.Combine(_tempDir, "input.txt");
        var outputMmf = Path.Combine(_tempDir, "mmf.txt");
        var outputStream = Path.Combine(_tempDir, "stream.txt");

        var lines = Enumerable.Range(1, 2000)
            .Select(i => $"{i % 50}. Phrase{i % 30}")
            .ToArray();

        await File.WriteAllLinesAsync(input, lines);

        var options = new SortOptions { TempDirectory = _tempDir };

        var mmfSorter = new MmfSorter(options);
        var streamSorter = new ExternalSorter(options);

        await mmfSorter.SortAsync(input, outputMmf);
        await streamSorter.SortAsync(input, outputStream);

        var mmfResult = await File.ReadAllLinesAsync(outputMmf);
        var streamResult = await File.ReadAllLinesAsync(outputStream);

        mmfResult.Should().Equal(streamResult,
            "MMF and stream strategies must produce identical output");
    }
}
