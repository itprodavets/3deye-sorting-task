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

        // Verify sort order: text alphabetically, then number ascending
        for (var i = 1; i < result.Length; i++)
        {
            var prev = LineParser.Parse(result[i - 1]);
            var curr = LineParser.Parse(result[i]);
            var cmp = string.Compare(prev.Text, curr.Text, StringComparison.Ordinal);
            cmp.Should().BeLessThanOrEqualTo(0,
                $"line {i - 1} '{result[i - 1]}' should come before '{result[i]}'");

            if (cmp == 0)
                prev.Number.Should().BeLessThanOrEqualTo(curr.Number);
        }
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
