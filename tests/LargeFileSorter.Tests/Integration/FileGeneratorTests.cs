using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

public class FileGeneratorTests : IDisposable
{
    private readonly string _tempDir;

    public FileGeneratorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"gen_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public async Task GenerateAsync_CreatesFileOfApproximateSize()
    {
        var outputPath = Path.Combine(_tempDir, "test.txt");
        var targetSize = 10 * 1024L; // 10 KB
        var generator = new FileGenerator(new GeneratorOptions { Seed = 42 });

        await generator.GenerateAsync(outputPath, targetSize);

        var fileInfo = new FileInfo(outputPath);
        fileInfo.Exists.Should().BeTrue();
        // allow some overshoot since we write until we exceed the target
        fileInfo.Length.Should().BeGreaterThanOrEqualTo(targetSize);
        fileInfo.Length.Should().BeLessThan(targetSize * 2);
    }

    [Fact]
    public async Task GenerateAsync_AllLinesMatchExpectedFormat()
    {
        var outputPath = Path.Combine(_tempDir, "format_test.txt");
        var generator = new FileGenerator(new GeneratorOptions { Seed = 123 });

        await generator.GenerateAsync(outputPath, 5 * 1024);

        var lines = await File.ReadAllLinesAsync(outputPath);
        lines.Should().NotBeEmpty();

        foreach (var line in lines)
        {
            // Should not throw
            var entry = LineParser.Parse(line);
            entry.Number.Should().BePositive();
            entry.Text.Should().NotBeNullOrWhiteSpace();
        }
    }

    [Fact]
    public async Task GenerateAsync_ProducesDuplicateStrings()
    {
        var outputPath = Path.Combine(_tempDir, "dup_test.txt");
        var generator = new FileGenerator(new GeneratorOptions
        {
            Seed = 7,
            UniquePhraseCount = 10 // small pool forces duplicates
        });

        await generator.GenerateAsync(outputPath, 20 * 1024);

        var lines = await File.ReadAllLinesAsync(outputPath);
        var texts = lines.Select(l => LineParser.Parse(l).Text).ToList();
        var uniqueTexts = texts.Distinct().Count();

        uniqueTexts.Should().BeLessThan(texts.Count,
            "generator should produce duplicate string values");
    }

    [Fact]
    public async Task GenerateAsync_WithSeed_IsReproducible()
    {
        var path1 = Path.Combine(_tempDir, "seed1.txt");
        var path2 = Path.Combine(_tempDir, "seed2.txt");
        var opts = new GeneratorOptions { Seed = 999 };
        var targetSize = 5 * 1024L;

        await new FileGenerator(opts).GenerateAsync(path1, targetSize);
        await new FileGenerator(opts).GenerateAsync(path2, targetSize);

        var content1 = await File.ReadAllTextAsync(path1);
        var content2 = await File.ReadAllTextAsync(path2);
        content1.Should().Be(content2);
    }
}
