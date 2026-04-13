using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

[MemoryDiagnoser]
[Config(typeof(Config))]
public class SortingBenchmarks
{
    private string _inputFile = null!;
    private string _outputFile = null!;
    private string _tempDir = null!;

    private sealed class Config : ManualConfig
    {
        public Config()
        {
            SummaryStyle = SummaryStyle.Default.WithRatioStyle(RatioStyle.Trend);
        }
    }

    [Params(1, 10, 50)]
    public int FileSizeMb { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"bench_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        _inputFile = Path.Combine(_tempDir, "input.txt");
        _outputFile = Path.Combine(_tempDir, "output.txt");

        var generator = new FileGenerator(new GeneratorOptions
        {
            Seed = 42,
            UniquePhraseCount = 200
        });

        generator.GenerateAsync(_inputFile, FileSizeMb * 1024L * 1024).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Benchmark(Baseline = true)]
    public async Task Sort_DefaultOptions()
    {
        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(_inputFile, _outputFile);
    }

    [Benchmark]
    public async Task Sort_SmallChunks()
    {
        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024 * 1024, // 4 MB chunks
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(_inputFile, _outputFile);
    }

    [Benchmark]
    public async Task Sort_NarrowMerge()
    {
        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024 * 1024,
            MergeWidth = 8,
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(_inputFile, _outputFile);
    }
}
