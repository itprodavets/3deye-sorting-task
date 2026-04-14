using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

/// <summary>
/// Head-to-head comparison of the two <see cref="IFileSorter"/> strategies.
///
/// <b>Stream</b> (PipeReader + Channel + TextPool):
///   Concurrent read/sort pipeline. String deduplication via TextPool keeps
///   managed heap small for data with repeating phrases — ideal for typical
///   workloads where phrase count ≪ line count.
///
/// <b>MMF</b> (MemoryMappedFile + NativeMemory):
///   No managed allocations during index/sort phase. Text stays as byte
///   offsets into the MMF; NativeBuffer&lt;EntryIndex&gt; is invisible to GC.
///   Eliminates all GC pauses regardless of data cardinality.
///
/// Each benchmark generates a deterministic file with seed=42 and runs
/// both strategies on the same data. The "small chunks" variants force
/// multi-chunk merge to compare the external merge paths.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(Config))]
public class StrategyBenchmarks
{
    private string _inputFile = null!;
    private string _tempDir = null!;

    private sealed class Config : ManualConfig
    {
        public Config()
        {
            SummaryStyle = SummaryStyle.Default.WithRatioStyle(RatioStyle.Trend);
            AddColumn(StatisticColumn.Median);

            // Larger files need fewer iterations — benchmark is I/O-bound.
            AddJob(Job.ShortRun
                .WithWarmupCount(1)
                .WithIterationCount(3)
                .WithId("ShortRun"));
        }
    }

    [Params(10, 50, 200)]
    public int FileSizeMb { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"bench_strat_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        _inputFile = Path.Combine(_tempDir, "input.txt");

        var generator = new FileGenerator(new GeneratorOptions
        {
            Seed = 42,
            UniquePhraseCount = 500
        });
        generator.GenerateAsync(_inputFile, FileSizeMb * 1024L * 1024).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Stream strategy: PipeReader → TextPool dedup → Channel → parallel sort → binary chunks.
    /// Best when phrase cardinality is low (text repeats frequently).
    /// </summary>
    [Benchmark(Baseline = true, Description = "Stream (PipeReader + Channel)")]
    public async Task Stream()
    {
        var output = Path.Combine(_tempDir, "out_stream.txt");
        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(_inputFile, output);
    }

    /// <summary>
    /// MMF strategy: MemoryMappedFile → NativeBuffer&lt;EntryIndex&gt; → pointer sort.
    /// Best when strings are high-cardinality or GC pressure must be zero.
    /// </summary>
    [Benchmark(Description = "MMF (MemoryMapped + NativeMemory)")]
    public async Task Mmf()
    {
        var output = Path.Combine(_tempDir, "out_mmf.txt");
        var sorter = new MmfSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(_inputFile, output);
    }

    /// <summary>
    /// Stream with 4 MB chunks — forces multi-chunk external merge.
    /// </summary>
    [Benchmark(Description = "Stream (4MB chunks)")]
    public async Task Stream_SmallChunks()
    {
        var output = Path.Combine(_tempDir, "out_stream_sm.txt");
        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024 * 1024,
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(_inputFile, output);
    }

    /// <summary>
    /// MMF with 4 MB chunks — forces multi-chunk external merge.
    /// </summary>
    [Benchmark(Description = "MMF (4MB chunks)")]
    public async Task Mmf_SmallChunks()
    {
        var output = Path.Combine(_tempDir, "out_mmf_sm.txt");
        var sorter = new MmfSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024 * 1024,
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(_inputFile, output);
    }
}
