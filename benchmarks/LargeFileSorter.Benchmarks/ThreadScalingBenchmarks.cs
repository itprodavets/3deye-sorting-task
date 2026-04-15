using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

/// <summary>
/// Sweeps the unified parallelism budget (<see cref="SortOptions.MaxDegreeOfParallelism"/>)
/// from 1 to 16 to answer: "does the sorter actually scale with cores, or does some
/// shared resource (merge PQ, output stream, disk) pin it?"
///
/// Both <c>stream</c> and <c>mmf</c> strategies are swept with identical budgets so
/// ratio columns show which strategy uses the budget more effectively at each width.
///
/// Small-chunk mode (16 MB) is used so Phase 2 (merge) actually runs — with the default
/// 8 GB chunk budget a 50 MB file is a single chunk and the merge path isn't exercised.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(Config))]
public class ThreadScalingBenchmarks
{
    private string _inputFile = null!;
    private string _tempDir = null!;

    private sealed class Config : ManualConfig
    {
        public Config()
        {
            SummaryStyle = SummaryStyle.Default.WithRatioStyle(RatioStyle.Trend);
            AddColumn(StatisticColumn.Median);

            // External sort is I/O- and allocation-heavy; short runs keep wall time
            // manageable while still producing stable medians across the sweep.
            AddJob(Job.ShortRun
                .WithWarmupCount(1)
                .WithIterationCount(3)
                .WithId("ShortRun"));
        }
    }

    // File size kept small (50 MB) so the full sweep completes in minutes, not hours.
    // Parallelism scaling shape is preserved at this size — the limiting resource
    // (PQ contention, output serialization) doesn't change with file size.
    [Params(50)]
    public int FileSizeMb { get; set; }

    [Params(1, 2, 4, 8, 16)]
    public int Threads { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"bench_scale_{Guid.NewGuid():N}");
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

    // Force multi-chunk so both Phase 1 (parallel sort) and Phase 2 (merge) run —
    // otherwise we'd only see the intra-chunk scaling and miss PQ/merge overhead.
    private SortOptions Options() => new()
    {
        MaxMemoryPerChunk = 16 * 1024 * 1024,
        TempDirectory = _tempDir,
        MaxDegreeOfParallelism = Threads
    };

    [Benchmark(Baseline = true, Description = "Stream")]
    public async Task Stream()
    {
        var output = Path.Combine(_tempDir, $"out_stream_{Threads}.txt");
        var sorter = new ExternalSorter(Options());
        await sorter.SortAsync(_inputFile, output);
    }

    [Benchmark(Description = "MMF")]
    public async Task Mmf()
    {
        var output = Path.Combine(_tempDir, $"out_mmf_{Threads}.txt");
        var sorter = new MmfSorter(Options());
        await sorter.SortAsync(_inputFile, output);
    }
}
