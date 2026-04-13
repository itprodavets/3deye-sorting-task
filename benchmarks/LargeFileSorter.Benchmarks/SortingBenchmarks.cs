using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

/// <summary>
/// Compares sorting strategies to justify the chosen approach.
///
/// Algorithms under test:
///
/// 1. NaiveInMemory — reads entire file into memory, Array.Sort, write.
///    + Fastest for small files (no chunk overhead).
///    + Simplest code path.
///    − O(file_size) memory. OOM on large inputs.
///    − No concurrency. CPU idle during I/O.
///
/// 2. SequentialExternal — basic external merge sort, single-threaded.
///    + Handles files larger than RAM.
///    + Predictable, simple control flow.
///    − CPU idle during I/O (no pipelining).
///    − Single-threaded sort (wastes cores).
///    − Per-line I/O in merge (no batching).
///    − ToString() allocation per line on writes.
///
/// 3. ExternalSort_Default — our optimized implementation.
///    + Concurrent pipeline (read ‖ sort overlapped via Channel).
///    + Parallel merge sort within chunks.
///    + ArrayPool reuse — no LOH pressure.
///    + Buffered merge (8K lines/batch).
///    + Direct write formatting — zero per-line allocations.
///    − More complex code.
///    − Slight overhead for very small files (pipeline setup).
///
/// 4. ExternalSort_SmallChunks — same as #3 but with 4 MB chunks.
///    Shows the cost of excessive chunk splitting: more temp files,
///    more merge work, more I/O overhead.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(Config))]
public class SortingBenchmarks
{
    private string _inputFile = null!;
    private string _tempDir = null!;

    private sealed class Config : ManualConfig
    {
        public Config()
        {
            SummaryStyle = SummaryStyle.Default.WithRatioStyle(RatioStyle.Trend);
            AddColumn(StatisticColumn.Median);
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

    /// <summary>
    /// Baseline: everything in memory. Fast for small files, impossible for 100 GB.
    /// </summary>
    [Benchmark(Description = "1. Naive in-memory")]
    public void NaiveInMemory()
    {
        var output = Path.Combine(_tempDir, "out_naive.txt");
        NaiveInMemorySorter.Sort(_inputFile, output);
    }

    /// <summary>
    /// External sort without concurrency or batching.
    /// Shows the cost of a naive external sort implementation.
    /// </summary>
    [Benchmark(Description = "2. Sequential external")]
    public void SequentialExternal()
    {
        var output = Path.Combine(_tempDir, "out_seq.txt");
        var sorter = new SequentialExternalSorter(
            maxChunkBytes: 4 * 1024 * 1024, // 4 MB to force multiple chunks
            tempDirectory: _tempDir);
        sorter.Sort(_inputFile, output);
    }

    /// <summary>
    /// Our optimized pipeline: concurrent read/sort, parallel merge sort,
    /// ArrayPool, buffered merge, direct formatting.
    /// </summary>
    [Benchmark(Baseline = true, Description = "3. Optimized external (default)")]
    public async Task ExternalSort_Default()
    {
        var output = Path.Combine(_tempDir, "out_opt.txt");
        var sorter = new ExternalSorter(new SortOptions { TempDirectory = _tempDir });
        await sorter.SortAsync(_inputFile, output);
    }

    /// <summary>
    /// Same optimized pipeline but with small 4 MB chunks.
    /// Shows the overhead of too many chunks: more merge work, more I/O.
    /// </summary>
    [Benchmark(Description = "4. Optimized external (4MB chunks)")]
    public async Task ExternalSort_SmallChunks()
    {
        var output = Path.Combine(_tempDir, "out_small.txt");
        var sorter = new ExternalSorter(new SortOptions
        {
            MaxMemoryPerChunk = 4 * 1024 * 1024,
            TempDirectory = _tempDir
        });
        await sorter.SortAsync(_inputFile, output);
    }
}
