using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

/// <summary>
/// Isolates Phase 2 (k-way merge) from the full end-to-end sort so we can see
/// where merge time actually goes. Answers the practical question for the 100 GB
/// run: is merge CPU-bound (PriorityQueue pop + compare + byte copy) or I/O-bound
/// (disk read from N chunks + disk write to output)?
///
/// Setup pre-builds N sorted binary chunks once in <see cref="GlobalSetup"/>;
/// each iteration reads those fixed inputs and writes a fresh text output. Merge
/// width is set high enough to keep the run single-level — we're measuring the
/// inner merge loop, not cascaded merges.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(Config))]
public class MergePhaseBenchmarks
{
    private string _tempDir = null!;
    private List<string> _chunkFiles = null!;
    private string _outputPath = null!;

    private sealed class Config : ManualConfig
    {
        public Config()
        {
            SummaryStyle = SummaryStyle.Default.WithRatioStyle(RatioStyle.Trend);
            AddColumn(StatisticColumn.Median);

            AddJob(Job.ShortRun
                .WithWarmupCount(1)
                .WithIterationCount(5)
                .WithId("ShortRun"));
        }
    }

    // 16 chunks × 8 MB ≈ 128 MB total — BenchmarkDotNet wants iterations ≥ 100 ms for
    // stable medians, and 128 MB keeps that comfortably above threshold on an NVMe SSD.
    // The 16-way merge matches typical production fan-in (17 chunks at 100 GB × 8 GB budget).
    [Params(16)]
    public int ChunkCount { get; set; }

    [Params(8)]
    public int ChunkSizeMb { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"bench_merge_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        _outputPath = Path.Combine(_tempDir, "merged.txt");
        _chunkFiles = new List<string>(ChunkCount);

        // Build each chunk by generating an independent small file, sorting it in memory,
        // and writing the binary chunk. Using the real pipeline (ExternalSorter with a
        // chunk budget sized to capture the whole mini-file) means the chunks have the
        // exact binary layout the merger consumes in production.
        for (var i = 0; i < ChunkCount; i++)
        {
            var rawPath = Path.Combine(_tempDir, $"raw_{i:D3}.txt");
            var chunkPath = Path.Combine(_tempDir, $"chunk_{i:D3}.bin");

            var generator = new FileGenerator(new GeneratorOptions
            {
                Seed = 1000 + i, // deterministic, different data per chunk
                UniquePhraseCount = 200
            });
            generator.GenerateAsync(rawPath, ChunkSizeMb * 1024L * 1024).GetAwaiter().GetResult();

            // One-chunk sort → single binary file — that's our pre-built merge input.
            var sorter = new ExternalSorter(new SortOptions
            {
                TempDirectory = _tempDir,
                MaxMemoryPerChunk = (ChunkSizeMb + 4) * 1024L * 1024
            });
            var tmpOut = Path.Combine(_tempDir, $"tmp_out_{i}.txt");
            sorter.SortAsync(rawPath, tmpOut).GetAwaiter().GetResult();

            // Rebuild the binary chunk from the sorted text output. Reusing the
            // end-to-end sorted result guarantees the binary format matches exactly
            // what the real Phase 1 would produce.
            RebuildBinaryChunk(tmpOut, chunkPath);
            _chunkFiles.Add(chunkPath);

            File.Delete(rawPath);
            File.Delete(tmpOut);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    /// <summary>
    /// Baseline: current ChunkMerger implementation (single-threaded k-way merge
    /// with PriorityQueue + RawLineEntry byte-level output).
    /// Serves as reference once we add the shard-merge variant in Step C.
    /// </summary>
    [Benchmark(Baseline = true, Description = "K-way merge (current)")]
    public void KWayMerge()
    {
        var merger = new ChunkMerger(
            bufferSize: 4 * 1024 * 1024,
            mergeWidth: 64,
            path => new BinaryChunkReader(path, 4 * 1024 * 1024),
            path => new BinaryRawChunkReader(path, 4 * 1024 * 1024));

        // Pass a copy because MergeAll may mutate the list during cascaded merges;
        // we want each iteration to see the same fresh input set.
        merger.MergeAll(new List<string>(_chunkFiles), _outputPath, _tempDir,
            progress: null, ct: CancellationToken.None);
    }

    private static void RebuildBinaryChunk(string sortedTextPath, string binaryChunkPath)
    {
        // Read sorted text, parse each "N. Text" line, write binary chunk format
        // (Int64 Number + BinaryWriter 7-bit varint length + UTF-8 bytes).
        using var inStream = File.OpenRead(sortedTextPath);
        using var reader = new StreamReader(inStream);
        using var outStream = new FileStream(binaryChunkPath, FileMode.Create, FileAccess.Write,
            FileShare.None, 64 * 1024, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(outStream);

        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            var dot = line.IndexOf('.');
            if (dot < 0) continue;
            var number = long.Parse(line.AsSpan(0, dot));
            var text = line[(dot + 2)..]; // skip ". "
            bw.Write(number);
            bw.Write(text);
        }
    }
}
