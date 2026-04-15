using System.Buffers.Binary;
using System.Runtime;

namespace LargeFileSorter.Core;

/// <summary>
/// External merge sort with partitioned parallel merge — a variant of <see cref="ExternalSorter"/>
/// designed to remove the single-threaded merge bottleneck on multi-core machines.
///
/// Phase 1 (reused verbatim from <see cref="ExternalSorter"/>):
///   PipeReader → parse UTF-8 → Channel → parallel chunk sort → binary chunk files.
///
/// Phase 2 — the point of this strategy:
/// <list type="number">
///   <item>2a. Split each chunk into K sub-chunks by UTF-8 first-2-bytes range
///     (sequential scan per chunk). K = <see cref="SortOptions.MaxDegreeOfParallelism"/>,
///     bounded by a file-descriptor budget so macOS's 256-fd default doesn't blow up.</item>
///   <item>2b. Run K independent k-way merges in parallel — each shard merges its own
///     N sub-chunks into a text fragment covering one contiguous key range.</item>
///   <item>2c. Concatenate shard_{0..K-1}.txt → output.txt (byte-level append, no sort).</item>
/// </list>
///
/// <b>When it wins:</b> merge phase is CPU-bound (PriorityQueue pop + compare dominates).
/// On NVMe at 100 GB scale we measured 154 MB/s end-to-end against ~500 MB/s disk bandwidth —
/// plenty of disk headroom for K parallel readers to run unthrottled.
///
/// <b>When it doesn't:</b> I/O-bound workloads (spinning rust, tiny files where merge is
/// already &lt; 100 ms). In those cases the split-phase overhead outweighs the merge parallelism.
/// </summary>
public sealed class ShardSorter : IFileSorter
{
    // macOS default RLIMIT_NOFILE is 256. Each shard holds (chunks + 1) file descriptors open
    // concurrently during merge. This cap leaves headroom for the runtime's own fds (stdout,
    // pipes, socket), and on Linux — which allows 1024+ by default — it just caps cleanly.
    private const int FileDescriptorBudget = 200;

    private readonly SortOptions _options;

    public ShardSorter(SortOptions? options = null)
    {
        _options = options ?? new SortOptions();
    }

    public async Task SortAsync(string inputPath, string outputPath,
        IProgress<string>? progress = null, CancellationToken ct = default)
    {
        if (!File.Exists(inputPath))
            throw new FileNotFoundException("Input file not found.", inputPath);

        var tempDir = Path.Combine(
            _options.TempDirectory ?? Path.GetTempPath(),
            $"filesort_shard_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        ValidateDiskSpace(inputPath, tempDir);
        EnsureMinThreads();

        try
        {
            var previousLatency = GCSettings.LatencyMode;
            try
            {
                progress?.Report("Phase 1: splitting and sorting chunks...");
                var phase1 = new ExternalSorter(_options);
                var chunkFiles = await phase1.SortChunksOnlyAsync(inputPath, tempDir, progress, ct);

                if (chunkFiles.Count == 0)
                {
                    await using (File.Create(outputPath)) { }
                    return;
                }

                if (chunkFiles.Count == 1)
                {
                    // Single-chunk fast path — no merge of any kind. Matches ExternalSorter's behaviour.
                    BinaryChunkWriter.ConvertToText(chunkFiles[0], outputPath, _options.BufferSize);
                    progress?.Report("Done (single chunk).");
                    return;
                }

                GCSettings.LatencyMode = previousLatency;
                GC.Collect(2, GCCollectionMode.Optimized, blocking: false);

                var shardCount = PickShardCount(chunkFiles.Count);
                var boundaries = ComputeShardBoundaries(shardCount);

                progress?.Report($"Phase 2a: splitting {chunkFiles.Count} chunks into {shardCount} shards...");
                var shardInputs = SplitChunksIntoShards(chunkFiles, boundaries, tempDir, progress, ct);

                // Chunks no longer needed once they've been fanned out into per-shard pieces.
                // Free ~100 GB of disk early at scale instead of waiting for tempDir cleanup.
                foreach (var chunk in chunkFiles)
                    TryDeleteFile(chunk);

                progress?.Report($"Phase 2b: merging {shardCount} shards in parallel...");
                var shardOutputs = await MergeShardsInParallelAsync(shardInputs, tempDir, progress, ct);

                progress?.Report("Phase 2c: concatenating shard outputs...");
                ConcatShardOutputs(shardOutputs, outputPath);
                progress?.Report("Done.");
            }
            finally
            {
                GCSettings.LatencyMode = previousLatency;
            }
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    /// <summary>
    /// Runs only Phase 2 (split + parallel merge + concat) starting from already-built sorted
    /// binary chunks. Exposed for <see cref="MergePhaseBenchmarks"/> so the benchmark can
    /// isolate the merge phase from Phase 1 — the same surface <see cref="ChunkMerger.MergeAll"/>
    /// gives the stream strategy. Without this, a "merge phase" benchmark would silently include
    /// the chunk-sort cost and the comparison wouldn't be apples-to-apples.
    /// </summary>
    internal async Task MergeChunksOnlyAsync(
        List<string> chunkFiles, string outputPath, string tempDir,
        IProgress<string>? progress = null, CancellationToken ct = default)
    {
        if (chunkFiles.Count == 0)
        {
            await using (File.Create(outputPath)) { }
            return;
        }

        if (chunkFiles.Count == 1)
        {
            BinaryChunkWriter.ConvertToText(chunkFiles[0], outputPath, _options.BufferSize);
            return;
        }

        var shardCount = PickShardCount(chunkFiles.Count);
        var boundaries = ComputeShardBoundaries(shardCount);

        progress?.Report($"Phase 2a: splitting {chunkFiles.Count} chunks into {shardCount} shards...");
        var shardInputs = SplitChunksIntoShards(chunkFiles, boundaries, tempDir, progress, ct);

        progress?.Report($"Phase 2b: merging {shardCount} shards in parallel...");
        var shardOutputs = await MergeShardsInParallelAsync(shardInputs, tempDir, progress, ct);

        progress?.Report("Phase 2c: concatenating shard outputs...");
        ConcatShardOutputs(shardOutputs, outputPath);
    }

    // -------------------------------------------------------------------
    //  Shard planning
    // -------------------------------------------------------------------

    /// <summary>
    /// Picks a shard count that respects both the user's parallelism budget and the
    /// OS file-descriptor limit. Each parallel merge needs (chunks + 1) fds, so for
    /// large chunk counts we must reduce shards to stay under the budget.
    /// </summary>
    private int PickShardCount(int chunkCount)
    {
        var requested = Math.Max(2, _options.MaxDegreeOfParallelism);
        var fdCapped = Math.Max(2, FileDescriptorBudget / (chunkCount + 2));
        return Math.Min(requested, fdCapped);
    }

    /// <summary>
    /// Uniform partition of the 16-bit first-2-bytes key space into K shards.
    /// Returns K+1 boundaries: <c>boundaries[0] = 0x0000</c>, <c>boundaries[K] = 0x10000</c>.
    /// Record goes to shard s iff <c>boundaries[s] &lt;= key2 &lt; boundaries[s+1]</c>.
    ///
    /// Uniform over full 16-bit space is simple but data-blind: ASCII-heavy inputs (like
    /// the generator's "Capitalized. word-list" format) cluster in [0x41, 0x7A] which is
    /// ~21% of the space. Downstream shards in empty ranges finish in ~0ms — wasted
    /// parallelism but not correctness risk. A future refinement would sample chunk first
    /// keys to pick adaptive boundaries; for now the simple path is good enough.
    /// </summary>
    private static int[] ComputeShardBoundaries(int shardCount)
    {
        var boundaries = new int[shardCount + 1];
        for (var i = 0; i <= shardCount; i++)
            boundaries[i] = (int)((long)i * 0x10000 / shardCount);
        boundaries[shardCount] = 0x10000; // explicit sentinel, avoids off-by-one at top
        return boundaries;
    }

    // -------------------------------------------------------------------
    //  Phase 2a — split each chunk into per-shard sub-chunks
    // -------------------------------------------------------------------

    /// <summary>
    /// Scans each sorted chunk in order and appends each record to the shard file matching
    /// its first-2-bytes key range. Chunks are processed sequentially — the split phase is
    /// read-bound on a single file handle per chunk, so parallel splits just fight over disk
    /// bandwidth. Within a chunk, records are already sorted, so the sub-chunks produced are
    /// also sorted — downstream k-way merge works on them unchanged.
    /// </summary>
    private string[][] SplitChunksIntoShards(
        List<string> chunkFiles, int[] boundaries, string tempDir,
        IProgress<string>? progress, CancellationToken ct)
    {
        var shardCount = boundaries.Length - 1;
        // shardInputs[s] holds the list of per-chunk sub-chunk files that feed shard s.
        var shardInputs = new List<string>[shardCount];
        for (var s = 0; s < shardCount; s++) shardInputs[s] = new List<string>(chunkFiles.Count);

        // Reused across all chunks to bound FD usage at exactly shardCount open writers at a time.
        var shardWriters = new FileStream[shardCount];
        for (var c = 0; c < chunkFiles.Count; c++)
        {
            ct.ThrowIfCancellationRequested();

            // Open shard writers for this chunk's contribution. Files named
            // shard_{s}_chunk_{c}.bin so the downstream merger can pick them up
            // deterministically (sorted by chunk index preserves ordering within shard).
            for (var s = 0; s < shardCount; s++)
            {
                var path = Path.Combine(tempDir, $"shard_{s:D3}_chunk_{c:D4}.bin");
                shardWriters[s] = new FileStream(path, FileMode.Create, FileAccess.Write,
                    FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
                shardInputs[s].Add(path);
            }

            try
            {
                SplitOneChunk(chunkFiles[c], shardWriters, boundaries, ct);
            }
            finally
            {
                // CRITICAL: dispose between chunks. Reassigning shardWriters[s] without
                // disposing leaks an undisposed FileStream whose internal buffer never
                // flushes — meaning every chunk except the last gets silently dropped.
                // Caught by ShardSorterTests.SortAsync_PreservesLineCount.
                for (var s = 0; s < shardCount; s++)
                {
                    shardWriters[s]?.Dispose();
                    shardWriters[s] = null!;
                }
            }
        }

        progress?.Report($"  split {chunkFiles.Count} chunks into {shardCount} × {chunkFiles.Count} sub-chunks");
        return shardInputs.Select(l => l.ToArray()).ToArray();
    }

    /// <summary>
    /// Streams one binary chunk, classifies each record into a shard by the first 2 UTF-8
    /// bytes of its text, and appends the raw record bytes (Int64 number + varint length +
    /// UTF-8 text) to the matching shard writer. Uses <see cref="BinaryRawChunkReader"/>
    /// so text is kept as byte slices — no string allocations across 4.5B records at 100 GB.
    /// </summary>
    private void SplitOneChunk(string chunkPath, FileStream[] shardWriters,
        int[] boundaries, CancellationToken ct)
    {
        using var reader = new BinaryRawChunkReader(chunkPath, _options.BufferSize);
        Span<byte> headerBuf = stackalloc byte[16]; // Int64 (8) + varint length (up to 5) + slack

        while (reader.HasCurrent)
        {
            ct.ThrowIfCancellationRequested();
            var entry = reader.Current;
            var shardIdx = FindShard(entry.TextUtf8, boundaries);

            // Serialize the record in the same binary layout BinaryChunkReader consumes.
            // BinaryWriter-compatible: Int64 little-endian + 7-bit-encoded length + UTF-8 bytes.
            BinaryPrimitives.WriteInt64LittleEndian(headerBuf, entry.Number);
            var hdrLen = 8 + Write7BitEncodedInt(headerBuf[8..], entry.TextUtf8.Length);

            var writer = shardWriters[shardIdx];
            writer.Write(headerBuf[..hdrLen]);
            writer.Write(entry.TextUtf8);

            reader.Advance();
        }
    }

    /// <summary>
    /// Packs the first 2 UTF-8 bytes into a 16-bit unsigned int and binary-searches
    /// the boundary array. Array is small (K+1 ≤ 17), so a linear scan from the bottom
    /// would also be fine, but binary search costs ~4 comparisons and is branch-friendly.
    /// </summary>
    private static int FindShard(ReadOnlySpan<byte> text, int[] boundaries)
    {
        int key = text.Length switch
        {
            0 => 0,
            1 => text[0] << 8,
            _ => (text[0] << 8) | text[1]
        };

        // Linear scan (boundaries is always ≤ 17 entries in practice — K is CPU count).
        // Faster than Array.BinarySearch for such tiny arrays due to branch prediction
        // and no allocation of an IComparer.
        for (var s = boundaries.Length - 2; s >= 0; s--)
            if (key >= boundaries[s]) return s;
        return 0;
    }

    /// <summary>
    /// Writes a 7-bit-encoded int32 into the target span (same format as
    /// <see cref="BinaryWriter.Write7BitEncodedInt"/>). Returns bytes written.
    /// Inlined locally to avoid BinaryWriter's instance-method overhead in the hot loop.
    /// </summary>
    private static int Write7BitEncodedInt(Span<byte> dest, int value)
    {
        var i = 0;
        var v = (uint)value;
        while (v >= 0x80)
        {
            dest[i++] = (byte)(v | 0x80);
            v >>= 7;
        }
        dest[i++] = (byte)v;
        return i;
    }

    // -------------------------------------------------------------------
    //  Phase 2b — parallel k-way merge per shard
    // -------------------------------------------------------------------

    private async Task<string[]> MergeShardsInParallelAsync(
        string[][] shardInputs, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        var shardOutputs = new string[shardInputs.Length];
        var merger = new ChunkMerger(
            _options.BufferSize,
            _options.MergeWidth,
            path => new BinaryChunkReader(path, _options.BufferSize),
            path => new BinaryRawChunkReader(path, _options.BufferSize));

        // Run up to shardCount merges concurrently — that's the whole point of sharding.
        // Each merge is independent: reads its own N sub-chunk files, writes its own output.
        // No shared mutable state → no locks, no contention beyond disk bandwidth.
        await Parallel.ForEachAsync(
            Enumerable.Range(0, shardInputs.Length),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = shardInputs.Length,
                CancellationToken = ct
            },
            (shardIdx, innerCt) =>
            {
                var shardFiles = shardInputs[shardIdx].ToList();
                // Drop empty sub-chunks (empty shards produce zero-byte files). MergeAll
                // tolerates an empty list but we'd rather save it the bookkeeping.
                shardFiles.RemoveAll(f => new FileInfo(f).Length == 0);

                var shardOut = Path.Combine(tempDir, $"shard_{shardIdx:D3}.txt");
                if (shardFiles.Count == 0)
                {
                    File.Create(shardOut).Dispose(); // empty shard file — concat still works
                }
                else
                {
                    merger.MergeAll(shardFiles, shardOut, tempDir, progress: null, innerCt);
                }
                shardOutputs[shardIdx] = shardOut;

                // Clean up sub-chunks as we go — avoids ~2× input size on disk at the
                // concat step. Best-effort; leftover files will be swept by TryDeleteDirectory.
                foreach (var f in shardInputs[shardIdx])
                    TryDeleteFile(f);

                return ValueTask.CompletedTask;
            });

        return shardOutputs;
    }

    // -------------------------------------------------------------------
    //  Phase 2c — byte-level concatenation of shard outputs
    // -------------------------------------------------------------------

    /// <summary>
    /// Appends shard outputs in shard-index order. Because shard boundaries were chosen
    /// so that shard i's key range is strictly less than shard i+1's, the concatenation
    /// preserves total sort order without re-comparing.
    ///
    /// Uses a single output stream + sync FileStream.CopyTo, which internally uses a
    /// kernel-side sendfile on Linux and a fast BlockCopy path on other platforms.
    /// </summary>
    private void ConcatShardOutputs(string[] shardOutputs, string outputPath)
    {
        using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, _options.BufferSize, FileOptions.SequentialScan);

        foreach (var shard in shardOutputs)
        {
            using var inStream = new FileStream(shard, FileMode.Open, FileAccess.Read,
                FileShare.Read, _options.BufferSize, FileOptions.SequentialScan);
            inStream.CopyTo(outStream, _options.BufferSize);
        }
    }

    // -------------------------------------------------------------------
    //  Helpers (shared in spirit with ExternalSorter/MmfSorter — kept private here
    //  to avoid touching stable files for this feature branch).
    // -------------------------------------------------------------------

    private static void ValidateDiskSpace(string inputPath, string tempDir)
    {
        var inputSize = new FileInfo(inputPath).Length;
        // Shard strategy briefly holds 2× input size on disk (chunks + sub-chunks + shard outputs
        // overlap). 2.4× gives ~20% headroom over the theoretical peak.
        var requiredSpace = (long)(inputSize * 2.4);

        var driveInfo = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(tempDir))!);
        if (driveInfo.IsReady && driveInfo.AvailableFreeSpace < requiredSpace)
        {
            throw new IOException(
                $"Insufficient disk space for shard strategy. " +
                $"Required: {SizeFormatter.Format(requiredSpace)} (2.4× input), " +
                $"Available: {SizeFormatter.Format(driveInfo.AvailableFreeSpace)}. " +
                $"Use --temp-dir to point to a drive with more space, " +
                $"or use --strategy stream (needs only 1.2× input).");
        }
    }

    private static void EnsureMinThreads()
    {
        var desired = Environment.ProcessorCount;
        ThreadPool.GetMinThreads(out var currentWorker, out var currentIo);
        if (currentWorker < desired)
            ThreadPool.SetMinThreads(desired, currentIo);
    }

    private static void TryDeleteDirectory(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); }
        catch { /* best-effort cleanup */ }
    }

    private static void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best-effort cleanup */ }
    }
}
