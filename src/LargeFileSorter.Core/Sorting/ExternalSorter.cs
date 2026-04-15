using System.Buffers;
using System.IO.Pipelines;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace LargeFileSorter.Core;

/// <summary>
/// External merge sort optimized for very large files (~100 GB+).
///
/// Orchestrates a two-phase pipeline:
///   Phase 1: PipeReader → parse UTF-8 → Channel → parallel sort → binary chunk write
///   Phase 2: Buffered binary k-way merge → text output
///
/// Delegates sorting to <see cref="ChunkSorter"/>, binary I/O to
/// <see cref="BinaryChunkWriter"/>/<see cref="BinaryChunkReader"/>,
/// and merge orchestration to <see cref="ChunkMerger"/>.
/// </summary>
public sealed class ExternalSorter : IFileSorter
{
    private readonly SortOptions _options;

    // Custom pool that can cache large chunk arrays (up to 256M entries).
    // ArrayPool<T>.Shared silently drops arrays > 1M elements on Return,
    // defeating the entire purpose of pooling for our chunk sizes.
    // On a 64 GB machine with 8 GB chunk budget and TextPool deduplication,
    // a single chunk can hold 200-300M entries — the pool must handle that.
    private static readonly ArrayPool<LineEntry> ChunkPool =
        ArrayPool<LineEntry>.Create(maxArrayLength: 256 * 1024 * 1024, maxArraysPerBucket: 4);

    public ExternalSorter(SortOptions? options = null)
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
            $"filesort_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        ValidateDiskSpace(inputPath, tempDir);

        // Pre-warm the thread pool so Parallel.For inside sort workers
        // doesn't stall waiting for thread injection.
        EnsureMinThreads();

        try
        {
            // Reduce GC pause frequency during the allocation-heavy split phase.
            // SustainedLowLatency prevents full blocking Gen2 collections while
            // Gen0/Gen1 still run — keeps throughput high without OOM risk.
            var previousLatency = GCSettings.LatencyMode;
            try
            {
                GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

                progress?.Report("Phase 1: splitting and sorting chunks...");
                var chunkFiles = await SplitAndSortAsync(inputPath, tempDir, progress, ct);

                if (chunkFiles.Count == 0)
                {
                    await using (File.Create(outputPath)) { }
                    return;
                }

                if (chunkFiles.Count == 1)
                {
                    BinaryChunkWriter.ConvertToText(chunkFiles[0], outputPath, _options.BufferSize);
                    progress?.Report("Done (single chunk).");
                    return;
                }

                // Restore default GC mode for the I/O-heavy merge phase
                GCSettings.LatencyMode = previousLatency;

                // Hint to the GC: a good time to collect Phase 1 allocations
                // before the I/O-heavy merge phase begins.
                GC.Collect(2, GCCollectionMode.Optimized, blocking: false);

                progress?.Report($"Phase 2: merging {chunkFiles.Count} sorted chunks...");
                var merger = new ChunkMerger(
                    _options.BufferSize,
                    _options.MergeWidth,
                    path => new BinaryChunkReader(path, _options.BufferSize),
                    path => new BinaryRawChunkReader(path, _options.BufferSize));
                merger.MergeAll(chunkFiles, outputPath, tempDir, progress, ct);
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
    /// Runs only Phase 1 (chunk sort) and returns the resulting sorted binary chunk files.
    /// Exposed for <see cref="ShardSorter"/>, which reuses this pipeline unchanged and
    /// substitutes its own parallel merge for Phase 2. Keeps the two strategies in sync
    /// on the allocation-heavy split path without copy-pasting ~200 lines of pipeline code.
    /// </summary>
    internal async Task<List<string>> SortChunksOnlyAsync(
        string inputPath, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        EnsureMinThreads();
        var previousLatency = GCSettings.LatencyMode;
        try
        {
            GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
            return await SplitAndSortAsync(inputPath, tempDir, progress, ct);
        }
        finally
        {
            GCSettings.LatencyMode = previousLatency;
        }
    }

    // -------------------------------------------------------------------
    //  Phase 1 — PipeReader → parse UTF-8 → Channel → parallel sort → binary write
    // -------------------------------------------------------------------

    private async Task<List<string>> SplitAndSortAsync(
        string inputPath, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        // Cap workers by the total parallelism budget so "--threads 2" never spawns 4 workers.
        var workerCount = Math.Max(1, Math.Min(_options.SortWorkers, _options.MaxDegreeOfParallelism));
        // Segments per chunk = leftover CPU budget after workers. Minimum 2 so large chunks
        // still benefit from parallel sort even when the budget is tight.
        var sortParallelism = Math.Max(2, _options.MaxDegreeOfParallelism / workerCount);

        var channel = Channel.CreateBounded<ChunkPayload>(
            new BoundedChannelOptions(workerCount)
            {
                SingleWriter = true,
                SingleReader = workerCount == 1
            });

        var chunkPaths = new List<string>();
        var pathLock = new Lock();

        var readerTask = Task.Run(async () =>
        {
            await ReadInputAsync(inputPath, channel.Writer, progress, ct);
            channel.Writer.Complete();
        }, ct);

        // Spawn N sort workers — Channel ensures each chunk is processed by exactly one worker.
        var sortTasks = new Task[workerCount];
        for (var w = 0; w < workerCount; w++)
        {
            var parallelism = sortParallelism;
            var ioBuffer = _options.BufferSize;
            sortTasks[w] = Task.Run(async () =>
            {
                await foreach (var payload in channel.Reader.ReadAllAsync(ct))
                {
                    try
                    {
                        var path = Path.Combine(tempDir, $"chunk_{payload.Index:D6}.bin");

                        // Sort and write in a single pass — for parallel-sorted chunks,
                        // the k-way merge streams entries directly to disk instead of
                        // allocating a full-copy merge buffer (~8 GB at 100 GB scale).
                        ChunkSorter.SortAndWrite(
                            payload.Data, payload.Count, parallelism, path, ioBuffer);

                        lock (pathLock)
                            chunkPaths.Add(path);
                    }
                    finally
                    {
                        ChunkPool.Return(payload.Data, clearArray: true);
                    }
                }
            }, ct);
        }

        await readerTask;
        await Task.WhenAll(sortTasks);

        chunkPaths.Sort(StringComparer.Ordinal);
        return chunkPaths;
    }

    private async Task ReadInputAsync(
        string inputPath, ChannelWriter<ChunkPayload> writer,
        IProgress<string>? progress, CancellationToken ct)
    {
        var textPool = new TextPool();

        var capacity = EstimateChunkCapacity();
        var array = ChunkPool.Rent(capacity);
        var count = 0;
        long estimatedBytes = 0;
        var chunkIndex = 0;

        await using var stream = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
            FileShare.Read, _options.BufferSize, FileOptions.SequentialScan | FileOptions.Asynchronous);

        var pipe = PipeReader.Create(stream, new StreamPipeReaderOptions(
            bufferSize: _options.BufferSize,
            minimumReadSize: _options.BufferSize / 4));

        var isFirstRead = true;

        try
        {
            while (true)
            {
                var readResult = await pipe.ReadAsync(ct);
                var buffer = readResult.Buffer;

                // Skip UTF-8 BOM if present at the start of the file
                if (isFirstRead && buffer.Length >= 3)
                {
                    ReadOnlySpan<byte> bom = [0xEF, 0xBB, 0xBF];
                    var first3 = buffer.Slice(0, 3);
                    if (first3.IsSingleSegment
                            ? first3.FirstSpan.SequenceEqual(bom)
                            : CopyAndCheck(first3, bom))
                    {
                        buffer = buffer.Slice(3);
                    }
                    isFirstRead = false;
                }

                while (TryReadLine(ref buffer, out var lineSeq))
                {
                    ct.ThrowIfCancellationRequested();

                    var entry = ParseAndIntern(lineSeq, textPool, out var isDuplicate);

                    if (count >= array.Length)
                    {
                        var bigger = ChunkPool.Rent(array.Length * 2);
                        array.AsSpan(0, count).CopyTo(bigger);
                        ChunkPool.Return(array, clearArray: true);
                        array = bigger;
                    }

                    array[count++] = entry;

                    // Only count string memory for the first occurrence.
                    // Deduplicated entries share the same string reference,
                    // so counting them again would underestimate actual capacity.
                    estimatedBytes += 32; // struct overhead (Number + ref + sortKey)
                    if (!isDuplicate)
                        estimatedBytes += 40 + entry.Text.Length * 2; // string obj + chars

                    if (estimatedBytes >= _options.MaxMemoryPerChunk)
                    {
                        var idx = chunkIndex++;
                        progress?.Report($"  chunk {idx}: {count:N0} lines queued");
                        await writer.WriteAsync(new ChunkPayload(array, count, idx), ct);

                        capacity = EstimateChunkCapacity();
                        array = ChunkPool.Rent(capacity);
                        count = 0;
                        estimatedBytes = 0;
                        textPool.Clear();
                    }
                }

                if (readResult.IsCompleted)
                {
                    // Handle last line (no trailing newline) BEFORE AdvanceTo —
                    // the pipe may reclaim buffer memory after AdvanceTo.
                    if (!buffer.IsEmpty)
                    {
                        var entry = ParseAndIntern(buffer, textPool, out _);

                        if (count >= array.Length)
                        {
                            var bigger = ChunkPool.Rent(array.Length * 2);
                            array.AsSpan(0, count).CopyTo(bigger);
                            ChunkPool.Return(array, clearArray: true);
                            array = bigger;
                        }
                        array[count++] = entry;
                    }

                    pipe.AdvanceTo(buffer.End);
                    break;
                }

                pipe.AdvanceTo(buffer.Start, buffer.End);
            }
        }
        finally
        {
            await pipe.CompleteAsync();
        }

        if (count > 0)
        {
            progress?.Report($"  chunk {chunkIndex}: {count:N0} lines (final)");
            await writer.WriteAsync(new ChunkPayload(array, count, chunkIndex), ct);
        }
        else
        {
            ChunkPool.Return(array, clearArray: true);
        }
    }

    // -------------------------------------------------------------------
    //  PipeReader helpers
    // -------------------------------------------------------------------

    private static bool TryReadLine(ref ReadOnlySequence<byte> buffer, out ReadOnlySequence<byte> line)
    {
        var position = buffer.PositionOf((byte)'\n');
        if (position == null)
        {
            line = default;
            return false;
        }

        line = buffer.Slice(0, position.Value);
        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
        return true;
    }

    /// <summary>
    /// Parses a line from a byte sequence and interns the text in a single step.
    /// Combines parsing + deduplication to avoid allocating a string for duplicate text.
    /// Uses <see cref="LineParser.ParseNumberUtf8"/> (no string alloc) followed by
    /// <see cref="TextPool.Intern"/> (AlternateLookup — string alloc only for new text).
    /// </summary>
    private static LineEntry ParseAndIntern(
        ReadOnlySequence<byte> lineSeq, TextPool textPool, out bool isDuplicate)
    {
        // Trim \r
        if (lineSeq.Length > 0)
        {
            var last = lineSeq.Slice(lineSeq.Length - 1).FirstSpan[0];
            if (last == (byte)'\r')
                lineSeq = lineSeq.Slice(0, lineSeq.Length - 1);
        }

        if (lineSeq.Length == 0)
            throw new FormatException("Empty line");

        if (lineSeq.IsSingleSegment)
            return ParseAndInternSpan(lineSeq.FirstSpan, textPool, out isDuplicate);

        // Multi-segment (rare — only at buffer boundaries)
        var length = (int)lineSeq.Length;
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            lineSeq.CopyTo(rented);
            return ParseAndInternSpan(rented.AsSpan(0, length), textPool, out isDuplicate);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    [SkipLocalsInit]
    private static LineEntry ParseAndInternSpan(
        ReadOnlySpan<byte> utf8Line, TextPool textPool, out bool isDuplicate)
    {
        var number = LineParser.ParseNumberUtf8(utf8Line, out var textStart);
        var text = textPool.Intern(utf8Line[textStart..], out isDuplicate);
        return new LineEntry(number, text);
    }

    private static bool CopyAndCheck(ReadOnlySequence<byte> seq, ReadOnlySpan<byte> expected)
    {
        Span<byte> tmp = stackalloc byte[(int)seq.Length];
        seq.CopyTo(tmp);
        return tmp.SequenceEqual(expected);
    }

    // -------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------

    /// <summary>
    /// Initial capacity hint for the chunk array. Starts conservative (1M entries)
    /// and grows via doubling as entries arrive. This keeps the first Rent within
    /// the pool's range, avoiding an immediate LOH allocation for the full estimated
    /// chunk size (which may be 100M+ entries with aggressive TextPool deduplication).
    /// </summary>
    private static int EstimateChunkCapacity() => 1024 * 1024;

    /// <summary>
    /// Checks that the temp directory has enough free disk space before starting.
    /// Binary chunks are slightly larger than the original text, so we require
    /// at least 1.2× the input file size as headroom.
    /// </summary>
    private static void ValidateDiskSpace(string inputPath, string tempDir)
    {
        var inputSize = new FileInfo(inputPath).Length;
        var requiredSpace = (long)(inputSize * 1.2);

        var driveInfo = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(tempDir))!);
        if (driveInfo.IsReady && driveInfo.AvailableFreeSpace < requiredSpace)
        {
            throw new IOException(
                $"Insufficient disk space in temp directory. " +
                $"Required: {SizeFormatter.Format(requiredSpace)}, " +
                $"Available: {SizeFormatter.Format(driveInfo.AvailableFreeSpace)}. " +
                $"Use --temp-dir to point to a drive with more space.");
        }
    }

    /// <summary>
    /// Pre-warms the thread pool so <see cref="Parallel.For"/> and
    /// <see cref="Task.Run"/> don't stall waiting for thread injection
    /// on the first few chunks.
    /// </summary>
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

    private readonly record struct ChunkPayload(LineEntry[] Data, int Count, int Index);
}
