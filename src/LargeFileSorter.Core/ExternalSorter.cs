using System.Buffers;
using System.IO.Pipelines;
using System.Runtime;
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
                    path => new BinaryChunkReader(path, _options.BufferSize));
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

    // -------------------------------------------------------------------
    //  Phase 1 — PipeReader → parse UTF-8 → Channel → parallel sort → binary write
    // -------------------------------------------------------------------

    private async Task<List<string>> SplitAndSortAsync(
        string inputPath, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        // Scale sort workers to available cores: each worker runs sort + binary write
        // concurrently, overlapping CPU-heavy sort with I/O-heavy write.
        var workerCount = Math.Clamp(Environment.ProcessorCount / 2, 1, 4);
        var sortParallelism = Math.Max(2, Environment.ProcessorCount / workerCount);

        var channel = Channel.CreateBounded<ChunkPayload>(
            new BoundedChannelOptions(workerCount)
            {
                SingleWriter = true,
                SingleReader = workerCount == 1
            });

        var chunkPaths = new List<string>();
        var pathLock = new object();

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
            sortTasks[w] = Task.Run(async () =>
            {
                await foreach (var payload in channel.Reader.ReadAllAsync(ct))
                {
                    try
                    {
                        ChunkSorter.Sort(payload.Data, payload.Count, parallelism);

                        var path = Path.Combine(tempDir, $"chunk_{payload.Index:D6}.bin");
                        BinaryChunkWriter.Write(path, payload.Data, payload.Count);

                        lock (pathLock)
                            chunkPaths.Add(path);
                    }
                    finally
                    {
                        ArrayPool<LineEntry>.Shared.Return(payload.Data, clearArray: true);
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
        var pool = ArrayPool<LineEntry>.Shared;
        var stringPool = new Dictionary<string, string>(4096, StringComparer.Ordinal);

        var capacity = EstimateChunkCapacity();
        var array = pool.Rent(capacity);
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

                    var entry = ParseLineFromSequence(lineSeq);
                    entry = DeduplicateText(entry, stringPool);

                    if (count >= array.Length)
                    {
                        var bigger = pool.Rent(array.Length * 2);
                        array.AsSpan(0, count).CopyTo(bigger);
                        pool.Return(array, clearArray: true);
                        array = bigger;
                    }

                    array[count++] = entry;
                    estimatedBytes += 32 + 40 + entry.Text.Length * 2;

                    if (estimatedBytes >= _options.MaxMemoryPerChunk)
                    {
                        var idx = chunkIndex++;
                        progress?.Report($"  chunk {idx}: {count:N0} lines queued");
                        await writer.WriteAsync(new ChunkPayload(array, count, idx), ct);

                        capacity = EstimateChunkCapacity();
                        array = pool.Rent(capacity);
                        count = 0;
                        estimatedBytes = 0;
                        stringPool.Clear();
                    }
                }

                if (readResult.IsCompleted)
                {
                    // Handle last line (no trailing newline) BEFORE AdvanceTo —
                    // the pipe may reclaim buffer memory after AdvanceTo.
                    if (!buffer.IsEmpty)
                    {
                        var entry = ParseLineFromSequence(buffer);
                        entry = DeduplicateText(entry, stringPool);

                        if (count >= array.Length)
                        {
                            var bigger = pool.Rent(array.Length * 2);
                            array.AsSpan(0, count).CopyTo(bigger);
                            pool.Return(array, clearArray: true);
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
            pool.Return(array, clearArray: true);
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

    private static LineEntry ParseLineFromSequence(ReadOnlySequence<byte> lineSeq)
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
            return LineParser.ParseUtf8(lineSeq.FirstSpan);

        // Multi-segment (rare — only at buffer boundaries)
        var length = (int)lineSeq.Length;
        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            lineSeq.CopyTo(rented);
            return LineParser.ParseUtf8(rented.AsSpan(0, length));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static LineEntry DeduplicateText(LineEntry entry, Dictionary<string, string> pool)
    {
        if (pool.TryGetValue(entry.Text, out var existing))
            return new LineEntry(entry.Number, existing);

        pool[entry.Text] = entry.Text;
        return entry;
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

    private int EstimateChunkCapacity()
        => (int)Math.Min(_options.MaxMemoryPerChunk / 96, int.MaxValue / 2);

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
