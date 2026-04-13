using System.Buffers;
using System.Text;
using System.Threading.Channels;

namespace LargeFileSorter.Core;

/// <summary>
/// External merge sort optimized for very large files (~100 GB+).
///
/// Phase 1 (split &amp; sort):
///   A reader fills chunks from the input file using sync I/O (avoids per-line
///   async state machine overhead). Filled chunks are sent via a bounded channel
///   to a sort worker that sorts in parallel and writes to temp files.
///   The reader and sort worker run concurrently — overlapping disk I/O and CPU.
///   ArrayPool is used to reuse chunk arrays across iterations.
///
/// Phase 2 (k-way merge):
///   Sorted chunks are merged using a PriorityQueue-based k-way merge.
///   Each chunk is wrapped in a BufferedChunkReader that pre-fetches batches
///   of lines, reducing per-line I/O overhead. Multi-level merge keeps
///   file handle count within bounds for very large inputs.
/// </summary>
public sealed class ExternalSorter
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

        try
        {
            progress?.Report("Phase 1: splitting and sorting chunks...");
            var chunkFiles = await SplitAndSortAsync(inputPath, tempDir, progress, ct);

            if (chunkFiles.Count == 0)
            {
                await using (File.Create(outputPath)) { }
                return;
            }

            if (chunkFiles.Count == 1)
            {
                File.Move(chunkFiles[0], outputPath, overwrite: true);
                progress?.Report("Done (single chunk, no merge needed).");
                return;
            }

            progress?.Report($"Phase 2: merging {chunkFiles.Count} sorted chunks...");
            MergeAll(chunkFiles, outputPath, tempDir, progress, ct);
            progress?.Report("Done.");
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    // -------------------------------------------------------------------
    //  Phase 1 — concurrent read → sort → write pipeline
    // -------------------------------------------------------------------

    private async Task<List<string>> SplitAndSortAsync(
        string inputPath, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        // Bounded channel provides backpressure: if the sort worker is busy,
        // the reader blocks — preventing unbounded memory growth.
        var channel = Channel.CreateBounded<ChunkPayload>(
            new BoundedChannelOptions(1) { SingleWriter = true });

        var chunkPaths = new List<string>();
        var pathLock = new object();

        // Producer: reads input file sequentially, fills chunks, pushes to channel.
        var readerTask = Task.Run(() =>
        {
            ReadInputIntoChunks(inputPath, channel.Writer, progress, ct);
            channel.Writer.Complete();
        }, ct);

        // Consumer: receives chunks, sorts with parallel merge sort, writes to disk.
        // Single worker keeps memory bounded (at most ~3 chunks in flight).
        // Parallelism comes from Parallel.For inside SortChunk.
        var sortTask = Task.Run(async () =>
        {
            await foreach (var payload in channel.Reader.ReadAllAsync(ct))
            {
                try
                {
                    SortChunk(payload.Data, payload.Count);

                    var path = Path.Combine(tempDir, $"chunk_{payload.Index:D6}.txt");
                    WriteChunkFile(path, payload.Data, payload.Count);

                    lock (pathLock)
                        chunkPaths.Add(path);
                }
                finally
                {
                    ArrayPool<LineEntry>.Shared.Return(payload.Data, clearArray: true);
                }
            }
        }, ct);

        await readerTask;
        await sortTask;

        chunkPaths.Sort(StringComparer.Ordinal);
        return chunkPaths;
    }

    private void ReadInputIntoChunks(
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

        // Sync ReadLine avoids allocating an async state machine per line.
        // With millions of lines per chunk, this overhead adds up significantly.
        using var stream = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
            FileShare.Read, _options.BufferSize, FileOptions.SequentialScan);
        using var reader = new StreamReader(stream, Encoding.UTF8, true, _options.BufferSize);

        while (reader.ReadLine() is { } line)
        {
            ct.ThrowIfCancellationRequested();

            var entry = LineParser.Parse(line);

            // Deduplicate text references within the chunk.
            // The task guarantees duplicate strings — pooling avoids storing
            // multiple copies of the same text in memory.
            if (stringPool.TryGetValue(entry.Text, out var existing))
                entry = new LineEntry(entry.Number, existing);
            else
                stringPool[entry.Text] = entry.Text;

            // Grow array if needed (ArrayPool may return larger than requested)
            if (count >= array.Length)
            {
                var bigger = pool.Rent(array.Length * 2);
                array.AsSpan(0, count).CopyTo(bigger);
                pool.Return(array, clearArray: true);
                array = bigger;
            }

            array[count++] = entry;
            estimatedBytes += 24 + 40 + entry.Text.Length * 2;

            if (estimatedBytes >= _options.MaxMemoryPerChunk)
            {
                var idx = chunkIndex++;
                progress?.Report($"  chunk {idx}: {count:N0} lines queued");

                // Blocks if channel is full — backpressure from the sort worker
                writer.WriteAsync(new ChunkPayload(array, count, idx), ct)
                    .AsTask().GetAwaiter().GetResult();

                capacity = EstimateChunkCapacity();
                array = pool.Rent(capacity);
                count = 0;
                estimatedBytes = 0;
                stringPool.Clear();
            }
        }

        if (count > 0)
        {
            progress?.Report($"  chunk {chunkIndex}: {count:N0} lines (final)");
            writer.WriteAsync(new ChunkPayload(array, count, chunkIndex), ct)
                .AsTask().GetAwaiter().GetResult();
        }
        else
        {
            pool.Return(array, clearArray: true);
        }
    }

    /// <summary>
    /// Sorts <paramref name="count"/> elements in <paramref name="data"/>.
    /// For large chunks, sorts segments in parallel then merges them via a PriorityQueue.
    /// </summary>
    private static void SortChunk(LineEntry[] data, int count)
    {
        const int parallelThreshold = 50_000;

        if (count < parallelThreshold)
        {
            Array.Sort(data, 0, count);
            return;
        }

        var segCount = Math.Clamp(Environment.ProcessorCount, 2, 8);
        var segSize = count / segCount;

        // Sort each segment on a separate thread
        Parallel.For(0, segCount, i =>
        {
            var start = i * segSize;
            var len = (i == segCount - 1) ? count - start : segSize;
            Array.Sort(data, start, len);
        });

        // K-way merge the sorted segments into a temp array, then copy back
        MergeSortedSegments(data, count, segCount, segSize);
    }

    private static void MergeSortedSegments(LineEntry[] data, int count, int segCount, int segSize)
    {
        var merged = new LineEntry[count];
        var positions = new int[segCount];
        var limits = new int[segCount];
        var pq = new PriorityQueue<int, LineEntry>(segCount);

        for (var i = 0; i < segCount; i++)
        {
            positions[i] = i * segSize;
            limits[i] = (i == segCount - 1) ? count : (i + 1) * segSize;
            pq.Enqueue(i, data[positions[i]]);
        }

        var idx = 0;
        while (pq.Count > 0)
        {
            pq.TryDequeue(out var seg, out var entry);
            merged[idx++] = entry;

            positions[seg]++;
            if (positions[seg] < limits[seg])
                pq.Enqueue(seg, data[positions[seg]]);
        }

        merged.AsSpan(0, count).CopyTo(data);
    }

    /// <summary>
    /// Writes a sorted chunk to disk using sync I/O with direct formatting
    /// to avoid ToString() allocations on every line.
    /// </summary>
    private void WriteChunkFile(string path, LineEntry[] data, int count)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
        using var writer = new StreamWriter(stream, Encoding.UTF8, _options.BufferSize);

        for (var i = 0; i < count; i++)
        {
            WriteEntry(writer, in data[i]);
        }
    }

    // -------------------------------------------------------------------
    //  Phase 2 — buffered k-way merge
    // -------------------------------------------------------------------

    private void MergeAll(
        List<string> chunkFiles, string outputPath, string tempDir,
        IProgress<string>? progress, CancellationToken ct)
    {
        var current = chunkFiles;
        var level = 0;

        while (current.Count > _options.MergeWidth)
        {
            var next = new List<string>();
            var groups = Partition(current, _options.MergeWidth);

            for (var g = 0; g < groups.Count; g++)
            {
                ct.ThrowIfCancellationRequested();
                var mergedPath = Path.Combine(tempDir, $"merge_L{level}_{g:D4}.txt");
                KWayMerge(groups[g], mergedPath, ct);
                next.Add(mergedPath);

                foreach (var f in groups[g])
                    File.Delete(f);
            }

            progress?.Report($"  merge level {level}: {current.Count} -> {next.Count} files");
            current = next;
            level++;
        }

        KWayMerge(current, outputPath, ct);
    }

    /// <summary>
    /// K-way merge using buffered chunk readers and PriorityQueue.
    /// Each reader pre-fetches <see cref="BufferedChunkReader.BatchSize"/> lines at a time
    /// into an internal buffer, drastically reducing I/O syscall frequency.
    /// </summary>
    private void KWayMerge(List<string> inputFiles, string outputPath, CancellationToken ct)
    {
        var readers = new BufferedChunkReader[inputFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>(inputFiles.Count);

            for (var i = 0; i < inputFiles.Count; i++)
            {
                readers[i] = new BufferedChunkReader(inputFiles[i], _options.BufferSize);
                if (readers[i].HasCurrent)
                    pq.Enqueue(i, readers[i].Current);
            }

            using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
            using var writer = new StreamWriter(outStream, Encoding.UTF8, _options.BufferSize);

            while (pq.Count > 0)
            {
                ct.ThrowIfCancellationRequested();

                pq.TryDequeue(out var readerIdx, out var entry);
                WriteEntry(writer, in entry);

                readers[readerIdx].Advance();
                if (readers[readerIdx].HasCurrent)
                    pq.Enqueue(readerIdx, readers[readerIdx].Current);
            }
        }
        finally
        {
            foreach (var r in readers)
                r?.Dispose();
        }
    }

    // -------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------

    private int EstimateChunkCapacity()
    {
        // Rough estimate: ~80 bytes per entry in memory
        return (int)Math.Min(_options.MaxMemoryPerChunk / 80, int.MaxValue / 2);
    }

    /// <summary>
    /// Writes a line entry directly without allocating an intermediate string.
    /// TextWriter.Write(long) formats the number straight into its buffer.
    /// </summary>
    private static void WriteEntry(TextWriter writer, in LineEntry entry)
    {
        writer.Write(entry.Number);
        writer.Write(". ");
        writer.WriteLine(entry.Text);
    }

    private static List<List<string>> Partition(List<string> items, int groupSize)
    {
        var result = new List<List<string>>();
        for (var i = 0; i < items.Count; i += groupSize)
            result.Add(items.GetRange(i, Math.Min(groupSize, items.Count - i)));
        return result;
    }

    private static void TryDeleteDirectory(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, true); }
        catch { /* best-effort cleanup */ }
    }

    // -------------------------------------------------------------------
    //  Nested types
    // -------------------------------------------------------------------

    private readonly record struct ChunkPayload(LineEntry[] Data, int Count, int Index);

    /// <summary>
    /// Reads parsed lines from a sorted chunk file in batches.
    /// Instead of one ReadLine call per PriorityQueue dequeue, we read
    /// <see cref="BatchSize"/> lines at once into a local buffer, then
    /// serve them from memory. This reduces I/O syscall frequency by ~8000x.
    /// </summary>
    private sealed class BufferedChunkReader : IDisposable
    {
        internal const int BatchSize = 8192;

        private readonly StreamReader _reader;
        private readonly LineEntry[] _buffer = new LineEntry[BatchSize];
        private int _pos;
        private int _count;
        private bool _eof;

        public BufferedChunkReader(string path, int ioBufferSize)
        {
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.Read, ioBufferSize, FileOptions.SequentialScan);
            _reader = new StreamReader(stream, Encoding.UTF8, false, ioBufferSize);
            Refill();
        }

        public bool HasCurrent => _pos < _count;
        public LineEntry Current => _buffer[_pos];

        public void Advance()
        {
            _pos++;
            if (_pos >= _count && !_eof)
                Refill();
        }

        private void Refill()
        {
            _pos = 0;
            _count = 0;

            while (_count < BatchSize)
            {
                var line = _reader.ReadLine();
                if (line == null) { _eof = true; break; }
                _buffer[_count++] = LineParser.Parse(line);
            }
        }

        public void Dispose() => _reader.Dispose();
    }
}
