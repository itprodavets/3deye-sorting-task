using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using System.Threading.Channels;

namespace LargeFileSorter.Core;

/// <summary>
/// External merge sort optimized for very large files (~100 GB+).
///
/// Phase 1 (split &amp; sort):
///   PipeReader reads the input in large blocks — no per-line string allocation.
///   Lines are parsed directly from UTF-8 bytes (only the Text part becomes a string).
///   Filled chunks are sent via a bounded Channel to a sort worker.
///   Sort uses parallel segment sorting + k-way merge within each chunk.
///   Sorted chunks are written in binary format (BinaryWriter) to avoid
///   re-parsing text during the merge phase.
///   ArrayPool reuses chunk arrays across iterations.
///
/// Phase 2 (k-way merge):
///   Binary chunk readers pre-fetch batches of records (no text parsing).
///   PriorityQueue-based k-way merge with multi-level support.
///   Output is written as text (the required format).
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
                MergeSingleChunkToText(chunkFiles[0], outputPath);
                progress?.Report("Done (single chunk).");
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
    //  Phase 1 — PipeReader → parse UTF-8 → Channel → parallel sort → binary write
    // -------------------------------------------------------------------

    private async Task<List<string>> SplitAndSortAsync(
        string inputPath, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        var channel = Channel.CreateBounded<ChunkPayload>(
            new BoundedChannelOptions(1) { SingleWriter = true });

        var chunkPaths = new List<string>();
        var pathLock = new object();

        var readerTask = Task.Run(async () =>
        {
            await ReadInputWithPipeReaderAsync(inputPath, channel.Writer, progress, ct);
            channel.Writer.Complete();
        }, ct);

        var sortTask = Task.Run(async () =>
        {
            await foreach (var payload in channel.Reader.ReadAllAsync(ct))
            {
                try
                {
                    SortChunk(payload.Data, payload.Count);

                    var path = Path.Combine(tempDir, $"chunk_{payload.Index:D6}.bin");
                    WriteChunkBinary(path, payload.Data, payload.Count);

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

    private async Task ReadInputWithPipeReaderAsync(
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

                    if (stringPool.TryGetValue(entry.Text, out var existing))
                        entry = new LineEntry(entry.Number, existing);
                    else
                        stringPool[entry.Text] = entry.Text;

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
                        if (stringPool.TryGetValue(entry.Text, out var existing))
                            entry = new LineEntry(entry.Number, existing);
                        else
                            stringPool[entry.Text] = entry.Text;

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

    private static bool CopyAndCheck(ReadOnlySequence<byte> seq, ReadOnlySpan<byte> expected)
    {
        Span<byte> tmp = stackalloc byte[(int)seq.Length];
        seq.CopyTo(tmp);
        return tmp.SequenceEqual(expected);
    }

    // -------------------------------------------------------------------
    //  Parallel chunk sort
    // -------------------------------------------------------------------

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

        Parallel.For(0, segCount, i =>
        {
            var start = i * segSize;
            var len = (i == segCount - 1) ? count - start : segSize;
            Array.Sort(data, start, len);
        });

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

    // -------------------------------------------------------------------
    //  Binary chunk I/O
    // -------------------------------------------------------------------

    private static void WriteChunkBinary(string path, LineEntry[] data, int count)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, 65536, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: false);

        for (var i = 0; i < count; i++)
        {
            bw.Write(data[i].Number);
            bw.Write(data[i].Text);
        }
    }

    private void MergeSingleChunkToText(string binaryChunkPath, string outputPath)
    {
        using var inStream = new FileStream(binaryChunkPath, FileMode.Open, FileAccess.Read,
            FileShare.Read, _options.BufferSize, FileOptions.SequentialScan);
        using var br = new BinaryReader(inStream, Encoding.UTF8, leaveOpen: false);

        using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
        using var writer = new StreamWriter(outStream, Encoding.UTF8, _options.BufferSize);

        try
        {
            while (true)
            {
                var number = br.ReadInt64();
                var text = br.ReadString();
                writer.Write(number);
                writer.Write(". ");
                writer.WriteLine(text);
            }
        }
        catch (EndOfStreamException) { }
    }

    // -------------------------------------------------------------------
    //  Phase 2 — buffered binary k-way merge
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
                var mergedPath = Path.Combine(tempDir, $"merge_L{level}_{g:D4}.bin");
                KWayMergeBinary(groups[g], mergedPath, ct);
                next.Add(mergedPath);

                foreach (var f in groups[g])
                    File.Delete(f);
            }

            progress?.Report($"  merge level {level}: {current.Count} -> {next.Count} files");
            current = next;
            level++;
        }

        KWayMergeToText(current, outputPath, ct);
    }

    private void KWayMergeBinary(List<string> inputFiles, string outputPath, CancellationToken ct)
    {
        var readers = new BinaryChunkReader[inputFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>(inputFiles.Count);
            for (var i = 0; i < inputFiles.Count; i++)
            {
                readers[i] = new BinaryChunkReader(inputFiles[i], _options.BufferSize);
                if (readers[i].HasCurrent)
                    pq.Enqueue(i, readers[i].Current);
            }

            using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
            using var bw = new BinaryWriter(outStream, Encoding.UTF8, leaveOpen: false);

            while (pq.Count > 0)
            {
                ct.ThrowIfCancellationRequested();
                pq.TryDequeue(out var readerIdx, out var entry);
                bw.Write(entry.Number);
                bw.Write(entry.Text);
                readers[readerIdx].Advance();
                if (readers[readerIdx].HasCurrent)
                    pq.Enqueue(readerIdx, readers[readerIdx].Current);
            }
        }
        finally
        {
            foreach (var r in readers) r?.Dispose();
        }
    }

    private void KWayMergeToText(List<string> inputFiles, string outputPath, CancellationToken ct)
    {
        var readers = new BinaryChunkReader[inputFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>(inputFiles.Count);
            for (var i = 0; i < inputFiles.Count; i++)
            {
                readers[i] = new BinaryChunkReader(inputFiles[i], _options.BufferSize);
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
                writer.Write(entry.Number);
                writer.Write(". ");
                writer.WriteLine(entry.Text);
                readers[readerIdx].Advance();
                if (readers[readerIdx].HasCurrent)
                    pq.Enqueue(readerIdx, readers[readerIdx].Current);
            }
        }
        finally
        {
            foreach (var r in readers) r?.Dispose();
        }
    }

    // -------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------

    private int EstimateChunkCapacity()
        => (int)Math.Min(_options.MaxMemoryPerChunk / 96, int.MaxValue / 2);

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
    /// Reads LineEntry records from a binary chunk file in batches.
    /// No text parsing — BinaryReader.ReadInt64() + ReadString().
    /// </summary>
    private sealed class BinaryChunkReader : IDisposable
    {
        private const int BatchSize = 8192;

        private readonly BinaryReader _reader;
        private readonly LineEntry[] _buffer = new LineEntry[BatchSize];
        private int _pos;
        private int _count;
        private bool _eof;

        public BinaryChunkReader(string path, int ioBufferSize)
        {
            var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.Read, ioBufferSize, FileOptions.SequentialScan);
            _reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);
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
                try
                {
                    var number = _reader.ReadInt64();
                    var text = _reader.ReadString();
                    _buffer[_count++] = new LineEntry(number, text);
                }
                catch (EndOfStreamException)
                {
                    _eof = true;
                    break;
                }
            }
        }

        public void Dispose() => _reader.Dispose();
    }
}
