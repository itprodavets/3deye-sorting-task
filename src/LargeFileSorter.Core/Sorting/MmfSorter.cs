using System.Buffers;
using System.Buffers.Text;
using System.IO.MemoryMappedFiles;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// External merge sort backed by memory-mapped files and native memory.
///
/// Differences from <see cref="ExternalSorter"/> (stream-based):
/// <list type="bullet">
///   <item><b>Zero string allocations</b> — text stays in the MMF; only byte offsets are stored</item>
///   <item><b>No GC pressure from indexes</b> — <see cref="NativeBuffer{T}"/> uses <see cref="NativeMemory"/></item>
///   <item><b>No PipeReader overhead</b> — direct pointer scan over memory-mapped bytes</item>
///   <item><b>8-byte sort key</b> — first 8 raw UTF-8 bytes give 90%+ prefix discrimination</item>
/// </list>
///
/// Falls back to multi-chunk external merge when the file exceeds the memory budget.
/// Binary chunk format is identical to <see cref="BinaryChunkWriter"/>, so the merge
/// phase reuses <see cref="ChunkMerger"/> and <see cref="BinaryChunkReader"/> unchanged.
/// </summary>
public sealed class MmfSorter : IFileSorter
{
    private static readonly SearchValues<byte> NewlineByte = SearchValues.Create("\n"u8);
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    private readonly SortOptions _options;

    public MmfSorter(SortOptions? options = null)
    {
        _options = options ?? new SortOptions();
    }

    public async Task SortAsync(string inputPath, string outputPath,
        IProgress<string>? progress = null, CancellationToken ct = default)
    {
        if (!File.Exists(inputPath))
            throw new FileNotFoundException("Input file not found.", inputPath);

        var fileSize = new FileInfo(inputPath).Length;
        if (fileSize == 0)
        {
            await using (File.Create(outputPath)) { }
            return;
        }

        var tempDir = Path.Combine(
            _options.TempDirectory ?? Path.GetTempPath(),
            $"filesort_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        ValidateDiskSpace(inputPath, tempDir);
        EnsureMinThreads();

        try
        {
            await Task.Run(() => SortCore(inputPath, outputPath, fileSize, tempDir, progress, ct), ct);
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    // ------------------------------------------------------------------
    //  Core sort: MMF scan → sort → write
    // ------------------------------------------------------------------

    private unsafe void SortCore(string inputPath, string outputPath, long fileSize,
        string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        using var mmf = MemoryMappedFile.CreateFromFile(
            inputPath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        using var accessor = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);

        byte* basePtr = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
        basePtr += accessor.PointerOffset;

        try
        {
            var maxEntriesPerChunk = (int)Math.Min(
                _options.MaxMemoryPerChunk / sizeof(EntryIndex),
                int.MaxValue / 2);

            var previousLatency = GCSettings.LatencyMode;
            try
            {
                GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

                progress?.Report("Phase 1: indexing and sorting (memory-mapped)...");
                var chunkPaths = ScanSortAndWrite(
                    basePtr, fileSize, maxEntriesPerChunk, tempDir, progress, ct);

                GCSettings.LatencyMode = previousLatency;

                if (chunkPaths.Count == 0)
                {
                    using (File.Create(outputPath)) { }
                    return;
                }

                if (chunkPaths.Count == 1)
                {
                    BinaryChunkWriter.ConvertToText(chunkPaths[0], outputPath, _options.BufferSize);
                    progress?.Report("Done (single chunk, memory-mapped).");
                    return;
                }

                GC.Collect(2, GCCollectionMode.Optimized, blocking: false);

                progress?.Report($"Phase 2: merging {chunkPaths.Count} sorted chunks...");
                var merger = new ChunkMerger(
                    _options.BufferSize,
                    _options.MergeWidth,
                    path => new BinaryChunkReader(path, _options.BufferSize),
                    path => new BinaryRawChunkReader(path, _options.BufferSize));
                merger.MergeAll(chunkPaths, outputPath, tempDir, progress, ct);
                progress?.Report("Done.");
            }
            finally
            {
                GCSettings.LatencyMode = previousLatency;
            }
        }
        finally
        {
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    // ------------------------------------------------------------------
    //  Phase 1: Scan → Index → Sort → Write binary chunks
    // ------------------------------------------------------------------

    private unsafe List<string> ScanSortAndWrite(
        byte* basePtr, long fileSize, int maxEntriesPerChunk,
        string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        var chunkPaths = new List<string>();
        // MMF keeps one chunk in memory at a time (the mapped view), so the whole
        // parallelism budget goes to intra-chunk segmentation — no outer worker fan-out.
        var parallelism = Math.Max(2, _options.MaxDegreeOfParallelism);
        long pos = 0;
        var chunkIndex = 0;

        // Skip UTF-8 BOM if present
        if (fileSize >= 3 && basePtr[0] == 0xEF && basePtr[1] == 0xBB && basePtr[2] == 0xBF)
            pos = 3;

        while (pos < fileSize)
        {
            ct.ThrowIfCancellationRequested();

            using var entries = new NativeBuffer<EntryIndex>(
                Math.Min(maxEntriesPerChunk, 1024 * 1024));

            pos = ScanChunk(basePtr, pos, fileSize, entries, maxEntriesPerChunk, ct);

            if (entries.Count == 0)
                break;

            progress?.Report($"  chunk {chunkIndex}: {entries.Count:N0} lines indexed");

            SortEntries(entries, basePtr, parallelism);

            var chunkPath = Path.Combine(tempDir, $"chunk_{chunkIndex:D6}.bin");
            WriteBinaryChunk(chunkPath, entries, basePtr);
            chunkPaths.Add(chunkPath);
            chunkIndex++;
        }

        return chunkPaths;
    }

    /// <summary>
    /// Scans the memory-mapped file from <paramref name="startPos"/>, building
    /// <see cref="EntryIndex"/> entries in native memory until the chunk budget is reached.
    /// Returns the file position after the last consumed line.
    /// </summary>
    [SkipLocalsInit]
    private static unsafe long ScanChunk(
        byte* basePtr, long startPos, long fileSize,
        NativeBuffer<EntryIndex> entries, int maxEntries, CancellationToken ct)
    {
        var pos = startPos;

        while (pos < fileSize && entries.Count < maxEntries)
        {
            if ((entries.Count & 0xFFFF) == 0) // check every 64K lines
                ct.ThrowIfCancellationRequested();

            // Find end of line
            var lineEnd = FindNewline(basePtr, pos, fileSize);

            // Trim \r
            var lineLength = lineEnd - pos;
            if (lineLength > 0 && basePtr[lineEnd - 1] == (byte)'\r')
                lineLength--;

            if (lineLength > 0)
            {
                var entry = ParseEntry(basePtr, pos, (int)lineLength);
                entries.Add(entry);
            }

            pos = lineEnd + 1; // skip \n (or past EOF)
        }

        return pos;
    }

    /// <summary>
    /// Parses a single line at the given pointer offset into an <see cref="EntryIndex"/>.
    /// No managed allocations — all data stays as byte offsets into the file.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    private static unsafe EntryIndex ParseEntry(byte* basePtr, long lineStart, int lineLength)
    {
        var linePtr = basePtr + lineStart;

        // Find the '.' separator — number part is digits only, so first '.' is ours
        int dotIdx = 0;
        while (dotIdx < lineLength && linePtr[dotIdx] != (byte)'.')
            dotIdx++;

        if (dotIdx < 1 || dotIdx + 1 >= lineLength || linePtr[dotIdx + 1] != (byte)' ')
            throw new FormatException("Invalid line format, expected '<Number>. <Text>'");

        // Parse number from ASCII digits (no allocation)
        if (!Utf8Parser.TryParse(new ReadOnlySpan<byte>(linePtr, dotIdx), out long number, out _))
            throw new FormatException("Failed to parse number from line");

        var textStart = lineStart + dotIdx + 2; // skip ". "
        var textLength = lineLength - dotIdx - 2;
        var textPtr = basePtr + textStart;

        var sortKey = EntryIndex.ComputeSortKey(textPtr, textLength);

        return new EntryIndex(number, textStart, textLength, sortKey);
    }

    /// <summary>
    /// Finds the next newline byte using vectorized search within 2 GB spans.
    /// For files larger than <see cref="int.MaxValue"/>, processes in blocks.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe long FindNewline(byte* basePtr, long start, long fileSize)
    {
        var remaining = fileSize - start;

        // Fast path: most lines are short, check within a single span
        if (remaining <= int.MaxValue)
        {
            var span = new ReadOnlySpan<byte>(basePtr + start, (int)remaining);
            var idx = span.IndexOfAny(NewlineByte);
            return idx >= 0 ? start + idx : fileSize;
        }

        // Large file: process in 1 GB blocks
        var pos = start;
        while (pos < fileSize)
        {
            var blockLen = (int)Math.Min(fileSize - pos, 1024L * 1024 * 1024);
            var span = new ReadOnlySpan<byte>(basePtr + pos, blockLen);
            var idx = span.IndexOfAny(NewlineByte);
            if (idx >= 0)
                return pos + idx;
            pos += blockLen;
        }

        return fileSize;
    }

    // ------------------------------------------------------------------
    //  Sort with file-pointer-based comparison
    // ------------------------------------------------------------------

    private static unsafe void SortEntries(
        NativeBuffer<EntryIndex> entries, byte* filePtr, int parallelism)
    {
        var count = entries.Count;
        if (count < 50_000)
        {
            entries.AsSpan().Sort((a, b) => CompareEntries(a, b, filePtr));
            return;
        }

        // No artificial cap — caller's budget is authoritative. See comment in ChunkSorter.
        var segCount = Math.Max(2, parallelism);
        var segSize = count / segCount;

        Parallel.For(0, segCount, new ParallelOptions { MaxDegreeOfParallelism = segCount }, i =>
        {
            var start = i * segSize;
            var len = (i == segCount - 1) ? count - start : segSize;
            entries.AsSpan(start, len).Sort((a, b) => CompareEntries(a, b, filePtr));
        });

        MergeSortedSegments(entries, count, segCount, segSize, filePtr);
    }

    /// <summary>
    /// Compares two <see cref="EntryIndex"/> entries using the memory-mapped file data.
    ///
    /// Fast path: 8-byte sort key resolves 90%+ of comparisons (single ulong compare).
    /// Slow path: reads text bytes from MMF via pointer for full ordinal comparison.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe int CompareEntries(EntryIndex a, EntryIndex b, byte* filePtr)
    {
        // Fast path: precomputed 8-byte prefix (covers 8 ASCII chars)
        if (a.SortKey != b.SortKey)
            return a.SortKey.CompareTo(b.SortKey);

        // Slow path: full byte-level ordinal comparison on MMF data
        var textA = new ReadOnlySpan<byte>(filePtr + a.TextOffset, a.TextLength);
        var textB = new ReadOnlySpan<byte>(filePtr + b.TextOffset, b.TextLength);
        var cmp = textA.SequenceCompareTo(textB);

        return cmp != 0 ? cmp : a.Number.CompareTo(b.Number);
    }

    /// <summary>
    /// K-way merge of sorted segments using a <see cref="PriorityQueue{TElement, TPriority}"/>
    /// with a custom comparer that reads text from the memory-mapped file.
    /// </summary>
    private static unsafe void MergeSortedSegments(
        NativeBuffer<EntryIndex> entries, int count, int segCount, int segSize, byte* filePtr)
    {
        using var merged = new NativeBuffer<EntryIndex>(count);
        var positions = new int[segCount];
        var limits = new int[segCount];

        // PriorityQueue with EntryIndex as priority — uses custom comparer
        // that reads text from the MMF pointer for full comparison.
        var comparer = Comparer<EntryIndex>.Create(
            (a, b) => CompareEntries(a, b, filePtr));
        var pq = new PriorityQueue<int, EntryIndex>(segCount, comparer);

        for (var i = 0; i < segCount; i++)
        {
            positions[i] = i * segSize;
            limits[i] = (i == segCount - 1) ? count : (i + 1) * segSize;
            pq.Enqueue(i, entries[positions[i]]);
        }

        while (pq.Count > 0)
        {
            pq.TryDequeue(out var seg, out var entry);
            merged.Add(entry);
            positions[seg]++;
            if (positions[seg] < limits[seg])
                pq.Enqueue(seg, entries[positions[seg]]);
        }

        merged.AsSpan().CopyTo(entries.AsSpan(0, count));
    }

    // ------------------------------------------------------------------
    //  Binary chunk writer (reads text bytes from MMF, no string alloc)
    // ------------------------------------------------------------------

    /// <summary>
    /// Writes a sorted chunk of <see cref="EntryIndex"/> entries to the binary format
    /// used by <see cref="BinaryChunkReader"/>. Text bytes are copied directly from
    /// the memory-mapped file — no string materialization needed.
    /// </summary>
    private unsafe void WriteBinaryChunk(
        string path, NativeBuffer<EntryIndex> entries, byte* filePtr)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(stream, Utf8NoBom, leaveOpen: false);

        for (var i = 0; i < entries.Count; i++)
        {
            ref readonly var entry = ref entries[i];
            bw.Write(entry.Number);

            // Write text as length-prefixed UTF-8 string (BinaryWriter.Write(string) format).
            // 7-bit encoded length + raw UTF-8 bytes — identical to BinaryWriter.Write(string).
            bw.Write7BitEncodedInt(entry.TextLength);
            bw.Write(new ReadOnlySpan<byte>(filePtr + entry.TextOffset, entry.TextLength));
        }
    }

    // ------------------------------------------------------------------
    //  Helpers
    // ------------------------------------------------------------------

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
}
