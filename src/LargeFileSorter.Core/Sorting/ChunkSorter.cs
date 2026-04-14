using System.Runtime.CompilerServices;
using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// Sorts a chunk of LineEntry records in-place and writes directly to a binary file.
///
/// Uses parallel segment sorting + streaming k-way merge for large chunks
/// to take advantage of multiple CPU cores while avoiding a full-copy merge buffer.
///
/// Sort strategy:
///   - Small chunks (&lt; 50K entries): <see cref="Span{T}.Sort()"/> in-place, then sequential write.
///   - Large chunks: split into segments, sort each via <see cref="Parallel.For"/>,
///     then k-way merge directly into a <see cref="BinaryWriter"/> — no temporary
///     merge buffer, no copy-back. Eliminates ~8 GB allocation at 100 GB scale.
/// </summary>
[SkipLocalsInit]
internal static class ChunkSorter
{
    private const int ParallelThreshold = 50_000;
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    /// <summary>
    /// Sorts the chunk in-place. Used for testing and scenarios where
    /// the caller needs the sorted data in memory (e.g., verification).
    /// For production chunk processing, prefer <see cref="SortAndWrite"/>
    /// which streams the merge directly to disk.
    /// </summary>
    public static void Sort(LineEntry[] data, int count, int maxParallelism = -1)
    {
        if (count < ParallelThreshold)
        {
            data.AsSpan(0, count).Sort();
            return;
        }

        var segCount = maxParallelism > 0
            ? Math.Clamp(maxParallelism, 2, 8)
            : Math.Clamp(Environment.ProcessorCount, 2, 8);
        var segSize = count / segCount;

        Parallel.For(0, segCount, new ParallelOptions { MaxDegreeOfParallelism = segCount }, i =>
        {
            var start = i * segSize;
            var len = (i == segCount - 1) ? count - start : segSize;
            data.AsSpan(start, len).Sort();
        });

        MergeSortedSegmentsInPlace(data, count, segCount, segSize);
    }

    /// <summary>
    /// Sorts the chunk and writes the result directly to a binary chunk file.
    /// For parallel-sorted chunks, the k-way merge streams entries to the file
    /// without allocating a merge buffer — peak memory stays at 1× chunk size.
    /// </summary>
    /// <param name="data">Array of entries to sort.</param>
    /// <param name="count">Number of valid entries in the array.</param>
    /// <param name="maxParallelism">
    ///   Maximum number of parallel segments to sort simultaneously.
    ///   Pass a lower value when multiple sort workers run concurrently
    ///   to avoid over-subscribing CPU cores.
    /// </param>
    /// <param name="outputPath">Path to the binary chunk file to write.</param>
    /// <param name="bufferSize">I/O buffer size for the output file stream.</param>
    public static void SortAndWrite(LineEntry[] data, int count, int maxParallelism,
        string outputPath, int bufferSize)
    {
        if (count < ParallelThreshold)
        {
            data.AsSpan(0, count).Sort();
            WriteSequential(data, count, outputPath, bufferSize);
            return;
        }

        var segCount = maxParallelism > 0
            ? Math.Clamp(maxParallelism, 2, 8)
            : Math.Clamp(Environment.ProcessorCount, 2, 8);
        var segSize = count / segCount;

        Parallel.For(0, segCount, new ParallelOptions { MaxDegreeOfParallelism = segCount }, i =>
        {
            var start = i * segSize;
            var len = (i == segCount - 1) ? count - start : segSize;
            data.AsSpan(start, len).Sort();
        });

        MergeAndWrite(data, count, segCount, segSize, outputPath, bufferSize);
    }

    /// <summary>
    /// In-place k-way merge for <see cref="Sort"/> — allocates a temporary buffer.
    /// Only used in tests and verification; production path uses <see cref="MergeAndWrite"/>.
    /// </summary>
    private static void MergeSortedSegmentsInPlace(
        LineEntry[] data, int count, int segCount, int segSize)
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
    /// Sequential write for small or already-sorted chunks.
    /// </summary>
    private static void WriteSequential(LineEntry[] data, int count,
        string outputPath, int bufferSize)
    {
        using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(stream, Utf8NoBom, leaveOpen: false);

        for (var i = 0; i < count; i++)
        {
            bw.Write(data[i].Number);
            bw.Write(data[i].Text);
        }
    }

    /// <summary>
    /// K-way merge of sorted segments, streaming directly into the binary file.
    /// No temporary merge buffer — peak memory is 1× the chunk array only.
    /// At 100 GB scale (250M entries per chunk), this saves ~6-8 GB per sort worker.
    /// </summary>
    private static void MergeAndWrite(LineEntry[] data, int count, int segCount, int segSize,
        string outputPath, int bufferSize)
    {
        using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(stream, Utf8NoBom, leaveOpen: false);

        var positions = new int[segCount];
        var limits = new int[segCount];
        var pq = new PriorityQueue<int, LineEntry>(segCount);

        for (var i = 0; i < segCount; i++)
        {
            positions[i] = i * segSize;
            limits[i] = (i == segCount - 1) ? count : (i + 1) * segSize;
            pq.Enqueue(i, data[positions[i]]);
        }

        while (pq.Count > 0)
        {
            pq.TryDequeue(out var seg, out var entry);
            bw.Write(entry.Number);
            bw.Write(entry.Text);
            positions[seg]++;
            if (positions[seg] < limits[seg])
                pq.Enqueue(seg, data[positions[seg]]);
        }
    }
}
