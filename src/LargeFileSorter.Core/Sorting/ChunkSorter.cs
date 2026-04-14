using System.Runtime.CompilerServices;

namespace LargeFileSorter.Core;

/// <summary>
/// Sorts a chunk of LineEntry records in-place.
/// Uses parallel segment sorting + k-way merge for large chunks
/// to take advantage of multiple CPU cores.
///
/// Uses <see cref="MemoryExtensions.Sort{T}(Span{T})"/> instead of
/// <see cref="Array.Sort{T}(T[], int, int)"/> — Span-based sort avoids
/// array bounds validation overhead on every call.
/// </summary>
[SkipLocalsInit]
internal static class ChunkSorter
{
    private const int ParallelThreshold = 50_000;

    /// <param name="data">Array of entries to sort.</param>
    /// <param name="count">Number of valid entries in the array.</param>
    /// <param name="maxParallelism">
    ///   Maximum number of parallel segments to sort simultaneously.
    ///   Defaults to <see cref="Environment.ProcessorCount"/> when &lt;= 0.
    ///   Pass a lower value when multiple sort workers run concurrently
    ///   to avoid over-subscribing CPU cores.
    /// </param>
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
}
