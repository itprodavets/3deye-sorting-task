using System.Text;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

/// <summary>
/// Basic external merge sort without concurrency or parallel sorting.
///
/// Pros:
///   - Can handle files larger than RAM (external sort)
///   - Simple, sequential code — easy to reason about
///   - Predictable memory usage
///
/// Cons:
///   - No pipelining — CPU is idle while reading/writing, disk is idle while sorting
///   - Single-threaded sort — doesn't use multi-core CPUs
///   - Per-line async overhead (ReadLineAsync state machine per line)
///   - No ArrayPool — new array allocation per chunk
///   - No buffered merge — one I/O call per line during merge phase
///   - ToString() allocation per line during writes
/// </summary>
public sealed class SequentialExternalSorter
{
    private readonly long _maxChunkBytes;
    private readonly string? _tempDirectory;
    private const int IoBuffer = 65536;

    public SequentialExternalSorter(long maxChunkBytes, string? tempDirectory = null)
    {
        _maxChunkBytes = maxChunkBytes;
        _tempDirectory = tempDirectory;
    }

    public void Sort(string inputPath, string outputPath)
    {
        var tempDir = Path.Combine(
            _tempDirectory ?? Path.GetTempPath(),
            $"seqsort_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var chunkFiles = SplitAndSort(inputPath, tempDir);

            if (chunkFiles.Count == 0)
            {
                File.Create(outputPath).Dispose();
                return;
            }

            if (chunkFiles.Count == 1)
            {
                File.Move(chunkFiles[0], outputPath, overwrite: true);
                return;
            }

            KWayMerge(chunkFiles, outputPath);
        }
        finally
        {
            try { Directory.Delete(tempDir, true); } catch { }
        }
    }

    private List<string> SplitAndSort(string inputPath, string tempDir)
    {
        var chunkFiles = new List<string>();
        var chunk = new List<LineEntry>();
        long estimated = 0;

        // No pipelining: read → sort → write → read → sort → write → ...
        using var reader = new StreamReader(inputPath, Encoding.UTF8, true, IoBuffer);

        while (reader.ReadLine() is { } line)
        {
            var entry = LineParser.Parse(line);
            chunk.Add(entry);
            estimated += 24 + 40 + entry.Text.Length * 2;

            if (estimated >= _maxChunkBytes)
            {
                chunkFiles.Add(SortAndWrite(chunk, tempDir, chunkFiles.Count));
                chunk = new List<LineEntry>();
                estimated = 0;
            }
        }

        if (chunk.Count > 0)
            chunkFiles.Add(SortAndWrite(chunk, tempDir, chunkFiles.Count));

        return chunkFiles;
    }

    private static string SortAndWrite(List<LineEntry> chunk, string tempDir, int index)
    {
        // Single-threaded sort — no Parallel.For
        var array = chunk.ToArray();
        Array.Sort(array);

        var path = Path.Combine(tempDir, $"chunk_{index:D6}.txt");

        // ToString() allocation per line
        using var writer = new StreamWriter(path, false, Encoding.UTF8, IoBuffer);
        foreach (var entry in array)
            writer.WriteLine(entry.ToString());

        return path;
    }

    private static void KWayMerge(List<string> chunkFiles, string outputPath)
    {
        var readers = new StreamReader[chunkFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>();

            for (var i = 0; i < chunkFiles.Count; i++)
            {
                readers[i] = new StreamReader(chunkFiles[i], Encoding.UTF8, false, IoBuffer);
                var line = readers[i].ReadLine();
                if (line != null)
                    pq.Enqueue(i, LineParser.Parse(line));
            }

            // No buffering — one ReadLine per dequeue
            using var writer = new StreamWriter(outputPath, false, Encoding.UTF8, IoBuffer);

            while (pq.Count > 0)
            {
                pq.TryDequeue(out var idx, out var entry);
                writer.WriteLine(entry.ToString());

                var next = readers[idx].ReadLine();
                if (next != null)
                    pq.Enqueue(idx, LineParser.Parse(next));
            }
        }
        finally
        {
            foreach (var r in readers)
                r?.Dispose();
        }
    }
}
