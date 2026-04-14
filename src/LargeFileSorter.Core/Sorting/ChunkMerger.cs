using System.Text;

namespace LargeFileSorter.Core;

// Cached encoding instance — avoids allocating a new UTF8Encoding(false) per reader/writer.
// Each BinaryReader/BinaryWriter was creating its own; this eliminates ~6 allocations per sort.

/// <summary>
/// K-way merge for sorted binary chunk files.
/// Supports multi-level merging when the number of chunks exceeds the merge width.
/// Depends on <see cref="IChunkReader"/> via a factory delegate (DIP).
/// </summary>
internal sealed class ChunkMerger
{
    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    private readonly int _bufferSize;
    private readonly int _mergeWidth;
    private readonly Func<string, IChunkReader> _readerFactory;

    public ChunkMerger(int bufferSize, int mergeWidth, Func<string, IChunkReader> readerFactory)
    {
        _bufferSize = bufferSize;
        _mergeWidth = mergeWidth;
        _readerFactory = readerFactory;
    }

    public void MergeAll(
        List<string> chunkFiles, string outputPath, string tempDir,
        IProgress<string>? progress, CancellationToken ct)
    {
        var current = chunkFiles;
        var level = 0;

        while (current.Count > _mergeWidth)
        {
            var next = new List<string>();
            var groups = Partition(current, _mergeWidth);

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
        var readers = new IChunkReader[inputFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>(inputFiles.Count);
            for (var i = 0; i < inputFiles.Count; i++)
            {
                readers[i] = _readerFactory(inputFiles[i]);
                if (readers[i].HasCurrent)
                    pq.Enqueue(i, readers[i].Current);
            }

            using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                FileShare.None, _bufferSize, FileOptions.SequentialScan);
            using var bw = new BinaryWriter(outStream, Utf8NoBom, leaveOpen: false);

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
        var readers = new IChunkReader[inputFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>(inputFiles.Count);
            for (var i = 0; i < inputFiles.Count; i++)
            {
                readers[i] = _readerFactory(inputFiles[i]);
                if (readers[i].HasCurrent)
                    pq.Enqueue(i, readers[i].Current);
            }

            using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                FileShare.None, _bufferSize, FileOptions.SequentialScan);

            // Direct UTF-8 output: Utf8Formatter for numbers, bulk buffer flush.
            // Avoids StreamWriter's per-write char→byte encoding overhead.
            using var writer = new Utf8LineWriter(outStream, _bufferSize);

            while (pq.Count > 0)
            {
                ct.ThrowIfCancellationRequested();
                pq.TryDequeue(out var readerIdx, out var entry);
                writer.WriteEntry(entry);
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

    private static List<List<string>> Partition(List<string> items, int groupSize)
    {
        var result = new List<List<string>>();
        for (var i = 0; i < items.Count; i += groupSize)
            result.Add(items.GetRange(i, Math.Min(groupSize, items.Count - i)));
        return result;
    }
}
