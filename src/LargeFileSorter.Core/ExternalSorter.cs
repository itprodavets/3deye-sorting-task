using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// External merge sort for files that don't fit in memory.
///
/// Algorithm:
///   1. Split: read input in chunks, sort each chunk in memory, write to temp files.
///   2. Merge: k-way merge of sorted chunks using a priority queue.
///      If chunk count exceeds <see cref="SortOptions.MergeWidth"/>, multiple merge passes are applied.
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
                // empty input — create empty output
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
            await MergeAllAsync(chunkFiles, outputPath, tempDir, progress, ct);
            progress?.Report("Done.");
        }
        finally
        {
            TryDeleteDirectory(tempDir);
        }
    }

    // ---------------------------------------------------------------
    //  Phase 1 — split input into sorted chunks
    // ---------------------------------------------------------------

    private async Task<List<string>> SplitAndSortAsync(
        string inputPath, string tempDir, IProgress<string>? progress, CancellationToken ct)
    {
        var chunkFiles = new List<string>();
        var chunk = new List<LineEntry>();
        var chunkIndex = 0;
        long estimatedBytes = 0;

        // String pool to deduplicate text within a chunk — saves memory when
        // many lines share the same text part (which is expected by design).
        var stringPool = new Dictionary<string, string>(StringComparer.Ordinal);

        await using var fileStream = new FileStream(
            inputPath, FileMode.Open, FileAccess.Read, FileShare.Read,
            _options.BufferSize, FileOptions.SequentialScan);
        using var reader = new StreamReader(fileStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true,
            bufferSize: _options.BufferSize);

        // We sort the previous chunk on a background thread while reading
        // the next chunk from disk — overlapping I/O and CPU.
        Task<string>? pendingSortTask = null;

        while (await reader.ReadLineAsync(ct) is { } line)
        {
            ct.ThrowIfCancellationRequested();

            var entry = LineParser.Parse(line);

            // Deduplicate the text part via pool
            if (stringPool.TryGetValue(entry.Text, out var pooled))
            {
                entry = new LineEntry(entry.Number, pooled);
            }
            else
            {
                stringPool[entry.Text] = entry.Text;
            }

            chunk.Add(entry);
            // Rough memory estimate: struct overhead + string object + chars
            estimatedBytes += 24 + 40 + entry.Text.Length * 2;

            if (estimatedBytes >= _options.MaxMemoryPerChunk)
            {
                // Wait for previous sort/write to finish before reusing chunk list
                if (pendingSortTask != null)
                    chunkFiles.Add(await pendingSortTask);

                var chunkToSort = chunk;
                var idx = chunkIndex;
                pendingSortTask = Task.Run(() => SortAndWriteChunk(chunkToSort, tempDir, idx, ct), ct);

                progress?.Report($"  chunk {chunkIndex}: {chunk.Count:N0} lines queued for sorting");
                chunk = new List<LineEntry>();
                stringPool.Clear();
                estimatedBytes = 0;
                chunkIndex++;
            }
        }

        // Flush remaining lines
        if (chunk.Count > 0)
        {
            if (pendingSortTask != null)
                chunkFiles.Add(await pendingSortTask);

            var path = await Task.Run(() => SortAndWriteChunk(chunk, tempDir, chunkIndex, ct), ct);
            chunkFiles.Add(path);
            progress?.Report($"  chunk {chunkIndex}: {chunk.Count:N0} lines (final)");
        }
        else if (pendingSortTask != null)
        {
            chunkFiles.Add(await pendingSortTask);
        }

        return chunkFiles;
    }

    private string SortAndWriteChunk(List<LineEntry> chunk, string tempDir, int index, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        // Array.Sort uses IntroSort — O(n log n) worst case
        var array = chunk.ToArray();
        Array.Sort(array);

        var chunkPath = Path.Combine(tempDir, $"chunk_{index:D6}.txt");

        using var stream = new FileStream(chunkPath, FileMode.Create, FileAccess.Write,
            FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
        using var writer = new StreamWriter(stream, Encoding.UTF8, _options.BufferSize);

        foreach (var entry in array)
        {
            ct.ThrowIfCancellationRequested();
            writer.WriteLine(entry.ToString());
        }

        writer.Flush();
        return chunkPath;
    }

    // ---------------------------------------------------------------
    //  Phase 2 — k-way merge (multi-level if needed)
    // ---------------------------------------------------------------

    private async Task MergeAllAsync(
        List<string> chunkFiles, string outputPath, string tempDir,
        IProgress<string>? progress, CancellationToken ct)
    {
        var currentFiles = chunkFiles;
        var level = 0;

        // Multi-level merge: if we have more chunks than MergeWidth,
        // merge in groups to avoid opening too many file handles at once.
        while (currentFiles.Count > _options.MergeWidth)
        {
            var nextLevel = new List<string>();
            var groups = Partition(currentFiles, _options.MergeWidth);

            for (var g = 0; g < groups.Count; g++)
            {
                ct.ThrowIfCancellationRequested();
                var mergedPath = Path.Combine(tempDir, $"merge_L{level}_{g:D4}.txt");
                await KWayMergeAsync(groups[g], mergedPath, ct);
                nextLevel.Add(mergedPath);

                // Clean up intermediate files
                foreach (var f in groups[g])
                    File.Delete(f);
            }

            progress?.Report($"  merge level {level}: {currentFiles.Count} -> {nextLevel.Count} files");
            currentFiles = nextLevel;
            level++;
        }

        await KWayMergeAsync(currentFiles, outputPath, ct);
    }

    private async Task KWayMergeAsync(List<string> inputFiles, string outputPath, CancellationToken ct)
    {
        var readers = new StreamReader[inputFiles.Count];
        try
        {
            var pq = new PriorityQueue<int, LineEntry>();

            for (var i = 0; i < inputFiles.Count; i++)
            {
                var stream = new FileStream(inputFiles[i], FileMode.Open, FileAccess.Read,
                    FileShare.Read, _options.BufferSize, FileOptions.SequentialScan);
                readers[i] = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false,
                    bufferSize: _options.BufferSize);

                var firstLine = await readers[i].ReadLineAsync(ct);
                if (firstLine != null)
                    pq.Enqueue(i, LineParser.Parse(firstLine));
            }

            await using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
            await using var writer = new StreamWriter(outStream, Encoding.UTF8, _options.BufferSize);

            while (pq.Count > 0)
            {
                ct.ThrowIfCancellationRequested();

                pq.TryDequeue(out var readerIdx, out var entry);
                await writer.WriteLineAsync(entry.ToString().AsMemory(), ct);

                var nextLine = await readers[readerIdx].ReadLineAsync(ct);
                if (nextLine != null)
                    pq.Enqueue(readerIdx, LineParser.Parse(nextLine));
            }
        }
        finally
        {
            foreach (var r in readers)
                r?.Dispose();
        }
    }

    // ---------------------------------------------------------------
    //  Helpers
    // ---------------------------------------------------------------

    private static List<List<string>> Partition(List<string> items, int groupSize)
    {
        var result = new List<List<string>>();
        for (var i = 0; i < items.Count; i += groupSize)
        {
            var count = Math.Min(groupSize, items.Count - i);
            result.Add(items.GetRange(i, count));
        }
        return result;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // best-effort cleanup
        }
    }
}
