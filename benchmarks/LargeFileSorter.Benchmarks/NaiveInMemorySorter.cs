using System.Text;
using LargeFileSorter.Core;

namespace LargeFileSorter.Benchmarks;

/// <summary>
/// Naive approach: reads the entire file into memory, sorts using Array.Sort, writes output.
///
/// Pros:
///   - Simplest implementation
///   - Fast for small files (no chunk/merge overhead)
///   - Single pass — no temp files needed
///
/// Cons:
///   - Memory usage = O(file size) — impossible for files larger than available RAM
///   - Will throw OutOfMemoryException on ~100 GB inputs
///   - GC pressure from large arrays on the LOH
///   - No concurrency — CPU idle during I/O
/// </summary>
public static class NaiveInMemorySorter
{
    public static void Sort(string inputPath, string outputPath)
    {
        var lines = File.ReadAllLines(inputPath);
        var entries = new LineEntry[lines.Length];

        for (var i = 0; i < lines.Length; i++)
            entries[i] = LineParser.Parse(lines[i]);

        Array.Sort(entries);

        using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, 65536, FileOptions.SequentialScan);
        using var writer = new StreamWriter(stream, Encoding.UTF8, 65536);

        foreach (ref readonly var entry in entries.AsSpan())
        {
            writer.Write(entry.Number);
            writer.Write(". ");
            writer.WriteLine(entry.Text);
        }
    }
}
