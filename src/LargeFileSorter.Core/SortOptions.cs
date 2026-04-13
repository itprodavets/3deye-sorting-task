namespace LargeFileSorter.Core;

public sealed class SortOptions
{
    /// <summary>
    /// Maximum memory budget per chunk (in bytes).
    /// Defaults to ~25% of available memory, capped at 2 GB.
    /// </summary>
    public long MaxMemoryPerChunk { get; init; } = GetDefaultChunkMemory();

    /// <summary>
    /// Maximum number of sorted files to merge in a single pass.
    /// If more chunks exist, multi-level merging is used.
    /// </summary>
    public int MergeWidth { get; init; } = 64;

    /// <summary>
    /// Directory for temporary chunk files. Uses system temp if not specified.
    /// </summary>
    public string? TempDirectory { get; init; }

    /// <summary>
    /// I/O buffer size for readers and writers.
    /// </summary>
    public int BufferSize { get; init; } = 64 * 1024;

    private static long GetDefaultChunkMemory()
    {
        try
        {
            var totalMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
            return Math.Min(totalMemory / 4, 2L * 1024 * 1024 * 1024);
        }
        catch
        {
            return 256L * 1024 * 1024; // fallback: 256 MB
        }
    }
}
