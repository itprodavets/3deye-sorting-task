namespace LargeFileSorter.Core;

public sealed class SortOptions
{
    /// <summary>
    /// Maximum memory budget per chunk (in bytes).
    /// Default: auto-scales to the machine — 25% of available RAM,
    /// capped proportionally (1 GB on 4 GB machine, up to 8 GB on 64 GB+).
    /// </summary>
    public long MaxMemoryPerChunk { get; init; } = GetDefaultChunkMemory();

    /// <summary>
    /// Maximum number of sorted files to merge in a single pass.
    /// If more chunks exist, multi-level merging is used.
    /// </summary>
    public int MergeWidth { get; init; } = 64;

    /// <summary>
    /// Directory for temporary chunk files. Uses system temp if not specified.
    /// Point to a fast SSD (ideally NVMe) for best performance.
    /// </summary>
    public string? TempDirectory { get; init; }

    /// <summary>
    /// I/O buffer size for file streams and writers.
    /// Default: auto-scales from 1 MB (≤ 4 GB RAM) to 16 MB (≥ 64 GB RAM).
    /// Larger buffers reduce syscall overhead for sequential I/O on modern SSDs.
    /// </summary>
    public int BufferSize { get; init; } = GetDefaultBufferSize();

    /// <summary>
    /// Number of concurrent sort workers that process chunks in parallel.
    /// Each worker runs its own parallel intra-chunk sort + binary write.
    /// Default: auto-scales to half of available logical cores, clamped 1–4.
    /// Set lower to leave CPU headroom for other processes.
    /// </summary>
    public int SortWorkers { get; init; } = GetDefaultSortWorkers();

    /// <summary>
    /// Reports the auto-detected hardware profile for diagnostics.
    /// </summary>
    public static HardwareProfile DetectHardware() => new();

    private static long GetDefaultChunkMemory()
    {
        var totalMemory = GetTotalMemory();

        // Use 25% of available RAM, with a cap that scales with machine size.
        // Small machines get a lower cap to avoid swapping.
        var quarterRam = totalMemory / 4;
        var cap = totalMemory switch
        {
            >= 64L * 1024 * 1024 * 1024 => 8L * 1024 * 1024 * 1024,  // 64 GB+ → cap 8 GB
            >= 32L * 1024 * 1024 * 1024 => 4L * 1024 * 1024 * 1024,  // 32 GB+ → cap 4 GB
            >= 8L * 1024 * 1024 * 1024  => 2L * 1024 * 1024 * 1024,  // 8 GB+  → cap 2 GB
            _                           => 1L * 1024 * 1024 * 1024    // < 8 GB → cap 1 GB
        };

        return Math.Min(quarterRam, cap);
    }

    private static int GetDefaultBufferSize()
    {
        var totalMemory = GetTotalMemory();

        // Scale I/O buffers to available memory.
        // NVMe SSDs reach peak sequential throughput at 4–16 MB.
        // Smaller machines get 1 MB to avoid wasting limited RAM on buffers.
        return totalMemory switch
        {
            >= 64L * 1024 * 1024 * 1024 => 16 * 1024 * 1024,  // 64 GB+ → 16 MB
            >= 32L * 1024 * 1024 * 1024 => 8 * 1024 * 1024,   // 32 GB+ → 8 MB
            >= 16L * 1024 * 1024 * 1024 => 4 * 1024 * 1024,   // 16 GB+ → 4 MB
            >= 8L * 1024 * 1024 * 1024  => 2 * 1024 * 1024,   // 8 GB+  → 2 MB
            _                           => 1 * 1024 * 1024     // < 8 GB → 1 MB
        };
    }

    private static int GetDefaultSortWorkers()
        => Math.Clamp(Environment.ProcessorCount / 2, 1, 4);

    private static long GetTotalMemory()
    {
        try
        {
            return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        }
        catch
        {
            return 4L * 1024 * 1024 * 1024; // fallback: assume 4 GB
        }
    }
}

/// <summary>
/// Snapshot of auto-detected hardware used for diagnostics and tuning visibility.
/// </summary>
public sealed class HardwareProfile
{
    public long TotalMemoryBytes { get; }
    public int LogicalCores { get; }
    public long DefaultChunkMemory { get; }
    public int DefaultBufferSize { get; }
    public int SortWorkers { get; }

    internal HardwareProfile()
    {
        try
        {
            TotalMemoryBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
        }
        catch
        {
            TotalMemoryBytes = 4L * 1024 * 1024 * 1024;
        }

        LogicalCores = Environment.ProcessorCount;

        var opts = new SortOptions();
        DefaultChunkMemory = opts.MaxMemoryPerChunk;
        DefaultBufferSize = opts.BufferSize;
        SortWorkers = opts.SortWorkers;
    }

    public override string ToString()
    {
        return $"RAM: {SizeFormatter.Format(TotalMemoryBytes)}, " +
               $"Cores: {LogicalCores}, " +
               $"Chunk budget: {SizeFormatter.Format(DefaultChunkMemory)}, " +
               $"I/O buffer: {SizeFormatter.Format(DefaultBufferSize)}, " +
               $"Sort workers: {SortWorkers}";
    }
}
