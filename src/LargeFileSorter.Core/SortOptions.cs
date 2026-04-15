namespace LargeFileSorter.Core;

/// <summary>
/// Runtime configuration for the sorter.
///
/// Declared as a <see langword="readonly record struct"/> rather than a class —
/// config is small (≈ 32 bytes), immutable, and passed by value a handful of times
/// at startup. Value-type semantics avoid a GC allocation per construction and
/// let the JIT inline field reads inside sort workers.
/// </summary>
public readonly record struct SortOptions
{
    /// <summary>
    /// Parameterless constructor is required so that property initializers run
    /// when the struct is created via <c>new SortOptions()</c> or an object
    /// initializer. Without it, <c>default(SortOptions)</c> would bypass
    /// the initializers and produce zero values.
    /// </summary>
    public SortOptions() { }

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
    public string? TempDirectory { get; init; } = null;

    /// <summary>
    /// I/O buffer size for file streams and writers.
    /// Default: auto-scales from 1 MB (≤ 4 GB RAM) to 16 MB (≥ 64 GB RAM).
    /// Larger buffers reduce syscall overhead for sequential I/O on modern SSDs.
    /// </summary>
    public int BufferSize { get; init; } = GetDefaultBufferSize();

    /// <summary>
    /// Number of concurrent sort workers that process chunks in parallel.
    /// Each worker runs its own parallel intra-chunk sort + binary write.
    /// Default: memory-bounded (roughly ½ RAM / chunk budget), never exceeding ½ cores.
    /// Set lower to leave CPU headroom for other processes.
    /// </summary>
    public int SortWorkers { get; init; } = GetDefaultSortWorkers();

    /// <summary>
    /// Total CPU parallelism budget shared across all sort work (workers × segments-per-chunk).
    /// Default: <see cref="Environment.ProcessorCount"/>. Expose via the CLI flag
    /// <c>--threads N</c> so every strategy (stream / mmf / shard) can be benchmarked under
    /// identical concurrency — no strategy is artificially throttled in comparisons.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

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
    {
        // Each worker holds roughly one chunk in memory (see MaxMemoryPerChunk —
        // 1 GB on small machines, up to 8 GB on 64 GB+). Use half of total RAM as
        // the worker budget so the OS, I/O buffers and TextPool have headroom;
        // the other half is the hard cap that protects small machines from OOM.
        var perWorker = Math.Max(1L, GetDefaultChunkMemory());
        var maxByMemory = (int)Math.Max(1, (GetTotalMemory() / 2) / perWorker);
        var halfCores = Math.Max(1, Environment.ProcessorCount / 2);
        return Math.Min(halfCores, maxByMemory);
    }

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
/// Value type — the profile is a small immutable record that's logged once at startup
/// and never mutated. Using a struct avoids a GC allocation for the snapshot.
/// </summary>
public readonly record struct HardwareProfile
{
    public long TotalMemoryBytes { get; }
    public int LogicalCores { get; }
    public long DefaultChunkMemory { get; }
    public int DefaultBufferSize { get; }
    public int SortWorkers { get; }
    public int MaxDegreeOfParallelism { get; }

    // Parameterless struct constructor must be public (C# rules for record struct).
    // Behaviourally equivalent to SortOptions.DetectHardware() — both paths produce
    // the same snapshot by reading Environment/GC.GetGCMemoryInfo().
    public HardwareProfile()
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
        MaxDegreeOfParallelism = opts.MaxDegreeOfParallelism;
    }

    public override string ToString()
    {
        return $"RAM: {SizeFormatter.Format(TotalMemoryBytes)}, " +
               $"Cores: {LogicalCores}, " +
               $"Chunk budget: {SizeFormatter.Format(DefaultChunkMemory)}, " +
               $"I/O buffer: {SizeFormatter.Format(DefaultBufferSize)}, " +
               $"Sort workers: {SortWorkers}, " +
               $"Parallelism: {MaxDegreeOfParallelism}";
    }
}
