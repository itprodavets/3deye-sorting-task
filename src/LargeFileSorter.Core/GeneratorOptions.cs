namespace LargeFileSorter.Core;

public sealed class GeneratorOptions
{
    /// <summary>
    /// Number of unique phrases to generate. Lines will randomly pick from this pool,
    /// ensuring duplicates appear naturally.
    /// </summary>
    public int UniquePhraseCount { get; init; } = 500;

    /// <summary>
    /// Maximum number value for generated lines.
    /// </summary>
    public long MaxNumber { get; init; } = 100_000;

    /// <summary>
    /// Optional seed for reproducible output.
    /// </summary>
    public int? Seed { get; init; }

    /// <summary>
    /// Write buffer size in bytes.
    /// </summary>
    public int BufferSize { get; init; } = 64 * 1024;
}
