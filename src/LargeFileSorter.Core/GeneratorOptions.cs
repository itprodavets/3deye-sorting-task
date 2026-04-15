namespace LargeFileSorter.Core;

/// <summary>
/// Runtime configuration for the file generator.
/// Small, immutable, passed by value at startup — a <c>readonly record struct</c>
/// avoids the heap allocation of a class for no behavioural loss.
/// </summary>
public readonly record struct GeneratorOptions
{
    /// <summary>
    /// Parameterless constructor so property initializers run for <c>new GeneratorOptions()</c>
    /// and object initializers. <c>default(GeneratorOptions)</c> still produces zeros,
    /// which is the standard struct footgun — always prefer <c>new GeneratorOptions()</c>.
    /// </summary>
    public GeneratorOptions() { }

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
    public int? Seed { get; init; } = null;

    /// <summary>
    /// Write buffer size in bytes.
    /// </summary>
    public int BufferSize { get; init; } = 64 * 1024;
}
