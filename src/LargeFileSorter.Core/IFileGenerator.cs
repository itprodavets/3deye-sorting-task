namespace LargeFileSorter.Core;

/// <summary>
/// Generates test files in the format: {Number}. {Text}, one entry per line.
/// </summary>
public interface IFileGenerator
{
    Task GenerateAsync(string outputPath, long targetSizeBytes,
        IProgress<long>? progress = null, CancellationToken ct = default);
}
