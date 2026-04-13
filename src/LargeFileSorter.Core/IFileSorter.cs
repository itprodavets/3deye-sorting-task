namespace LargeFileSorter.Core;

/// <summary>
/// Sorts a text file where each line follows the format: {Number}. {Text}.
/// Primary sort by Text (alphabetically), secondary by Number (ascending).
/// </summary>
public interface IFileSorter
{
    Task SortAsync(string inputPath, string outputPath,
        IProgress<string>? progress = null, CancellationToken ct = default);
}
