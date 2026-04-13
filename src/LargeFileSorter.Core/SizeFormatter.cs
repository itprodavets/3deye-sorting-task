namespace LargeFileSorter.Core;

/// <summary>
/// Converts byte counts to human-readable size strings (KB, MB, GB).
/// Centralizes formatting logic used by diagnostics, progress reporting, and error messages.
/// </summary>
public static class SizeFormatter
{
    public static string Format(long bytes) => bytes switch
    {
        >= 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024 * 1024):F1} GB",
        >= 1024L * 1024 => $"{bytes / (1024.0 * 1024):F0} MB",
        >= 1024L => $"{bytes / 1024.0:F0} KB",
        _ => $"{bytes} B"
    };
}
