using System.Buffers.Text;
using System.Text;

namespace LargeFileSorter.Core;

public static class LineParser
{
    private const string Separator = ". ";
    private static readonly byte[] SeparatorUtf8 = ". "u8.ToArray();

    public static LineEntry Parse(string line)
    {
        var dotIndex = line.IndexOf(Separator, StringComparison.Ordinal);
        if (dotIndex < 1)
            throw new FormatException($"Invalid line format, expected '<Number>. <Text>': \"{Truncate(line)}\"");

        if (!long.TryParse(line.AsSpan(0, dotIndex), out var number))
            throw new FormatException($"Failed to parse number from: \"{Truncate(line)}\"");

        var text = line[(dotIndex + Separator.Length)..];
        return new LineEntry(number, text);
    }

    public static LineEntry Parse(ReadOnlySpan<char> line)
    {
        var dotIndex = line.IndexOf(Separator.AsSpan(), StringComparison.Ordinal);
        if (dotIndex < 1)
            throw new FormatException("Invalid line format, expected '<Number>. <Text>'");

        if (!long.TryParse(line[..dotIndex], out var number))
            throw new FormatException("Failed to parse number from line");

        var text = line[(dotIndex + Separator.Length)..].ToString();
        return new LineEntry(number, text);
    }

    /// <summary>
    /// Parses a line directly from UTF-8 bytes, avoiding the intermediate string allocation
    /// for the full line. Only the Text part is materialized as a managed string.
    /// Used by the PipeReader-based input path.
    /// </summary>
    public static LineEntry ParseUtf8(ReadOnlySpan<byte> utf8Line)
    {
        var sepIdx = utf8Line.IndexOf(SeparatorUtf8);
        if (sepIdx < 1)
            throw new FormatException("Invalid line format, expected '<Number>. <Text>'");

        if (!Utf8Parser.TryParse(utf8Line[..sepIdx], out long number, out _))
            throw new FormatException("Failed to parse number from UTF-8 line");

        var text = Encoding.UTF8.GetString(utf8Line[(sepIdx + SeparatorUtf8.Length)..]);
        return new LineEntry(number, text);
    }

    /// <summary>
    /// Parses only the number from UTF-8 bytes and returns the byte offset where text begins.
    /// Does NOT allocate a string — caller decides when and how to materialize the text.
    /// Used together with <see cref="TextPool"/> for zero-allocation deduplication.
    /// </summary>
    public static long ParseNumberUtf8(ReadOnlySpan<byte> utf8Line, out int textStartOffset)
    {
        var sepIdx = utf8Line.IndexOf(SeparatorUtf8);
        if (sepIdx < 1)
            throw new FormatException("Invalid line format, expected '<Number>. <Text>'");

        if (!Utf8Parser.TryParse(utf8Line[..sepIdx], out long number, out _))
            throw new FormatException("Failed to parse number from UTF-8 line");

        textStartOffset = sepIdx + SeparatorUtf8.Length;
        return number;
    }

    public static string Format(in LineEntry entry) => entry.ToString();

    private static string Truncate(string value, int maxLength = 80)
        => value.Length <= maxLength ? value : string.Concat(value.AsSpan(0, maxLength), "...");
}
