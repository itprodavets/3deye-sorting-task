using System.Buffers;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Text;

namespace LargeFileSorter.Core;

public static class LineParser
{
    private const string Separator = ". ";

    // SIMD-accelerated byte search (.NET 8+). The number part is digits only,
    // so the first '.' in the line is always our separator dot.
    // SearchValues selects the optimal SIMD path per platform (SSE2/AVX2/AVX-512).
    private static readonly SearchValues<byte> DotByte = SearchValues.Create("."u8);

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
    /// Uses <see cref="SearchValues{T}"/> for SIMD-accelerated separator detection.
    /// </summary>
    [SkipLocalsInit]
    public static LineEntry ParseUtf8(ReadOnlySpan<byte> utf8Line)
    {
        var dotIdx = FindSeparatorDot(utf8Line);

        if (!Utf8Parser.TryParse(utf8Line[..dotIdx], out long number, out _))
            throw new FormatException("Failed to parse number from UTF-8 line");

        var text = Encoding.UTF8.GetString(utf8Line[(dotIdx + 2)..]); // skip ". "
        return new LineEntry(number, text);
    }

    /// <summary>
    /// Parses only the number from UTF-8 bytes and returns the byte offset where text begins.
    /// Does NOT allocate a string — caller decides when and how to materialize the text.
    /// Uses <see cref="SearchValues{T}"/> for SIMD-accelerated separator detection.
    /// </summary>
    [SkipLocalsInit]
    public static long ParseNumberUtf8(ReadOnlySpan<byte> utf8Line, out int textStartOffset)
    {
        var dotIdx = FindSeparatorDot(utf8Line);

        if (!Utf8Parser.TryParse(utf8Line[..dotIdx], out long number, out _))
            throw new FormatException("Failed to parse number from UTF-8 line");

        textStartOffset = dotIdx + 2; // skip ". "
        return number;
    }

    /// <summary>
    /// Finds the separator dot using SIMD-accelerated <see cref="SearchValues{T}"/>.
    /// The number part is digits only, so the first '.' is always the separator.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FindSeparatorDot(ReadOnlySpan<byte> utf8Line)
    {
        var dotIdx = utf8Line.IndexOfAny(DotByte);
        if (dotIdx < 1 || dotIdx + 1 >= utf8Line.Length || utf8Line[dotIdx + 1] != (byte)' ')
            throw new FormatException("Invalid line format, expected '<Number>. <Text>'");
        return dotIdx;
    }

    public static string Format(in LineEntry entry) => entry.ToString();

    private static string Truncate(string value, int maxLength = 80)
        => value.Length <= maxLength ? value : string.Concat(value.AsSpan(0, maxLength), "...");
}
