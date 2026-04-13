namespace LargeFileSorter.Core;

public static class LineParser
{
    private const string Separator = ". ";

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

    public static string Format(in LineEntry entry) => entry.ToString();

    private static string Truncate(string value, int maxLength = 80)
        => value.Length <= maxLength ? value : string.Concat(value.AsSpan(0, maxLength), "...");
}
