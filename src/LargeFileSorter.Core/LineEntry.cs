namespace LargeFileSorter.Core;

/// <summary>
/// Represents a single line from the input file in format: {Number}. {Text}
/// </summary>
public readonly record struct LineEntry(long Number, string Text) : IComparable<LineEntry>
{
    public int CompareTo(LineEntry other)
    {
        var cmp = string.Compare(Text, other.Text, StringComparison.Ordinal);
        return cmp != 0 ? cmp : Number.CompareTo(other.Number);
    }

    public override string ToString() => $"{Number}. {Text}";
}
