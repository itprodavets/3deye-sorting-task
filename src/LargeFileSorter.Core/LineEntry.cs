using System.Runtime.CompilerServices;

namespace LargeFileSorter.Core;

/// <summary>
/// Represents a single line from the input file in format: {Number}. {Text}
///
/// Stores a precomputed sort key (first 4 chars of Text encoded as big-endian ulong)
/// so that ~80% of comparisons are resolved by a single integer compare,
/// avoiding the full string.Compare call in the hot sort path.
/// </summary>
public readonly struct LineEntry : IComparable<LineEntry>, IEquatable<LineEntry>
{
    public long Number { get; }
    public string Text { get; }

    // First 4 chars encoded as big-endian ulong (2 bytes per char, preserving Ordinal order).
    // Matches StringComparison.Ordinal for the first 4 characters of any Unicode string.
    private readonly ulong _sortKey;

    public LineEntry(long number, string text)
    {
        Number = number;
        Text = text;
        _sortKey = ComputeSortKey(text);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareTo(LineEntry other)
    {
        // Fast path: most entries differ in the first 4 chars
        if (_sortKey != other._sortKey)
            return _sortKey.CompareTo(other._sortKey);

        // Slow path: full ordinal comparison
        var cmp = string.Compare(Text, other.Text, StringComparison.Ordinal);
        return cmp != 0 ? cmp : Number.CompareTo(other.Number);
    }

    public bool Equals(LineEntry other) => Number == other.Number && Text == other.Text;
    public override bool Equals(object? obj) => obj is LineEntry other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Number, Text);
    public override string ToString() => $"{Number}. {Text}";

    public static bool operator ==(LineEntry left, LineEntry right) => left.Equals(right);
    public static bool operator !=(LineEntry left, LineEntry right) => !left.Equals(right);

    private static ulong ComputeSortKey(string text)
    {
        // Pack up to 4 UTF-16 code units into a ulong in big-endian order.
        // This preserves StringComparison.Ordinal ordering for the prefix.
        ulong key = 0;
        var len = Math.Min(text.Length, 4);
        for (var i = 0; i < len; i++)
            key = (key << 16) | text[i];
        if (len < 4)
            key <<= (4 - len) * 16;
        return key;
    }
}
