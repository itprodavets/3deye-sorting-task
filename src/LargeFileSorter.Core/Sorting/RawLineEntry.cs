using System.Runtime.CompilerServices;

namespace LargeFileSorter.Core;

/// <summary>
/// Byte-level representation of a line used during the final merge pass.
///
/// Unlike <see cref="LineEntry"/> (which materializes <see cref="string"/> from UTF-8),
/// RawLineEntry keeps text as a slice into a reader-owned byte buffer.
/// At 100 GB scale this eliminates ~4.5 billion string allocations during Phase 2 —
/// the only remaining string allocation hotspot after TextPool deduplication.
///
/// Ordering is preserved: UTF-8 byte ordering matches UTF-16 <see cref="StringComparison.Ordinal"/>
/// ordering because UTF-8 was designed so that lexicographic byte order equals codepoint order.
/// </summary>
public readonly struct RawLineEntry : IComparable<RawLineEntry>
{
    public long Number { get; }

    private readonly byte[] _buffer;
    private readonly int _textOffset;
    private readonly int _textLength;

    // First 8 UTF-8 bytes packed as big-endian ulong — resolves ~80% of
    // comparisons with a single integer compare (same idea as LineEntry._sortKey,
    // but 8 bytes instead of 4 chars because we're in UTF-8 space here).
    private readonly ulong _sortKey;

    public RawLineEntry(long number, byte[] buffer, int textOffset, int textLength)
    {
        Number = number;
        _buffer = buffer;
        _textOffset = textOffset;
        _textLength = textLength;
        _sortKey = ComputeSortKey(buffer.AsSpan(textOffset, textLength));
    }

    public ReadOnlySpan<byte> TextUtf8 =>
        _buffer.AsSpan(_textOffset, _textLength);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int CompareTo(RawLineEntry other)
    {
        // Fast path: differing first 8 UTF-8 bytes.
        if (_sortKey != other._sortKey)
            return _sortKey.CompareTo(other._sortKey);

        // Slow path: full byte comparison (ordinal), then number as tiebreaker.
        var cmp = TextUtf8.SequenceCompareTo(other.TextUtf8);
        return cmp != 0 ? cmp : Number.CompareTo(other.Number);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ComputeSortKey(ReadOnlySpan<byte> text)
    {
        // Pack up to 8 bytes into a ulong in big-endian order so that
        // numeric comparison of the ulong matches lexicographic byte order.
        ulong key = 0;
        var len = Math.Min(text.Length, 8);
        for (var i = 0; i < len; i++)
            key = (key << 8) | text[i];
        if (len < 8)
            key <<= (8 - len) * 8;
        return key;
    }
}
