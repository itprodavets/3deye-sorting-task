using System.Runtime.CompilerServices;
using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// Represents a single line from the input file in format: {Number}. {Text}
///
/// Stores a precomputed sort key (first up-to-8 UTF-8 bytes packed big-endian) so that
/// ~80% of comparisons resolve with a single integer compare, avoiding the full
/// text-comparison call in the hot sort path.
///
/// <b>Sort order is UTF-8 lexicographic byte order</b> (equivalent to Unicode code-point
/// order). This matches <see cref="RawLineEntry"/> and <see cref="EntryIndex"/> byte-for-byte
/// — critical so stream / mmf / shard strategies all produce identical output. Note this is
/// NOT the same as <see cref="StringComparison.Ordinal"/>, which is UTF-16 code-unit order
/// and disagrees with UTF-8 byte order whenever the input mixes BMP code points above the
/// surrogate range (<c>U+E000..U+FFFF</c>) with supplementary-plane code points
/// (<c>U+10000+</c>). See <see cref="CrossStrategyParityTests"/> for the exact witness.
///
/// <see cref="SkipLocalsInitAttribute"/> — CompareTo is the innermost loop of the sort;
/// even saving a few nanoseconds per call matters across hundreds of millions of invocations.
/// </summary>
[SkipLocalsInit]
public readonly struct LineEntry : IComparable<LineEntry>, IEquatable<LineEntry>
{
    public long Number { get; }
    public string Text { get; }

    // First up-to-8 UTF-8 bytes packed big-endian. Identical formula to
    // RawLineEntry.ComputeSortKey and EntryIndex.ComputeSortKey so all three
    // strategies agree on the fast path without any cross-encoding.
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
        // Fast path: most entries differ in the first ≤8 UTF-8 bytes.
        if (_sortKey != other._sortKey)
            return _sortKey.CompareTo(other._sortKey);

        return CompareToSlow(other);
    }

    // Kept out-of-line so the hot "sort keys differ" path stays tiny and inlines
    // into Span<LineEntry>.Sort's comparison callback.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private int CompareToSlow(LineEntry other)
    {
        // ASCII fast path: when every char on both sides is in [U+0000, U+007F],
        // UTF-16 code units coincide with UTF-8 bytes 1:1, so string.CompareOrdinal
        // (SIMD-vectorized in the BCL) yields the same order as UTF-8 byte comparison.
        // Ascii.IsValid is itself SIMD, so a typical slow-path comparison on realistic
        // ASCII input costs ~O(len) SIMD cycles instead of per-char Rune decoding.
        //
        // Before adding this branch, the P1 correctness fix (replacing StringComparison.Ordinal
        // with Rune iteration to unify with UTF-8 byte order) roughly doubled stream/shard
        // Phase 1 sort time on 1 GB input — the Rune enumerator decodes char-by-char where
        // the old memcmp-based path hit SIMD. For ASCII-only content (which includes the
        // generator's output and realistic log-style text) the ordering coincides, so the
        // slow path reclaims the perf without losing Unicode correctness.
        if (Ascii.IsValid(Text) && Ascii.IsValid(other.Text))
        {
            var cmp = string.CompareOrdinal(Text, other.Text);
            return cmp != 0 ? cmp : Number.CompareTo(other.Number);
        }

        // Unicode path: at least one side has non-ASCII content. Iterate Runes so
        // supplementary-plane code points (U+10000+) sort after U+E000..U+FFFF, matching
        // UTF-8 byte order — see CrossStrategyParityTests for the exact witness.
        // The enumerator is a struct, so no heap allocations.
        var aIter = Text.EnumerateRunes();
        var bIter = other.Text.EnumerateRunes();
        while (true)
        {
            var aHas = aIter.MoveNext();
            var bHas = bIter.MoveNext();
            if (!aHas) return bHas ? -1 : Number.CompareTo(other.Number);
            if (!bHas) return 1;
            var cmp = aIter.Current.Value - bIter.Current.Value;
            if (cmp != 0) return cmp;
        }
    }

    public bool Equals(LineEntry other) => Number == other.Number && Text == other.Text;
    public override bool Equals(object? obj) => obj is LineEntry other && Equals(other);
    public override int GetHashCode() => HashCode.Combine(Number, Text);
    public override string ToString() => $"{Number}. {Text}";

    public static bool operator ==(LineEntry left, LineEntry right) => left.Equals(right);
    public static bool operator !=(LineEntry left, LineEntry right) => !left.Equals(right);

    [SkipLocalsInit]
    private static ulong ComputeSortKey(string text)
    {
        // Encode Runes to UTF-8 into a stack buffer until we have ≥8 bytes (or run out
        // of input). One Rune is 1–4 UTF-8 bytes, so 16 bytes is always enough for
        // "first 8 bytes plus slack from the last partial rune".
        Span<byte> buffer = stackalloc byte[16];
        var written = 0;
        foreach (var rune in text.EnumerateRunes())
        {
            written += rune.EncodeToUtf8(buffer[written..]);
            if (written >= 8) break;
        }

        // Pack first up-to-8 bytes big-endian so numeric comparison of the ulong
        // matches lexicographic byte order.
        ulong key = 0;
        var len = Math.Min(written, 8);
        for (var i = 0; i < len; i++)
            key = (key << 8) | buffer[i];
        if (len < 8)
            key <<= (8 - len) * 8;
        return key;
    }
}
