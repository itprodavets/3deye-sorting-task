using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace LargeFileSorter.Core;

/// <summary>
/// Lightweight file index entry with no managed references — suitable for storage
/// in <see cref="NativeMemory"/>-backed buffers that the GC never scans.
///
/// At 28 bytes per entry (vs ~120+ bytes for a managed LineEntry + string object),
/// ~230 million entries occupy ~6 GB of native memory with zero GC pressure.
/// Text data stays in the memory-mapped file and is accessed by (Offset, Length)
/// during comparison — zero string allocations throughout the entire sort.
///
/// Sort key: first 8 raw UTF-8 bytes packed as big-endian ulong.
/// For ASCII text this covers 8 characters — resolving 90%+ of comparisons
/// without touching the file data (pure register comparison).
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal readonly struct EntryIndex
{
    /// <summary>The numeric part of the line (e.g., 415 in "415. Apple").</summary>
    public readonly long Number;

    /// <summary>Byte offset where the text starts in the source file.</summary>
    public readonly long TextOffset;

    /// <summary>Byte length of the text portion in UTF-8.</summary>
    public readonly int TextLength;

    /// <summary>
    /// First 8 raw UTF-8 bytes packed as big-endian ulong.
    /// Covers 8 ASCII characters or 2-4 multibyte characters.
    /// Resolves 90%+ of comparisons in the sort inner loop.
    /// </summary>
    public readonly ulong SortKey;

    public EntryIndex(long number, long textOffset, int textLength, ulong sortKey)
    {
        Number = number;
        TextOffset = textOffset;
        TextLength = textLength;
        SortKey = sortKey;
    }

    /// <summary>
    /// Computes a sort key from the first 8 bytes of UTF-8 text.
    /// Packs bytes in big-endian order to preserve lexicographic ordering.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe ulong ComputeSortKey(byte* textPtr, int textLength)
    {
        ulong key = 0;
        var len = Math.Min(textLength, 8);
        for (var i = 0; i < len; i++)
            key = (key << 8) | textPtr[i];
        if (len < 8)
            key <<= (8 - len) * 8;
        return key;
    }
}
