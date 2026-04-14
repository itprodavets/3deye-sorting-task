using System.Buffers;
using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// Zero-allocation string interning pool using Dictionary.GetAlternateLookup (.NET 9+).
///
/// During input parsing, every line's text would normally be allocated as a new string
/// via <see cref="Encoding.UTF8.GetString"/>. With 500 unique phrases and millions of lines,
/// ~99% of those allocations are wasted duplicates immediately eligible for GC.
///
/// TextPool avoids this: it decodes UTF-8 bytes into a stack-allocated char buffer,
/// looks up the dictionary by <see cref="ReadOnlySpan{T}"/> (no string allocation),
/// and only calls <c>new string(...)</c> for genuinely new text values.
/// </summary>
internal sealed class TextPool
{
    private readonly Dictionary<string, string> _pool;
    private readonly Dictionary<string, string>.AlternateLookup<ReadOnlySpan<char>> _lookup;

    public TextPool(int capacity = 4096)
    {
        _pool = new Dictionary<string, string>(capacity, StringComparer.Ordinal);
        _lookup = _pool.GetAlternateLookup<ReadOnlySpan<char>>();
    }

    /// <summary>
    /// Interns text from raw UTF-8 bytes. Returns an existing string reference
    /// if the text was seen before, otherwise allocates and caches a new string.
    /// </summary>
    /// <param name="textUtf8">UTF-8 encoded text bytes (the part after ". ").</param>
    /// <param name="isDuplicate">True if the text was already in the pool.</param>
    /// <returns>Interned string reference.</returns>
    public string Intern(ReadOnlySpan<byte> textUtf8, out bool isDuplicate)
    {
        // Decode UTF-8 → UTF-16 chars into a stack buffer (fast path for typical strings)
        // or a rented array (fallback for very long text).
        int maxChars = Encoding.UTF8.GetMaxCharCount(textUtf8.Length);
        char[]? rented = null;
        Span<char> buffer = maxChars <= 512
            ? stackalloc char[maxChars]
            : (rented = ArrayPool<char>.Shared.Rent(maxChars));

        try
        {
            int charCount = Encoding.UTF8.GetChars(textUtf8, buffer);
            var textSpan = buffer[..charCount];

            // AlternateLookup: dictionary lookup by Span<char> — no string allocated
            if (_lookup.TryGetValue(textSpan, out var existing, out _))
            {
                isDuplicate = true;
                return existing;
            }

            // First occurrence — allocate the string and cache it
            var text = new string(textSpan);
            _pool[text] = text;
            isDuplicate = false;
            return text;
        }
        finally
        {
            if (rented is not null)
                ArrayPool<char>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// Clears the pool between chunks to release references to strings
    /// that have already been written to disk.
    /// </summary>
    public void Clear() => _pool.Clear();
}
