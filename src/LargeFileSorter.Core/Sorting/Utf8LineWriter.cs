using System.Buffers;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// High-performance text output writer that formats LineEntry records directly as UTF-8 bytes,
/// bypassing <see cref="StreamWriter"/> and its per-write char→byte encoding overhead.
///
/// Improvements over StreamWriter:
/// <list type="bullet">
///   <item>Numbers formatted via <see cref="Utf8Formatter.TryFormat"/> — no <c>long.ToString()</c> allocation</item>
///   <item>Text encoded via <see cref="Encoding.UTF8.GetBytes(string, Span{byte})"/> — single pass, no intermediate buffer</item>
///   <item>All fields written to a single pooled byte buffer, flushed in bulk — fewer I/O syscalls</item>
///   <item>Buffer managed via <see cref="ArrayPool{T}"/> — no GC pressure from the writer itself</item>
///   <item><see cref="SkipLocalsInitAttribute"/> — no stack zeroing overhead on hot path</item>
/// </list>
/// </summary>
[SkipLocalsInit]
internal sealed class Utf8LineWriter : IDisposable
{
    private static readonly byte[] SeparatorBytes = ". "u8.ToArray();

    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _position;

    public Utf8LineWriter(Stream stream, int bufferSize)
    {
        _stream = stream;
        _buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteEntry(in LineEntry entry)
    {
        // Worst case: 20 digits (long) + 2 (". ") + text × 3 (UTF-8 max) + 1 (\n)
        int maxNeeded = 20 + 2 + Encoding.UTF8.GetMaxByteCount(entry.Text.Length) + 1;

        if (_position + maxNeeded > _buffer.Length)
            Flush();

        // Entries larger than the entire buffer are extremely rare but handled safely
        if (maxNeeded > _buffer.Length)
        {
            WriteEntryDirect(entry);
            return;
        }

        var span = _buffer.AsSpan(_position);

        // Number → UTF-8 bytes (no string allocation)
        Utf8Formatter.TryFormat(entry.Number, span, out var written);
        _position += written;

        // ". " separator
        SeparatorBytes.CopyTo(_buffer.AsSpan(_position));
        _position += SeparatorBytes.Length;

        // Text → UTF-8 bytes
        _position += Encoding.UTF8.GetBytes(entry.Text, _buffer.AsSpan(_position));

        // Newline
        _buffer[_position++] = (byte)'\n';
    }

    public void Flush()
    {
        if (_position > 0)
        {
            _stream.Write(_buffer.AsSpan(0, _position));
            _position = 0;
        }
    }

    /// <summary>
    /// Handles the rare case where a single entry exceeds the buffer size.
    /// Rents a temporary buffer, formats into it, and writes directly.
    /// </summary>
    private void WriteEntryDirect(in LineEntry entry)
    {
        Flush();

        int maxNeeded = 20 + 2 + Encoding.UTF8.GetMaxByteCount(entry.Text.Length) + 1;
        var temp = ArrayPool<byte>.Shared.Rent(maxNeeded);
        try
        {
            int pos = 0;
            Utf8Formatter.TryFormat(entry.Number, temp.AsSpan(pos), out var written);
            pos += written;
            SeparatorBytes.CopyTo(temp.AsSpan(pos));
            pos += SeparatorBytes.Length;
            pos += Encoding.UTF8.GetBytes(entry.Text, temp.AsSpan(pos));
            temp[pos++] = (byte)'\n';
            _stream.Write(temp.AsSpan(0, pos));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(temp);
        }
    }

    public void Dispose()
    {
        Flush();
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}
