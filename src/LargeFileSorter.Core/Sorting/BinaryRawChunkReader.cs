using System.Buffers.Binary;

namespace LargeFileSorter.Core;

/// <summary>
/// Batch reader that exposes entries as raw UTF-8 byte slices instead of allocating
/// a <see cref="string"/> per record.
///
/// Why this exists: at 100 GB scale the final merge processes ~4.5 billion records.
/// <see cref="BinaryReader.ReadString"/> allocates a new string per call — that's
/// 4.5 billion strings in Phase 2 alone, defeating all of TextPool's work from Phase 1
/// (which kept dedup to ~500 unique strings per chunk).
///
/// This reader parses <see cref="BinaryWriter"/>'s length-prefixed format directly
/// (Int64 little-endian + 7-bit encoded length + UTF-8 bytes), keeping the bytes in a
/// reusable I/O buffer and emitting <see cref="RawLineEntry"/> values that point into it.
///
/// Buffer lifetime invariant: the k-way merge enqueues at most one entry per reader.
/// That entry's bytes stay valid until the reader is advanced past it — and by then
/// the caller has already written the entry to the output stream. Refill only runs
/// when the previous batch is fully consumed, so overwriting the buffer is always safe.
/// </summary>
internal sealed class BinaryRawChunkReader : IRawChunkReader
{
    // Records pre-parsed per batch. 8192 matches BinaryChunkReader's BatchSize so
    // the two readers have comparable per-record overhead in amortized terms.
    private const int BatchSize = 8192;

    // Initial I/O buffer size. Chosen to comfortably hold 8192 records of
    // typical 30-100 UTF-8 byte text plus 8-byte numbers + varint headers.
    // Grown lazily if a single record exceeds remaining capacity.
    private const int InitialBufferSize = 512 * 1024;

    private readonly FileStream _stream;

    private byte[] _ioBuffer;
    private int _ioPos;
    private int _ioEnd;
    private bool _streamEof;

    // Per-batch metadata — separate arrays (SoA) to keep the RawLineEntry construction
    // a plain three-integer read. Using parallel arrays instead of an array of structs
    // avoids copying 32 bytes per Current access.
    private readonly long[] _numbers = new long[BatchSize];
    private readonly int[] _textOffsets = new int[BatchSize];
    private readonly int[] _textLengths = new int[BatchSize];
    private int _batchPos;
    private int _batchCount;

    public BinaryRawChunkReader(string path, int ioBufferSize)
    {
        _stream = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.Read, ioBufferSize, FileOptions.SequentialScan);
        _ioBuffer = new byte[Math.Max(ioBufferSize, InitialBufferSize)];
        Refill();
    }

    public bool HasCurrent => _batchPos < _batchCount;

    public RawLineEntry Current => new(
        _numbers[_batchPos],
        _ioBuffer,
        _textOffsets[_batchPos],
        _textLengths[_batchPos]);

    public void Advance()
    {
        _batchPos++;
        if (_batchPos >= _batchCount)
            Refill();
    }

    private void Refill()
    {
        // Compact any unread bytes to the start of the buffer.
        // Safe to overwrite the earlier region because the previous batch
        // has been fully consumed before Refill is invoked.
        if (_ioPos < _ioEnd)
        {
            var remaining = _ioEnd - _ioPos;
            Buffer.BlockCopy(_ioBuffer, _ioPos, _ioBuffer, 0, remaining);
            _ioEnd = remaining;
        }
        else
        {
            _ioEnd = 0;
        }
        _ioPos = 0;
        _batchPos = 0;
        _batchCount = 0;

        // Top up the buffer from the stream until it's full or the stream is done.
        while (!_streamEof && _ioEnd < _ioBuffer.Length)
        {
            var read = _stream.Read(_ioBuffer, _ioEnd, _ioBuffer.Length - _ioEnd);
            if (read == 0)
            {
                _streamEof = true;
                break;
            }
            _ioEnd += read;
        }

        // Parse as many records as fit in the buffered range.
        // On partial trailing data we rewind _ioPos to the record's start
        // and leave those bytes for the next Refill to pick up.
        while (_batchCount < BatchSize)
        {
            var recordStart = _ioPos;

            // Int64 number + minimum 1-byte varint = 9 bytes needed at minimum.
            if (_ioEnd - _ioPos < 9)
                break;

            var number = BinaryPrimitives.ReadInt64LittleEndian(
                _ioBuffer.AsSpan(_ioPos));
            _ioPos += 8;

            if (!TryReadVarint(out var length))
            {
                _ioPos = recordStart;
                break;
            }

            if (_ioEnd - _ioPos < length)
            {
                // Text doesn't fit in the current buffered range.
                if (recordStart == 0 && _ioEnd == _ioBuffer.Length)
                {
                    // Pathological: a single record exceeds the entire buffer.
                    // Grow the buffer once and retry. Safe here because no external
                    // RawLineEntry references the buffer during Refill (invariant
                    // documented above).
                    GrowBuffer(Math.Max(_ioBuffer.Length * 2, 8 + 5 + length));
                    _ioPos = recordStart;
                    continue;
                }

                // More data needed — rewind and let the next Refill extend the buffer.
                _ioPos = recordStart;
                break;
            }

            _numbers[_batchCount] = number;
            _textOffsets[_batchCount] = _ioPos;
            _textLengths[_batchCount] = length;
            _ioPos += length;
            _batchCount++;
        }

        // If we still have unread bytes and nothing was parsed, try once more after
        // topping up the stream. This handles the case where a record straddled the
        // end of the previous buffer fill.
        if (_batchCount == 0 && !_streamEof)
        {
            var beforeRefill = _ioEnd;
            while (_ioEnd < _ioBuffer.Length)
            {
                var read = _stream.Read(_ioBuffer, _ioEnd, _ioBuffer.Length - _ioEnd);
                if (read == 0) { _streamEof = true; break; }
                _ioEnd += read;
            }
            if (_ioEnd > beforeRefill)
                Refill();  // recurse once — data grew, retry parse
        }
    }

    private bool TryReadVarint(out int value)
    {
        value = 0;
        var shift = 0;
        while (true)
        {
            if (_ioPos >= _ioEnd) return false;
            var b = _ioBuffer[_ioPos++];
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return true;
            shift += 7;
            if (shift > 35)
                throw new InvalidDataException("Varint length prefix exceeds 5 bytes");
        }
    }

    private void GrowBuffer(int newSize)
    {
        var bigger = new byte[newSize];
        Buffer.BlockCopy(_ioBuffer, 0, bigger, 0, _ioEnd);
        _ioBuffer = bigger;
    }

    public void Dispose() => _stream.Dispose();
}
