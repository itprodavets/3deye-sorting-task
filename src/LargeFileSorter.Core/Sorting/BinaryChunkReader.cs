using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// Reads LineEntry records from a binary chunk file in batches.
/// No text parsing — uses BinaryReader.ReadInt64() + ReadString().
/// Pre-fetches <see cref="BatchSize"/> records at a time to reduce I/O syscalls.
/// </summary>
internal sealed class BinaryChunkReader : IChunkReader
{
    private const int BatchSize = 8192;

    private readonly BinaryReader _reader;
    private readonly LineEntry[] _buffer = new LineEntry[BatchSize];
    private int _pos;
    private int _count;
    private bool _eof;

    public BinaryChunkReader(string path, int ioBufferSize)
    {
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.Read, ioBufferSize, FileOptions.SequentialScan);
        _reader = new BinaryReader(stream, new UTF8Encoding(false), leaveOpen: false);
        Refill();
    }

    public bool HasCurrent => _pos < _count;
    public LineEntry Current => _buffer[_pos];

    public void Advance()
    {
        _pos++;
        if (_pos >= _count && !_eof)
            Refill();
    }

    private void Refill()
    {
        _pos = 0;
        _count = 0;
        while (_count < BatchSize)
        {
            try
            {
                var number = _reader.ReadInt64();
                var text = _reader.ReadString();
                _buffer[_count++] = new LineEntry(number, text);
            }
            catch (EndOfStreamException)
            {
                _eof = true;
                break;
            }
        }
    }

    public void Dispose() => _reader.Dispose();
}
