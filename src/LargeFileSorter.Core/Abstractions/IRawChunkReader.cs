namespace LargeFileSorter.Core;

/// <summary>
/// Reads sorted records from a chunk file, exposing text as raw UTF-8 bytes
/// rather than materializing <see cref="string"/> values.
///
/// Used in the final merge pass to skip the ReadString → new string → WriteString
/// roundtrip that would otherwise allocate one string per record
/// (≈ 4.5 billion allocations at 100 GB scale).
/// </summary>
public interface IRawChunkReader : IDisposable
{
    bool HasCurrent { get; }
    RawLineEntry Current { get; }
    void Advance();
}
