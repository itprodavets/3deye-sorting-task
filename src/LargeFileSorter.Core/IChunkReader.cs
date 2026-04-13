namespace LargeFileSorter.Core;

/// <summary>
/// Reads sorted LineEntry records sequentially from a chunk file.
/// Implementations may use buffering for efficiency.
/// </summary>
public interface IChunkReader : IDisposable
{
    bool HasCurrent { get; }
    LineEntry Current { get; }
    void Advance();
}
