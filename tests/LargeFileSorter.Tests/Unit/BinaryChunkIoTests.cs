using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests.Unit;

public class BinaryChunkIoTests : IDisposable
{
    private readonly string _tempDir;

    public BinaryChunkIoTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"chunk_io_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public void WriteAndRead_RoundTripsCorrectly()
    {
        var path = Path.Combine(_tempDir, "test.bin");
        var entries = new[]
        {
            new LineEntry(1, "Apple"),
            new LineEntry(42, "Banana"),
            new LineEntry(999, "Cherry is the best")
        };

        BinaryChunkWriter.Write(path, entries, entries.Length);

        using var reader = new BinaryChunkReader(path, 4096);
        var result = new List<LineEntry>();
        while (reader.HasCurrent)
        {
            result.Add(reader.Current);
            reader.Advance();
        }

        result.Should().HaveCount(3);
        result[0].Should().Be(new LineEntry(1, "Apple"));
        result[1].Should().Be(new LineEntry(42, "Banana"));
        result[2].Should().Be(new LineEntry(999, "Cherry is the best"));
    }

    [Fact]
    public void WritePartialArray_OnlyWritesCountEntries()
    {
        var path = Path.Combine(_tempDir, "partial.bin");
        var entries = new[]
        {
            new LineEntry(1, "First"),
            new LineEntry(2, "Second"),
            new LineEntry(3, "Third")
        };

        BinaryChunkWriter.Write(path, entries, 2); // only first 2

        using var reader = new BinaryChunkReader(path, 4096);
        var result = new List<LineEntry>();
        while (reader.HasCurrent)
        {
            result.Add(reader.Current);
            reader.Advance();
        }

        result.Should().HaveCount(2);
    }

    [Fact]
    public void ConvertToText_ProducesCorrectFormat()
    {
        var binPath = Path.Combine(_tempDir, "data.bin");
        var txtPath = Path.Combine(_tempDir, "data.txt");

        var entries = new[]
        {
            new LineEntry(1, "Apple"),
            new LineEntry(415, "Apple"),
            new LineEntry(2, "Banana")
        };

        BinaryChunkWriter.Write(binPath, entries, entries.Length);
        BinaryChunkWriter.ConvertToText(binPath, txtPath, 4096);

        var lines = File.ReadAllLines(txtPath);
        lines.Should().Equal("1. Apple", "415. Apple", "2. Banana");
    }

    [Fact]
    public void Reader_EmptyFile_HasNoEntries()
    {
        var path = Path.Combine(_tempDir, "empty.bin");
        BinaryChunkWriter.Write(path, Array.Empty<LineEntry>(), 0);

        using var reader = new BinaryChunkReader(path, 4096);
        reader.HasCurrent.Should().BeFalse();
    }
}
