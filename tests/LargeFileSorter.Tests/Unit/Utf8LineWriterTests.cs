using System.Text;
using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests.Unit;

public class Utf8LineWriterTests : IDisposable
{
    private readonly MemoryStream _stream = new();

    public void Dispose() => _stream.Dispose();

    [Fact]
    public void WriteEntry_ProducesCorrectFormat()
    {
        using (var writer = new Utf8LineWriter(_stream, 4096))
        {
            writer.WriteEntry(new LineEntry(42, "Apple"));
        }

        GetOutput().Should().Be("42. Apple\n");
    }

    [Fact]
    public void WriteEntry_MultipleEntries_AllFormatted()
    {
        using (var writer = new Utf8LineWriter(_stream, 4096))
        {
            writer.WriteEntry(new LineEntry(1, "Apple"));
            writer.WriteEntry(new LineEntry(415, "Banana"));
            writer.WriteEntry(new LineEntry(999999, "Cherry"));
        }

        var lines = GetOutput().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().Equal("1. Apple", "415. Banana", "999999. Cherry");
    }

    [Fact]
    public void WriteEntry_LargeNumber_FormattedCorrectly()
    {
        using (var writer = new Utf8LineWriter(_stream, 4096))
        {
            writer.WriteEntry(new LineEntry(long.MaxValue, "Max"));
        }

        GetOutput().Should().Be($"{long.MaxValue}. Max\n");
    }

    [Fact]
    public void WriteEntry_UnicodeText_EncodedAsUtf8()
    {
        using (var writer = new Utf8LineWriter(_stream, 4096))
        {
            writer.WriteEntry(new LineEntry(1, "Привет 🌍"));
        }

        GetOutput().Should().Be("1. Привет 🌍\n");
    }

    [Fact]
    public void WriteEntry_EmptyText_ProducesNumberAndSeparatorOnly()
    {
        using (var writer = new Utf8LineWriter(_stream, 4096))
        {
            writer.WriteEntry(new LineEntry(7, ""));
        }

        GetOutput().Should().Be("7. \n");
    }

    [Fact]
    public void WriteEntry_SmallBuffer_FlushesAutomatically()
    {
        // Use tiny buffer (64 bytes) to force multiple flushes
        using (var writer = new Utf8LineWriter(_stream, 64))
        {
            for (var i = 0; i < 10; i++)
                writer.WriteEntry(new LineEntry(i, "SomeText"));
        }

        var lines = GetOutput().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        lines.Should().HaveCount(10);
        for (var i = 0; i < 10; i++)
            lines[i].Should().Be($"{i}. SomeText");
    }

    [Fact]
    public void WriteEntry_VeryLongText_UsesDirectWriteFallback()
    {
        // Create text that exceeds the buffer to trigger WriteEntryDirect
        var longText = new string('A', 500);

        using (var writer = new Utf8LineWriter(_stream, 64))
        {
            writer.WriteEntry(new LineEntry(1, longText));
        }

        GetOutput().Should().Be($"1. {longText}\n");
    }

    [Fact]
    public void Flush_NoData_DoesNothing()
    {
        using var writer = new Utf8LineWriter(_stream, 4096);
        writer.Flush(); // should not throw or write anything

        _stream.Length.Should().Be(0);
    }

    [Fact]
    public void Dispose_FlushesRemainingData()
    {
        var writer = new Utf8LineWriter(_stream, 4096);
        writer.WriteEntry(new LineEntry(1, "Test"));

        // Data should not be in the stream yet (buffered)
        _stream.Position = 0;
        var beforeDispose = _stream.Length;

        writer.Dispose();

        _stream.Length.Should().BeGreaterThan(0);
        GetOutput().Should().Be("1. Test\n");
    }

    [Fact]
    public void WriteEntry_MatchesLineParserFormat()
    {
        var entries = new[]
        {
            new LineEntry(1, "Apple"),
            new LineEntry(415, "Apple"),
            new LineEntry(30432, "Something something something")
        };

        using (var writer = new Utf8LineWriter(_stream, 4096))
        {
            foreach (var e in entries)
                writer.WriteEntry(e);
        }

        var lines = GetOutput().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        for (var i = 0; i < entries.Length; i++)
        {
            var parsed = LineParser.Parse(lines[i]);
            parsed.Number.Should().Be(entries[i].Number);
            parsed.Text.Should().Be(entries[i].Text);
        }
    }

    private string GetOutput()
    {
        _stream.Position = 0;
        return Encoding.UTF8.GetString(_stream.ToArray());
    }
}
