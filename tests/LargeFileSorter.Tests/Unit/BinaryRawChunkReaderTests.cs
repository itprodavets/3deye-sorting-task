using System.Text;
using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests.Unit;

public class BinaryRawChunkReaderTests : IDisposable
{
    private readonly string _tempDir;

    public BinaryRawChunkReaderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"raw_reader_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    [Fact]
    public void ReadsBackEntriesWrittenByBinaryChunkWriter()
    {
        var path = Path.Combine(_tempDir, "raw.bin");
        var entries = new[]
        {
            new LineEntry(1, "Apple"),
            new LineEntry(42, "Banana"),
            new LineEntry(999, "Cherry is the best")
        };
        BinaryChunkWriter.Write(path, entries, entries.Length);

        using var reader = new BinaryRawChunkReader(path, 4096);

        var collected = new List<(long Number, string Text)>();
        while (reader.HasCurrent)
        {
            var entry = reader.Current;
            collected.Add((entry.Number, Encoding.UTF8.GetString(entry.TextUtf8)));
            reader.Advance();
        }

        collected.Should().Equal(
            (1L, "Apple"),
            (42L, "Banana"),
            (999L, "Cherry is the best"));
    }

    [Fact]
    public void EmptyFile_HasNoEntries()
    {
        var path = Path.Combine(_tempDir, "empty.bin");
        BinaryChunkWriter.Write(path, Array.Empty<LineEntry>(), 0);

        using var reader = new BinaryRawChunkReader(path, 4096);
        reader.HasCurrent.Should().BeFalse();
    }

    [Fact]
    public void HandlesLargeBatches_CrossingBatchBoundaries()
    {
        // 20K entries — more than one BatchSize (8192) to exercise multi-batch refill path.
        var path = Path.Combine(_tempDir, "large.bin");
        var entries = Enumerable.Range(0, 20_000)
            .Select(i => new LineEntry(i, $"Text_{i:D6}"))
            .ToArray();
        BinaryChunkWriter.Write(path, entries, entries.Length);

        using var reader = new BinaryRawChunkReader(path, 4096);
        var count = 0;
        while (reader.HasCurrent)
        {
            var entry = reader.Current;
            entry.Number.Should().Be(count);
            Encoding.UTF8.GetString(entry.TextUtf8).Should().Be($"Text_{count:D6}");
            reader.Advance();
            count++;
        }

        count.Should().Be(20_000);
    }

    [Fact]
    public void HandlesOversizedRecord_TriggersBufferGrowth()
    {
        // Single record with text larger than the initial I/O buffer — exercises the
        // grow-buffer path in BinaryRawChunkReader.Refill.
        var path = Path.Combine(_tempDir, "huge.bin");
        var hugeText = new string('A', 600_000); // > 512 KB default buffer
        var entries = new[] { new LineEntry(7, hugeText) };
        BinaryChunkWriter.Write(path, entries, entries.Length);

        using var reader = new BinaryRawChunkReader(path, 4096);
        reader.HasCurrent.Should().BeTrue();
        var entry = reader.Current;
        entry.Number.Should().Be(7);
        Encoding.UTF8.GetString(entry.TextUtf8).Should().Be(hugeText);
        reader.Advance();
        reader.HasCurrent.Should().BeFalse();
    }

    [Fact]
    public void Utf8Text_IsReturnedVerbatim()
    {
        // Multi-byte UTF-8 sequences must not be mangled by the raw reader.
        var path = Path.Combine(_tempDir, "utf8.bin");
        var entries = new[]
        {
            new LineEntry(1, "Привет"),       // Cyrillic
            new LineEntry(2, "Café"),          // accented Latin
            new LineEntry(3, "日本語テキスト")  // CJK
        };
        BinaryChunkWriter.Write(path, entries, entries.Length);

        using var reader = new BinaryRawChunkReader(path, 4096);
        var texts = new List<string>();
        while (reader.HasCurrent)
        {
            texts.Add(Encoding.UTF8.GetString(reader.Current.TextUtf8));
            reader.Advance();
        }

        texts.Should().Equal("Привет", "Café", "日本語テキスト");
    }
}

public class RawLineEntryTests
{
    /// <summary>
    /// RawLineEntry orders by UTF-8 lexicographic byte order, which is equivalent to
    /// Unicode code-point order. For code points below U+D800 this also happens to match
    /// <see cref="StringComparison.Ordinal"/> (UTF-16 code-unit order) — the previous
    /// version of this test asserted general equivalence with Ordinal and reinforced the
    /// wrong mental model. The supplementary-plane case in
    /// <see cref="OrderingMatchesCodePointOrder_NotUtf16CodeUnitOrder"/> pins down the
    /// distinction so nobody "fixes" RawLineEntry to match Ordinal again.
    /// </summary>
    [Fact]
    public void OrderingMatchesUtf8ByteOrder()
    {
        var pairs = new[]
        {
            ("Apple", "Banana"),
            ("AA", "AB"),
            ("Short", "Shorter"),
            ("Cherry", "Cherry"),   // equal
            ("Cafe", "Café"),       // ASCII < multi-byte
        };

        foreach (var (a, b) in pairs)
        {
            var rawA = MakeRaw(1, a);
            var rawB = MakeRaw(1, b);
            var byteCmp = Encoding.UTF8.GetBytes(a).AsSpan()
                .SequenceCompareTo(Encoding.UTF8.GetBytes(b));

            Math.Sign(rawA.CompareTo(rawB)).Should().Be(Math.Sign(byteCmp),
                $"comparison of '{a}' vs '{b}' must match UTF-8 byte order");
        }
    }

    /// <summary>
    /// Supplementary-plane code points (<c>U+10000+</c>) vs BMP code points in
    /// <c>[U+E000, U+FFFF]</c> are the witness case where UTF-16 Ordinal and UTF-8 byte
    /// order DISAGREE. <see cref="RawLineEntry"/> must follow the UTF-8 order so that
    /// stream / mmf / shard all produce byte-identical output.
    /// </summary>
    [Fact]
    public void OrderingMatchesCodePointOrder_NotUtf16CodeUnitOrder()
    {
        // U+10000 "𐀀" (surrogate pair 0xD800 0xDC00 in UTF-16, F0 90 80 80 in UTF-8)
        // U+FF80 "ﾀ"  (single code unit 0xFF80 in UTF-16, EF BE 80 in UTF-8)
        var supplementary = MakeRaw(1, "𐀀");
        var bmpHigh       = MakeRaw(1, "ﾀ");

        // UTF-8 / code-point order: ﾀ (0xEF..) < 𐀀 (0xF0..)
        bmpHigh.CompareTo(supplementary).Should().BeLessThan(0,
            "UTF-8 byte order puts ﾀ (0xEF…) before 𐀀 (0xF0…)");

        // But UTF-16 Ordinal would say the opposite — guard so nobody switches back.
        string.Compare("ﾀ", "𐀀", StringComparison.Ordinal).Should().BeGreaterThan(0,
            "UTF-16 Ordinal puts 𐀀 (surrogate 0xD800) before ﾀ (0xFF80) — " +
            "that's exactly the ordering we must NOT reintroduce into RawLineEntry");
    }

    [Fact]
    public void Number_IsUsedAsTiebreaker_WhenTextEqual()
    {
        var a = MakeRaw(1, "Same");
        var b = MakeRaw(2, "Same");

        a.CompareTo(b).Should().BeLessThan(0);
        b.CompareTo(a).Should().BeGreaterThan(0);
        a.CompareTo(a).Should().Be(0);
    }

    private static RawLineEntry MakeRaw(long number, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return new RawLineEntry(number, bytes, 0, bytes.Length);
    }
}
