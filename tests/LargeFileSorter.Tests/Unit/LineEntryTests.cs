using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

public class LineEntryTests
{
    [Fact]
    public void CompareTo_DifferentText_SortsByTextAlphabetically()
    {
        var apple = new LineEntry(100, "Apple");
        var banana = new LineEntry(1, "Banana");

        apple.CompareTo(banana).Should().BeNegative();
        banana.CompareTo(apple).Should().BePositive();
    }

    [Fact]
    public void CompareTo_SameText_SortsByNumber()
    {
        var first = new LineEntry(1, "Apple");
        var second = new LineEntry(415, "Apple");

        first.CompareTo(second).Should().BeNegative();
        second.CompareTo(first).Should().BePositive();
    }

    [Fact]
    public void CompareTo_Identical_ReturnsZero()
    {
        var a = new LineEntry(42, "Test");
        var b = new LineEntry(42, "Test");

        a.CompareTo(b).Should().Be(0);
    }

    [Fact]
    public void ToString_FormatsCorrectly()
    {
        var entry = new LineEntry(30432, "Something something something");
        entry.ToString().Should().Be("30432. Something something something");
    }

    [Fact]
    public void Sort_MatchesExpectedOrder()
    {
        var entries = new[]
        {
            new LineEntry(415, "Apple"),
            new LineEntry(30432, "Something something something"),
            new LineEntry(1, "Apple"),
            new LineEntry(32, "Cherry is the best"),
            new LineEntry(2, "Banana is yellow"),
        };

        Array.Sort(entries);

        entries.Should().Equal(
            new LineEntry(1, "Apple"),
            new LineEntry(415, "Apple"),
            new LineEntry(2, "Banana is yellow"),
            new LineEntry(32, "Cherry is the best"),
            new LineEntry(30432, "Something something something")
        );
    }

    /// <summary>
    /// Pins the UTF-8 byte-order contract at the lowest level. Supplementary-plane code
    /// points (<c>U+10000+</c>) must sort AFTER BMP code points in <c>[U+E000, U+FFFF]</c>
    /// so that <see cref="LineEntry"/> matches <see cref="RawLineEntry"/> byte-for-byte.
    /// Guards against regressing back to <see cref="StringComparison.Ordinal"/>, which
    /// would invert this pair because the high surrogate 0xD800 &lt; 0xFF80 in UTF-16.
    /// </summary>
    [Fact]
    public void CompareTo_UsesUtf8ByteOrder_NotUtf16CodeUnitOrder()
    {
        var supplementary = new LineEntry(1, "𐀀"); // U+10000, UTF-8 F0 90 80 80
        var bmpHigh       = new LineEntry(1, "ﾀ");  // U+FF80,  UTF-8 EF BE 80

        bmpHigh.CompareTo(supplementary).Should().BeNegative(
            "UTF-8 byte order (the cross-strategy contract) puts ﾀ before 𐀀");

        // Witness: the old Ordinal-based comparison would say the opposite.
        string.Compare("ﾀ", "𐀀", StringComparison.Ordinal).Should().BeGreaterThan(0,
            "guard against silently reverting to StringComparison.Ordinal");
    }
}
