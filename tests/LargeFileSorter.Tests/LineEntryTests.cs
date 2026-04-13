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
}
