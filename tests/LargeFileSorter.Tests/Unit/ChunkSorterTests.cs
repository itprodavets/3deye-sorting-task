using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests.Unit;

public class ChunkSorterTests
{
    [Fact]
    public void Sort_SmallArray_SortsCorrectly()
    {
        var data = new[]
        {
            new LineEntry(3, "Cherry"),
            new LineEntry(1, "Apple"),
            new LineEntry(2, "Banana")
        };

        ChunkSorter.Sort(data, data.Length);

        data[0].Text.Should().Be("Apple");
        data[1].Text.Should().Be("Banana");
        data[2].Text.Should().Be("Cherry");
    }

    [Fact]
    public void Sort_DuplicateTexts_SortsByNumberSecondary()
    {
        var data = new[]
        {
            new LineEntry(5, "Apple"),
            new LineEntry(1, "Apple"),
            new LineEntry(3, "Apple")
        };

        ChunkSorter.Sort(data, data.Length);

        data[0].Number.Should().Be(1);
        data[1].Number.Should().Be(3);
        data[2].Number.Should().Be(5);
    }

    [Fact]
    public void Sort_SingleElement_NoOp()
    {
        var data = new[] { new LineEntry(42, "Only") };

        ChunkSorter.Sort(data, 1);

        data[0].Should().Be(new LineEntry(42, "Only"));
    }

    [Fact]
    public void Sort_PartialArray_SortsOnlyCountElements()
    {
        var data = new[]
        {
            new LineEntry(3, "Cherry"),
            new LineEntry(1, "Apple"),
            new LineEntry(999, "ZZZ"),  // should not participate
            new LineEntry(999, "ZZZ")
        };

        ChunkSorter.Sort(data, 2);

        data[0].Text.Should().Be("Apple");
        data[1].Text.Should().Be("Cherry");
        data[2].Text.Should().Be("ZZZ"); // untouched
    }

    [Fact]
    public void Sort_RespectsMaxParallelism()
    {
        // Just ensure it doesn't throw with explicit parallelism hints
        var data = Enumerable.Range(0, 100)
            .Select(i => new LineEntry(100 - i, $"Word{100 - i:D3}"))
            .ToArray();

        ChunkSorter.Sort(data, data.Length, maxParallelism: 2);

        for (var i = 1; i < data.Length; i++)
            data[i].CompareTo(data[i - 1]).Should().BeGreaterThanOrEqualTo(0);
    }
}
