using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests;

public class LineParserTests
{
    [Theory]
    [InlineData("1. Apple", 1, "Apple")]
    [InlineData("415. Apple", 415, "Apple")]
    [InlineData("30432. Something something something", 30432, "Something something something")]
    [InlineData("999999999. A", 999999999, "A")]
    public void Parse_ValidLine_ReturnsCorrectEntry(string line, long expectedNumber, string expectedText)
    {
        var entry = LineParser.Parse(line);

        entry.Number.Should().Be(expectedNumber);
        entry.Text.Should().Be(expectedText);
    }

    [Theory]
    [InlineData("1. Apple")]
    [InlineData("30432. Something something something")]
    public void ParseSpan_ValidLine_MatchesStringOverload(string line)
    {
        var fromString = LineParser.Parse(line);
        var fromSpan = LineParser.Parse(line.AsSpan());

        fromSpan.Should().Be(fromString);
    }

    [Theory]
    [InlineData("")]
    [InlineData("no dot here")]
    [InlineData(". missing number")]
    public void Parse_InvalidFormat_ThrowsFormatException(string line)
    {
        var act = () => LineParser.Parse(line);
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void Parse_NonNumericPrefix_ThrowsFormatException()
    {
        var act = () => LineParser.Parse("abc. Text");
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void Format_ReturnsOriginalLine()
    {
        var entry = new LineEntry(42, "Hello World");
        LineParser.Format(entry).Should().Be("42. Hello World");
    }
}
