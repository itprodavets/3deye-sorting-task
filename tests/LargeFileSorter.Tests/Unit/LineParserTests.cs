using System.Text;
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

    [Theory]
    [InlineData("1. Apple", 1, "Apple")]
    [InlineData("415. Apple", 415, "Apple")]
    [InlineData("30432. Something something something", 30432, "Something something something")]
    public void ParseUtf8_ValidLine_ReturnsCorrectEntry(string line, long expectedNumber, string expectedText)
    {
        var utf8 = Encoding.UTF8.GetBytes(line);
        var entry = LineParser.ParseUtf8(utf8);

        entry.Number.Should().Be(expectedNumber);
        entry.Text.Should().Be(expectedText);
    }

    [Theory]
    [InlineData("1. Apple", 1, 3)]
    [InlineData("415. Apple", 415, 5)]
    [InlineData("999999999. A", 999999999, 11)]
    public void ParseNumberUtf8_ValidLine_ReturnsNumberAndOffset(
        string line, long expectedNumber, int expectedOffset)
    {
        var utf8 = Encoding.UTF8.GetBytes(line);
        var number = LineParser.ParseNumberUtf8(utf8, out var textStart);

        number.Should().Be(expectedNumber);
        textStart.Should().Be(expectedOffset);

        // Verify offset points to the correct text
        var text = Encoding.UTF8.GetString(utf8.AsSpan(textStart));
        text.Should().Be(line[(line.IndexOf(". ", StringComparison.Ordinal) + 2)..]);
    }

    [Fact]
    public void ParseNumberUtf8_InvalidFormat_ThrowsFormatException()
    {
        var utf8 = Encoding.UTF8.GetBytes("no dot here");
        var act = () => LineParser.ParseNumberUtf8(utf8, out _);
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void ParseNumberUtf8_NonNumericPrefix_ThrowsFormatException()
    {
        var utf8 = Encoding.UTF8.GetBytes("abc. Text");
        var act = () => LineParser.ParseNumberUtf8(utf8, out _);
        act.Should().Throw<FormatException>();
    }

    [Fact]
    public void Format_ReturnsOriginalLine()
    {
        var entry = new LineEntry(42, "Hello World");
        LineParser.Format(entry).Should().Be("42. Hello World");
    }
}
