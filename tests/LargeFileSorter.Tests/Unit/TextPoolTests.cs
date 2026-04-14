using System.Text;
using FluentAssertions;
using LargeFileSorter.Core;

namespace LargeFileSorter.Tests.Unit;

public class TextPoolTests
{
    [Fact]
    public void Intern_FirstOccurrence_ReturnsNewString()
    {
        var pool = new TextPool();
        var utf8 = Encoding.UTF8.GetBytes("Apple");

        var result = pool.Intern(utf8, out var isDuplicate);

        result.Should().Be("Apple");
        isDuplicate.Should().BeFalse();
    }

    [Fact]
    public void Intern_SecondOccurrence_ReturnsSameReference()
    {
        var pool = new TextPool();
        var utf8 = Encoding.UTF8.GetBytes("Apple");

        var first = pool.Intern(utf8, out _);
        var second = pool.Intern(utf8, out var isDuplicate);

        second.Should().BeSameAs(first);
        isDuplicate.Should().BeTrue();
    }

    [Fact]
    public void Intern_DifferentTexts_ReturnsDifferentStrings()
    {
        var pool = new TextPool();

        var apple = pool.Intern(Encoding.UTF8.GetBytes("Apple"), out var dup1);
        var banana = pool.Intern(Encoding.UTF8.GetBytes("Banana"), out var dup2);

        apple.Should().Be("Apple");
        banana.Should().Be("Banana");
        dup1.Should().BeFalse();
        dup2.Should().BeFalse();
        apple.Should().NotBeSameAs(banana);
    }

    [Fact]
    public void Intern_UnicodeText_HandledCorrectly()
    {
        var pool = new TextPool();
        var utf8 = Encoding.UTF8.GetBytes("Привет мир 🌍");

        var first = pool.Intern(utf8, out var dup1);
        var second = pool.Intern(utf8, out var dup2);

        first.Should().Be("Привет мир 🌍");
        second.Should().BeSameAs(first);
        dup1.Should().BeFalse();
        dup2.Should().BeTrue();
    }

    [Fact]
    public void Intern_EmptyBytes_ReturnsEmptyString()
    {
        var pool = new TextPool();

        var result = pool.Intern(ReadOnlySpan<byte>.Empty, out var isDuplicate);

        result.Should().BeEmpty();
        isDuplicate.Should().BeFalse();
    }

    [Fact]
    public void Intern_LongText_UsesArrayPoolFallback()
    {
        var pool = new TextPool();
        // Create text longer than 512 chars to trigger ArrayPool rental
        var longText = new string('X', 600);
        var utf8 = Encoding.UTF8.GetBytes(longText);

        var first = pool.Intern(utf8, out var dup1);
        var second = pool.Intern(utf8, out var dup2);

        first.Should().Be(longText);
        second.Should().BeSameAs(first);
        dup1.Should().BeFalse();
        dup2.Should().BeTrue();
    }

    [Fact]
    public void Clear_AllowsReinsertionWithNewReference()
    {
        var pool = new TextPool();
        var utf8 = Encoding.UTF8.GetBytes("Apple");

        var before = pool.Intern(utf8, out _);
        pool.Clear();
        var after = pool.Intern(utf8, out var isDuplicate);

        before.Should().Be(after);
        isDuplicate.Should().BeFalse();
        // After Clear, it's a fresh string, not the same reference
        after.Should().NotBeSameAs(before);
    }

    [Fact]
    public void Intern_ManyDuplicates_AllReturnSameReference()
    {
        var pool = new TextPool();
        var utf8 = Encoding.UTF8.GetBytes("Repeated");
        var first = pool.Intern(utf8, out _);

        for (var i = 0; i < 1000; i++)
        {
            var result = pool.Intern(utf8, out var isDuplicate);
            result.Should().BeSameAs(first);
            isDuplicate.Should().BeTrue();
        }
    }
}
