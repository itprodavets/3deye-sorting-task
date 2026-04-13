using System.Text;

namespace LargeFileSorter.Core;

public sealed class FileGenerator
{
    private static readonly string[] WordPool =
    [
        "Apple", "Banana", "Cherry", "Date", "Elderberry", "Fig", "Grape",
        "Honeydew", "Kiwi", "Lemon", "Mango", "Nectarine", "Orange", "Papaya",
        "Quince", "Raspberry", "Strawberry", "Tangerine", "Watermelon",
        "is", "the", "best", "fruit", "around", "in", "world",
        "yellow", "red", "green", "sweet", "sour", "juicy", "fresh",
        "something", "nothing", "everything", "always", "never", "maybe",
        "today", "tomorrow", "yesterday", "here", "there", "everywhere"
    ];

    private readonly GeneratorOptions _options;

    public FileGenerator(GeneratorOptions? options = null)
    {
        _options = options ?? new GeneratorOptions();
    }

    public async Task GenerateAsync(string outputPath, long targetSizeBytes,
        IProgress<long>? progress = null, CancellationToken ct = default)
    {
        var random = new Random(_options.Seed ?? Environment.TickCount);
        var phrases = BuildPhrasePool(random);

        await using var stream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, _options.BufferSize, FileOptions.SequentialScan);
        await using var writer = new StreamWriter(stream, new UTF8Encoding(false), _options.BufferSize);

        long bytesWritten = 0;
        var lastReported = 0L;

        while (bytesWritten < targetSizeBytes)
        {
            ct.ThrowIfCancellationRequested();

            var number = random.NextInt64(1, _options.MaxNumber + 1);
            var phrase = phrases[random.Next(phrases.Length)];

            // Write parts directly — avoids allocating an interpolated string per line.
            // TextWriter.Write(long) formats the number straight into its internal buffer.
            writer.Write(number);
            writer.Write(". ");
            writer.WriteLine(phrase);

            // Estimate byte length without calling Encoding.GetByteCount per line.
            // Our phrases are ASCII, so char count ≈ byte count in UTF-8.
            bytesWritten += DigitCount(number) + 2 + phrase.Length + 1;

            if (progress != null && bytesWritten - lastReported >= 10 * 1024 * 1024)
            {
                progress.Report(bytesWritten);
                lastReported = bytesWritten;
            }
        }

        await writer.FlushAsync(ct);
        progress?.Report(bytesWritten);
    }

    private string[] BuildPhrasePool(Random random)
    {
        var phrases = new string[_options.UniquePhraseCount];

        for (var i = 0; i < phrases.Length; i++)
        {
            var wordCount = random.Next(1, 5);
            var sb = new StringBuilder();

            for (var w = 0; w < wordCount; w++)
            {
                if (w > 0) sb.Append(' ');
                sb.Append(WordPool[random.Next(WordPool.Length)]);
            }

            if (sb.Length > 0 && char.IsLower(sb[0]))
                sb[0] = char.ToUpperInvariant(sb[0]);

            phrases[i] = sb.ToString();
        }

        return phrases;
    }

    private static int DigitCount(long n)
    {
        if (n < 10L) return 1;
        if (n < 100L) return 2;
        if (n < 1_000L) return 3;
        if (n < 10_000L) return 4;
        if (n < 100_000L) return 5;
        if (n < 1_000_000L) return 6;
        return (int)Math.Log10(n) + 1;
    }
}
