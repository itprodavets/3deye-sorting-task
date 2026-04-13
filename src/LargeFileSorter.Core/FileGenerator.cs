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
        await using var writer = new StreamWriter(stream, Encoding.UTF8, _options.BufferSize);

        long bytesWritten = 0;
        var lastReported = 0L;

        while (bytesWritten < targetSizeBytes)
        {
            ct.ThrowIfCancellationRequested();

            var number = random.NextInt64(1, _options.MaxNumber + 1);
            var phrase = phrases[random.Next(phrases.Length)];
            var line = $"{number}. {phrase}";

            await writer.WriteLineAsync(line.AsMemory(), ct);
            bytesWritten += Encoding.UTF8.GetByteCount(line) + 1; // +1 for newline

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

            // Capitalize first letter
            if (sb.Length > 0 && char.IsLower(sb[0]))
                sb[0] = char.ToUpperInvariant(sb[0]);

            phrases[i] = sb.ToString();
        }

        return phrases;
    }
}
