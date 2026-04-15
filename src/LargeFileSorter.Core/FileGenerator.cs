using System.Text;

namespace LargeFileSorter.Core;

public sealed class FileGenerator : IFileGenerator
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
        // Uniqueness matters: GeneratorOptions.UniquePhraseCount and the --phrases CLI flag
        // are both documented as "number of unique phrases", and benchmarks rely on cardinality
        // to estimate TextPool dedup effectiveness. The previous implementation just filled a
        // fixed-size array, so random collisions (e.g. "Apple", "Apple is the best" generated
        // twice from different rolls) silently left the pool with fewer distinct values than
        // requested — a 500-phrase pool frequently ended up with ~420 distinct strings.
        //
        // We keep an ordered List so the array returned is stable across runs with the same
        // seed (HashSet.ToArray() would be non-deterministic in .NET because of randomized
        // string hash codes), plus a HashSet for O(1) membership checks.
        var seen = new HashSet<string>(capacity: _options.UniquePhraseCount);
        var phrases = new List<string>(_options.UniquePhraseCount);
        var sb = new StringBuilder();

        // The word pool's combinatorial space (Σ |W|^k for k ∈ {1..4}) is ~3.5M; 100×
        // the requested count is plenty for realistic inputs and still guards against
        // pathological calls (UniquePhraseCount set above the combinatorial ceiling).
        var attemptBudget = Math.Max(1000, _options.UniquePhraseCount * 100);

        for (var attempts = 0; phrases.Count < _options.UniquePhraseCount && attempts < attemptBudget; attempts++)
        {
            var wordCount = random.Next(1, 5);
            sb.Clear();

            for (var w = 0; w < wordCount; w++)
            {
                if (w > 0) sb.Append(' ');
                sb.Append(WordPool[random.Next(WordPool.Length)]);
            }

            if (sb.Length > 0 && char.IsLower(sb[0]))
                sb[0] = char.ToUpperInvariant(sb[0]);

            var phrase = sb.ToString();
            if (seen.Add(phrase))
                phrases.Add(phrase);
        }

        if (phrases.Count < _options.UniquePhraseCount)
        {
            throw new InvalidOperationException(
                $"Could not generate {_options.UniquePhraseCount} distinct phrases from the word pool " +
                $"(got {phrases.Count} after {attemptBudget} attempts). Either lower --phrases or enlarge " +
                $"the word pool in FileGenerator.WordPool.");
        }

        return phrases.ToArray();
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
