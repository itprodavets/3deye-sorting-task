using System.Diagnostics;
using System.Globalization;
using LargeFileSorter.Core;

if (args.Length < 2)
{
    Console.WriteLine("Usage: LargeFileSorter.Generator <output-file> <size>");
    Console.WriteLine();
    Console.WriteLine("  <size>  Target file size, e.g. 1GB, 500MB, 100KB");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine("  --phrases <count>    Number of unique phrases (default: 500)");
    Console.WriteLine("  --max-number <num>   Maximum number value (default: 100000)");
    Console.WriteLine("  --seed <value>       Random seed for reproducible output");
    return 1;
}

var outputPath = args[0];
var targetSize = ParseSize(args[1]);

var phraseCount = 500;
var maxNumber = 100_000L;
int? seed = null;

for (var i = 2; i < args.Length - 1; i++)
{
    switch (args[i])
    {
        case "--phrases":
            phraseCount = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--max-number":
            maxNumber = long.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
        case "--seed":
            seed = int.Parse(args[++i], CultureInfo.InvariantCulture);
            break;
    }
}

var options = new GeneratorOptions
{
    UniquePhraseCount = phraseCount,
    MaxNumber = maxNumber,
    Seed = seed
};

Console.WriteLine($"Generating file: {outputPath}");
Console.WriteLine($"Target size: {SizeFormatter.Format(targetSize)}");
Console.WriteLine($"Unique phrases: {options.UniquePhraseCount}");

var progress = new Progress<long>(bytes =>
    Console.Write($"\rProgress: {SizeFormatter.Format(bytes)} / {SizeFormatter.Format(targetSize)}    "));

var sw = Stopwatch.StartNew();
var generator = new FileGenerator(options);
await generator.GenerateAsync(outputPath, targetSize, progress, CancellationToken.None);
sw.Stop();

var fileInfo = new FileInfo(outputPath);
Console.WriteLine();
Console.WriteLine($"Done in {sw.Elapsed:hh\\:mm\\:ss\\.fff}. Actual size: {SizeFormatter.Format(fileInfo.Length)}");

return 0;

static long ParseSize(string input)
{
    input = input.Trim().ToUpperInvariant();

    // Parse the numeric prefix with InvariantCulture + strict NumberStyles:
    //   * InvariantCulture so "1.5GB" works regardless of the user's system locale.
    //     Without it, machines where the decimal separator is ',' (ru-RU, de-DE, fr-FR, etc.)
    //     rejected "1.5GB" with FormatException and silently accepted "1,5GB".
    //   * No AllowThousands — default double.Parse allows ',' as a thousand separator, which
    //     would silently turn ru-RU-looking "1,5KB" into 15 KB instead of failing loudly.
    // Decimal point only; anything else (commas, exponent notation, currency symbols) is rejected.
    const NumberStyles DoubleStyle = NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint;
    const NumberStyles IntStyle = NumberStyles.AllowLeadingSign;

    if (input.EndsWith("GB"))
        return (long)(double.Parse(input[..^2], DoubleStyle, CultureInfo.InvariantCulture) * 1024 * 1024 * 1024);
    if (input.EndsWith("MB"))
        return (long)(double.Parse(input[..^2], DoubleStyle, CultureInfo.InvariantCulture) * 1024 * 1024);
    if (input.EndsWith("KB"))
        return (long)(double.Parse(input[..^2], DoubleStyle, CultureInfo.InvariantCulture) * 1024);

    return long.Parse(input, IntStyle, CultureInfo.InvariantCulture);
}

