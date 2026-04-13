using System.Diagnostics;
using LargeFileSorter.Core;

if (args.Length < 2)
{
    Console.WriteLine("Usage: LargeFileSorter.Sorter <input-file> <output-file>");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine("  --memory <size>       Max memory per chunk, e.g. 512MB, 1GB (default: auto)");
    Console.WriteLine("  --merge-width <num>   Max files per merge pass (default: 64)");
    Console.WriteLine("  --temp-dir <path>     Directory for temporary files");
    Console.WriteLine("  --buffer <size>       I/O buffer size, e.g. 64KB, 1MB (default: 64KB)");
    return 1;
}

var inputPath = args[0];
var outputPath = args[1];

if (!File.Exists(inputPath))
{
    Console.Error.WriteLine($"Error: file not found — {inputPath}");
    return 1;
}

var options = new SortOptions();
for (var i = 2; i < args.Length - 1; i++)
{
    switch (args[i])
    {
        case "--memory":
            options = new SortOptions
            {
                MaxMemoryPerChunk = ParseSize(args[++i]),
                MergeWidth = options.MergeWidth,
                TempDirectory = options.TempDirectory,
                BufferSize = options.BufferSize
            };
            break;
        case "--merge-width":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = int.Parse(args[++i]),
                TempDirectory = options.TempDirectory,
                BufferSize = options.BufferSize
            };
            break;
        case "--temp-dir":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = options.MergeWidth,
                TempDirectory = args[++i],
                BufferSize = options.BufferSize
            };
            break;
        case "--buffer":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = options.MergeWidth,
                TempDirectory = options.TempDirectory,
                BufferSize = (int)ParseSize(args[++i])
            };
            break;
    }
}

var inputInfo = new FileInfo(inputPath);
Console.WriteLine($"Input:  {inputPath} ({FormatSize(inputInfo.Length)})");
Console.WriteLine($"Output: {outputPath}");
Console.WriteLine($"Chunk memory budget: {FormatSize(options.MaxMemoryPerChunk)}");
Console.WriteLine($"Merge width: {options.MergeWidth}");
Console.WriteLine();

var progress = new Progress<string>(msg => Console.WriteLine(msg));

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
    Console.WriteLine("\nCancellation requested, finishing current operation...");
};

var sw = Stopwatch.StartNew();

try
{
    var sorter = new ExternalSorter(options);
    await sorter.SortAsync(inputPath, outputPath, progress, cts.Token);
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Sort cancelled by user.");
    return 130;
}

sw.Stop();

var outputInfo = new FileInfo(outputPath);
Console.WriteLine();
Console.WriteLine($"Completed in {sw.Elapsed:hh\\:mm\\:ss\\.fff}");
Console.WriteLine($"Output size: {FormatSize(outputInfo.Length)}");

return 0;

static long ParseSize(string input)
{
    input = input.Trim().ToUpperInvariant();

    if (input.EndsWith("GB"))
        return (long)(double.Parse(input[..^2]) * 1024 * 1024 * 1024);
    if (input.EndsWith("MB"))
        return (long)(double.Parse(input[..^2]) * 1024 * 1024);
    if (input.EndsWith("KB"))
        return (long)(double.Parse(input[..^2]) * 1024);

    return long.Parse(input);
}

static string FormatSize(long bytes) => bytes switch
{
    >= 1024L * 1024 * 1024 => $"{bytes / (1024.0 * 1024 * 1024):F2} GB",
    >= 1024L * 1024 => $"{bytes / (1024.0 * 1024):F2} MB",
    >= 1024L => $"{bytes / 1024.0:F2} KB",
    _ => $"{bytes} B"
};
