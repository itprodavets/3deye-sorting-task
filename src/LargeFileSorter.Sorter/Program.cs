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
    Console.WriteLine("  --buffer <size>       I/O buffer size, e.g. 1MB, 4MB (default: auto)");
    Console.WriteLine("  --workers <num>       Concurrent sort workers (default: auto)");
    Console.WriteLine("  --strategy <name>     Sort strategy: stream, mmf, auto (default: auto)");
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
var strategy = "auto";
for (var i = 2; i < args.Length - 1; i++)
{
    switch (args[i])
    {
        case "--strategy":
            strategy = args[++i].ToLowerInvariant();
            break;
        case "--memory":
            options = new SortOptions
            {
                MaxMemoryPerChunk = ParseSize(args[++i]),
                MergeWidth = options.MergeWidth,
                TempDirectory = options.TempDirectory,
                BufferSize = options.BufferSize,
                SortWorkers = options.SortWorkers
            };
            break;
        case "--merge-width":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = int.Parse(args[++i]),
                TempDirectory = options.TempDirectory,
                BufferSize = options.BufferSize,
                SortWorkers = options.SortWorkers
            };
            break;
        case "--temp-dir":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = options.MergeWidth,
                TempDirectory = args[++i],
                BufferSize = options.BufferSize,
                SortWorkers = options.SortWorkers
            };
            break;
        case "--buffer":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = options.MergeWidth,
                TempDirectory = options.TempDirectory,
                BufferSize = (int)ParseSize(args[++i]),
                SortWorkers = options.SortWorkers
            };
            break;
        case "--workers":
            options = new SortOptions
            {
                MaxMemoryPerChunk = options.MaxMemoryPerChunk,
                MergeWidth = options.MergeWidth,
                TempDirectory = options.TempDirectory,
                BufferSize = options.BufferSize,
                SortWorkers = int.Parse(args[++i])
            };
            break;
    }
}

var hw = SortOptions.DetectHardware();
Console.WriteLine($"Hardware: {hw}");
Console.WriteLine();

var inputInfo = new FileInfo(inputPath);
Console.WriteLine($"Input:  {inputPath} ({SizeFormatter.Format(inputInfo.Length)})");
Console.WriteLine($"Output: {outputPath}");
Console.WriteLine($"Chunk memory budget: {SizeFormatter.Format(options.MaxMemoryPerChunk)}");
Console.WriteLine($"I/O buffer: {SizeFormatter.Format(options.BufferSize)}");
Console.WriteLine($"Sort workers: {options.SortWorkers}");
Console.WriteLine($"Merge width: {options.MergeWidth}");

// Strategy selection: mmf (memory-mapped, native memory), stream (PipeReader), auto (pick best)
IFileSorter sorter = strategy switch
{
    "mmf" => new MmfSorter(options),
    "stream" => new ExternalSorter(options),
    "auto" => inputInfo.Length <= options.MaxMemoryPerChunk
        ? new MmfSorter(options)  // file fits in one chunk → MMF is optimal
        : new ExternalSorter(options), // multi-chunk → stream pipeline is better
    _ => throw new ArgumentException($"Unknown strategy: {strategy}. Use: stream, mmf, auto")
};

var strategyName = sorter is MmfSorter ? "mmf (memory-mapped + native memory)" : "stream (PipeReader + Channel)";
Console.WriteLine($"Strategy: {strategyName}");
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
Console.WriteLine($"Output size: {SizeFormatter.Format(outputInfo.Length)}");

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

