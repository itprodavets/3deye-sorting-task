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
    Console.WriteLine("  --threads <num>       Total parallelism budget shared by all strategies");
    Console.WriteLine("                        (workers × segments-per-chunk). Default: logical cores.");
    Console.WriteLine("                        Use the same value across stream/mmf/shard for fair comparison.");
    Console.WriteLine("  --strategy <name>     Sort strategy: stream, mmf, shard, auto (default: auto)");
    return 1;
}

var inputPath = args[0];
var outputPath = args[1];

if (!File.Exists(inputPath))
{
    Console.Error.WriteLine($"Error: file not found — {inputPath}");
    return 1;
}

// SortOptions is a readonly record struct — `with` expressions keep each option
// setter to a single line and mean adding a new knob doesn't force us to re-list
// every other property. Much less error-prone than the manual field-copy pattern.
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
            options = options with { MaxMemoryPerChunk = ParseSize(args[++i]) };
            break;
        case "--merge-width":
            options = options with { MergeWidth = int.Parse(args[++i]) };
            break;
        case "--temp-dir":
            options = options with { TempDirectory = args[++i] };
            break;
        case "--buffer":
            options = options with { BufferSize = (int)ParseSize(args[++i]) };
            break;
        case "--workers":
            options = options with { SortWorkers = int.Parse(args[++i]) };
            break;
        case "--threads":
            options = options with { MaxDegreeOfParallelism = int.Parse(args[++i]) };
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
Console.WriteLine($"Parallelism budget: {options.MaxDegreeOfParallelism}");
Console.WriteLine($"Merge width: {options.MergeWidth}");

// Strategy selection:
//   mmf    — memory-mapped + native memory, best when input fits in one chunk
//   stream — PipeReader + Channel + single-threaded k-way merge, the workhorse
//   shard  — stream Phase 1 + partitioned parallel merge, best on multi-core for large files
//   auto   — pick automatically based on file size and chunk budget
IFileSorter sorter = strategy switch
{
    "mmf" => new MmfSorter(options),
    "stream" => new ExternalSorter(options),
    "shard" => new ShardSorter(options),
    "auto" => inputInfo.Length <= options.MaxMemoryPerChunk
        ? new MmfSorter(options)                                    // single chunk → MMF
        : inputInfo.Length >= options.MaxMemoryPerChunk * 4L        // many chunks + multi-core
          && options.MaxDegreeOfParallelism >= 4
            ? new ShardSorter(options)                              // → parallel merge
            : new ExternalSorter(options),                          // few chunks → plain merge
    _ => throw new ArgumentException($"Unknown strategy: {strategy}. Use: stream, mmf, shard, auto")
};

var strategyName = sorter switch
{
    MmfSorter => "mmf (memory-mapped + native memory)",
    ShardSorter => "shard (stream Phase 1 + partitioned parallel merge)",
    _ => "stream (PipeReader + Channel)"
};
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

