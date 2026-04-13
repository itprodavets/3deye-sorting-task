# Large File Sorter

External merge sort implementation for very large text files. Handles files that don't fit in memory (tested up to 100 GB).

## Problem

Sort a text file where each line follows the format `<Number>. <String>`:
- Primary sort: by the string part (alphabetically)
- Secondary sort: by the number (ascending, for equal strings)

## Project Structure

```
src/
  LargeFileSorter.Core         Core library (models, parser, sorter, generator)
  LargeFileSorter.Generator    CLI tool to generate test files
  LargeFileSorter.Sorter       CLI tool to sort files
tests/
  LargeFileSorter.Tests        Unit & integration tests (xUnit)
benchmarks/
  LargeFileSorter.Benchmarks   Performance benchmarks (BenchmarkDotNet)
```

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)

## Build

```bash
dotnet build
```

## Usage

### Generate a test file

```bash
dotnet run --project src/LargeFileSorter.Generator -- output.txt 1GB

# Options
dotnet run --project src/LargeFileSorter.Generator -- output.txt 500MB \
    --phrases 1000 \
    --max-number 999999 \
    --seed 42
```

### Sort a file

```bash
dotnet run --project src/LargeFileSorter.Sorter -- input.txt sorted.txt

# Options
dotnet run --project src/LargeFileSorter.Sorter -- input.txt sorted.txt \
    --memory 1GB \
    --merge-width 32 \
    --temp-dir /mnt/fast-ssd/tmp \
    --buffer 128KB
```

### Run tests

```bash
dotnet test
```

### Run benchmarks

```bash
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release
```

## Algorithm

The sorter uses a two-phase **external merge sort**:

### Phase 1 — Split & Sort
1. Read the input file sequentially, accumulating lines into a chunk.
2. When the chunk reaches the memory budget, sort it in-place and write to a temporary file.
3. While one chunk is being sorted (background thread), the next chunk is read from disk — overlapping I/O and CPU.
4. String deduplication within each chunk reduces memory pressure when many lines share the same text.

### Phase 2 — K-Way Merge
1. Open all sorted chunk files.
2. Use a `PriorityQueue` (min-heap) to efficiently select the smallest entry across all chunks.
3. If the chunk count exceeds `MergeWidth`, multi-level merging is used to stay within file handle limits.
4. The final merged result is streamed to the output file.

### Memory Management
- Chunk memory budget auto-tunes to ~25% of available RAM (capped at 2 GB).
- Buffered I/O (64 KB by default) reduces system call overhead.
- String pooling within chunks avoids duplicate allocations for repeated text values.
- Sorted chunk arrays are released before reading the next chunk.

### Performance Tuning

| Parameter       | Flag             | Effect                                        |
|-----------------|------------------|-----------------------------------------------|
| Chunk memory    | `--memory`       | Larger = fewer chunks, faster merge phase     |
| Merge width     | `--merge-width`  | Wider = fewer merge levels, more file handles |
| Buffer size     | `--buffer`       | Larger = fewer I/O syscalls                   |
| Temp directory  | `--temp-dir`     | Point to fast SSD for temp files              |

## License

MIT
