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

### Phase 1 — Split & Sort (concurrent pipeline)
1. A **reader task** reads the input file using sync `ReadLine` (avoids per-line async state machine overhead) and fills chunk arrays rented from `ArrayPool<LineEntry>`.
2. Filled chunks are sent through a **bounded `Channel`** to a sort worker — the reader and sort worker run concurrently, overlapping disk I/O and CPU.
3. The sort worker uses **parallel merge sort**: splits the chunk into segments, sorts each via `Parallel.For`, then k-way merges the sorted segments via `PriorityQueue`.
4. Sorted chunks are written to temp files using **direct formatting** (`TextWriter.Write(long)` + `Write(string)`) instead of `ToString()`, eliminating per-line string allocations.
5. Chunk arrays are returned to `ArrayPool` after writing — no GC pressure from repeated large array allocations.
6. String deduplication via a per-chunk dictionary avoids storing multiple copies of the same text in memory.

### Phase 2 — K-Way Merge (buffered)
1. Each sorted chunk is wrapped in a **`BufferedChunkReader`** that pre-fetches 8192 lines at a time into a local buffer, reducing I/O syscall frequency by orders of magnitude vs. per-line reads.
2. A `PriorityQueue` (min-heap) selects the smallest entry across all chunk readers.
3. Output is written with direct formatting to avoid allocations.
4. If chunk count exceeds `MergeWidth`, **multi-level merging** groups chunks to stay within file handle limits.

### Memory Management
- Chunk memory budget auto-tunes to ~25% of available RAM (capped at 2 GB).
- `ArrayPool<LineEntry>` reuses chunk arrays across iterations — avoids LOH allocations.
- Bounded channel (capacity 1) limits in-flight chunks to ~3 (reading + queued + sorting), keeping peak memory predictable.
- String pooling within chunks deduplicates repeated text values.
- Buffered I/O (64 KB default) on all file streams reduces system call overhead.
- Sync I/O in hot loops avoids async state machine allocations per line.

### Performance Tuning

| Parameter       | Flag             | Effect                                        |
|-----------------|------------------|-----------------------------------------------|
| Chunk memory    | `--memory`       | Larger = fewer chunks, faster merge phase     |
| Merge width     | `--merge-width`  | Wider = fewer merge levels, more file handles |
| Buffer size     | `--buffer`       | Larger = fewer I/O syscalls                   |
| Temp directory  | `--temp-dir`     | Point to fast SSD for temp files              |

## License

MIT
