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

## Quick Start

Interactive menu with all operations:

```bash
./run.sh
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
1. A **reader task** reads the input file using `System.IO.Pipelines` (`PipeReader`), parsing lines directly from UTF-8 byte buffers — no per-line string allocation for the full line.
2. Lines are parsed via `Utf8Parser.TryParse` + `Encoding.UTF8.GetString` — only the `Text` portion becomes a managed string.
3. Filled chunks are sent through a **bounded `Channel`** to a sort worker — the reader and sort worker run concurrently, overlapping disk I/O and CPU.
4. The sort worker uses **parallel merge sort**: splits the chunk into segments, sorts each via `Parallel.For`, then k-way merges the sorted segments via `PriorityQueue`.
5. Sorted chunks are written to temp files in **binary format** (`BinaryWriter`: `Int64` + length-prefixed UTF-8 string) — eliminates text re-parsing during the merge phase.
6. Chunk arrays are returned to `ArrayPool` after writing — no GC pressure from repeated large array allocations.
7. String deduplication via a per-chunk dictionary avoids storing multiple copies of the same text in memory.

### Phase 2 — K-Way Merge (buffered)
1. Each sorted chunk is wrapped in a **`BinaryChunkReader`** that reads binary records in batches of 8192, using `BinaryReader.ReadInt64()` + `ReadString()` — no text parsing at all.
2. A `PriorityQueue` (min-heap) selects the smallest entry across all chunk readers.
3. Output is written with direct formatting to avoid allocations.
4. If chunk count exceeds `MergeWidth`, **multi-level merging** groups chunks to stay within file handle limits.

### Comparison Optimization
`LineEntry` stores a **precomputed sort key**: the first 4 UTF-16 characters packed as a big-endian `ulong`. This resolves ~80% of comparisons with a single integer compare, falling back to full `string.Compare` only when prefixes collide.

### Memory Management
- Chunk memory budget auto-tunes to ~25% of available RAM (capped at 2 GB).
- `ArrayPool<LineEntry>` reuses chunk arrays across iterations — avoids LOH allocations.
- Bounded channel (capacity 1) limits in-flight chunks to ~3 (reading + queued + sorting), keeping peak memory predictable.
- String pooling within chunks deduplicates repeated text values.
- PipeReader reads large blocks with zero per-line allocation for the full input line.
- Binary chunk format avoids text re-parsing during merge — only the final output writes text.
- Buffered I/O (64 KB default) on all file streams reduces system call overhead.

### Performance Tuning

| Parameter       | Flag             | Effect                                        |
|-----------------|------------------|-----------------------------------------------|
| Chunk memory    | `--memory`       | Larger = fewer chunks, faster merge phase     |
| Merge width     | `--merge-width`  | Wider = fewer merge levels, more file handles |
| Buffer size     | `--buffer`       | Larger = fewer I/O syscalls                   |
| Temp directory  | `--temp-dir`     | Point to fast SSD for temp files              |

## Algorithm Comparison

The benchmark suite (`dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release`) compares four approaches to justify the design:

### 1. Naive In-Memory Sort

Read entire file → `Array.Sort` → write output.

| Pros | Cons |
|------|------|
| Fastest for small files (no chunk overhead) | O(file_size) memory — **impossible for ~100 GB** |
| Simplest implementation | `OutOfMemoryException` on large inputs |
| Single pass, no temp files | No concurrency — CPU idle during I/O |
| | GC pressure from large arrays on LOH |

### 2. Sequential External Sort

Read chunks → sort one at a time → write temp files → merge.

| Pros | Cons |
|------|------|
| Handles files larger than RAM | CPU idle while reading/writing (no pipelining) |
| Predictable, simple control flow | Single-threaded sort — wastes multi-core CPUs |
| Bounded memory usage | Per-line I/O in merge — no batching |
| | `ToString()` allocation per line on writes |
| | No array pooling — LOH allocation per chunk |

### 3. Optimized External Sort (our solution)

PipeReader + binary chunks + concurrent pipeline + parallel sort + sort key.

| Pros | Cons |
|------|------|
| Handles 100 GB+ files within bounded memory | More complex implementation |
| PipeReader — zero per-line allocation for input parsing | Slight overhead for very small files |
| UTF-8 byte parsing — avoids full-line string materialization | |
| Binary chunk format — no text re-parsing during merge | |
| Precomputed sort key — ~80% of comparisons via single ulong | |
| Concurrent pipeline — disk I/O overlaps CPU | |
| Parallel merge sort within chunks (multi-core) | |
| ArrayPool — no repeated LOH allocations | |
| Buffered binary merge (8K records/batch) — fewer syscalls | |
| String deduplication — less memory for repeated text | |
| Auto-tuned chunk size (~25% of RAM, capped at 2 GB) | |

### Why external merge sort?

For a ~100 GB file that does not fit in memory, external merge sort is the standard approach:

- **Merge sort** is naturally suited for sequential disk access (reads and writes are linear, not random).
- **K-way merge** with `PriorityQueue` is O(N log K) where K = number of chunks — nearly linear in total lines.
- Alternatives like **external quicksort** require random access patterns that perform poorly on disk.
- **External radix sort** could work but depends on the data distribution and is harder to generalize for variable-length string keys.

The concurrent pipeline and parallel chunk sorting are orthogonal improvements that maximize hardware utilization without changing the algorithmic complexity.

## License

MIT
