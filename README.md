# Large File Sorter

External merge sort implementation for very large text files. Handles files that don't fit in memory (verified on 5 GB, designed for 100 GB+).

## Problem

Sort a text file where each line follows the format `<Number>. <String>`:
- Primary sort: by the string part (alphabetically)
- Secondary sort: by the number (ascending, for equal strings)

## Project Structure

```
src/
  LargeFileSorter.Core/
    Abstractions/              Interfaces (IFileSorter, IFileGenerator, IChunkReader)
    Sorting/                   Sort pipeline (ExternalSorter, ChunkSorter, ChunkMerger,
                               BinaryChunkReader, BinaryChunkWriter)
    LineEntry.cs               Core model with precomputed sort key
    LineParser.cs              UTF-8 & string parsing
    SortOptions.cs             Sorter configuration
    GeneratorOptions.cs        Generator configuration
    FileGenerator.cs           Test file generator
  LargeFileSorter.Generator/   CLI tool to generate test files
  LargeFileSorter.Sorter/      CLI tool to sort files
tests/
  LargeFileSorter.Tests/
    Unit/                      Isolated tests (LineEntry, LineParser, ChunkSorter, BinaryChunkIO)
    Integration/               File I/O tests (ExternalSorter, FileGenerator)
benchmarks/
  LargeFileSorter.Benchmarks/  Performance benchmarks (BenchmarkDotNet)
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
    --buffer 4MB \
    --workers 2
```

### Run tests

```bash
dotnet test
```

### Run benchmarks

```bash
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release
```

## Performance Results

Measured on Apple M4 Max (16 cores), 64 GB RAM, macOS, .NET 10.

### Throughput

| File Size | Lines | Chunks | Time | Throughput |
|-----------|-------|--------|------|------------|
| 1 GB | 44.5 M | 1 (single chunk) | 8.0 s | 128 MB/s |
| 1 GB | 44.5 M | 6 (256 MB chunks) | 7.8 s | 131 MB/s |
| 5 GB | 224.5 M | 4 (2 GB chunks) | 36.2 s | 141 MB/s |

> Extrapolated: **100 GB in ~12 minutes** at sustained 141 MB/s.

### Correctness Verification

- **Line count**: input and output always match exactly.
- **Determinism**: single-chunk and multi-chunk modes produce **byte-identical** output.
- **Sort order**: verified alphabetical primary sort, numerical secondary sort.
- **No BOM**: output is clean UTF-8 without byte order mark.

### Resource Usage

| Resource | Behavior |
|----------|----------|
| Peak memory | ~25% of available RAM (auto-tuned, capped per machine — up to 8 GB per chunk on 64 GB+) |
| Temp disk | ~1x input size during sort (binary chunks are slightly larger than text) |
| CPU | N sort workers (1–4) with parallel intra-chunk sort, avoids oversubscription |
| File handles | Bounded by MergeWidth (default: 64); multi-level merge if exceeded |

## Algorithm

The sorter uses a two-phase **external merge sort**:

### Phase 1 — Split & Sort (concurrent pipeline)
1. A **reader task** reads the input file using `System.IO.Pipelines` (`PipeReader`), parsing lines directly from UTF-8 byte buffers — no per-line string allocation for the full line.
2. Lines are parsed via `Utf8Parser.TryParse` + `Encoding.UTF8.GetString` — only the `Text` portion becomes a managed string.
3. Filled chunks are sent through a **bounded `Channel`** to 1–4 sort workers — reader and sort workers run concurrently, overlapping disk I/O and CPU.
4. Each sort worker uses **parallel merge sort**: splits the chunk into segments, sorts each via `Parallel.For` (with `MaxDegreeOfParallelism` to prevent oversubscription), then k-way merges the sorted segments via `PriorityQueue`.
5. Sorted chunks are written to temp files in **binary format** (`BinaryWriter`: `Int64` + length-prefixed UTF-8 string) — eliminates text re-parsing during the merge phase.
6. Chunk arrays are returned to a **custom `ArrayPool<LineEntry>`** (max 32M elements) after writing. The default `Shared` pool silently drops arrays > 1M elements, so a dedicated pool is required for actual reuse.
7. String deduplication via a per-chunk dictionary avoids storing multiple copies of the same text in memory. Memory estimation only counts string bytes on first occurrence, resulting in fewer, larger chunks and less merge overhead.

### Phase 2 — K-Way Merge (buffered)
1. Each sorted chunk is wrapped in a **`BinaryChunkReader`** that reads binary records in batches of 8,192, using `BinaryReader.ReadInt64()` + `ReadString()` — no text parsing at all.
2. A `PriorityQueue` (min-heap) selects the smallest entry across all chunk readers.
3. Output is written with direct formatting to avoid allocations.
4. If chunk count exceeds `MergeWidth`, **multi-level merging** groups chunks to stay within file handle limits.

### Comparison Optimization
`LineEntry` stores a **precomputed sort key**: the first 4 UTF-16 characters packed as a big-endian `ulong` with `[MethodImpl(AggressiveInlining)]`. This resolves ~80% of comparisons with a single integer compare, falling back to full `string.Compare` only when prefixes collide.

### GC & Runtime Tuning

| Optimization | Purpose |
|-------------|---------|
| Server GC | One heap per logical core, parallel collection threads |
| Concurrent GC | Background Gen2 — application threads never block |
| RetainVM | Keep committed pages after GC; next chunk reuses the same memory without a kernel call |
| SustainedLowLatency | Suppress full blocking Gen2 during Phase 1 (Gen0/Gen1 still run) |
| GC.Collect between phases | Hint to reclaim Phase 1 allocations before I/O-heavy merge |
| TieredPGO | JIT re-compiles hot methods using runtime profiling data |
| AggressiveInlining | CompareTo inlined into sort inner loop |
| ThreadPool pre-warm | SetMinThreads = ProcessorCount to avoid injection stall |

### Memory Management
- Chunk memory budget auto-tunes to ~25% of available RAM (cap scales with machine: 1 GB on small, up to 8 GB on 64 GB+).
- Custom `ArrayPool<LineEntry>` (32M max) reuses chunk arrays across iterations — the default `Shared` pool only handles up to ~1M elements.
- Bounded channel (capacity = worker count) limits in-flight chunks, keeping peak memory predictable.
- String pooling within chunks deduplicates repeated text values; memory estimation counts string bytes only once per unique text.
- PipeReader reads large blocks (auto-scaled: 1–16 MB) with zero per-line allocation for the full input line.
- Binary chunk format avoids text re-parsing during merge — only the final output writes text.
- All output uses `UTF8Encoding(false)` — no BOM in generated files.

### Hardware Auto-Tuning

All key parameters scale automatically to the machine's available RAM at startup. No manual tuning is needed for typical workloads — the sorter prints its detected hardware profile before starting:

```
Hardware: RAM: 64.0 GB, Cores: 16, Chunk budget: 8.0 GB, I/O buffer: 16 MB, Sort workers: 4
```

| Parameter | < 8 GB RAM | 8 GB | 16 GB | 32 GB | 64 GB+ |
|-----------|-----------|------|-------|-------|--------|
| Chunk memory cap | 1 GB | 2 GB | 2 GB | 4 GB | 8 GB |
| I/O buffer | 1 MB | 2 MB | 4 MB | 8 MB | 16 MB |

Larger I/O buffers reduce syscall overhead on NVMe SSDs (peak sequential throughput at 4–16 MB). Larger chunks reduce the number of merge passes, directly improving total sort time.

Disk space is validated before sorting starts — the sorter requires at least 1.2× the input file size as free space in the temp directory.

### Performance Tuning

| Parameter       | Flag             | Default | Effect                                        |
|-----------------|------------------|---------|-----------------------------------------------|
| Chunk memory    | `--memory`       | auto (~25% RAM, capped) | Larger = fewer chunks, faster merge phase     |
| Merge width     | `--merge-width`  | 64      | Wider = fewer merge levels, more file handles |
| Buffer size     | `--buffer`       | auto (1–16 MB) | Larger = fewer I/O syscalls                   |
| Sort workers    | `--workers`      | auto (1–4)  | More workers = higher CPU usage, faster sort  |
| Temp directory  | `--temp-dir`     | system temp | Point to fast SSD for temp files              |

## Algorithm Comparison

The benchmark suite (`dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release`) compares three approaches to justify the design:

### 1. Naive In-Memory Sort

Read entire file into memory → `Array.Sort` → write output.

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

### 3. Optimized External Sort (this implementation)

PipeReader + binary chunks + concurrent pipeline + parallel sort + sort key + GC tuning.

| Pros | Cons |
|------|------|
| 141 MB/s on 5 GB — **~12 min for 100 GB** | More complex implementation |
| PipeReader — zero per-line allocation for input parsing | |
| UTF-8 byte parsing — avoids full-line string materialization | |
| Binary chunk format — no text re-parsing during merge | |
| Precomputed sort key — ~80% of comparisons via single ulong | |
| 1–4 concurrent sort workers with parallel intra-chunk sort | |
| Custom ArrayPool (32M max) — actual reuse of large arrays | |
| Buffered binary merge (8K records/batch) — fewer syscalls | |
| String deduplication — less memory for repeated text | |
| Server GC + SustainedLowLatency + RetainVM — minimal pauses | |
| Auto-tuned chunk size (~25% of RAM, capped per machine) | |
| Auto-scaled I/O buffers (1–16 MB based on available RAM) | |
| Disk space validation before sort starts | |

### Why external merge sort?

For a ~100 GB file that does not fit in memory, external merge sort is the standard approach:

- **Merge sort** is naturally suited for sequential disk access (reads and writes are linear, not random).
- **K-way merge** with `PriorityQueue` is O(N log K) where K = number of chunks — nearly linear in total lines.
- Alternatives like **external quicksort** require random access patterns that perform poorly on disk.
- **External radix sort** could work but depends on the data distribution and is harder to generalize for variable-length string keys.

The concurrent pipeline, parallel chunk sorting, and GC tuning are orthogonal improvements that maximize hardware utilization without changing the algorithmic complexity.

## Design Principles

The codebase follows **SOLID** principles:

| Principle | Implementation |
|-----------|---------------|
| **Single Responsibility** | `ExternalSorter` orchestrates; `ChunkSorter`, `ChunkMerger`, `BinaryChunkReader`, `BinaryChunkWriter` each handle one concern |
| **Open/Closed** | New sort strategies can implement `IFileSorter`; new chunk formats can implement `IChunkReader` |
| **Liskov Substitution** | All interface implementations honor their contracts |
| **Interface Segregation** | `IFileSorter` (sort), `IFileGenerator` (generate), `IChunkReader` (read) — each focused on one operation |
| **Dependency Inversion** | `ChunkMerger` depends on `Func<string, IChunkReader>` factory, not concrete `BinaryChunkReader` |

## License

MIT
