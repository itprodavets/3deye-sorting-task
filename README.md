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
    Sorting/                   Sort pipeline (ExternalSorter, MmfSorter, ChunkSorter,
                               ChunkMerger, BinaryChunkReader, BinaryChunkWriter, Utf8LineWriter)
    EntryIndex.cs              File offset index (28 bytes, no managed refs, NativeMemory)
    NativeBuffer.cs            Growable array via NativeMemory.AlignedAlloc (GC-invisible)
    LineEntry.cs               Core model with precomputed sort key
    LineParser.cs              UTF-8 & string parsing
    TextPool.cs                Zero-allocation string interning (AlternateLookup)
    SortOptions.cs             Configuration + HardwareProfile (auto-tuning)
    SizeFormatter.cs           Shared byte-size formatting utility
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

The easiest way to try everything — interactive menu with guided prompts:

```bash
chmod +x run.sh
./run.sh
```

The menu offers: generate a test file, sort a file, full pipeline (generate + sort), run tests, benchmarks, build, and clean.

## Usage

### 1. Generate a test file

Creates a file in the required `<Number>. <String>` format with random data.

```bash
dotnet run --project src/LargeFileSorter.Generator -- <output-file> <size>
```

**Arguments:**

| Argument | Required | Description |
|----------|----------|-------------|
| `<output-file>` | yes | Path to the file to create |
| `<size>` | yes | Target file size: `100KB`, `500MB`, `1GB`, etc. |

**Options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--phrases <count>` | 500 | Number of unique text phrases in the file |
| `--max-number <num>` | 100000 | Maximum value for the number part of each line |
| `--seed <value>` | random | Fixed seed for reproducible output |

**Examples:**

```bash
# Basic — generate a 1 GB test file
dotnet run --project src/LargeFileSorter.Generator -- data/test.txt 1GB

# Reproducible — same seed always produces the same file
dotnet run --project src/LargeFileSorter.Generator -- data/test.txt 500MB --seed 42

# Many unique phrases — more variety in text values
dotnet run --project src/LargeFileSorter.Generator -- data/test.txt 5GB \
    --phrases 10000 --max-number 999999
```

**Example output:**

```
Generating file: data/test.txt
Target size: 1.0 GB
Unique phrases: 500
Progress: 1.0 GB / 1.0 GB
Done in 00:00:03.214. Actual size: 1.0 GB
```

### 2. Sort a file

Sorts the generated file using external merge sort. By default, all parameters are auto-tuned to the available hardware — no manual configuration needed.

```bash
dotnet run --project src/LargeFileSorter.Sorter -- <input-file> <output-file>
```

**Arguments:**

| Argument | Required | Description |
|----------|----------|-------------|
| `<input-file>` | yes | Path to the unsorted input file |
| `<output-file>` | yes | Path where the sorted result will be written |

**Options (all optional — defaults auto-tune to hardware):**

| Flag | Default | Description |
|------|---------|-------------|
| `--memory <size>` | auto (~25% RAM) | Max memory per chunk, e.g. `512MB`, `1GB`, `4GB` |
| `--buffer <size>` | auto (1–16 MB) | I/O buffer size, e.g. `1MB`, `4MB`, `16MB` |
| `--workers <num>` | auto (1–4) | Number of concurrent sort workers |
| `--merge-width <num>` | 64 | Max files to merge in a single pass |
| `--temp-dir <path>` | system temp | Directory for temporary chunk files |
| `--strategy <name>` | auto | Sort strategy: `stream`, `mmf`, `auto` |

**Examples:**

```bash
# Basic — auto-tuned to hardware, no flags needed
dotnet run --project src/LargeFileSorter.Sorter -- data/test.txt data/sorted.txt

# Limit memory — useful if other processes need RAM
dotnet run --project src/LargeFileSorter.Sorter -- data/test.txt data/sorted.txt \
    --memory 1GB --workers 1

# Maximum performance — large buffers, all workers, fast temp drive
dotnet run --project src/LargeFileSorter.Sorter -- data/test.txt data/sorted.txt \
    --buffer 16MB --workers 4 --temp-dir /mnt/nvme/tmp

# Release mode for best performance (recommended for large files)
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt

# Force memory-mapped strategy (NativeMemory + MMF, zero managed allocations)
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --strategy mmf
```

**Example output (64 GB machine):**

```
Hardware: RAM: 64.0 GB, Cores: 16, Chunk budget: 8.0 GB, I/O buffer: 16 MB, Sort workers: 4

Input:  data/test.txt (5.0 GB)
Output: data/sorted.txt
Chunk memory budget: 8.0 GB
I/O buffer: 16 MB
Sort workers: 4
Merge width: 64

Phase 1: splitting and sorting chunks...
  chunk 0: 67,107,899 lines queued
  chunk 1: 67,107,899 lines queued
  chunk 2: 67,107,899 lines queued
  chunk 3: 21,496,295 lines (final)
Phase 2: merging 4 sorted chunks...
Done.

Completed in 00:00:29.626
Output size: 5.0 GB
```

### 3. Full pipeline (generate + sort)

The interactive menu (`./run.sh`, option 3) runs both steps in sequence and verifies the line count matches.

```bash
# Or manually:
dotnet run --project src/LargeFileSorter.Generator -c Release -- data/input.txt 1GB --seed 42
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/input.txt data/sorted.txt
```

### 4. Run tests

```bash
dotnet test

# Verbose output
dotnet test --verbosity normal
```

### 5. Run benchmarks

Compares sorting strategies and approaches:

```bash
# Strategy comparison: stream vs mmf
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*Strategy*'

# Approach comparison: naive vs sequential vs optimized
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*Sorting*'

# Run all benchmarks
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*'
```

### 6. Publish native binaries (NativeAOT)

Ahead-of-time compilation produces a self-contained native binary — no .NET runtime required, ~5 ms startup, ~2.7 MB binary size.

```bash
# Auto-detect platform (macOS/Linux/Windows, arm64/x64)
./run.sh  # option 7

# Or manually:
dotnet publish src/LargeFileSorter.Sorter -c Release -r osx-arm64 /p:PublishAot=true -o publish/
dotnet publish src/LargeFileSorter.Generator -c Release -r osx-arm64 /p:PublishAot=true -o publish/
```

**Supported runtime identifiers:** `osx-arm64`, `osx-x64`, `linux-arm64`, `linux-x64`, `win-x64`, `win-arm64`.

**Run the native binaries directly — no `dotnet` needed:**

```bash
./publish/LargeFileSorter.Generator data/test.txt 1GB --seed 42
./publish/LargeFileSorter.Sorter data/test.txt data/sorted.txt --strategy mmf
```

### Cancellation

The sorter supports graceful cancellation via `Ctrl+C`. It finishes the current operation and cleans up temporary files before exiting.

## Performance Results

Measured on Apple M4 Max (16 cores), 64 GB RAM, macOS, .NET 10.

### Throughput

| File Size | Lines | Chunks | Time | Throughput |
|-----------|-------|--------|------|------------|
| 1 GB | 44.6 M | 1 (single chunk) | 7.6 s | 134 MB/s |
| 1 GB | 44.6 M | 6 (256 MB chunks) | 6.3 s | 162 MB/s |
| 5 GB | 222.8 M | 4 (2 GB chunks) | 29.6 s | 173 MB/s |

> Extrapolated: **100 GB in ~10 minutes** at sustained 173 MB/s.

### Strategy Comparison: Stream vs MMF

Both strategies produce **byte-identical output** (verified with MD5). The table below compares sort time (excluding JIT warmup) on the same data:

**Single chunk (file fits in memory budget):**

| File Size | Phrases | Stream (JIT) | MMF (JIT) | Stream (AOT) | MMF (AOT) |
|-----------|---------|-------------|-----------|-------------|-----------|
| 200 MB | 500 | 2.19 s | 2.09 s | 2.76 s | **1.92 s** |
| 200 MB | 10,000 | 2.21 s | **2.14 s** | — | — |
| 1 GB | 500 | **7.66 s** | 9.63 s | 9.13 s | 10.44 s |

**Multi-chunk (6 chunks × 256 MB from 1 GB file):**

| Runtime | Stream | MMF | Winner |
|---------|--------|-----|--------|
| JIT | **6.33 s** (162 MB/s) | 10.42 s (98 MB/s) | Stream 1.65x |
| AOT | **7.06 s** (145 MB/s) | 11.29 s (91 MB/s) | Stream 1.60x |

**When to use which:**

| Scenario | Best Strategy | Why |
|----------|--------------|-----|
| Low-cardinality text (few unique phrases) | **Stream** | `TextPool` deduplicates strings; comparisons become pointer equality |
| High-cardinality text (many unique strings) | **MMF** | No `TextPool` overhead; 8-byte sort key resolves 90%+ without memory access |
| Small files (single chunk, < 500 MB) | **MMF** | Less overhead; no pipeline setup; zero GC pressure |
| Large files (multi-chunk, > 1 GB) | **Stream** | Concurrent pipeline overlaps I/O and sort; `Channel<T>` keeps all cores busy |
| Latency-sensitive (GC pauses unacceptable) | **MMF** | `NativeBuffer<EntryIndex>` is invisible to GC — zero pauses regardless of size |

The `auto` strategy (default) picks MMF when the file fits in a single chunk, and stream otherwise. This matches the benchmarks: MMF avoids pipeline overhead for small files, while stream's concurrent pipeline dominates on large multi-chunk workloads.

### JIT vs NativeAOT

| Metric | JIT (.NET 10) | NativeAOT |
|--------|--------------|-----------|
| Startup time | ~680 ms | **~5 ms** (136x faster) |
| Binary size | ~97 MB (with runtime) | **2.7 MB** (36x smaller) |
| 200 MB sort (mmf) | 2.09 s | **1.92 s** (8% faster) |
| 1 GB sort (stream) | **7.66 s** | 9.13 s (16% slower) |
| 1 GB multi-chunk (stream) | **6.33 s** | 7.06 s (10% slower) |
| Runtime optimization | TieredPGO re-compiles hot loops | Static compilation, no profiling |

**Recommendation:** Use **JIT for large files** — TieredPGO optimizes the sort comparison loop with runtime profiling data, giving 10–16% better throughput on sustained workloads. Use **NativeAOT for deployment** where instant startup and small binary size matter (containers, CLI tools, serverless).

### Correctness Verification

- **Line count**: input and output always match exactly.
- **Determinism**: single-chunk and multi-chunk modes produce **byte-identical** output.
- **Sort order**: verified alphabetical primary sort, numerical secondary sort.
- **Strategies**: stream and MMF produce **byte-identical** output for the same input.
- **Runtimes**: JIT and NativeAOT produce **byte-identical** output.
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
2. Separator detection uses `SearchValues<byte>` (.NET 8+) for SIMD-accelerated dot search; numbers are parsed via `Utf8Parser.TryParse` — only the `Text` portion becomes a managed string.
3. Filled chunks are sent through a **bounded `Channel`** to 1–4 sort workers — reader and sort workers run concurrently, overlapping disk I/O and CPU.
4. Each sort worker uses **parallel merge sort**: splits the chunk into segments, sorts each via `Span<T>.Sort()` inside `Parallel.For` (with `MaxDegreeOfParallelism` to prevent oversubscription), then k-way merges the sorted segments via `PriorityQueue`.
5. Sorted chunks are written to temp files in **binary format** (`BinaryWriter`: `Int64` + length-prefixed UTF-8 string) — eliminates text re-parsing during the merge phase.
6. Chunk arrays are returned to a **custom `ArrayPool<LineEntry>`** (max 32M elements) after writing. The default `Shared` pool silently drops arrays > 1M elements, so a dedicated pool is required for actual reuse.
7. **Zero-allocation string deduplication** via `TextPool` — uses .NET 9+ `Dictionary.GetAlternateLookup<ReadOnlySpan<char>>` to check for duplicate text without allocating a string first. `stackalloc` is used for the UTF-8 → char conversion when text is ≤ 512 chars; larger text rents from `ArrayPool<char>`. Memory estimation counts string bytes only on first occurrence, resulting in fewer, larger chunks and less merge overhead.

### Phase 2 — K-Way Merge (buffered)
1. Each sorted chunk is wrapped in a **`BinaryChunkReader`** that reads binary records in batches of 8,192, using `BinaryReader.ReadInt64()` + `ReadString()` — no text parsing at all.
2. A `PriorityQueue` (min-heap) selects the smallest entry across all chunk readers.
3. Output is written via `Utf8LineWriter` — formats numbers directly to UTF-8 with `Utf8Formatter.TryFormat` (no `long.ToString()` allocation), and uses a pooled byte buffer with bulk flush to minimize syscalls.
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
| `[SkipLocalsInit]` | No stack zeroing on hot paths (TextPool, Utf8LineWriter, LineEntry, ChunkSorter) |
| `SearchValues<byte>` | SIMD-accelerated separator search — SSE2/AVX2/AVX-512 per platform |
| `Span<T>.Sort()` | Span-based sort avoids Array.Sort bounds validation overhead |
| ThreadPool pre-warm | SetMinThreads = ProcessorCount to avoid injection stall |

### Memory Management
- Chunk memory budget auto-tunes to ~25% of available RAM (cap scales with machine: 1 GB on small, up to 8 GB on 64 GB+).
- Custom `ArrayPool<LineEntry>` (32M max) reuses chunk arrays across iterations — the default `Shared` pool only handles up to ~1M elements.
- Bounded channel (capacity = worker count) limits in-flight chunks, keeping peak memory predictable.
- `TextPool` with `AlternateLookup<ReadOnlySpan<char>>` deduplicates strings without allocating — uses `stackalloc` for small text, `ArrayPool<char>` for large text.
- `Utf8LineWriter` formats output directly to UTF-8 using `Utf8Formatter.TryFormat` — eliminates `long.ToString()` and StreamWriter char→byte encoding overhead.
- PipeReader reads large blocks (auto-scaled: 1–16 MB) with zero per-line allocation for the full input line.
- Binary chunk format avoids text re-parsing during merge — only the final output writes text.
- All output uses cached `UTF8Encoding(false)` — no BOM in generated files, no per-writer allocation.

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

### Sort Strategies

Two `IFileSorter` implementations (Strategy pattern), selectable via `--strategy`:

| Strategy | Flag | When to use | How it works |
|----------|------|-------------|--------------|
| **stream** | `--strategy stream` | High-throughput (default for large files) | PipeReader → `TextPool` string dedup → `Channel<T>` → parallel sort → binary chunks |
| **mmf** | `--strategy mmf` | Zero managed allocations | `MemoryMappedFile` → `NativeBuffer<EntryIndex>` (NativeMemory) → pointer-based sort → binary chunks |
| **auto** | `--strategy auto` (default) | Best of both | File ≤ chunk budget → MMF; larger → stream pipeline |

The stream strategy excels on data with repeating text (string dedup via `TextPool` keeps unique count low) and multi-chunk workloads (concurrent pipeline). The MMF strategy excels on small files that fit in one chunk and on high-cardinality data where millions of unique strings would cause GC pressure. See [Strategy Comparison](#strategy-comparison-stream-vs-mmf) for detailed benchmarks.

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

### 3. Optimized External Sort — Stream Strategy (PipeReader + Channel)

PipeReader + binary chunks + concurrent pipeline + parallel sort + sort key + GC tuning.

| Pros | Cons |
|------|------|
| 173 MB/s on 5 GB — **~10 min for 100 GB** | More complex implementation |
| PipeReader — zero per-line allocation for input parsing | String dedup less effective for high-cardinality data |
| Zero-allocation string deduplication via `AlternateLookup` (.NET 9+) | |
| Concurrent pipeline — I/O and sort overlap via `Channel<T>` | |
| 1–4 sort workers with parallel intra-chunk sort | |
| Custom ArrayPool (32M max) — actual reuse of large arrays | |
| Precomputed sort key — ~80% of comparisons via single ulong | |
| `SearchValues<byte>` SIMD separator search | |
| `[SkipLocalsInit]` on hot paths — no JIT stack zeroing | |
| Auto-tuned to hardware (chunk size, I/O buffers, workers) | |

### 4. Optimized External Sort — MMF Strategy (MemoryMappedFile + NativeMemory)

MemoryMappedFile + NativeBuffer&lt;EntryIndex&gt; + pointer-based sort + zero managed allocations.

| Pros | Cons |
|------|------|
| **Zero managed allocations** during sort — GC never sees the data | Sequential chunk processing (no concurrent pipeline) |
| `NativeBuffer<EntryIndex>` — 28-byte structs in native memory | No string deduplication — every comparison reads file bytes |
| 8-byte UTF-8 prefix sort key — 90%+ resolved without MMF access | Slower on low-cardinality data where TextPool wins |
| `MemoryMappedFile` — OS manages page cache, no explicit reads | |
| `[SkipLocalsInit]` + `SearchValues<byte>` on hot paths | |
| Zero GC pauses regardless of data size or cardinality | |
| NativeAOT-friendly — no reflection, no dynamic code | |
| Binary chunk output — reuses same `ChunkMerger` for merge phase | |

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
| **Single Responsibility** | `ExternalSorter` orchestrates; `ChunkSorter`, `ChunkMerger`, `BinaryChunkReader`, `BinaryChunkWriter` each handle one concern; `TextPool` owns string deduplication; `Utf8LineWriter` owns UTF-8 output formatting; `SizeFormatter` centralizes size formatting; `HardwareProfile` encapsulates diagnostics |
| **Open/Closed** | `ExternalSorter` and `MmfSorter` implement `IFileSorter` — new strategies added without modifying existing code; new chunk formats implement `IChunkReader` |
| **Liskov Substitution** | All interface implementations honor their contracts |
| **Interface Segregation** | `IFileSorter` (sort), `IFileGenerator` (generate), `IChunkReader` (read) — each focused on one operation |
| **Dependency Inversion** | `ChunkMerger` depends on `Func<string, IChunkReader>` factory, not concrete `BinaryChunkReader` |

## License

MIT
