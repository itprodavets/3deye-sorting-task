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
    Abstractions/              Interfaces (IFileSorter, IFileGenerator, IChunkReader, IRawChunkReader)
    Sorting/                   Sort pipeline (ExternalSorter, MmfSorter, ChunkSorter,
                               ChunkMerger, BinaryChunkReader, BinaryChunkWriter,
                               BinaryRawChunkReader, RawLineEntry, Utf8LineWriter)
    EntryIndex.cs              File offset index (28 bytes, no managed refs, NativeMemory)
    NativeBuffer.cs            Growable array via NativeMemory.AlignedAlloc (GC-invisible)
    LineEntry.cs               Core model with precomputed sort key (UTF-16 prefix)
    LineParser.cs              UTF-8 & string parsing
    TextPool.cs                Zero-allocation string interning (AlternateLookup)
    SortOptions.cs             Configuration + HardwareProfile (readonly record struct, auto-tuning)
    SizeFormatter.cs           Shared byte-size formatting utility
    GeneratorOptions.cs        Generator configuration (readonly record struct)
    FileGenerator.cs           Test file generator
  LargeFileSorter.Generator/   CLI tool to generate test files
  LargeFileSorter.Sorter/      CLI tool to sort files
tests/
  LargeFileSorter.Tests/
    Unit/                      Isolated tests (LineEntry, LineParser, ChunkSorter, BinaryChunkIO,
                               BinaryRawChunkReader, TextPool, Utf8LineWriter)
    Integration/               File I/O tests (ExternalSorter, MmfSorter, FileGenerator)
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

The menu offers: generate a test file, sort a file, full pipeline (generate + sort), run tests, benchmarks, build, publish native binaries (NativeAOT), and clean.

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

**Example output — auto strategy on 1 GB file (64 GB machine):**

```
Hardware: RAM: 64.0 GB, Cores: 16, Chunk budget: 8.0 GB, I/O buffer: 16 MB, Sort workers: 4

Input:  data/test.txt (1.0 GB)
Output: data/sorted.txt
Chunk memory budget: 8.0 GB
I/O buffer: 16 MB
Sort workers: 4
Merge width: 64
Strategy: mmf (memory-mapped + native memory)

Phase 1: indexing and sorting (memory-mapped)...
  chunk 0: 44,562,938 lines indexed
Done (single chunk, memory-mapped).

Completed in 00:00:09.633
Output size: 1.0 GB
```

**Example output — stream strategy on 1 GB file with 256 MB chunks:**

```
Hardware: RAM: 64.0 GB, Cores: 16, Chunk budget: 8.0 GB, I/O buffer: 16 MB, Sort workers: 4

Input:  data/test.txt (1.0 GB)
Output: data/sorted.txt
Chunk memory budget: 256 MB
I/O buffer: 16 MB
Sort workers: 4
Merge width: 64
Strategy: stream (PipeReader + Channel)

Phase 1: splitting and sorting chunks...
  chunk 0: 8,387,643 lines queued
  chunk 1: 8,387,643 lines queued
  chunk 2: 8,387,643 lines queued
  chunk 3: 8,387,643 lines queued
  chunk 4: 8,387,643 lines queued
  chunk 5: 2,624,723 lines (final)
Phase 2: merging 6 sorted chunks...
Done.

Completed in 00:00:06.331
Output size: 1.0 GB
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

Ahead-of-time compilation produces a self-contained native binary — no .NET runtime required, near-instant startup, minimal binary footprint.

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

The tables below are **regenerated automatically** by [`.github/workflows/benchmark.yml`](.github/workflows/benchmark.yml) on every push to `main`. No hand-edited numbers — every figure comes from a fresh CI run on the same commit that is currently at `HEAD`.

### Test Results

<!-- TESTS:START -->
> Auto-generated by `.github/workflows/benchmark.yml` on 2026-04-15 06:40 UTC.

| Passed | Failed | Skipped | Total | Status |
|-------:|-------:|--------:|------:|:------:|
| 77 | 0 | 0 | 77 | pass |
<!-- TESTS:END -->

### Benchmarks

<!-- BENCH:START -->
> Auto-generated by `.github/workflows/benchmark.yml` on 2026-04-15 06:40 UTC.
> Runner: `Linux 6.17.0-1010-azure x86_64`, .NET 10.0.202. GitHub Actions shared runners
> are ~2–3× slower than modern consumer hardware — compare rows within this table,
> not absolute throughput vs. your workstation.

| Size | Strategy | Lines | Time | Throughput | Peak RSS | Result |
|------|----------|-------|------|------------|----------|--------|
| 10MB | stream | 435423 | 0.41 s | 24 MB/s | 82 MB | ok |
| 10MB | mmf | 435423 | 0.38 s | 26 MB/s | 75 MB | ok |
| 50MB | stream | 2177667 | 1.35 s | 37 MB/s | 228 MB | ok |
| 50MB | mmf | 2177667 | 1.43 s | 35 MB/s | 222 MB | ok |
| 200MB | stream | 8705750 | 4.04 s | 49 MB/s | 807 MB | ok |
| 200MB | mmf | 8705750 | 5.85 s | 34 MB/s | 770 MB | ok |
<!-- BENCH:END -->

> **Note on absolute numbers.** GitHub's shared runners are 2-core VMs with ~7 GB RAM and are typically **2–3× slower** than modern consumer hardware. Use the table to compare **rows** (e.g. `stream` vs `mmf` on the same size) — not absolute throughput against your workstation. For a locally-measured run on beefy hardware, invoke `scripts/bench.sh` directly.

### Strategy Comparison: When to Use Which

Both strategies produce **byte-identical output** (verified with MD5). Tradeoffs are qualitative rather than numeric — the CI benchmarks above show the timing for the current commit:

| Scenario | Best Strategy | Why |
|----------|--------------|-----|
| Low-cardinality text (few unique phrases) | **Stream** | `TextPool` deduplicates strings; comparisons become pointer equality |
| High-cardinality text (many unique strings) | **MMF** | No `TextPool` overhead; 8-byte sort key resolves 90%+ without memory access |
| Small files (single chunk) | **MMF** | Less overhead; no pipeline setup; zero GC pressure |
| Large files (multi-chunk) | **Stream** | Concurrent pipeline overlaps I/O and sort; `Channel<T>` keeps all cores busy |
| Latency-sensitive (GC pauses unacceptable) | **MMF** | `NativeBuffer<EntryIndex>` is invisible to GC — zero pauses regardless of size |

The `auto` strategy (default) picks MMF when the file fits in a single chunk, and stream otherwise.

### JIT vs NativeAOT

| Aspect | JIT (.NET 10) | NativeAOT |
|--------|--------------|-----------|
| Startup | Slower (CoreCLR boot + TieredPGO warmup) | Near-instant — no runtime dependency |
| Binary size | Requires installed runtime | Single self-contained native binary |
| Throughput on sustained sort loops | TieredPGO re-compiles hot methods using profiling data | Static compilation — no runtime profiling |
| Deployment target | Full .NET environment | Containers, serverless, CLI tools |

**Recommendation:** JIT for sustained large-file workloads (TieredPGO wins on hot sort loops), NativeAOT when startup latency or binary footprint dominate.

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

The sorter uses a two-phase **external merge sort** with two interchangeable strategies (see [Sort Strategies](#sort-strategies)):

### Phase 1 — Split & Sort

**Stream strategy** (PipeReader + Channel — concurrent pipeline):

1. A **reader task** reads the input file using `System.IO.Pipelines` (`PipeReader`), parsing lines directly from UTF-8 byte buffers — no per-line string allocation for the full line.
2. Separator detection uses `SearchValues<byte>` (.NET 8+) for SIMD-accelerated dot search; numbers are parsed via `Utf8Parser.TryParse` — only the `Text` portion becomes a managed string.
3. Filled chunks are sent through a **bounded `Channel`** to 1–4 sort workers — reader and sort workers run concurrently, overlapping disk I/O and CPU.
4. Each sort worker uses **parallel merge sort**: splits the chunk into segments, sorts each via `Span<T>.Sort()` inside `Parallel.For` (with `MaxDegreeOfParallelism` to prevent oversubscription), then k-way merges the sorted segments via `PriorityQueue`.
5. Sorted chunks are written to temp files in **binary format** (`BinaryWriter`: `Int64` + length-prefixed UTF-8 string) — eliminates text re-parsing during the merge phase.
6. Chunk arrays are returned to a **custom `ArrayPool<LineEntry>`** (max 32M elements) after writing. The default `Shared` pool silently drops arrays > 1M elements, so a dedicated pool is required for actual reuse.
7. **Zero-allocation string deduplication** via `TextPool` — uses .NET 9+ `Dictionary.GetAlternateLookup<ReadOnlySpan<char>>` to check for duplicate text without allocating a string first. `stackalloc` is used for the UTF-8 → char conversion when text is ≤ 512 chars; larger text rents from `ArrayPool<char>`. Memory estimation counts string bytes only on first occurrence, resulting in fewer, larger chunks and less merge overhead.

**MMF strategy** (MemoryMappedFile + NativeMemory — zero managed allocations):

1. The entire file is mapped into virtual memory via `MemoryMappedFile.CreateFromFile`. The OS manages page caching — no explicit reads needed.
2. A raw `byte*` pointer scans the mapped region, finding newlines with `SearchValues<byte>` SIMD search. Each line is parsed into an `EntryIndex` (28-byte struct: number, text offset, text length, 8-byte sort key) — **no string allocation, no GC involvement**.
3. `EntryIndex` entries are stored in `NativeBuffer<T>` — a growable array backed by `NativeMemory.AlignedAlloc` (64-byte aligned). The GC has zero knowledge of this buffer.
4. Sorting uses `Span<T>.Sort()` with parallel segments and k-way merge, comparing entries via the 8-byte UTF-8 prefix sort key (fast path) and falling back to `SequenceCompareTo` on the MMF bytes (slow path).
5. Sorted entries are written to binary chunks by copying text bytes directly from the MMF — no string materialization needed. The binary format is identical to the stream strategy, so both share the same merge phase.

### Phase 2 — K-Way Merge (buffered)

Shared by both strategies — the binary chunk format is identical:

1. **Intermediate merges** (only when chunk count > `MergeWidth`) wrap each chunk in a `BinaryChunkReader` and re-serialize to a new binary chunk — `LineEntry` values are kept in-memory at this level.
2. **Final merge** (binary → text) wraps each chunk in a **`BinaryRawChunkReader`** that exposes text as a raw UTF-8 byte slice (`RawLineEntry.TextUtf8`) into a reusable buffer. `BinaryReader.ReadString()` is skipped entirely — eliminating ~4.5 billion per-record string allocations at 100 GB scale. UTF-8 lexicographic order matches `StringComparison.Ordinal`, so sort correctness is preserved without converting back to UTF-16.
3. A `PriorityQueue` (min-heap) selects the smallest entry across all chunk readers. The raw variant uses an 8-byte UTF-8 prefix sort key (`_sortKey`) — same trick as `LineEntry` but applied directly to the bytes, so most comparisons resolve without touching the buffer.
4. Output is written via `Utf8LineWriter.WriteEntry(long, ReadOnlySpan<byte>)` — formats numbers via `Utf8Formatter.TryFormat` (no `long.ToString()`), and memcopies the already-encoded text bytes straight into a pooled output buffer — no `Encoding.UTF8.GetBytes` call in the final path.
5. If chunk count exceeds `MergeWidth`, **multi-level merging** groups chunks to stay within file handle limits.

### Comparison Optimization

All three entry types use **precomputed sort keys** to avoid full comparisons in the inner loop:

- **Stream Phase 1** (`LineEntry`): first 4 UTF-16 characters packed as a big-endian `ulong`. Resolves ~80% of comparisons with a single integer compare, falling back to `string.Compare(Ordinal)` only when prefixes collide.
- **MMF Phase 1** (`EntryIndex`): first 8 raw UTF-8 bytes packed as a big-endian `ulong`. Resolves ~90% of comparisons without touching the memory-mapped file, falling back to `SequenceCompareTo` on the MMF byte range.
- **Phase 2 final merge** (`RawLineEntry`): first 8 raw UTF-8 bytes packed as a big-endian `ulong`. Same fast-path as `EntryIndex` but backed by the reader's buffer — allows byte-level comparison across all chunks without materializing strings.

### GC & Runtime Tuning

| Optimization | Strategy | Purpose |
|-------------|----------|---------|
| Server GC | both | One heap per logical core, parallel collection threads |
| Concurrent GC | both | Background Gen2 — application threads never block |
| RetainVM | both | Keep committed pages after GC; next chunk reuses same memory without kernel call |
| SustainedLowLatency | both | Suppress full blocking Gen2 during Phase 1 (Gen0/Gen1 still run) |
| GC.Collect between phases | both | Hint to reclaim Phase 1 allocations before I/O-heavy merge |
| TieredPGO | JIT | JIT re-compiles hot methods using runtime profiling data |
| NativeAOT | both | Ahead-of-time compilation: single self-contained binary, near-instant startup, no .NET runtime dependency |
| `NativeMemory.AlignedAlloc` | mmf | GC-invisible growable array (64-byte aligned) for `EntryIndex` entries |
| `MemoryMappedFile` | mmf | OS-managed page cache; text stays as byte offsets, no string allocation |
| AggressiveInlining | both | `CompareTo` / `CompareEntries` inlined into sort inner loop |
| `[SkipLocalsInit]` | both | No stack zeroing on hot paths (TextPool, Utf8LineWriter, LineEntry, ChunkSorter, MmfSorter) |
| `SearchValues<byte>` | both | SIMD-accelerated separator / newline search per platform |
| `Span<T>.Sort()` | both | Span-based sort avoids Array.Sort bounds validation overhead |
| ThreadPool pre-warm | both | SetMinThreads = ProcessorCount to avoid injection stall |

### Memory Management

**Shared (both strategies):**
- Chunk memory budget auto-tunes to ~25% of available RAM (cap scales with machine: 1 GB on small, up to 8 GB on 64 GB+).
- Binary chunk format avoids text re-parsing during merge — only the final output writes text.
- `Utf8LineWriter` formats output directly to UTF-8 using `Utf8Formatter.TryFormat` — eliminates `long.ToString()` and StreamWriter char→byte encoding overhead.
- `BinaryRawChunkReader` keeps text as raw UTF-8 byte slices during the final merge — no `string` allocation per record. Eliminates ~4.5 billion per-record string allocations in Phase 2 at 100 GB scale; the CI benchmarks above show the end-to-end impact on wall time and peak RSS.
- All output uses cached `UTF8Encoding(false)` — no BOM in generated files, no per-writer allocation.
- Config types (`SortOptions`, `GeneratorOptions`, `HardwareProfile`) are `readonly record struct` — no heap allocation for configuration, JIT inlines field reads inside sort workers.

**Stream strategy:**
- Custom `ArrayPool<LineEntry>` (32M max) reuses chunk arrays across iterations — the default `Shared` pool only handles up to ~1M elements.
- Bounded channel (capacity = worker count) limits in-flight chunks, keeping peak memory predictable.
- `TextPool` with `AlternateLookup<ReadOnlySpan<char>>` deduplicates strings without allocating — uses `stackalloc` for small text, `ArrayPool<char>` for large text.
- PipeReader reads large blocks (auto-scaled: 1–16 MB) with zero per-line allocation for the full input line.

**MMF strategy:**
- `NativeBuffer<EntryIndex>` stores 28-byte structs in `NativeMemory.AlignedAlloc` (64-byte aligned) — completely invisible to the GC, zero scan overhead regardless of buffer size.
- Text data stays in the memory-mapped file as byte offsets — no string allocation during index, sort, or chunk write phases.
- `MemoryMappedFile` delegates page management to the OS kernel — no explicit read buffers needed.

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
| Strategy        | `--strategy`     | auto    | `stream` (concurrent pipeline), `mmf` (zero GC), `auto` (pick best) |
| Chunk memory    | `--memory`       | auto (~25% RAM, capped) | Larger = fewer chunks, faster merge phase     |
| Merge width     | `--merge-width`  | 64      | Wider = fewer merge levels, more file handles |
| Buffer size     | `--buffer`       | auto (1–16 MB) | Larger = fewer I/O syscalls                   |
| Sort workers    | `--workers`      | auto (1–4)  | More workers = higher CPU usage, faster sort  |
| Temp directory  | `--temp-dir`     | system temp | Point to fast SSD for temp files              |

## Algorithm Comparison

The benchmark suite (`dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release`) compares four approaches to justify the design:

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
| Handles files larger than RAM with sustained sequential throughput — designed for 100 GB+ | More complex implementation |
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
