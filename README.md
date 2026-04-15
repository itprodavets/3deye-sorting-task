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
    Sorting/                   Sort pipeline (ExternalSorter, MmfSorter, ShardSorter, ChunkSorter,
                               ChunkMerger, BinaryChunkReader, BinaryChunkWriter,
                               BinaryRawChunkReader, RawLineEntry, Utf8LineWriter)
    EntryIndex.cs              File offset index (28 bytes, no managed refs, NativeMemory)
    NativeBuffer.cs            Growable array via NativeMemory.AlignedAlloc (GC-invisible)
    LineEntry.cs               Core model with precomputed sort key (8-byte UTF-8 prefix)
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
    Integration/               File I/O tests (ExternalSorter, MmfSorter, ShardSorter, FileGenerator)
benchmarks/
  LargeFileSorter.Benchmarks/  Performance benchmarks (BenchmarkDotNet)
```

## Prerequisites

- [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0)

## Build

```bash
dotnet build
```

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
| `--strategy <name>` | auto | Sort strategy: `stream`, `mmf`, `shard`, `auto` |
| `--threads <num>` | logical cores | Total parallelism budget shared by all strategies (workers × segments × shards). Use the same value across strategies for fair comparison. |

**Examples:**

```bash
# Basic — auto-tuned to hardware, no flags needed
dotnet run --project src/LargeFileSorter.Sorter -- data/test.txt data/sorted.txt

# Release mode for best performance (recommended for large files)
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt

# Limit memory — useful if other processes need RAM
dotnet run --project src/LargeFileSorter.Sorter -- data/test.txt data/sorted.txt \
    --memory 1GB --workers 1

# Maximum performance — large buffers, all workers, fast temp drive
dotnet run --project src/LargeFileSorter.Sorter -- data/test.txt data/sorted.txt \
    --buffer 16MB --workers 4 --temp-dir /mnt/nvme/tmp

# Force memory-mapped strategy (NativeMemory + MMF, zero managed allocations) —
# best when the whole file fits in one chunk
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --strategy mmf

# Force stream strategy (PipeReader + Channel, single-threaded k-way merge) —
# the workhorse for few chunks
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --strategy stream

# Force shard strategy (stream Phase 1 + partitioned parallel merge) —
# best on multi-core for large files with many chunks (~50+)
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --strategy shard

# Narrow merge width — useful on low-FD systems or to force cascaded merges
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --merge-width 8

# Fair cross-strategy comparison — pin the total parallelism budget the same way
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --strategy stream --threads 8
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --strategy shard  --threads 8

# Tight chunk budget to force multi-chunk path (stress-test the merge)
dotnet run --project src/LargeFileSorter.Sorter -c Release -- data/test.txt data/sorted.txt \
    --memory 256MB --strategy stream
```

**Example output — auto strategy on 1 GB file (64 GB machine):**

```
Hardware: RAM: 64.0 GB, Cores: 16, Chunk budget: 8.0 GB, I/O buffer: 16 MB, Sort workers: 4, Parallelism: 16

Input:  data/test.txt (1.0 GB)
Output: data/sorted.txt
Chunk memory budget: 8.0 GB
I/O buffer: 16 MB
Sort workers: 4
Parallelism budget: 16
Shard FD budget:    8128
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
Hardware: RAM: 64.0 GB, Cores: 16, Chunk budget: 8.0 GB, I/O buffer: 16 MB, Sort workers: 4, Parallelism: 16

Input:  data/test.txt (1.0 GB)
Output: data/sorted.txt
Chunk memory budget: 256 MB
I/O buffer: 16 MB
Sort workers: 4
Parallelism budget: 16
Shard FD budget:    8128
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

Run both steps in sequence:

```bash
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
# Strategy comparison: stream vs mmf vs shard (head-to-head)
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*Strategy*'

# Approach comparison: naive vs sequential vs optimized
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*Sorting*'

# Thread-scaling sweep (Threads × {1,2,4,8,16}) across all three strategies
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*ThreadScaling*'

# Phase 2 only: stream k-way merge vs shard partitioned parallel merge
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*MergePhase*'

# Run all benchmarks
dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release -- --filter '*'
```

### 6. Publish native binaries (NativeAOT)

Ahead-of-time compilation produces a self-contained native binary — no .NET runtime required, near-instant startup, minimal binary footprint.

```bash
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
> Auto-generated by `.github/workflows/benchmark.yml` on 2026-04-15 10:26 UTC.

| Passed | Failed | Skipped | Total | Status |
|-------:|-------:|--------:|------:|:------:|
| 97 | 0 | 0 | 97 | pass |
<!-- TESTS:END -->

### Benchmarks

<!-- BENCH:START -->
> Auto-generated by `.github/workflows/benchmark.yml` on 2026-04-15 10:26 UTC.
> Runner: `Linux 6.17.0-1010-azure x86_64`, .NET 10.0.202. GitHub Actions shared runners
> are ~2–3× slower than modern consumer hardware — compare rows within this table,
> not absolute throughput vs. your workstation.

| Size | Strategy | Lines | Chunks | Threads | Time | Throughput | Peak RSS | Result |
|------|----------|-------|-------:|--------:|------|------------|----------|--------|
| 10MB | stream | 388983 | 1 | 4 | 0.64 s | 16 MB/s | 85 MB | ok |
| 10MB | mmf | 388983 | 1 | 4 | 0.35 s | 29 MB/s | 71 MB | ok |
| 10MB | shard | 388983 | 1 | 4 | 0.64 s | 16 MB/s | 85 MB | ok |
| 50MB | stream | 1945702 | 1 | 4 | 2.19 s | 23 MB/s | 133 MB | ok |
| 50MB | mmf | 1945702 | 1 | 4 | 1.51 s | 33 MB/s | 208 MB | ok |
| 50MB | shard | 1945702 | 1 | 4 | 2.24 s | 22 MB/s | 133 MB | ok |
| 200MB | stream | 7782241 | 1 | 4 | 7.39 s | 27 MB/s | 420 MB | ok |
| 200MB | mmf | 7782241 | 1 | 4 | 5.31 s | 38 MB/s | 713 MB | ok |
| 200MB | shard | 7782241 | 1 | 4 | 7.47 s | 27 MB/s | 422 MB | ok |
| 1GB | stream | 39850773 | 1 | 4 | 40.54 s | 25 MB/s | 3.04 GB | ok |
| 1GB | mmf | 39850773 | 1 | 4 | 29.96 s | 34 MB/s | 3.41 GB | ok |
| 1GB | shard | 39850773 | 1 | 4 | 41.22 s | 25 MB/s | 3.04 GB | ok |
<!-- BENCH:END -->

> **Note on absolute numbers.** GitHub's shared runners are 2-core VMs with ~7 GB RAM and are typically **2–3× slower** than modern consumer hardware. Use the table to compare **rows** (e.g. `stream` vs `mmf` on the same size) — not absolute throughput against your workstation. For a locally-measured run on beefy hardware, invoke `scripts/bench.sh` directly.

### Large-File Reference Run (100 GB)

The CI matrix tops out at 1 GB because GitHub's shared runners have only 14 GB of disk. Below is a one-shot reference run on a single 100 GB file, measured on my own workstation:

| Size | Strategy | Lines | Time | Throughput | Peak RSS | Chunks |
|------|----------|------:|-----:|-----------:|---------:|-------:|
| 100 GB | stream | 4 456 315 686 | **11:04.24** | 154 MB/s | 20 GB | 17 |

Hardware: Apple M4 Max (16 cores), 64 GB RAM, APFS SSD, .NET 10.0.102, macOS.

Run command:

```bash
dotnet run --project src/LargeFileSorter.Generator -c Release -- data/input_100gb.txt 100GB --seed 42
/usr/bin/time -l dotnet run --project src/LargeFileSorter.Sorter -c Release -- \
    data/input_100gb.txt data/sorted_100gb.txt --strategy stream
```

Verified after the run: input and output line counts match exactly (4 456 315 686 lines each), first three output lines are `1. Always`, last three are `100000. Yesterday yesterday Honeydew Apple`. To reproduce, use `BENCH_SIZES='100GB' scripts/bench.sh` on a machine with at least ~200 GB of free space.

### Strategy Comparison: When to Use Which

All three strategies produce **byte-identical output** (verified with SHA-256 in CI). Tradeoffs are qualitative rather than numeric — the CI benchmarks above show the timing for the current commit:

| Scenario | Best Strategy | Why |
|----------|--------------|-----|
| Low-cardinality text (few unique phrases) | **Stream** | `TextPool` deduplicates strings; comparisons become pointer equality |
| High-cardinality text (many unique strings) | **MMF** | No `TextPool` overhead; 8-byte sort key resolves 90%+ without memory access |
| Small files (single chunk) | **MMF** | Less overhead; no pipeline setup; zero GC pressure |
| Large files (multi-chunk, modest core count) | **Stream** | Concurrent pipeline overlaps I/O and sort; `Channel<T>` keeps all cores busy |
| Large files (multi-chunk, many cores ≥ 8) | **Shard** | Phase 1 shared with stream; Phase 2 runs K parallel k-way merges over disjoint key ranges — 8% faster on 1 GB / 86 chunks at `--threads 16` (locally measured), more on bigger inputs where merge dominates |
| Latency-sensitive (GC pauses unacceptable) | **MMF** | `NativeBuffer<EntryIndex>` is invisible to GC — zero pauses regardless of size |

The `auto` strategy (default) picks MMF when the file fits in a single chunk, **shard** when the input is at least 4× the chunk budget AND `--threads ≥ 4`, and stream otherwise. Force a specific strategy with `--strategy stream|mmf|shard`. Use the same `--threads N` across strategies to compare them fairly.

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
- **Strategies**: stream, MMF and shard produce **byte-identical** output for the same input.
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

The sorter uses a two-phase **external merge sort** with three interchangeable strategies (see [Sort Strategies](#sort-strategies)):

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

All three strategies share the same binary chunk format produced by Phase 1. Stream and MMF use the single-threaded k-way merge described below; shard uses a partitioned variant covered in the next subsection.

1. **Intermediate merges** (only when chunk count > `MergeWidth`) wrap each chunk in a `BinaryChunkReader` and re-serialize to a new binary chunk — `LineEntry` values are kept in-memory at this level.
2. **Final merge** (binary → text) wraps each chunk in a **`BinaryRawChunkReader`** that exposes text as a raw UTF-8 byte slice (`RawLineEntry.TextUtf8`) into a reusable buffer. `BinaryReader.ReadString()` is skipped entirely — eliminating ~4.5 billion per-record string allocations at 100 GB scale. The order is UTF-8 lexicographic byte order, equivalent to Unicode code-point order — **not** `StringComparison.Ordinal` (which is UTF-16 code-unit order and disagrees with UTF-8 on the supplementary plane). `LineEntry.CompareTo` iterates `Rune.EnumerateRunes()` so every strategy sorts under the same code-point order without converting back to UTF-16.
3. A `PriorityQueue` (min-heap) selects the smallest entry across all chunk readers. The raw variant uses an 8-byte UTF-8 prefix sort key (`_sortKey`) — same trick as `LineEntry` but applied directly to the bytes, so most comparisons resolve without touching the buffer.
4. Output is written via `Utf8LineWriter.WriteEntry(long, ReadOnlySpan<byte>)` — formats numbers via `Utf8Formatter.TryFormat` (no `long.ToString()`), and memcopies the already-encoded text bytes straight into a pooled output buffer — no `Encoding.UTF8.GetBytes` call in the final path.
5. If chunk count exceeds `MergeWidth`, **multi-level merging** groups chunks to stay within file handle limits.

### Phase 2 — Shard Strategy (Partitioned Parallel Merge)

The shard strategy replaces step 3 above (single-threaded `PriorityQueue`) with K independent merges running in parallel:

1. **Partition** — first-2-bytes UTF-8 keys are sampled from every chunk to find K−1 split boundaries that balance record counts across shards. K adapts to `MaxDegreeOfParallelism` and the per-shard file-descriptor budget (`RLIMIT_NOFILE` queried at startup).
2. **Split** — each Phase 1 chunk is scanned once and records are routed to K per-shard writers according to their sort key. Writers are flushed and disposed between chunks so FileStream buffers can't strand data (each shard ends up with K sorted-fragment files, one per original chunk).
3. **Parallel merge** — K shards are merged concurrently via `Parallel.ForEachAsync`, each using the same `ChunkMerger` as stream/MMF. A per-shard temp subdirectory prevents cascaded-merge filename collisions between shards.
4. **Concat** — because the partition boundaries are sort-order-preserving, concatenating shard outputs in index order produces the globally sorted result without a final merge pass. Byte-level concat via `Stream.CopyTo` using the configured `--buffer` size (auto 1–16 MB).

The trade-off is ~20% extra disk I/O (the split pass rewrites chunk data) in exchange for parallelism on Phase 2, which is the single-threaded bottleneck on multi-core machines when chunk count is high.

### Comparison Optimization

All three entry types use **precomputed sort keys** to avoid full comparisons in the inner loop:

- **Stream Phase 1** (`LineEntry`): first 8 raw UTF-8 bytes packed as a big-endian `ulong` — same sort key as `EntryIndex` and `RawLineEntry`, so all three strategies agree byte-for-byte. Resolves most comparisons with a single integer compare; slow path iterates `Rune.EnumerateRunes()` for allocation-free Unicode code-point order.
- **MMF Phase 1** (`EntryIndex`): first 8 raw UTF-8 bytes packed as a big-endian `ulong`. Resolves ~90% of comparisons without touching the memory-mapped file, falling back to `SequenceCompareTo` on the MMF byte range.
- **Phase 2 final merge** (`RawLineEntry`): first 8 raw UTF-8 bytes packed as a big-endian `ulong`. Same fast-path as `EntryIndex` but backed by the reader's buffer — allows byte-level comparison across all chunks without materializing strings.

### GC & Runtime Tuning

| Optimization | Strategy | Purpose |
|-------------|----------|---------|
| Server GC | all | One heap per logical core, parallel collection threads |
| Concurrent GC | all | Background Gen2 — application threads never block |
| RetainVM | all | Keep committed pages after GC; next chunk reuses same memory without kernel call |
| SustainedLowLatency | all | Suppress full blocking Gen2 during Phase 1 (Gen0/Gen1 still run) |
| GC.Collect between phases | all | Hint to reclaim Phase 1 allocations before I/O-heavy merge |
| TieredPGO | JIT | JIT re-compiles hot methods using runtime profiling data |
| NativeAOT | all | Ahead-of-time compilation: single self-contained binary, near-instant startup, no .NET runtime dependency |
| `NativeMemory.AlignedAlloc` | mmf | GC-invisible growable array (64-byte aligned) for `EntryIndex` entries |
| `MemoryMappedFile` | mmf | OS-managed page cache; text stays as byte offsets, no string allocation |
| `Parallel.ForEachAsync` | shard | K independent k-way merges run concurrently over disjoint key ranges |
| `getrlimit(RLIMIT_NOFILE)` | shard | Runtime query of the FD budget caps K so parallel merges stay within limits |
| AggressiveInlining | all | `CompareTo` / `CompareEntries` inlined into sort inner loop |
| `[SkipLocalsInit]` | all | No stack zeroing on hot paths (TextPool, Utf8LineWriter, LineEntry, ChunkSorter, MmfSorter) |
| `SearchValues<byte>` | all | SIMD-accelerated separator / newline search per platform |
| `Span<T>.Sort()` | all | Span-based sort avoids Array.Sort bounds validation overhead |
| ThreadPool pre-warm | all | SetMinThreads = ProcessorCount to avoid injection stall |

### Memory Management

**Shared (all three strategies):**
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

**Shard strategy:**
- Shares stream's Phase 1 path verbatim (same `ExternalSorter` pipeline, same binary chunk output), so the Phase 1 memory characteristics match stream exactly.
- Split pass uses one `FileStream` output buffer per shard writer (sized from `--buffer`, auto 1–16 MB); writers are disposed between source chunks so the buffer count is bounded by K, not K × chunkCount.
- Each parallel shard merge runs its own `ChunkMerger` instance with its own temp subdirectory — no shared mutable state between parallel branches, no locks on the merge path.
- Shard count K adapts down when the `RLIMIT_NOFILE` budget tightens, so the total open-FD count across all parallel merges stays within system limits.

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

Three `IFileSorter` implementations (Strategy pattern), selectable via `--strategy`:

| Strategy | Flag | When to use | How it works |
|----------|------|-------------|--------------|
| **stream** | `--strategy stream` | High-throughput (default for large files) | PipeReader → `TextPool` string dedup → `Channel<T>` → parallel sort → binary chunks → single-threaded k-way merge |
| **mmf** | `--strategy mmf` | Zero managed allocations | `MemoryMappedFile` → `NativeBuffer<EntryIndex>` (NativeMemory) → pointer-based sort → binary chunks |
| **shard** | `--strategy shard` | Many cores + many chunks (merge-bound) | Reuses stream's Phase 1, then splits chunks by first-2-bytes UTF-8 key into K shards, runs K parallel k-way merges via `Parallel.ForEachAsync`, byte-level concat in shard order |
| **auto** | `--strategy auto` (default) | Best of all | File ≤ chunk budget → MMF; ≥ 4× chunk budget AND `--threads ≥ 4` → shard; otherwise → stream |

The stream strategy excels on data with repeating text (string dedup via `TextPool` keeps unique count low) and multi-chunk workloads (concurrent pipeline). The MMF strategy excels on small files that fit in one chunk and on high-cardinality data where millions of unique strings would cause GC pressure. The shard strategy is the newest — it removes the single-threaded merge bottleneck on multi-core machines, paying ~20% extra disk I/O during the split phase to get K-way parallel merge in return. The break-even point in our measurements is around 50+ chunks. See [Strategy Comparison](#strategy-comparison-when-to-use-which) for the detailed table.

### Performance Tuning

| Parameter         | Flag             | Default | Effect                                        |
|-------------------|------------------|---------|-----------------------------------------------|
| Strategy          | `--strategy`     | auto    | `stream` (concurrent pipeline), `mmf` (zero GC), `shard` (parallel merge), `auto` (pick best) |
| Chunk memory      | `--memory`       | auto (~25% RAM, capped) | Larger = fewer chunks, faster merge phase     |
| Merge width       | `--merge-width`  | 64      | Wider = fewer merge levels, more file handles |
| Buffer size       | `--buffer`       | auto (1–16 MB) | Larger = fewer I/O syscalls                   |
| Sort workers      | `--workers`      | auto (1–4)  | More workers = higher CPU usage, faster sort  |
| Parallelism budget| `--threads`      | logical cores | Shared budget across workers / segments / shards — pin the same N across strategies for fair comparison |
| Temp directory    | `--temp-dir`     | system temp | Point to fast SSD for temp files              |

## Algorithm Comparison

The benchmark suite (`dotnet run --project benchmarks/LargeFileSorter.Benchmarks -c Release`) compares the following approaches to justify the design:

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

### 5. Optimized External Sort — Shard Strategy (Partitioned Parallel Merge)

Stream's Phase 1 pipeline + split-by-key-prefix into K shards + `Parallel.ForEachAsync` over independent k-way merges + byte concat.

| Pros | Cons |
|------|------|
| Phase 2 scales with cores — removes the single-threaded `PriorityQueue` bottleneck | ~20% extra disk I/O from the split pass rewriting chunk data |
| Reuses stream's Phase 1 verbatim — same chunk format, same parity tests | Only helps when there are enough chunks (~50+) for parallel merge to outweigh split overhead |
| K-shard partition is sort-order-preserving → final output is a byte-level concat, no final merge | Per-shard temp subdir + per-chunk writer disposal adds complexity over plain k-way merge |
| `RLIMIT_NOFILE` queried at startup → K auto-caps to stay within FD budget | Needs multi-core host to shine — on 1–2 cores it degenerates to stream + overhead |
| Byte-for-byte identical output to stream (CI-verified) | No value on single-chunk inputs — falls back to stream/MMF fast path |

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
| **Open/Closed** | `ExternalSorter`, `MmfSorter` and `ShardSorter` implement `IFileSorter` — new strategies added without modifying existing code; new chunk formats implement `IChunkReader` |
| **Liskov Substitution** | All interface implementations honor their contracts |
| **Interface Segregation** | `IFileSorter` (sort), `IFileGenerator` (generate), `IChunkReader` (read) — each focused on one operation |
| **Dependency Inversion** | `ChunkMerger` depends on `Func<string, IChunkReader>` factory, not concrete `BinaryChunkReader` |

## License

MIT
