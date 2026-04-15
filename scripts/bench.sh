#!/usr/bin/env bash
# Runs the sorter across {size × strategy} combinations, collects wall time
# and peak RSS, and writes a markdown fragment with the results.
#
# Usage: scripts/bench.sh [--readme README.md]
#   --readme PATH   Update README between <!-- BENCH:START --> and <!-- BENCH:END -->.
#                   Without this flag, the fragment is printed to stdout.
#
# Expects Release binaries built already (dotnet build -c Release).
# Designed for GitHub Actions on ubuntu-latest; also works on macOS for local smoke tests.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

README_PATH=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --readme) README_PATH="$2"; shift 2 ;;
        *) echo "Unknown flag: $1" >&2; exit 2 ;;
    esac
done

# Size matrix is env-overridable so ad-hoc local runs (e.g. "BENCH_SIZES='10GB 100GB' scripts/bench.sh")
# don't require editing the script. Default matrix sized to fit GitHub Actions runners (16 GB RAM, 14 GB SSD).
# Use `read -ra` so the unset-default branch word-splits correctly; plain $(array=(${var:-"a b c"}))
# keeps the default as a single quoted element.
IFS=' ' read -ra SIZES <<<"${BENCH_SIZES:-10MB 50MB 200MB 1GB}"
# shard joins the matrix so the README table shows when partitioned parallel merge wins
# vs single-threaded k-way merge. With default chunk budget, sub-1GB inputs stay single-chunk
# and shard takes its single-chunk fast path — the shard column then matches stream's wall
# time, which is the correct signal that sharding adds no overhead at small sizes.
IFS=' ' read -ra STRATEGIES <<<"${BENCH_STRATEGIES:-stream mmf shard}"
SEED=42

# Per-cell iteration count. Each (size, strategy) cell runs 1 warmup + BENCH_ITERATIONS
# timed runs, and the minimum timed run is reported. Rationale:
#   * GitHub Actions shared runners have significant jitter (noisy neighbors, CPU
#     frequency scaling, transient I/O stalls). A single measurement often swings ±30%
#     between runs, which was flipping the ranking of strategies that actually differ
#     by 10-20% — users reading the README couldn't tell which strategy was faster.
#   * Min-of-N is more reproducible than median for this workload: it captures the
#     steady-state "no preemption" case. Mean/median drag in tail latency from
#     whatever else the runner happened to be doing.
#   * The warmup run primes the OS page cache (so timed runs see warm reads, same as
#     a CLI user who re-runs a sort) and JITs the hot paths (so timed runs measure
#     steady-state throughput, not cold-start overhead).
# Override with BENCH_ITERATIONS=N for faster local iteration (min 1, default 2).
BENCH_ITERATIONS="${BENCH_ITERATIONS:-2}"
if ! [[ "$BENCH_ITERATIONS" =~ ^[0-9]+$ ]] || (( BENCH_ITERATIONS < 1 )); then
    echo "BENCH_ITERATIONS must be a positive integer; got '$BENCH_ITERATIONS'" >&2
    exit 2
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

GENERATOR="dotnet src/LargeFileSorter.Generator/bin/Release/net10.0/LargeFileSorter.Generator.dll"
SORTER="dotnet src/LargeFileSorter.Sorter/bin/Release/net10.0/LargeFileSorter.Sorter.dll"

# Detect time style. GNU time ('/usr/bin/time -v') outputs "Maximum resident set size (kbytes): N".
# BSD time on macOS ('/usr/bin/time -l') reports "maximum resident set size" in bytes (newer) or pages.
# We degrade to stopwatch-only when neither is available.
TIME_STYLE="none"
if [[ -x /usr/bin/time ]]; then
    if /usr/bin/time -v true 2>/dev/null; then
        TIME_STYLE="gnu"
    elif /usr/bin/time -l true 2>/dev/null; then
        TIME_STYLE="bsd"
    fi
fi

parse_completion_seconds() {
    awk '/Completed in/ {
        for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+:[0-9]+:[0-9]+\.[0-9]+/) {
            split($i, p, ":"); printf "%.3f", p[1]*3600 + p[2]*60 + p[3]; exit
        }
    }'
}

# Chunk count: the sorter prints "Phase 2: merging N sorted chunks..." whenever the
# input splits into ≥2 chunks. The MMF single-chunk fast path skips Phase 2 entirely,
# so absence of that line means exactly one chunk was produced.
parse_chunk_count() {
    awk '
        /Phase 2: merging/ {
            for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+$/) { print $i; found=1; exit }
        }
        END { if (!found) print 1 }
    '
}

# Parallelism budget — the unified --threads value. Prints on the settings line
# "Parallelism budget: N". Field 3 after splitting on whitespace.
parse_threads() {
    awk '/^Parallelism budget:/ { print $3; exit }'
}

bytes_to_human() {
    awk -v b="$1" 'BEGIN {
        if (b >= 1073741824) printf "%.2f GB", b/1073741824
        else if (b >= 1048576) printf "%.0f MB", b/1048576
        else if (b >= 1024) printf "%.0f KB", b/1024
        else printf "%d B", b
    }'
}

size_flag_to_bytes() {
    awk -v s="$1" 'BEGIN {
        u=substr(s, length(s)-1); n=substr(s, 1, length(s)-2)
        if (u == "GB") printf "%d", n*1073741824
        else if (u == "MB") printf "%d", n*1048576
        else if (u == "KB") printf "%d", n*1024
        else printf "%d", s
    }'
}

peak_kb_from_time_file() {
    local file="$1"
    case "$TIME_STYLE" in
        gnu) awk -F': ' '/Maximum resident set size/ {gsub(/[^0-9]/, "", $2); print $2; exit}' "$file" ;;
        bsd)
            # macOS: "  123456789  maximum resident set size"   (bytes on recent macOS)
            awk '/maximum resident set size/ {printf "%d", $1 / 1024; exit}' "$file" ;;
        *) echo "" ;;
    esac
}

run_one() {
    local size="$1" strategy="$2"
    local size_bytes; size_bytes="$(size_flag_to_bytes "$size")"
    local input="$WORK_DIR/in_${size}.txt"
    local output="$WORK_DIR/out_${size}_${strategy}.txt"

    if [[ ! -f "$input" ]]; then
        $GENERATOR "$input" "$size" --seed "$SEED" >/dev/null
    fi

    # Warmup pass: primes OS page cache + JIT hot paths. Output discarded — we only
    # want the post-warmup state for the timed runs below.
    $SORTER "$input" "$output" --strategy "$strategy" >/dev/null 2>&1 || true

    # Timed runs: N iterations, take min. The first timed run still carries warm
    # caches from the warmup, so we're measuring steady-state throughput. Peak RSS
    # and chunk count come from the fastest iteration (all runs share the same
    # deterministic input + strategy, so these values agree across iterations modulo
    # allocator fragmentation noise).
    local best_seconds="" best_peak_kb="" best_chunks="" best_threads="" best_status="ok"
    for ((iter=1; iter <= BENCH_ITERATIONS; iter++)); do
        local time_file="$WORK_DIR/time_${size}_${strategy}_${iter}.txt"
        local sort_log="$WORK_DIR/sort_${size}_${strategy}_${iter}.log"

        case "$TIME_STYLE" in
            gnu) /usr/bin/time -v -o "$time_file" $SORTER "$input" "$output" --strategy "$strategy" >"$sort_log" 2>&1 || true ;;
            bsd) /usr/bin/time -l -o "$time_file" $SORTER "$input" "$output" --strategy "$strategy" >"$sort_log" 2>&1 || true ;;
            *)   $SORTER "$input" "$output" --strategy "$strategy" >"$sort_log" 2>&1 || true ;;
        esac

        local wall_seconds peak_kb chunks threads
        wall_seconds="$(parse_completion_seconds < "$sort_log" || true)"
        peak_kb=""
        [[ -f "$time_file" ]] && peak_kb="$(peak_kb_from_time_file "$time_file" || true)"
        chunks="$(parse_chunk_count < "$sort_log" || echo '')"
        threads="$(parse_threads < "$sort_log" || echo '')"

        # Validate output on every iteration — silent truncation would otherwise hide
        # behind a fast but wrong run (min over multiple runs would pick it).
        local in_lines out_lines
        in_lines="$(wc -l <"$input" | tr -d ' ')"
        out_lines="$(wc -l <"$output" 2>/dev/null | tr -d ' ' || echo 0)"
        if [[ "$in_lines" != "$out_lines" || -z "$wall_seconds" ]]; then
            best_status="fail"
        fi

        # Track best (minimum) wall time across timed iterations.
        if [[ -n "$wall_seconds" ]]; then
            if [[ -z "$best_seconds" ]] || awk -v new="$wall_seconds" -v best="$best_seconds" \
                'BEGIN { exit !(new < best) }'; then
                best_seconds="$wall_seconds"
                best_peak_kb="$peak_kb"
                best_chunks="$chunks"
                best_threads="$threads"
            fi
        fi
    done

    local throughput="—"
    if [[ -n "$best_seconds" && "$best_seconds" != "0.000" ]]; then
        throughput="$(awk -v b="$size_bytes" -v t="$best_seconds" 'BEGIN {printf "%.0f MB/s", b/1048576/t}')"
    fi
    local peak_human="—"
    [[ -n "$best_peak_kb" ]] && peak_human="$(bytes_to_human $((best_peak_kb * 1024)))"
    local time_display="—"
    [[ -n "$best_seconds" ]] && time_display="$(printf "%.2f s" "$best_seconds")"
    local chunks_display="${best_chunks:-—}"
    local threads_display="${best_threads:-—}"

    local in_lines
    in_lines="$(wc -l <"$input" | tr -d ' ')"

    printf "| %s | %s | %'d | %s | %s | %s | %s | %s | %s |\n" \
        "$size" "$strategy" "$in_lines" "$chunks_display" "$threads_display" \
        "$time_display" "$throughput" "$peak_human" "$best_status"
}

# ---------------------------------------------------------------------------
#  Tests
# ---------------------------------------------------------------------------

TEST_LOG="$WORK_DIR/tests.log"
TEST_STATUS="pass"
if ! dotnet test -c Release --no-build --logger "console;verbosity=minimal" >"$TEST_LOG" 2>&1; then
    TEST_STATUS="fail"
fi

# dotnet test summary line example:
#   Passed!  - Failed:     0, Passed:    77, Skipped:     0, Total:    77, Duration: 1 s - ...
TEST_SUMMARY="$(awk '
    /^(Passed|Failed)!.*- Failed:/ {
        for (i=1; i<=NF; i++) {
            if ($i == "Failed:") failed=$(i+1)
            else if ($i == "Passed:") passed=$(i+1)
            else if ($i == "Skipped:") skipped=$(i+1)
            else if ($i == "Total:") total=$(i+1)
        }
        gsub(",", "", failed); gsub(",", "", passed); gsub(",", "", skipped); gsub(",", "", total)
        printf "%s %s %s %s\n", passed, failed, skipped, total
        exit
    }
' "$TEST_LOG")"

TEST_PASSED="$(echo "$TEST_SUMMARY" | awk '{print $1}')"
TEST_FAILED="$(echo "$TEST_SUMMARY" | awk '{print $2}')"
TEST_SKIPPED="$(echo "$TEST_SUMMARY" | awk '{print $3}')"
TEST_TOTAL="$(echo "$TEST_SUMMARY" | awk '{print $4}')"
TEST_PASSED="${TEST_PASSED:-0}"
TEST_FAILED="${TEST_FAILED:-0}"
TEST_SKIPPED="${TEST_SKIPPED:-0}"
TEST_TOTAL="${TEST_TOTAL:-0}"

# ---------------------------------------------------------------------------
#  Build fragments
# ---------------------------------------------------------------------------

RUN_DATE="$(date -u +'%Y-%m-%d %H:%M UTC')"
RUNNER_INFO="$(uname -srm)"
DOTNET_VERSION="$(dotnet --version)"

BENCH_ROWS=""
for size in "${SIZES[@]}"; do
    for strategy in "${STRATEGIES[@]}"; do
        echo "  benchmarking: ${size} × ${strategy}" >&2
        BENCH_ROWS+="$(run_one "$size" "$strategy")"$'\n'
    done
done

BENCH_FRAGMENT="<!-- BENCH:START -->
> Auto-generated by \`.github/workflows/benchmark.yml\` on ${RUN_DATE}.
> Runner: \`${RUNNER_INFO}\`, .NET ${DOTNET_VERSION}. GitHub Actions shared runners
> are ~2–3× slower than modern consumer hardware — compare rows within this table,
> not absolute throughput vs. your workstation.
>
> Each cell is the **min of ${BENCH_ITERATIONS} timed runs after 1 warmup** (warmup primes page
> cache + JIT). Min-of-N is reproducible enough across CI runs to keep strategy
> rankings stable; single-shot wall time was swinging ±30% run-to-run and flipping
> which strategy looked fastest. Override iterations with \`BENCH_ITERATIONS=N\`.

| Size | Strategy | Lines | Chunks | Threads | Time | Throughput | Peak RSS | Result |
|------|----------|-------|-------:|--------:|------|------------|----------|--------|
${BENCH_ROWS}<!-- BENCH:END -->"

TESTS_FRAGMENT="<!-- TESTS:START -->
> Auto-generated by \`.github/workflows/benchmark.yml\` on ${RUN_DATE}.

| Passed | Failed | Skipped | Total | Status |
|-------:|-------:|--------:|------:|:------:|
| ${TEST_PASSED} | ${TEST_FAILED} | ${TEST_SKIPPED} | ${TEST_TOTAL} | ${TEST_STATUS} |
<!-- TESTS:END -->"

replace_block() {
    local file="$1" start="$2" end="$3" content="$4"
    python3 - "$file" "$start" "$end" "$content" <<'PY'
import sys, re, pathlib
file, start, end, content = sys.argv[1:5]
p = pathlib.Path(file)
text = p.read_text()
pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)
if not pattern.search(text):
    raise SystemExit(f"markers '{start}' / '{end}' not found in {file}")
text = pattern.sub(content.replace("\\", r"\\"), text)
p.write_text(text)
PY
}

if [[ -n "$README_PATH" ]]; then
    replace_block "$README_PATH" "<!-- BENCH:START -->" "<!-- BENCH:END -->" "$BENCH_FRAGMENT"
    replace_block "$README_PATH" "<!-- TESTS:START -->" "<!-- TESTS:END -->" "$TESTS_FRAGMENT"
    echo "updated: $README_PATH" >&2
else
    echo "$TESTS_FRAGMENT"
    echo ""
    echo "$BENCH_FRAGMENT"
fi
