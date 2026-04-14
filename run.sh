#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default paths
DATA_DIR="$SCRIPT_DIR/data"
GENERATOR_PROJECT="src/LargeFileSorter.Generator"
SORTER_PROJECT="src/LargeFileSorter.Sorter"
BENCHMARK_PROJECT="benchmarks/LargeFileSorter.Benchmarks"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BOLD}================================================${NC}"
    echo -e "${BOLD}       Large File Sorter — Interactive Menu${NC}"
    echo -e "${BOLD}================================================${NC}"
    echo ""
}

print_menu() {
    echo -e "  ${CYAN}1)${NC} Generate a test file"
    echo -e "  ${CYAN}2)${NC} Sort a file"
    echo -e "  ${CYAN}3)${NC} Generate + Sort (full pipeline)"
    echo -e "  ${CYAN}4)${NC} Run tests"
    echo -e "  ${CYAN}5)${NC} Run benchmarks"
    echo -e "  ${CYAN}6)${NC} Build solution"
    echo -e "  ${CYAN}7)${NC} Clean build artifacts"
    echo -e "  ${CYAN}0)${NC} Exit"
    echo ""
}

read_input() {
    local prompt="$1"
    local default="$2"
    local result

    if [ -n "$default" ]; then
        echo -ne "  ${prompt} ${YELLOW}[${default}]${NC}: "
    else
        echo -ne "  ${prompt}: "
    fi
    read -r result
    echo "${result:-$default}"
}

ensure_data_dir() {
    mkdir -p "$DATA_DIR"
}

format_size_human() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc) MB"
    elif [ "$bytes" -ge 1024 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc) KB"
    else
        echo "$bytes B"
    fi
}

do_generate() {
    ensure_data_dir
    echo ""
    echo -e "  ${BOLD}--- Generate Test File ---${NC}"
    echo ""

    local output
    output=$(read_input "Output file" "$DATA_DIR/test.txt")

    local size
    size=$(read_input "File size (e.g. 10MB, 1GB)" "10MB")

    local phrases
    phrases=$(read_input "Unique phrases count" "500")

    local max_num
    max_num=$(read_input "Max number value" "100000")

    local seed
    seed=$(read_input "Random seed (empty = random)" "")

    echo ""
    echo -e "  ${GREEN}Generating...${NC}"
    echo ""

    local cmd="dotnet run --project $GENERATOR_PROJECT -c Release -- \"$output\" $size --phrases $phrases --max-number $max_num"
    if [ -n "$seed" ]; then
        cmd="$cmd --seed $seed"
    fi

    eval "$cmd"

    echo ""
    if [ -f "$output" ]; then
        local actual_size
        actual_size=$(wc -c < "$output" | tr -d ' ')
        echo -e "  ${GREEN}File created:${NC} $output ($(format_size_human "$actual_size"))"
        echo -e "  ${GREEN}Line count:${NC}  $(wc -l < "$output" | tr -d ' ')"
        echo -e "  ${GREEN}First 5 lines:${NC}"
        head -5 "$output" | sed 's/^/    /'
    fi
}

do_sort() {
    echo ""
    echo -e "  ${BOLD}--- Sort File ---${NC}"
    echo ""

    # Try to suggest a default input
    local default_input="$DATA_DIR/test.txt"
    if [ ! -f "$default_input" ]; then
        default_input=""
    fi

    local input
    input=$(read_input "Input file" "$default_input")

    if [ ! -f "$input" ]; then
        echo -e "  ${RED}Error: file not found — $input${NC}"
        return
    fi

    local default_output
    default_output="$(dirname "$input")/$(basename "$input" .txt)_sorted.txt"

    local output
    output=$(read_input "Output file" "$default_output")

    local memory
    memory=$(read_input "Chunk memory (e.g. 256MB, 1GB, auto)" "auto")

    local buffer
    buffer=$(read_input "I/O buffer size (e.g. 1MB, 4MB, auto)" "auto")

    local workers
    workers=$(read_input "Sort workers (1-4, auto)" "auto")

    local merge_width
    merge_width=$(read_input "Merge width" "64")

    echo ""
    echo -e "  ${GREEN}Sorting...${NC}"
    echo ""

    local cmd="dotnet run --project $SORTER_PROJECT -c Release -- \"$input\" \"$output\" --merge-width $merge_width"
    if [ "$memory" != "auto" ]; then
        cmd="$cmd --memory $memory"
    fi
    if [ "$buffer" != "auto" ]; then
        cmd="$cmd --buffer $buffer"
    fi
    if [ "$workers" != "auto" ]; then
        cmd="$cmd --workers $workers"
    fi

    eval "$cmd"

    echo ""
    if [ -f "$output" ]; then
        local actual_size
        actual_size=$(wc -c < "$output" | tr -d ' ')
        echo -e "  ${GREEN}Output:${NC}      $output ($(format_size_human "$actual_size"))"
        echo -e "  ${GREEN}Line count:${NC}  $(wc -l < "$output" | tr -d ' ')"
        echo -e "  ${GREEN}First 5 lines:${NC}"
        head -5 "$output" | sed 's/^/    /'
    fi
}

do_full_pipeline() {
    ensure_data_dir
    echo ""
    echo -e "  ${BOLD}--- Full Pipeline: Generate + Sort ---${NC}"
    echo ""

    local size
    size=$(read_input "File size (e.g. 10MB, 1GB)" "10MB")

    local input="$DATA_DIR/pipeline_input.txt"
    local output="$DATA_DIR/pipeline_sorted.txt"

    echo ""
    echo -e "  ${CYAN}Step 1/2: Generating $size file...${NC}"
    echo ""
    dotnet run --project $GENERATOR_PROJECT -c Release -- "$input" "$size" --seed 42

    echo ""
    echo -e "  ${CYAN}Step 2/2: Sorting...${NC}"
    echo ""
    dotnet run --project $SORTER_PROJECT -c Release -- "$input" "$output"

    echo ""
    if [ -f "$output" ]; then
        local input_lines output_lines
        input_lines=$(wc -l < "$input" | tr -d ' ')
        output_lines=$(wc -l < "$output" | tr -d ' ')

        echo -e "  ${GREEN}Input:${NC}       $input ($input_lines lines)"
        echo -e "  ${GREEN}Output:${NC}      $output ($output_lines lines)"

        if [ "$input_lines" -eq "$output_lines" ]; then
            echo -e "  ${GREEN}Line count:  MATCH${NC}"
        else
            echo -e "  ${RED}Line count:  MISMATCH ($input_lines vs $output_lines)${NC}"
        fi

        echo ""
        echo -e "  ${GREEN}First 5 sorted lines:${NC}"
        head -5 "$output" | sed 's/^/    /'
    fi
}

do_test() {
    echo ""
    echo -e "  ${GREEN}Running tests...${NC}"
    echo ""
    dotnet test --verbosity normal
}

do_benchmark() {
    echo ""
    echo -e "  ${YELLOW}Running benchmarks (this may take several minutes)...${NC}"
    echo ""
    dotnet run --project $BENCHMARK_PROJECT -c Release
}

do_build() {
    echo ""
    echo -e "  ${GREEN}Building solution...${NC}"
    echo ""
    dotnet build -c Release
    echo ""
    echo -e "  ${GREEN}Build complete.${NC}"
}

do_clean() {
    echo ""
    echo -e "  ${GREEN}Cleaning...${NC}"
    dotnet clean -c Release --verbosity quiet
    dotnet clean -c Debug --verbosity quiet

    if [ -d "$DATA_DIR" ]; then
        local data_size
        data_size=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
        echo -ne "  Delete generated data in data/ ($data_size)? [y/N]: "
        read -r yn
        if [[ "$yn" =~ ^[Yy]$ ]]; then
            rm -rf "$DATA_DIR"
            echo -e "  ${GREEN}Data directory removed.${NC}"
        fi
    fi

    echo -e "  ${GREEN}Clean complete.${NC}"
}

# --- Main loop ---

print_header

while true; do
    print_menu
    echo -ne "  ${BOLD}Select option: ${NC}"
    read -r choice
    case "$choice" in
        1) do_generate ;;
        2) do_sort ;;
        3) do_full_pipeline ;;
        4) do_test ;;
        5) do_benchmark ;;
        6) do_build ;;
        7) do_clean ;;
        0|q|Q) echo -e "\n  ${GREEN}Bye!${NC}\n"; exit 0 ;;
        *) echo -e "\n  ${RED}Invalid option.${NC}" ;;
    esac
    echo ""
    echo -e "  ${BOLD}────────────────────────────────────────────${NC}"
done
