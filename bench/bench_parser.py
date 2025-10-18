#!/usr/bin/env python3

import sys
import re
from collections import defaultdict

# Read all input from stdin
data = sys.stdin.read()

# Parse benchmarks
benchmarks = defaultdict(dict)

for line in data.strip().split('\n'):
    parts = line.split()
    if not parts or not parts[0].startswith('Benchmark'):
        continue

    bench_info = parts[0]
    try:
        ns_op = float(parts[2])
    except (IndexError, ValueError):
        continue

    match = re.match(r'(\w+)/(\w+)(?:/(\w+))?-\d+', bench_info)
    if match:
        bench_type = match.group(1)
        db_name = match.group(2)
        sync_opt = match.group(3) or ""

        key = (bench_type, sync_opt) if sync_opt else bench_type
        benchmarks[key][db_name] = ns_op

# Sort keys properly
sorted_keys = sorted(benchmarks.keys(), key=lambda x: (x[0] if isinstance(x, tuple) else x, x[1] if isinstance(x, tuple) else ""))

# Generate markdown table
print("| Benchmark | Bbolt | Badger | Pebble | SQLite |")
print("|-----------|-------|--------|--------|--------|")

for benchmark in sorted_keys:
    if benchmarks[benchmark].get('Fredb') is None:
        continue

    fredb_time = benchmarks[benchmark]['Fredb']

    if isinstance(benchmark, tuple):
        bench_name, sync_opt = benchmark
        label = f"{bench_name} ({sync_opt})"
    else:
        label = benchmark

    row_data = [label]

    for db_name in ['Bbolt', 'Badger', 'Pebble', 'SQLite']:
        if db_name not in benchmarks[benchmark]:
            row_data.append("—")
            continue

        other_time = benchmarks[benchmark][db_name]
        pct_diff = ((other_time - fredb_time) / fredb_time) * 100

        if pct_diff < -50:
            emoji = "↓"
        elif pct_diff < 0:
            emoji = "↓"
        elif pct_diff > 500:
            emoji = "↑"
        elif pct_diff > 100:
            emoji = "↑"
        else:
            emoji = "↑"

        row_data.append(f"{pct_diff:+.1f}% {emoji}")

    print(f"| {' | '.join(row_data)} |")
