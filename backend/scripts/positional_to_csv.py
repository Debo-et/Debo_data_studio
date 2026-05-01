#!/usr/bin/env python3
"""
Convert a positional (fixed-width) file to CSV.
Usage: positional_to_csv.py <input_file> <columns_json> <output_file>
columns_json: [{"name": "col1", "start": 1, "length": 10}, ...] (1‑based start)
"""
import sys
import csv
import json
from logger import setup_logger, log_state_and_exit

log = setup_logger('positional_to_csv')

def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) != 4:
        print("Usage: positional_to_csv.py <input_file> <columns_json> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    columns_json = sys.argv[2]
    output_file = sys.argv[3]

    # Parse columns
    try:
        columns = json.loads(columns_json)
        log.info(f"Columns loaded: {len(columns)} columns - {[c['name'] for c in columns]}")
    except json.JSONDecodeError as e:
        log_state_and_exit(log, f"Error parsing columns JSON: {e}")

    if not isinstance(columns, list):
        log_state_and_exit(log, "Columns must be a JSON array")

    # Validate each column has required keys
    for i, col in enumerate(columns):
        if 'name' not in col or 'start' not in col or 'length' not in col:
            log_state_and_exit(log, f"Column {i} missing 'name', 'start', or 'length': {col}")

    try:
        with open(input_file, 'r', encoding='utf-8') as inf:
            lines = inf.readlines()
        log.info(f"Read {len(lines)} lines from {input_file}")
    except Exception as e:
        log_state_and_exit(log, f"Failed to read input file: {input_file}")

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            # Write header
            writer.writerow([col['name'] for col in columns])

            for line_no, line in enumerate(lines, 1):
                line = line.rstrip('\n')
                row = []
                for col in columns:
                    start = col['start'] - 1          # convert to 0‑based
                    end = start + col['length']
                    field = line[start:end].strip()
                    row.append(field)
                writer.writerow(row)
        log.info(f"Wrote {len(lines)} rows to {output_file}")
    except Exception as e:
        log_state_and_exit(log, f"Failed to write CSV output to {output_file}")

if __name__ == '__main__':
    main()