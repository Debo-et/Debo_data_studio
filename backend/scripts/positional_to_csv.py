#!/usr/bin/env python3
"""
Convert a positional (fixed-width) file to CSV.
Usage: positional_to_csv.py <input_file> <columns_json> <output_file>
columns_json: [{"name": "col1", "start": 1, "length": 10}, ...] (1‑based start)
"""
import sys
import csv
import json

def main():
    if len(sys.argv) != 4:
        print("Usage: positional_to_csv.py <input_file> <columns_json> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    columns_json = sys.argv[2]
    output_file = sys.argv[3]

    try:
        columns = json.loads(columns_json)
    except json.JSONDecodeError as e:
        print(f"Error parsing columns JSON: {e}", file=sys.stderr)
        sys.exit(1)

    with open(input_file, 'r', encoding='utf-8') as inf, \
         open(output_file, 'w', newline='', encoding='utf-8') as outf:

        writer = csv.writer(outf)
        # Write header
        writer.writerow([col['name'] for col in columns])

        for line in inf:
            row = []
            line = line.rstrip('\n')
            for col in columns:
                start = col['start'] - 1          # convert to 0‑based
                end = start + col['length']
                field = line[start:end].strip()
                row.append(field)
            writer.writerow(row)

if __name__ == '__main__':
    main()