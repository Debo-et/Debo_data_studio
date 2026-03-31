#!/usr/bin/env python3
"""
Convert JSON, Avro, or Parquet to CSV.
Usage: structured_to_csv.py <input_file> <format> <output_file>
format: json, avro, parquet
"""
import sys
import csv
import json
import fastavro
import pyarrow.parquet as pq

def main():
    if len(sys.argv) != 4:
        print("Usage: structured_to_csv.py <input_file> <format> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    fmt = sys.argv[2].lower()
    output_file = sys.argv[3]

    records = []
    try:
        if fmt == 'json':
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Assume JSON is an array of objects
                if isinstance(data, list):
                    records = data
                else:
                    records = [data]   # single object
        elif fmt == 'avro':
            with open(input_file, 'rb') as f:
                reader = fastavro.reader(f)
                records = list(reader)
        elif fmt == 'parquet':
            table = pq.read_table(input_file)
            # Convert to list of dicts
            records = table.to_pydict()
            if records:
                keys = list(records.keys())
                rows = []
                for i in range(len(records[keys[0]])):
                    row = {k: records[k][i] for k in keys}
                    rows.append(row)
                records = rows
        else:
            print(f"Unsupported format: {fmt}", file=sys.stderr)
            sys.exit(1)

        if not records:
            # Write empty CSV with no data
            with open(output_file, 'w', newline='', encoding='utf-8') as outf:
                pass
            return

        # Get all possible column names (union of keys)
        all_keys = set()
        for rec in records:
            all_keys.update(rec.keys())
        headers = sorted(all_keys)

        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=headers, restval='', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()