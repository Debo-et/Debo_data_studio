#!/usr/bin/env python3
"""
Convert a text file with regex matches to CSV.
Usage: regex_to_csv.py <input_file> <output_file> <pattern> [flags] [columns_json]
pattern: regex with named groups, e.g. (?P<name>\w+)\s+(?P<age>\d+)
flags: optional regex flags (e.g. MULTILINE)
columns_json: optional list of column names to reorder
"""
import sys
import csv
import re
import json

def main():
    if len(sys.argv) < 5:
        print("Usage: regex_to_csv.py <input_file> <output_file> <pattern> [flags] [columns_json]", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    pattern = sys.argv[3]
    flags = 0
    col_index = 5

    if len(sys.argv) >= 5 and sys.argv[4] != '':
        # Very basic flag parsing – you can extend as needed
        if 'MULTILINE' in sys.argv[4]:
            flags |= re.MULTILINE
        if 'IGNORECASE' in sys.argv[4]:
            flags |= re.IGNORECASE
        col_index = 5
    else:
        col_index = 4

    columns = []
    if len(sys.argv) > col_index:
        columns = json.loads(sys.argv[col_index])

    with open(input_file, 'r', encoding='utf-8') as f:
        text = f.read()

    regex = re.compile(pattern, flags)
    matches = list(regex.finditer(text))

    if not matches:
        # Write empty CSV with headers if columns provided
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            if columns:
                writer = csv.writer(outf)
                writer.writerow(columns)
        return

    # Get group names from the first match
    group_names = list(matches[0].groupdict().keys())
    if not group_names:
        print("Regex must contain named groups", file=sys.stderr)
        sys.exit(1)

    # If columns not provided, use group names
    if not columns:
        columns = group_names

    with open(output_file, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        writer.writerow(columns)

        for match in matches:
            row = [match.group(col) if col in match.groupdict() else '' for col in columns]
            writer.writerow(row)

if __name__ == '__main__':
    main()