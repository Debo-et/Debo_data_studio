#!/usr/bin/env python3
"""
Convert a text file with regex matches to CSV.

Backend (new) calling convention:
    regex_to_csv.py <input_file> <pattern> [flags] [columns_json] <output_file>

Legacy calling convention:
    regex_to_csv.py <input_file> <output_file> <pattern> [flags] [columns_json]

If the pattern contains named groups like (?P<name>...), column headers are taken from them.
If only numbered groups exist, column headers can be supplied via columns_json.
If no column names are given at all, default names 'col_1', 'col_2', ... are generated.
"""

import sys
import csv
import re
import json
import os
from logger import setup_logger, log_state_and_exit

log = setup_logger('regex_to_csv')

# Known output file extensions (used to guess the legacy calling convention)
KNOWN_EXTENSIONS = {'.csv', '.txt', '.log', '.json', '.xml', '.sql'}

def _is_likely_output_file(arg: str) -> bool:
    ext = os.path.splitext(arg)[1].lower()
    return ext in KNOWN_EXTENSIONS

def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) < 4:
        print("Usage: regex_to_csv.py <input_file> <pattern> [flags] [columns_json] <output_file>", file=sys.stderr)
        print("       or: regex_to_csv.py <input_file> <output_file> <pattern> [flags] [columns_json]", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Argument order detection
    # ------------------------------------------------------------------
    # Legacy: second argument has a known file extension
    if len(sys.argv) >= 5 and _is_likely_output_file(sys.argv[2]):
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        pattern_idx = 3
        log.debug("Using legacy argument order (input output pattern ...)")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[-1]
        pattern_idx = 2
        log.debug("Using backend argument order (input pattern ... output)")

    pattern = sys.argv[pattern_idx] if pattern_idx < len(sys.argv) else ''
    remaining_args = sys.argv[pattern_idx + 1:]
    # Trim the output file name from remaining_args (it's the last element in backend order)
    if _is_likely_output_file(sys.argv[-1]) and len(remaining_args) > 0 and remaining_args[-1] == output_file:
        remaining_args = remaining_args[:-1]

    if not pattern or pattern.strip() == '' or pattern == 'undefined':
        log_state_and_exit(log, "Invalid regex pattern: pattern is empty or 'undefined'")

    # ------------------------------------------------------------------
    # Parse flags and optional columns JSON
    # ------------------------------------------------------------------
    flags = 0
    flags_str = ''
    columns_json = ''

    for arg in remaining_args:
        if arg in ('MULTILINE', 'IGNORECASE'):
            flags_str = arg
        elif arg.startswith('[') or arg.startswith('{'):
            columns_json = arg

    if flags_str:
        if 'MULTILINE' in flags_str.upper():
            flags |= re.MULTILINE
        if 'IGNORECASE' in flags_str.upper():
            flags |= re.IGNORECASE

    # Parse the optional column list
    columns = []
    if columns_json and columns_json.strip():
        try:
            columns = json.loads(columns_json)
            if not isinstance(columns, list):
                log_state_and_exit(log, f"Columns JSON must be an array, got {type(columns)}")
        except json.JSONDecodeError as e:
            log_state_and_exit(log, f"Error parsing columns JSON: {e}")

    # ------------------------------------------------------------------
    # Read input file
    # ------------------------------------------------------------------
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            text = f.read()
    except Exception as e:
        log_state_and_exit(log, f"Failed to read input file: {input_file} ({e})")

    # ------------------------------------------------------------------
    # Compile and match
    # ------------------------------------------------------------------
    try:
        regex = re.compile(pattern, flags)
    except re.error as e:
        log_state_and_exit(log, f"Invalid regex pattern: {e}")

    matches = list(regex.finditer(text))
    log.info(f"Found {len(matches)} matches")

    # ------------------------------------------------------------------
    # Determine column headers
    # ------------------------------------------------------------------
    if not matches:
        # Write header only if columns are known (or leave empty)
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            if columns:
                writer.writerow(columns)
        log.warning("No matches found; empty CSV written (header only).")
        return

    first_match = matches[0]
    group_names = list(first_match.groupdict().keys())
    use_named = bool(group_names)

    if use_named:
        # Columns come from named groups if not already supplied
        if not columns:
            columns = group_names
            log.info(f"Inferred columns from named groups: {columns}")
    else:
        # Numbered groups
        num_groups = len(first_match.groups())
        if columns:
            # Explicit column names provided – validate count
            if len(columns) != num_groups:
                log_state_and_exit(
                    log,
                    f"Number of provided columns ({len(columns)}) does not match "
                    f"number of capturing groups ({num_groups})."
                )
        else:
            # No columns supplied → generate default names
            columns = [f"col_{i}" for i in range(1, num_groups + 1)]
            log.info(
                f"No column names provided. Using default names: {columns}. "
                "To use custom names, supply a columns_json array or use named groups."
            )

    # ------------------------------------------------------------------
    # Write CSV
    # ------------------------------------------------------------------
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            writer.writerow(columns)

            for match in matches:
                if use_named:
                    row = [match.group(col) if col in match.groupdict() else '' for col in columns]
                else:
                    # Numbered groups: match columns by order (group 1 → columns[0], etc.)
                    row = [match.group(i + 1) for i in range(len(columns))]
                writer.writerow(row)

        log.info(f"Wrote {len(matches)} rows to {output_file}")
    except Exception as e:
        log_state_and_exit(log, f"Failed to write CSV output: {e}")

if __name__ == '__main__':
    main()