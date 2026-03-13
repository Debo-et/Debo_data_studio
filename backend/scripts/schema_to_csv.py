#!/usr/bin/env python3
"""
Convert a data file using a schema file.
Usage: schema_to_csv.py <schema_file> <data_file> <output_file> <data_format> [delimiter]
data_format: delimited, positional, xml, json, avro, parquet, regex, ldif
"""
import sys
import csv
import json
import importlib.util

def main():
    if len(sys.argv) < 5:
        print("Usage: schema_to_csv.py <schema_file> <data_file> <output_file> <data_format> [delimiter]", file=sys.stderr)
        sys.exit(1)

    schema_file = sys.argv[1]
    data_file = sys.argv[2]
    output_file = sys.argv[3]
    data_format = sys.argv[4].lower()
    delimiter = sys.argv[5] if len(sys.argv) > 5 else ','

    # Load schema (expect JSON array of column definitions)
    with open(schema_file, 'r', encoding='utf-8')