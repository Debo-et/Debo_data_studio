#!/usr/bin/env python3
"""
Convert a data file using a schema file.
Usage: schema_to_csv.py <schema_file> <data_file> <output_file> <data_format> [delimiter]
data_format: delimited, positional, xml, json, avro, parquet, regex, ldif
"""
import sys
import csv
import json
from logger import setup_logger, log_state_and_exit

log = setup_logger('schema_to_csv')

def load_schema(schema_path):
    """Load and validate schema file (expect JSON array of column definitions)."""
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        if not isinstance(schema, list):
            raise ValueError("Schema must be a JSON array")
        for i, col in enumerate(schema):
            if not all(k in col for k in ('name', 'type')):
                raise ValueError(f"Column {i} missing 'name' or 'type': {col}")
        return schema
    except Exception as e:
        log_state_and_exit(log, f"Failed to load schema from {schema_path}")

def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) < 5:
        print("Usage: schema_to_csv.py <schema_file> <data_file> <output_file> <data_format> [delimiter]", file=sys.stderr)
        sys.exit(1)

    schema_file = sys.argv[1]
    data_file = sys.argv[2]
    output_file = sys.argv[3]
    data_format = sys.argv[4].lower()
    delimiter = sys.argv[5] if len(sys.argv) > 5 else ','

    schema = load_schema(schema_file)
    log.info(f"Loaded schema with {len(schema)} columns: {[c['name'] for c in schema]}")

    # Process based on format
    if data_format == 'delimited':
        # Read delimited file (CSV/TSV) using schema to map columns
        with open(data_file, 'r', encoding='utf-8') as inf, \
             open(output_file, 'w', newline='', encoding='utf-8') as outf:
            reader = csv.reader(inf, delimiter=delimiter)
            writer = csv.writer(outf)
            writer.writerow([col['name'] for col in schema])
            for row in reader:
                writer.writerow(row)

    elif data_format == 'positional':
        # Fixed width using start/length from schema
        with open(data_file, 'r', encoding='utf-8') as inf, \
             open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            writer.writerow([col['name'] for col in schema])
            for line in inf:
                line = line.rstrip('\n')
                row = []
                for col in schema:
                    start = col.get('start', 1) - 1
                    length = col.get('length', 0)
                    field = line[start:start+length].strip()
                    row.append(field)
                writer.writerow(row)

    elif data_format == 'xml':
        # Assume data_file is XML, schema columns have 'xpath' property
        from lxml import etree
        tree = etree.parse(data_file)
        # Determine row path from schema metadata (require a top-level rowPath key)
        row_xpath = schema[0].get('rowPath', '//record') if schema else '//record'
        rows = tree.xpath(row_xpath)
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            writer.writerow([col['name'] for col in schema])
            for elem in rows:
                row = []
                for col in schema:
                    nodes = elem.xpath(col.get('xpath', col['name']))
                    value = nodes[0].text if nodes else ''
                    row.append(value.strip() if value else '')
                writer.writerow(row)

    elif data_format == 'json':
        # JSON file (array or lines) – reuse logic from structured_to_csv
        with open(data_file, 'r', encoding='utf-8') as f:
            content = f.read()
        records = []
        try:
            data = json.loads(content)
            records = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            for line in content.splitlines():
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        if not records:
            log.warning("No records found in JSON.")
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=[c['name'] for c in schema], extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)

    elif data_format == 'avro':
        import fastavro
        with open(data_file, 'rb') as f:
            reader = fastavro.reader(f)
            records = list(reader)
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=[c['name'] for c in schema], extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)

    elif data_format == 'parquet':
        import pyarrow.parquet as pq
        table = pq.read_table(data_file)
        col_dict = table.to_pydict()
        keys = list(col_dict.keys())
        num_rows = len(col_dict[keys[0]])
        rows = [{k: col_dict[k][i] for k in keys} for i in range(num_rows)]
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=[c['name'] for c in schema], extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)

    elif data_format == 'regex':
        # Expect schema to have a 'pattern' property and named groups
        pattern = schema[0].get('pattern') if schema else None
        if not pattern:
            log_state_and_exit(log, "Regex schema must include a 'pattern' in the first column definition")
        import re
        with open(data_file, 'r', encoding='utf-8') as f:
            text = f.read()
        regex = re.compile(pattern)
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            writer.writerow([col['name'] for col in schema])
            for match in regex.finditer(text):
                row = [match.group(col['name']) if col['name'] in match.groupdict() else '' for col in schema]
                writer.writerow(row)

    elif data_format == 'ldif':
        import ldif
        with open(data_file, 'rb') as f:
            parser = ldif.LDIFRecordList(f)
            parser.parse()
        entries = []
        for dn, entry in parser.all_records:
            flat = {'dn': dn}
            for attr, values in entry.items():
                flat[attr] = '|'.join(v.decode('utf-8', errors='replace') for v in values)
            entries.append(flat)
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=[c['name'] for c in schema], extrasaction='ignore')
            writer.writeheader()
            writer.writerows(entries)

    else:
        log_state_and_exit(log, f"Unsupported data format: {data_format}")

    log.info(f"Conversion complete: {output_file}")

if __name__ == '__main__':
    main()