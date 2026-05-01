#!/usr/bin/env python3
"""
Convert XML to CSV.
Usage: xml_to_csv.py <input_file> <row_xpath> <columns_json> <output_file>
columns_json: [{"name": "col1", "type": "String"}, ...]
"""
import sys
import csv
import json
from lxml import etree
from logger import setup_logger, log_state_and_exit

log = setup_logger('xml_to_csv')

def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) != 5:
        print("Usage: xml_to_csv.py <input_file> <row_xpath> <columns_json> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    row_xpath = sys.argv[2]
    columns_json = sys.argv[3]
    output_file = sys.argv[4]

    # Parse columns
    try:
        columns = json.loads(columns_json)
        log.info(f"Columns: {[col['name'] for col in columns]}")
    except json.JSONDecodeError as e:
        log_state_and_exit(log, f"Error parsing columns JSON: {e}")

    # Parse XML
    try:
        tree = etree.parse(input_file)
        log.info(f"XML parsed: {input_file}")
    except Exception as e:
        log_state_and_exit(log, f"Failed to parse XML file: {e}")

    # Find rows
    try:
        rows = tree.xpath(row_xpath)
        log.info(f"Found {len(rows)} rows with XPath '{row_xpath}'")
    except Exception as e:
        log_state_and_exit(log, f"XPath evaluation failed: {e}")

    if not rows:
        log.warning("No rows matched; writing empty CSV.")
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            writer.writerow([col['name'] for col in columns])
        return

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.writer(outf)
            writer.writerow([col['name'] for col in columns])

            for row_elem in rows:
                row = []
                for col in columns:
                    nodes = row_elem.xpath(col['name'])  # use 'name' as the XPath
                    if nodes:
                        value = nodes[0].text
                        row.append(value.strip() if value else '')
                    else:
                        row.append('')
                writer.writerow(row)
        log.info(f"Wrote {len(rows)} rows to {output_file}")
    except Exception as e:
        log_state_and_exit(log, f"Failed to write CSV output: {e}")

if __name__ == '__main__':
    main()