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

def main():
    if len(sys.argv) != 5:
        print("Usage: xml_to_csv.py <input_file> <row_xpath> <columns_json> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    row_xpath = sys.argv[2]
    columns = json.loads(sys.argv[3])
    output_file = sys.argv[4]

    tree = etree.parse(input_file)
    rows = tree.xpath(row_xpath)

    with open(output_file, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.writer(outf)
        writer.writerow([col['name'] for col in columns])

        for elem in rows:
            row = []
            for col in columns:
                nodes = elem.xpath(col['name'])  # use 'name' as the XPath
                value = nodes[0].text if nodes else ''
                row.append(value.strip() if value else '')
            writer.writerow(row)

if __name__ == '__main__':
    main()