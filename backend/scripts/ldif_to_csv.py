#!/usr/bin/env python3
"""
Convert LDIF to CSV.
Usage: ldif_to_csv.py <input_file> <output_file>
"""
import sys
import csv
import ldif


def decode_value(v):
    """Decode bytes to string, handling None values."""
    if v is None:
        return ''
    return v.decode('utf-8', errors='replace')


def main():
    if len(sys.argv) != 3:
        print("Usage: ldif_to_csv.py <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    entries = []
    with open(input_file, 'rb') as f:
        parser = ldif.LDIFRecordList(f)
        parser.parse()
        for dn, entry in parser.all_records:
            flat = {}
            flat['dn'] = dn
            for attr, values in entry.items():
                if len(values) == 1:
                    flat[attr] = decode_value(values[0])
                else:
                    flat[attr] = '|'.join(decode_value(v) for v in values)
            entries.append(flat)

    if not entries:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            pass
        return

    all_keys = set()
    for e in entries:
        all_keys.update(e.keys())
    headers = sorted(all_keys)

    with open(output_file, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=headers, restval='', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(entries)


if __name__ == '__main__':
    main()