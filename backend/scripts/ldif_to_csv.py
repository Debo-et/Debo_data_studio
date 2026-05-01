#!/usr/bin/env python3
"""
Convert LDIF to CSV.
Usage: ldif_to_csv.py <input_file> <output_file>
"""
import sys
import csv
import ldif
from logger import setup_logger, log_state_and_exit

log = setup_logger('ldif_to_csv')

def decode_value(v):
    """Decode bytes to string, handling None values."""
    if v is None:
        return ''
    if isinstance(v, bytes):
        return v.decode('utf-8', errors='replace')
    return str(v)

class LDIFRecordCollector(ldif.LDIFParser):
    """Collects LDIF entries and flattens attributes."""
    def __init__(self, infile):
        super().__init__(infile)
        self.all_records = []

    def handle(self, dn, entry):
        """
        Called by the parser for each LDIF entry.
        `entry` is a dict: attribute name -> list of values (str or bytes).
        """
        processed_entry = {}
        for attr, values in entry.items():
            decoded_vals = [decode_value(v) for v in values]
            if len(decoded_vals) == 1:
                processed_entry[attr] = decoded_vals[0]
            else:
                processed_entry[attr] = '|'.join(decoded_vals)
        self.all_records.append((dn, processed_entry))

def main():
    log.debug(f"Starting with arguments: {sys.argv}")
    if len(sys.argv) != 3:
        print("Usage: ldif_to_csv.py <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    entries = []
    try:
        with open(input_file, 'rb') as f:
            parser = LDIFRecordCollector(f)
            parser.parse()
            log.info(f"Parsed {len(parser.all_records)} records from {input_file}")
            for dn, entry in parser.all_records:
                flat = {'dn': dn}
                flat.update(entry)
                entries.append(flat)
    except Exception as e:
        log_state_and_exit(log, f"Failed to parse LDIF file: {input_file}")

    if not entries:
        log.warning("No entries found; writing empty CSV.")
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            pass
        return

    all_keys = set()
    for e in entries:
        all_keys.update(e.keys())
    headers = sorted(all_keys)

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            writer = csv.DictWriter(outf, fieldnames=headers, restval='', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(entries)
        log.info(f"Wrote {len(entries)} rows to {output_file} with headers {headers}")
    except Exception as e:
        log_state_and_exit(log, f"Failed to write CSV output to {output_file}")

if __name__ == '__main__':
    main()