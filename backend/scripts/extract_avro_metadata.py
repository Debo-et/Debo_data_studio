#!/usr/bin/env python3
"""
Extract Avro schema and sample rows using fastavro.
Usage: extract_avro_metadata.py <input_file> <sample_count> <output_file>
"""
import sys
import json
import fastavro
from logger import setup_logger, log_state_and_exit

log = setup_logger('extract_avro_metadata')

def flatten_schema(schema_obj, path='', level=0):
    """
    Recursively flatten an Avro schema into a list of field definitions.
    Each field contains: name, type, path, level, nullable, logicalType.
    """
    fields = []
    if isinstance(schema_obj, dict):
        schema_type = schema_obj.get('type')
        if schema_type == 'record':
            for field in schema_obj.get('fields', []):
                fname = field['name']
                full_path = f"{path}.{fname}" if path else fname
                ftype = field['type']

                # Detect nullable (union with null)
                nullable = False
                resolved_type = ftype
                if isinstance(ftype, list):
                    non_null = [t for t in ftype if t != 'null']
                    nullable = len(non_null) < len(ftype)
                    resolved_type = non_null[0] if non_null else 'null'

                # Logical type may be inside the type dict
                logical = None
                if isinstance(resolved_type, dict):
                    logical = resolved_type.get('logicalType')
                    resolved_type = resolved_type.get('type', 'string')
                elif isinstance(resolved_type, str):
                    pass
                else:
                    resolved_type = str(resolved_type)

                fields.append({
                    'name': fname,
                    'type': resolved_type,
                    'path': full_path,
                    'level': level,
                    'nullable': nullable,
                    'logicalType': logical,
                })

                # Recurse into nested records
                if isinstance(ftype, dict) and ftype.get('type') == 'record':
                    fields.extend(flatten_schema(ftype, full_path, level + 1))
                elif isinstance(ftype, list):
                    for union_member in ftype:
                        if isinstance(union_member, dict) and union_member.get('type') == 'record':
                            fields.extend(flatten_schema(union_member, full_path, level + 1))

    return fields

def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) != 4:
        print("Usage: extract_avro_metadata.py <input_file> <sample_count> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    try:
        sample_count = int(sys.argv[2])
    except ValueError:
        log_state_and_exit(log, f"Invalid sample_count: {sys.argv[2]}")
    output_file = sys.argv[3]

    try:
        # Open Avro file with fastavro reader
        with open(input_file, 'rb') as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema
            records = list(reader)

        # Flatten schema for the frontend display
        flattened_fields = flatten_schema(schema)

        # Prepare sample rows (limit to sample_count)
        sample_rows = records[:sample_count]

        result = {
            'fields': flattened_fields,
            'recordCount': len(records),
            'sampleRows': sample_rows
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)

        log.info(f"Successfully extracted metadata from {input_file}")

    except Exception as e:
        error_result = {'error': str(e)}
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(error_result, f)
        log_state_and_exit(log, f"Failed to process Avro file: {e}")

if __name__ == '__main__':
    main()