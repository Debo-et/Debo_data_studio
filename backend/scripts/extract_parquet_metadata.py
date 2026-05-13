#!/usr/bin/env python3
"""
Extract Parquet schema and sample rows using pyarrow (or pure Python fallback).

Usage: extract_parquet_metadata.py <input_file> <sample_count> <output_file>
"""

import sys
import json
from io import BytesIO

# --------------------- logger setup ---------------------
try:
    from logger import setup_logger, log_state_and_exit
except ImportError:
    import logging
    def setup_logger(name):
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)
        if not log.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            log.addHandler(ch)
        return log
    def log_state_and_exit(log, msg, exit_code=1):
        log.error(msg)
        sys.exit(exit_code)

log = setup_logger('extract_parquet_metadata')

# ------------------------------------------------------------------------
#  pyarrow‑based extraction (robust, handles all compression codecs)
# ------------------------------------------------------------------------
try:
    import pyarrow.parquet as pq
    HAVE_PYARROW = True
except ImportError:
    HAVE_PYARROW = False

def extract_with_pyarrow(input_file: str, sample_count: int) -> dict:
    """Read Parquet metadata and sample rows using pyarrow."""
    parquet_file = pq.ParquetFile(input_file)
    schema = parquet_file.schema_arrow
    num_rows = parquet_file.metadata.num_rows
    num_row_groups = parquet_file.metadata.num_row_groups

    # Build field list
    fields = []
    for field in schema:
        fields.append({
            'name': field.name,
            'type': str(field.type),
            'nullable': field.nullable,
            'metadata': None,
        })

    # Read first row group to get sample rows
    sample_rows = []
    if num_rows > 0:
        # Read only the first row group (or part of it)
        first_rg = parquet_file.read_row_group(0)
        # Limit to sample_count rows
        table = first_rg.slice(0, min(sample_count, first_rg.num_rows))
        df = table.to_pandas()
        sample_rows = df.to_dict(orient='records')

    return {
        'fields': fields,
        'recordCount': num_rows,
        'sampleRows': sample_rows,
        'numRowGroups': num_row_groups,
    }

# ------------------------------------------------------------------------
#  Main
# ------------------------------------------------------------------------
def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) != 4:
        print("Usage: extract_parquet_metadata.py <input_file> <sample_count> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    try:
        sample_count = int(sys.argv[2])
    except ValueError:
        log_state_and_exit(log, f"Invalid sample_count: {sys.argv[2]}")

    output_file = sys.argv[3]

    try:
        if HAVE_PYARROW:
            result = extract_with_pyarrow(input_file, sample_count)
        else:
            raise RuntimeError(
                "pyarrow is not installed. Please install pyarrow: pip install pyarrow"
            )

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, default=str)

        log.info(f"Successfully extracted metadata from {input_file}")

    except Exception as e:
        error_result = {'error': str(e)}
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(error_result, f)
        log_state_and_exit(log, f"Failed to process Parquet file: {e}")

if __name__ == '__main__':
    main()