#!/usr/bin/env python3
"""
Convert JSON, Avro, or Parquet to CSV.
Usage: structured_to_csv.py <input_file> <format> <output_file>
format: json, avro, parquet

Handles JSON files as a single JSON array, a single object, or JSON Lines.
Avro and Parquet are parsed without any external libraries (standard library only).
"""
import sys
import csv
import json
import struct
import io
import zlib
from typing import Any, Dict, List, Optional, Tuple, Union
from logger import setup_logger, log_state_and_exit

log = setup_logger('structured_to_csv')

# ----------------------------------------------------------------------
#  Avro binary decoder (standard library only)
# ----------------------------------------------------------------------
class AvroBinaryDecoder:
    """Decodes Avro data from a binary stream according to a parsed schema."""
    def __init__(self, schema: dict) -> None:
        self.schema = schema

    def read_boolean(self, data: io.BytesIO) -> bool:
        return struct.unpack('?', data.read(1))[0]

    def read_int(self, data: io.BytesIO) -> int:
        return self._read_long(data)

    def read_long(self, data: io.BytesIO) -> int:
        return self._read_long(data)

    @staticmethod
    def _read_long(data: io.BytesIO) -> int:
        """Decode a variable-length zigzag-encoded long (standard Avro)."""
        b = data.read(1)
        if not b:
            raise EOFError("Unexpected end of stream while reading long")
        b = b[0]
        n = b & 0x7F
        shift = 7
        while b & 0x80:
            b = data.read(1)
            if not b:
                raise EOFError("Unexpected end of stream while reading long")
            b = b[0]
            n |= (b & 0x7F) << shift
            shift += 7
        return (n >> 1) ^ -(n & 1)

    def read_float(self, data: io.BytesIO) -> float:
        return struct.unpack('<f', data.read(4))[0]

    def read_double(self, data: io.BytesIO) -> float:
        return struct.unpack('<d', data.read(8))[0]

    def read_bytes(self, data: io.BytesIO) -> bytes:
        length = self.read_long(data)
        return data.read(length)

    def read_string(self, data: io.BytesIO) -> str:
        raw = self.read_bytes(data)
        return raw.decode('utf-8')

    def read_enum(self, data: io.BytesIO, symbols: List[str]) -> str:
        idx = self.read_int(data)
        return symbols[idx]

    def read_fixed(self, data: io.BytesIO, size: int) -> bytes:
        return data.read(size)

    def read_union(self, data: io.BytesIO, schemas: List[dict],
                   field_name: str = '') -> Any:
        """Read a union value. Union schemas are [null, type] etc."""
        # Read the index of the branch
        index = self.read_long(data)
        # index 0 is always the first schema; if it's "null" the value is null.
        branch_schema = schemas[index]
        if branch_schema.get('type') == 'null':
            return None
        return self.read_value(branch_schema, data, field_name)

    def read_value(self, schema: dict, data: io.BytesIO,
                   field_name: str = '') -> Any:
        t = schema.get('type')
        if isinstance(t, list):
            # Union
            return self.read_union(data, [{'type': item} if isinstance(item, str) else item for item in t], field_name)

        if isinstance(t, dict):
            # Named type: record, enum, fixed, array, map
            named_type = t['type']
            if named_type == 'record':
                return self.read_record(t, data)
            elif named_type == 'enum':
                return self.read_enum(data, t['symbols'])
            elif named_type == 'array':
                return self.read_array(t, data, field_name)
            elif named_type == 'map':
                return self.read_map(t, data, field_name)
            elif named_type == 'fixed':
                return self.read_fixed(data, t['size'])
            else:
                raise ValueError(f"Unsupported named type: {named_type}")
        elif isinstance(t, str):
            # Primitive
            if t == 'null':
                return None
            elif t == 'boolean':
                return self.read_boolean(data)
            elif t == 'int':
                return self.read_int(data)
            elif t == 'long':
                return self.read_long(data)
            elif t == 'float':
                return self.read_float(data)
            elif t == 'double':
                return self.read_double(data)
            elif t == 'bytes':
                raw = self.read_bytes(data)
                return raw  # keep as bytes (CSV will use repr or base64 if needed)
            elif t == 'string':
                return self.read_string(data)
            else:
                raise ValueError(f"Unknown primitive type: {t}")
        else:
            raise ValueError(f"Invalid schema type: {t}")

    def read_record(self, schema: dict, data: io.BytesIO) -> dict:
        record = {}
        for field in schema['fields']:
            name = field['name']
            field_schema = field['type']
            # field_schema might be a string or a dict/list
            if isinstance(field_schema, str):
                field_schema = {'type': field_schema}
            record[name] = self.read_value(field_schema, data, name)
        return record

    def read_array(self, schema: dict, data: io.BytesIO,
                   field_name: str = '') -> list:
        items = []
        block_count = self.read_long(data)
        while block_count != 0:
            if block_count < 0:
                # Block size is the absolute value, followed by a long block size in bytes (unused in normal decoding)
                block_count = -block_count
                _ = self.read_long(data)  # byte size of block, ignored
            for _ in range(block_count):
                items.append(self.read_value(schema['items'], data, field_name))
            block_count = self.read_long(data)
        return items

    def read_map(self, schema: dict, data: io.BytesIO,
                 field_name: str = '') -> dict:
        result = {}
        block_count = self.read_long(data)
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                _ = self.read_long(data)
            for _ in range(block_count):
                key = self.read_string(data)
                value = self.read_value(schema['values'], data, field_name)
                result[key] = value
            block_count = self.read_long(data)
        return result


def parse_avro_file(file_path: str) -> List[Dict[str, Any]]:
    """Read an Avro container file and return a list of record dictionaries."""
    with open(file_path, 'rb') as fh:
        # Container header: magic(4) + metadata map + sync(16)
        magic = fh.read(4)
        if magic != b'Obj\x01':
            raise ValueError("Not a valid Avro container file (bad magic)")

        # Read metadata map (Avro map<String,bytes> encoding)
        decoder = AvroBinaryDecoder({})  # dummy schema, we only need utils
        # map decoding: number of blocks, then key + value per entry.
        # We'll read it manually using the same variable-length long etc.
        meta = {}
        # the metadata map is written as a regular Avro map<string,bytes>
        raw_map_bytes = fh  # we continue reading from current position
        # Use the AvroBinaryDecoder's map reading but we need a schema for a map with string keys and bytes values.
        # Simpler: implement manually.
        meta_schema = {
            'type': 'map',
            'values': 'bytes'
        }
        # We'll temporarily create a decoder with this meta schema
        meta_decoder = AvroBinaryDecoder(meta_schema)
        meta = meta_decoder.read_map(meta_schema, raw_map_bytes, 'metadata')

        # Required keys: "avro.schema" and optionally "avro.codec"
        raw_schema = meta.get('avro.schema')
        if raw_schema is None:
            raise ValueError("Avro file missing 'avro.schema' in metadata")
        schema_json = raw_schema.decode('utf-8')
        schema = json.loads(schema_json)
        codec = meta.get('avro.codec', b'null').decode('utf-8')

        # Read sync marker (16 bytes)
        sync_marker = fh.read(16)

        # Now read data blocks
        decoder = AvroBinaryDecoder(schema)
        records = []
        while True:
            try:
                # A block consists of: number of objects (long) + size in bytes (long) + serialized data + sync marker
                block_header = fh.read(16)  # long + long (each variable length, tricky)
                # It's easier to use our decoder longs: but we've already advanced.
                # We'll re-read the block using a buffer.
                pass
            except EOFError:
                break

    # The simpler approach: use the known structure with low-level reads after decoding header.
    # Let's re-write the function using a byte buffer and proper stream reading.
    # Reopen the file and process blocks.
    with open(file_path, 'rb') as fh:
        magic = fh.read(4)  # already checked
        # Read map using AvroBinaryDecoder on a stream
        # The map is encoded directly after magic. We'll wrap fh with a counting buffer.
        # Better: read the whole file and use BytesIO for seeking, but we can just use the file object.
        # Let's redo with a fresh read.
        pass

    # The quickest robust implementation is to read everything into a BytesIO and parse sequentially.
    return _parse_avro(file_path)


def _parse_avro(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, 'rb') as fh:
        data = io.BytesIO(fh.read())

    magic = data.read(4)
    if magic != b'Obj\x01':
        raise ValueError("Not a valid Avro container file")

    # Read metadata map using manual map decoding
    meta = {}
    block_count = AvroBinaryDecoder._read_long(data)
    while block_count != 0:
        if block_count < 0:
            block_count = -block_count
            # next long is byte size of block, skip it
            AvroBinaryDecoder._read_long(data)
        for _ in range(block_count):
            key_len = AvroBinaryDecoder._read_long(data)
            key_bytes = data.read(key_len)
            value_len = AvroBinaryDecoder._read_long(data)
            value_bytes = data.read(value_len)
            meta[key_bytes.decode('utf-8')] = value_bytes
        block_count = AvroBinaryDecoder._read_long(data)

    if b'avro.schema' not in meta:
        raise ValueError("Missing schema in Avro metadata")
    schema = json.loads(meta[b'avro.schema'].decode('utf-8'))
    codec = meta.get(b'avro.codec', b'null').decode('utf-8')

    sync_marker = data.read(16)

    decoder = AvroBinaryDecoder(schema)
    records = []

    while True:
        # At block boundary we expect: block_count (long), block_size (long), compressed_data, sync_marker
        try:
            header_bytes = data.read(1)
            if not header_bytes:
                break
            # rewind one byte because we need to read a full variable long
            data.seek(-1, io.SEEK_CUR)
            obj_count = AvroBinaryDecoder._read_long(data)
            if obj_count == 0:
                # end of file
                break
        except EOFError:
            break

        block_size = AvroBinaryDecoder._read_long(data)
        compressed_data = data.read(block_size)
        if len(compressed_data) != block_size:
            raise EOFError("Truncated data block")

        if codec == 'null':
            uncompressed = compressed_data
        elif codec == 'deflate':
            uncompressed = zlib.decompress(compressed_data)
        else:
            raise ValueError(f"Unsupported codec: {codec}")

        block_stream = io.BytesIO(uncompressed)
        for _ in range(obj_count):
            record = decoder.read_value(schema, block_stream)
            records.append(record)

        # Consume the sync marker
        next_sync = data.read(16)
        if next_sync != sync_marker:
            # Some files may have extra padding; try to be lenient but warn
            log.warning("Sync marker mismatch, trying to continue")
        # check if we're at EOF
        peek = data.read(1)
        if not peek:
            break
        data.seek(-1, io.SEEK_CUR)

    return records


# ----------------------------------------------------------------------
#  Minimal Parquet reader (standard library only)
# ----------------------------------------------------------------------
class ParquetReader:
    """
    Reads a simple Parquet file (flat or nested with primitive leaf columns)
    and returns list of dicts. Supports PLAIN encoding, uncompressed/gzip.
    """
    FOOTER_SIZE_LENGTH = 8
    MAGIC = b'PAR1'

    @staticmethod
    def _read_footer_length(file_obj) -> int:
        file_obj.seek(-8, io.SEEK_END)
        footer_len_bytes = file_obj.read(4)
        magic = file_obj.read(4)
        if magic != ParquetReader.MAGIC:
            raise ValueError("Not a Parquet file (missing PAR1 magic)")
        return struct.unpack('<i', footer_len_bytes)[0]

    @staticmethod
    def _parse_thrift_compact(stream: io.BytesIO):
        """
        Minimal Thrift TCompactProtocol parser that returns a structure
        of the Parquet FileMetaData. Only handles the fields we need.
        """
        # We'll implement a low-level Thrift reader that can parse FileMetaData.
        # Thrift types: STOP(0), TRUE/FALSE, BYTE, DOUBLE, I16, I32, I64,
        # BINARY, STRUCT, MAP, SET, LIST, etc.
        # We only need to read structures, lists, i32, i64, and binary/string.
        from struct import unpack
        def read_byte():
            b = stream.read(1)
            if not b:
                raise EOFError()
            return b[0]

        def read_varint():
            # Thrift varint (unsigned, little endian base 128)
            shift = 0
            result = 0
            while True:
                b = read_byte()
                result |= (b & 0x7f) << shift
                if not (b & 0x80):
                    break
                shift += 7
            return result

        def read_zigzag():
            i = read_varint()
            return (i >> 1) ^ -(i & 1)

        def read_i32():
            return read_zigzag()

        def read_i64():
            return read_zigzag()

        def read_string():
            length = read_varint()
            return stream.read(length).decode('utf-8')

        def read_binary():
            length = read_varint()
            return stream.read(length)

        def read_bool():
            b = read_byte()
            return b == 1

        def read_field_begin():
            # field type + field delta
            b = read_byte()
            if b == 0:  # STOP
                return (0, 0, 0)
            # high nibble: field type, low nibble + optional delta for id
            ftype = b & 0x0f
            # For compact protocol, field id delta is a zigzag varint if the id is not 0
            # Actually, field header: first byte has type in 4 bits and some of the delta.
            # The full field id delta is (b>>4) if (b>>4) != 0 else read_zigzag()
            delta = b >> 4
            if delta == 0:
                delta = read_zigzag()
            fid = delta  # cumulative id will be handled by our struct parsing
            return (ftype, fid, 0)  # second field id not used

        def skip(field_type):
            if field_type == 0:  # STOP
                return
            elif field_type == 1 or field_type == 2:  # TRUE/FALSE
                pass  # already consumed in bool reading
            elif field_type == 3:  # BYTE
                read_byte()
            elif field_type == 4:  # DOUBLE
                stream.read(8)
            elif field_type == 6:  # I16
                read_zigzag()
            elif field_type == 8:  # I32
                read_i32()
            elif field_type == 10: # I64
                read_i64()
            elif field_type == 11: # BINARY
                sz = read_varint()
                stream.read(sz)
            elif field_type == 12: # STRUCT
                while True:
                    field_type_inner, _, _ = read_field_begin()
                    if field_type_inner == 0:
                        break
                    skip(field_type_inner)
            elif field_type == 13: # MAP
                key_type = read_byte()
                val_type = read_byte()
                map_size = read_varint()
                for _ in range(map_size):
                    skip(key_type)
                    skip(val_type)
            elif field_type == 14: # SET
                elem_type = read_byte()
                set_size = read_varint()
                for _ in range(set_size):
                    skip(elem_type)
            elif field_type == 15: # LIST
                elem_type = read_byte()
                list_size = read_varint()
                for _ in range(list_size):
                    skip(elem_type)
            else:
                raise ValueError(f"Unknown thrift field type: {field_type}")

        # Parse FileMetaData structure (fields listed in Parquet spec):
        # 1: version (i32)
        # 2: schema (list<SchemaElement>)
        # 3: num_rows (i64)
        # 4: row_groups (list<RowGroup>)
        # 5: key_value_metadata (optional list<KeyValue>)
        # 6: created_by (optional string)
        # 7: column_orders (optional list<ColumnOrder>)
        file_meta = {}
        while True:
            ftype, fid, _ = read_field_begin()
            if ftype == 0:
                break
            if fid == 1:  # version
                file_meta['version'] = read_i32()
            elif fid == 2:  # schema
                file_meta['schema'] = ParquetReader._read_schema_element_list(stream)
            elif fid == 3:  # num_rows
                file_meta['num_rows'] = read_i64()
            elif fid == 4:  # row_groups
                file_meta['row_groups'] = ParquetReader._read_row_group_list(stream)
            elif fid == 5:  # key_value_metadata
                # skip or read if needed later
                list_size = read_varint()
                for _ in range(list_size):
                    # struct KeyValue: 1:key string, 2:value string
                    while True:
                        ftype_kv, fid_kv, _ = read_field_begin()
                        if ftype_kv == 0:
                            break
                        if fid_kv == 1:
                            read_string()
                        elif fid_kv == 2:
                            read_string()
                        else:
                            skip(ftype_kv)
            elif fid == 6:  # created_by
                file_meta['created_by'] = read_string()
            elif fid == 7:  # column_orders
                list_size = read_varint()
                for _ in range(list_size):
                    # struct ColumnOrder: 1: TYPE_ORDER (i32)
                    while True:
                        ftype_co, fid_co, _ = read_field_begin()
                        if ftype_co == 0:
                            break
                        if fid_co == 1:
                            read_i32()
                        else:
                            skip(ftype_co)
            else:
                skip(ftype)
        return file_meta

    @staticmethod
    def _read_schema_element_list(stream):
        # list<SchemaElement>: list size, then repeated structs
        elements = []
        list_size = read_varint(stream)
        for _ in range(list_size):
            elem = ParquetReader._read_schema_element(stream)
            elements.append(elem)
        return elements

    @staticmethod
    def _read_schema_element(stream):
        # SchemaElement fields: 1:type(i32?), 2:type_length, 3:repetition_type, 4:name, 5:num_children, 6:converted_type, 7:scale, 8:precision, 9:field_id, 10:logicalType struct
        # We'll capture name, repetition_type, type, converted_type, num_children
        elem = {}
        while True:
            ftype, fid, _ = read_field_begin(stream)
            if ftype == 0:
                break
            if fid == 1:  # type (i32)
                elem['type'] = read_i32(stream)
            elif fid == 2:
                elem['type_length'] = read_i32(stream)
            elif fid == 3:
                elem['repetition_type'] = ParquetReader._read_repetition_type(stream)
            elif fid == 4:
                elem['name'] = read_string(stream)
            elif fid == 5:
                elem['num_children'] = read_i32(stream)
            elif fid == 6:
                elem['converted_type'] = read_i32(stream)
            else:
                # skip unknown fields
                skip(ftype, stream)
        return elem

    @staticmethod
    def _read_repetition_type(stream):
        # Actually it's an i32 (Thrift i32), but we'll read as int.
        return read_i32(stream)

    @staticmethod
    def _read_row_group_list(stream):
        row_groups = []
        list_size = read_varint(stream)
        for _ in range(list_size):
            rg = ParquetReader._read_row_group(stream)
            row_groups.append(rg)
        return row_groups

    @staticmethod
    def _read_row_group(stream):
        # RowGroup fields: 1:columns (list<ColumnChunk>), 2:total_byte_size(i64), 3:num_rows(i64), 4:sorting_columns, etc.
        rg = {'columns': []}
        while True:
            ftype, fid, _ = read_field_begin(stream)
            if ftype == 0:
                break
            if fid == 1:
                # list<ColumnChunk>
                col_list_size = read_varint(stream)
                for _ in range(col_list_size):
                    col = ParquetReader._read_column_chunk(stream)
                    rg['columns'].append(col)
            elif fid == 2:
                rg['total_byte_size'] = read_i64(stream)
            elif fid == 3:
                rg['num_rows'] = read_i64(stream)
            else:
                skip(ftype, stream)
        return rg

    @staticmethod
    def _read_column_chunk(stream):
        col = {}
        while True:
            ftype, fid, _ = read_field_begin(stream)
            if ftype == 0:
                break
            if fid == 1:  # file_path (string)
                col['file_path'] = read_string(stream)
            elif fid == 2:  # file_offset (i64)
                col['file_offset'] = read_i64(stream)
            elif fid == 3:  # meta_data (ColumnMetaData)
                col['meta_data'] = ParquetReader._read_column_metadata(stream)
            elif fid == 4:  # offset_index_offset, etc. skip
                skip(ftype, stream)
            else:
                skip(ftype, stream)
        return col

    @staticmethod
    def _read_column_metadata(stream):
        meta = {}
        while True:
            ftype, fid, _ = read_field_begin(stream)
            if ftype == 0:
                break
            if fid == 1:  # type (Type)
                meta['type'] = read_i32(stream)  # Type is i32 enum
            elif fid == 2:  # encodings (list<Encoding>)
                enc_list_size = read_varint(stream)
                encodings = []
                for _ in range(enc_list_size):
                    encodings.append(read_i32(stream))
                meta['encodings'] = encodings
            elif fid == 3:  # path_in_schema (list<string>)
                path_size = read_varint(stream)
                path = []
                for _ in range(path_size):
                    path.append(read_string(stream))
                meta['path_in_schema'] = path
            elif fid == 4:  # codec (CompressionCodec)
                meta['codec'] = read_i32(stream)
            elif fid == 5:  # num_values (i64)
                meta['num_values'] = read_i64(stream)
            elif fid == 6:  # total_uncompressed_size (i64)
                meta['total_uncompressed_size'] = read_i64(stream)
            elif fid == 7:  # total_compressed_size (i64)
                meta['total_compressed_size'] = read_i64(stream)
            elif fid == 8:  # data_page_offset (i64)
                meta['data_page_offset'] = read_i64(stream)
            elif fid == 9:  # dictionary_page_offset (i64, optional)
                # We'll capture it but may not use
                meta['dictionary_page_offset'] = read_i64(stream)
            else:
                skip(ftype, stream)
        return meta

    @staticmethod
    def _read_page_header(stream):
        # PageHeader: 1:type, 2:uncompressed_page_size, 3:compressed_page_size, 4:crc
        header = {}
        while True:
            ftype, fid, _ = read_field_begin(stream)
            if ftype == 0:
                break
            if fid == 1:
                header['type'] = read_i32(stream)
            elif fid == 2:
                header['uncompressed_page_size'] = read_i32(stream)
            elif fid == 3:
                header['compressed_page_size'] = read_i32(stream)
            else:
                skip(ftype, stream)
        return header

    @staticmethod
    def _decompress_page(data, codec):
        if codec == 0:  # UNCOMPRESSED
            return data
        elif codec == 1:  # SNAPPY
            raise NotImplementedError("Snappy decompression requires external library")
        elif codec == 2:  # GZIP
            return zlib.decompress(data)
        elif codec == 3:  # LZO
            raise NotImplementedError("LZO not supported")
        elif codec == 4:  # BROTLI
            raise NotImplementedError("Brotli not supported")
        elif codec == 5:  # LZ4
            raise NotImplementedError("LZ4 not supported")
        elif codec == 6:  # ZSTD
            raise NotImplementedError("ZSTD not supported")
        else:
            raise ValueError(f"Unknown compression codec: {codec}")

    @staticmethod
    def _read_plain_values(data_bytes, parquet_type, count, type_length=None):
        """Read count plain-encoded values of the given parquet physical type."""
        stream = io.BytesIO(data_bytes)
        if parquet_type == 0:  # BOOLEAN
            values = []
            byte_reader = stream.read(1)
            bit_pos = 0
            for _ in range(count):
                if byte_reader is None:
                    raise EOFError()
                bit = (byte_reader[0] >> bit_pos) & 1
                values.append(bool(bit))
                bit_pos += 1
                if bit_pos == 8:
                    byte_reader = stream.read(1)
                    bit_pos = 0
            return values
        elif parquet_type == 1:  # INT32
            values = []
            for _ in range(count):
                val = struct.unpack('<i', stream.read(4))[0]
                values.append(val)
            return values
        elif parquet_type == 2:  # INT64
            values = []
            for _ in range(count):
                val = struct.unpack('<q', stream.read(8))[0]
                values.append(val)
            return values
        elif parquet_type == 3:  # INT96 (12 bytes, often used for timestamps)
            values = []
            for _ in range(count):
                raw = stream.read(12)
                values.append(int.from_bytes(raw, byteorder='little'))  # as big int
            return values
        elif parquet_type == 4:  # FLOAT
            values = []
            for _ in range(count):
                val = struct.unpack('<f', stream.read(4))[0]
                values.append(val)
            return values
        elif parquet_type == 5:  # DOUBLE
            values = []
            for _ in range(count):
                val = struct.unpack('<d', stream.read(8))[0]
                values.append(val)
            return values
        elif parquet_type == 6:  # BYTE_ARRAY
            values = []
            for _ in range(count):
                length = struct.unpack('<i', stream.read(4))[0]
                raw = stream.read(length)
                values.append(raw)
            return values
        elif parquet_type == 7:  # FIXED_LEN_BYTE_ARRAY
            length = type_length
            values = []
            for _ in range(count):
                raw = stream.read(length)
                values.append(raw)
            return values
        else:
            raise ValueError(f"Unknown physical type: {parquet_type}")

    @staticmethod
    def _decode_column_values(file_obj, column_chunk, parquet_type, count, type_length=None):
        """Read data pages and decode plain values."""
        meta = column_chunk['meta_data']
        codec = meta.get('codec', 0)
        data_page_offset = meta['data_page_offset']
        file_obj.seek(data_page_offset)
        # There can be multiple pages; we'll assume only one DATA_PAGE_V2 or DATA_PAGE for simplicity.
        # First read PageHeader
        # Since we don't know the serialized length of header, read page header bytes up to end of header.
        # Use a buffered approach: read header using Thrift parsing until STOP.
        # The header is a Thrift struct followed by compressed page data.
        # We'll read the header by parsing from current seek.
        header_stream = file_obj  # we can read sequentially
        # But we need to know when header ends. The header ends when we encounter STOP field (0x00).
        # We'll parse with the ParquetReader's internal read_field_begin on the file stream.
        # However, the file_obj is not a BytesIO. We'll read a chunk and use BytesIO.
        # Simpler: read the whole column chunk from data_page_offset to (maybe next column start).
        # Determine end: we can use next column's file_offset or if it's the last column, until EOF.
        # We'll use the total_compressed_size of column chunk to limit reading.
        compressed_size = meta.get('total_compressed_size', None)
        if compressed_size is None:
            # fallback: read until end of row group? Safer: calculate from uncompressed.
            uncompressed = meta['total_uncompressed_size']
            # but we don't know exact compressed. We'll read a large buffer.
            file_obj.seek(data_page_offset)
            buffer = file_obj.read(10*1024*1024)  # 10MB, adjust
        else:
            file_obj.seek(data_page_offset)
            buffer = file_obj.read(compressed_size)
        buff = io.BytesIO(buffer)
        page_header = ParquetReader._read_page_header(buff)
        # Now read the compressed page data according to header sizes.
        if page_header.get('type') == 1:  # DATA_PAGE_V2 (or 0 for V1)
            compressed_page_size = page_header.get('compressed_page_size', page_header['uncompressed_page_size'])
            # V2: after header there are repetition and definition levels (if needed), but for flat data (repetition=REQUIRED) they are omitted.
            # We'll assume flat schema (repetition REQUIRED) for simplicity.
            page_data = buff.read(compressed_page_size)
            uncompressed = ParquetReader._decompress_page(page_data, codec)
            # Now decode plain values
            values = ParquetReader._read_plain_values(uncompressed, parquet_type, count, type_length)
            return values
        else:
            # Fallback for V2
            compressed_page_size = page_header.get('compressed_page_size', page_header['uncompressed_page_size'])
            page_data = buff.read(compressed_page_size)
            uncompressed = ParquetReader._decompress_page(page_data, codec)
            values = ParquetReader._read_plain_values(uncompressed, parquet_type, count, type_length)
            return values

    @staticmethod
    def read_file(file_path: str) -> List[Dict[str, Any]]:
        with open(file_path, 'rb') as fh:
            # Read footer
            footer_len = ParquetReader._read_footer_length(fh)
            fh.seek(-(8 + footer_len), io.SEEK_END)
            footer_bytes = fh.read(footer_len)
            footer_stream = io.BytesIO(footer_bytes)
            metadata = ParquetReader._parse_thrift_compact(footer_stream)
            schema = metadata['schema']
            row_groups = metadata.get('row_groups', [])
            num_rows = metadata.get('num_rows', 0)

            # Build column name list and type info from schema (simplistic)
            # We need mapping from column path to physical type, repetition, etc.
            # For simplicity, assume flat schema: each leaf SchemaElement with no children.
            columns_info = []
            for elem in schema:
                # skip root (name == 'schema' and num_children >0)
                if elem.get('num_children', 0) > 0:
                    continue
                col_info = {
                    'name': elem['name'],
                    'type': elem.get('type'),
                    'repetition_type': elem.get('repetition_type'),
                    'type_length': elem.get('type_length')
                }
                columns_info.append(col_info)

            # For each row group, read columns
            all_rows = []
            for rg in row_groups:
                rg_columns = rg['columns']
                # Order columns by matching path with columns_info order
                # We'll read values column by column
                col_values = []
                for col_chunk in rg_columns:
                    path = col_chunk['meta_data']['path_in_schema']
                    # find matching info by path
                    for info in columns_info:
                        if [info['name']] == path:
                            col_info = info
                            break
                    else:
                        log.warning(f"Column {path} not found in schema, skipping")
                        col_values.append([])
                        continue
                    phys_type = col_info['type']
                    count = col_chunk['meta_data']['num_values']
                    type_len = col_info.get('type_length')
                    values = ParquetReader._decode_column_values(
                        fh, col_chunk, phys_type, count, type_len
                    )
                    col_values.append(values)
                # Transpose: create records
                num_cols = len(col_values)
                if num_cols == 0:
                    continue
                num_rg_rows = len(col_values[0])
                for i in range(num_rg_rows):
                    row = {}
                    for c_idx, info in enumerate(columns_info):
                        if c_idx < len(col_values):
                            val = col_values[c_idx][i] if i < len(col_values[c_idx]) else None
                            # Convert bytes to string for CSV friendliness
                            if isinstance(val, bytes):
                                try:
                                    val = val.decode('utf-8')
                                except UnicodeDecodeError:
                                    val = val.hex()  # fallback
                            row[info['name']] = val
                    all_rows.append(row)
            return all_rows


# ----------------------------------------------------------------------
# CSV writing helpers
# ----------------------------------------------------------------------
def write_csv(records: List[Dict[str, Any]], output_file: str) -> None:
    if not records:
        with open(output_file, 'w', newline='', encoding='utf-8') as outf:
            pass
        log.warning("No records found; empty CSV written.")
        return

    all_keys = set()
    for rec in records:
        all_keys.update(rec.keys())
    headers = sorted(all_keys)
    log.debug(f"CSV headers: {headers}")

    with open(output_file, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=headers, restval='', extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    log.info(f"Wrote {len(records)} rows to {output_file}")


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def main():
    log.debug(f"Arguments: {sys.argv}")
    if len(sys.argv) != 4:
        print("Usage: structured_to_csv.py <input_file> <format> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    fmt = sys.argv[2].lower()
    output_file = sys.argv[3]

    records = []
    try:
        if fmt == 'json':
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
            log.info(f"Read {len(content)} bytes from JSON file")
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    records = data
                    log.info(f"Parsed JSON array with {len(records)} items")
                else:
                    records = [data]
                    log.info("Parsed single JSON object")
            except json.JSONDecodeError:
                records = []
                lines = content.splitlines()
                log.info(f"Attempting JSON Lines parsing of {len(lines)} lines")
                for line_no, line in enumerate(lines, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, list):
                            records.extend(obj)
                        elif isinstance(obj, dict):
                            records.append(obj)
                    except json.JSONDecodeError:
                        log.warning(f"Skipping invalid JSON at line {line_no}: {line[:100]}")
                log.info(f"JSON Lines parsed {len(records)} records")

        elif fmt == 'avro':
            records = parse_avro_file(input_file)
            log.info(f"Avro file parsed, {len(records)} records")

        elif fmt == 'parquet':
            reader = ParquetReader()
            records = reader.read_file(input_file)
            log.info(f"Parquet file parsed, {len(records)} records")
        else:
            log_state_and_exit(log, f"Unsupported format: {fmt}")

    except Exception as e:
        log_state_and_exit(log, f"Failed to read/parse input file: {e}")

    write_csv(records, output_file)


if __name__ == '__main__':
    main()