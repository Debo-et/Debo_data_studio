// src/components/layout/type-mapper.ts

/**
 * Maps PostgreSQL data types to UI-friendly types, including special handling for foreign tables.
 * @param dataType - PostgreSQL data type string (e.g., 'integer', 'varchar', 'timestamp')
 * @param foreignOptions - Optional foreign table options that may contain type hints or overrides
 * @returns UI-friendly type: 'string' | 'number' | 'boolean' | 'date' | 'json' | 'unknown'
 */
export function mapPgTypeToUiType(
  dataType: string | null | undefined,
  foreignOptions?: any
): 'string' | 'number' | 'boolean' | 'date' | 'json' | 'unknown' {
  if (!dataType) {
    return 'unknown';
  }
  
  const typeLower = dataType.toLowerCase().trim();
  
  // Check foreign options for type hints first (higher priority)
  if (foreignOptions) {
    // Extract type hints from foreign server options
    const foreignTypeHint = extractTypeHintFromForeignOptions(foreignOptions, typeLower);
    if (foreignTypeHint) {
      return foreignTypeHint;
    }
  }
  
  // Handle PostgreSQL arrays (e.g., integer[], text[])
  if (typeLower.endsWith('[]')) {
    return 'json'; // Arrays are treated as JSON in UI
  }
  
  // Integer types
  if (typeLower.includes('int') || 
      typeLower.includes('serial') || 
      typeLower.includes('smallserial') ||
      typeLower.includes('bigserial') ||
      typeLower === 'smallint' ||
      typeLower === 'integer' ||
      typeLower === 'bigint' ||
      typeLower === 'oid' ||
      typeLower === 'regproc') {
    return 'number';
  }
  
  // Decimal/Numeric types
  if (typeLower.includes('decimal') || 
      typeLower.includes('numeric') ||
      typeLower.includes('real') ||
      typeLower.includes('double') ||
      typeLower.includes('float') ||
      typeLower === 'money') {
    return 'number';
  }
  
  // Boolean types
  if (typeLower.includes('bool')) {
    return 'boolean';
  }
  
  // Date/Time types
  if (typeLower.includes('date') || 
      typeLower.includes('time') || 
      typeLower.includes('timestamp')) {
    return 'date';
  }
  
  // JSON types
  if (typeLower.includes('json')) {
    return 'json';
  }
  
  // UUID
  if (typeLower.includes('uuid')) {
    return 'string';
  }
  
  // Network address types
  if (typeLower.includes('inet') || 
      typeLower.includes('cidr') ||
      typeLower.includes('macaddr')) {
    return 'string';
  }
  
  // Geometric types
  if (typeLower.includes('point') || 
      typeLower.includes('line') ||
      typeLower.includes('lseg') ||
      typeLower.includes('box') ||
      typeLower.includes('path') ||
      typeLower.includes('polygon') ||
      typeLower.includes('circle')) {
    return 'string';
  }
  
  // Text/String types (catch-all for character types)
  if (typeLower.includes('char') || 
      typeLower.includes('text') || 
      typeLower.includes('string') ||
      typeLower.includes('name') ||
      typeLower.includes('bpchar') ||
      typeLower.includes('varchar') ||
      typeLower === 'xml' ||
      typeLower === 'tsquery' ||
      typeLower === 'tsvector' ||
      typeLower === 'txid_snapshot') {
    return 'string';
  }
  
  // Bit string types
  if (typeLower.includes('bit')) {
    return 'string';
  }
  
  // Bytea (binary data)
  if (typeLower.includes('bytea')) {
    return 'string';
  }
  
  // Enum types (treated as strings)
  if (typeLower.includes('enum')) {
    return 'string';
  }
  
  // Composite types (treated as JSON)
  if (typeLower.includes('composite') || 
      typeLower.includes('record') ||
      typeLower.includes('table')) {
    return 'json';
  }
  
  // Range types
  if (typeLower.includes('range')) {
    return 'string';
  }
  
  // Domain types (check underlying type)
  if (typeLower.includes('domain')) {
    // For domains, we'd need to check the underlying type
    // For simplicity, treat as string
    return 'string';
  }
  
  // Default fallback for unknown types
  return 'unknown';
}

/**
 * Extracts type hints from foreign table options for more accurate mapping.
 * Foreign tables often have server-specific type mappings that should override defaults.
 */
function extractTypeHintFromForeignOptions(
  foreignOptions: any,
  baseType: string
): 'string' | 'number' | 'boolean' | 'date' | 'json' | 'unknown' | null {
  try {
    // Handle different foreign option formats
    let options: string[] = [];
    
    if (Array.isArray(foreignOptions)) {
      options = foreignOptions;
    } else if (typeof foreignOptions === 'string') {
      // Parse comma-separated options string
      options = foreignOptions.split(',').map(opt => opt.trim());
    } else if (typeof foreignOptions === 'object') {
      // Convert object values to array
      options = Object.values(foreignOptions).map(String);
    }
    
    // Look for type hints in foreign options
    for (const option of options) {
      const optLower = option.toLowerCase();
      
      // Check for Excel-specific type hints
      if (optLower.includes('excel') || optLower.includes('xlsx') || optLower.includes('xls')) {
        // Excel files often treat all columns as strings initially
        if (baseType.includes('int') || baseType.includes('float') || baseType.includes('numeric')) {
          return 'number';
        }
        if (baseType.includes('date') || baseType.includes('time')) {
          return 'date';
        }
        return 'string';
      }
      
      // Check for CSV/delimited file hints
      if (optLower.includes('csv') || optLower.includes('delimited')) {
        // Delimited files often need string type detection
        return 'string';
      }
      
      // Check for JSON/Avro/Parquet hints
      if (optLower.includes('json') || optLower.includes('avro') || optLower.includes('parquet')) {
        // These formats preserve native types better
        if (baseType.includes('int') || baseType.includes('float') || baseType.includes('numeric')) {
          return 'number';
        }
        if (baseType.includes('bool')) {
          return 'boolean';
        }
        if (baseType.includes('date') || baseType.includes('time')) {
          return 'date';
        }
        if (baseType.includes('struct') || baseType.includes('array') || baseType.includes('map')) {
          return 'json';
        }
      }
      
      // Check for XML hints
      if (optLower.includes('xml')) {
        // XML elements are typically strings
        return 'string';
      }
      
      // Check for specific type overrides in options
      if (optLower.includes('type=') || optLower.includes('datatype=')) {
        const match = option.match(/(?:type|datatype)=['"]?([^'"\s,]+)['"]?/i);
        if (match) {
          const forcedType = match[1].toLowerCase();
          if (forcedType.includes('int') || forcedType.includes('num') || forcedType.includes('float')) {
            return 'number';
          }
          if (forcedType.includes('bool')) {
            return 'boolean';
          }
          if (forcedType.includes('date') || forcedType.includes('time')) {
            return 'date';
          }
          if (forcedType.includes('json')) {
            return 'json';
          }
          if (forcedType.includes('str') || forcedType.includes('text') || forcedType.includes('char')) {
            return 'string';
          }
        }
      }
      
      // Check for format hints
      if (optLower.includes('format=')) {
        const match = option.match(/format=['"]?([^'"\s,]+)['"]?/i);
        if (match) {
          const format = match[1].toLowerCase();
          if (format === 'date' || format === 'timestamp' || format === 'datetime') {
            return 'date';
          }
          if (format === 'integer' || format === 'decimal' || format === 'float') {
            return 'number';
          }
          if (format === 'boolean') {
            return 'boolean';
          }
        }
      }
    }
    
    return null;
  } catch (error) {
    console.warn('Error extracting type hints from foreign options:', error);
    return null;
  }
}

/**
 * Helper to check if a PostgreSQL type represents a foreign table column.
 * This is used to add visual indicators in the UI.
 */
export function isForeignColumn(
  columnMetadata: {
    is_foreign_column?: boolean;
    foreign_server?: string;
    [key: string]: any;
  }
): boolean {
  return Boolean(
    columnMetadata.is_foreign_column ||
    columnMetadata.foreign_server ||
    (columnMetadata.data_type && columnMetadata.data_type.toLowerCase().includes('foreign'))
  );
}

/**
 * Gets a human-readable description of a PostgreSQL type for UI display.
 */
export function getTypeDescription(
  dataType: string,
  characterMaximumLength?: number | null,
  numericPrecision?: number | null,
  numericScale?: number | null
): string {
  let description = dataType;
  
  if (characterMaximumLength !== null && characterMaximumLength !== undefined) {
    description += `(${characterMaximumLength})`;
  } else if (numericPrecision !== null && numericPrecision !== undefined) {
    if (numericScale !== null && numericScale !== undefined) {
      description += `(${numericPrecision},${numericScale})`;
    } else {
      description += `(${numericPrecision})`;
    }
  }
  
  return description;
}