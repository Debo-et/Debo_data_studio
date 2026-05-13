// src/generators/UniteSQLGenerator.ts (for testing purposes)
import { UniteComponentConfiguration, SchemaDefinition, FieldSchema, DataType } from '../../types/unified-pipeline.types';

export interface GenerateUniteSQLOptions {
  includeComments?: boolean;
  targetDialect?: 'postgresql';
}

export function generateUniteSQL(
  config: UniteComponentConfiguration,
  inputSchemas: SchemaDefinition[],
  options: GenerateUniteSQLOptions = {}
): string {
  if (!inputSchemas.length) {
    throw new Error('At least one input schema is required for UNITE operation');
  }

  const { unionMode, addSourceColumn, sourceColumnName, sourceColumnType, schemaHandling } = config;
  const unionKeyword = unionMode === 'DISTINCT' ? 'UNION' : 'UNION ALL';

  // Determine the unified column set based on schemaHandling
  let unifiedFields: FieldSchema[];

  if (schemaHandling === 'strict') {
    // All schemas must have identical field names and types (order can differ)
    const firstFields = inputSchemas[0].fields;
    for (let i = 1; i < inputSchemas.length; i++) {
      const otherFields = inputSchemas[i].fields;
      if (firstFields.length !== otherFields.length) {
        throw new Error(`Strict schema handling: schema ${inputSchemas[i].name} has different number of columns`);
      }
      // Check name & type equality
      for (let j = 0; j < firstFields.length; j++) {
        if (firstFields[j].name !== otherFields[j].name || firstFields[j].type !== otherFields[j].type) {
          throw new Error(`Strict schema handling: column mismatch at position ${j}: expected ${firstFields[j].name} (${firstFields[j].type}) but got ${otherFields[j].name} (${otherFields[j].type})`);
        }
      }
    }
    unifiedFields = firstFields.map(f => ({ ...f }));
  } else {
    // flexible: union of all column names
    const columnMap = new Map<string, FieldSchema>();
    for (const schema of inputSchemas) {
      for (const field of schema.fields) {
        if (!columnMap.has(field.name)) {
          columnMap.set(field.name, { ...field });
        }
      }
    }
    unifiedFields = Array.from(columnMap.values());
  }

  // Build SELECT statements for each input, aligning to unified fields
  const selects = inputSchemas.map((schema, idx) => {
    const sourceIdentifier = schema.name || `source_${idx + 1}`;
    const columns = unifiedFields.map(field => {
      const matchingField = schema.fields.find(f => f.name === field.name);
      if (matchingField) {
        // Direct column reference
        return `"${matchingField.name}"`;
      } else {
        // Missing column → NULL with appropriate cast
        const pgType = mapDataTypeToPostgreSQL(field.type);
        return `NULL::${pgType} AS "${field.name}"`;
      }
    });

    // Optionally add source column
    if (addSourceColumn) {
      const colName = sourceColumnName || 'source_table';
      const colType = sourceColumnType || 'STRING';
      const pgType = mapDataTypeToPostgreSQL(colType);
      columns.push(`'${sourceIdentifier}'::${pgType} AS "${colName}"`);
    }

    return `SELECT ${columns.join(', ')} FROM "${sourceIdentifier}"`;
  });

  // Combine with UNION / UNION ALL
  let sql = selects.join(`\n${unionKeyword}\n`);

  // Add optional comment
  if (options.includeComments) {
    sql = `-- Unite: ${unionMode} mode, schemaHandling=${schemaHandling}, addSourceColumn=${addSourceColumn}\n${sql}`;
  }

  return sql + ';';
}

// Helper: map DataType to PostgreSQL type string
function mapDataTypeToPostgreSQL(type: DataType | string): string {
  const t = type.toString().toUpperCase();
  switch (t) {
    case 'STRING': return 'TEXT';
    case 'INTEGER': return 'INTEGER';
    case 'DECIMAL': return 'NUMERIC';
    case 'BOOLEAN': return 'BOOLEAN';
    case 'DATE': return 'DATE';
    case 'TIMESTAMP': return 'TIMESTAMP';
    default: return 'TEXT';
  }
}