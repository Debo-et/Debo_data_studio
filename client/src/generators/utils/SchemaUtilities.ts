// src/generators/utils/SchemaUtilities.ts

import { CanvasConnection, CanvasNode, PostgreSQLDataType, SchemaMapping } from "@/types/pipeline-types";

/**
 * Schema inference and data type propagation utilities
 */
export class SchemaUtilities {
  
  /**
   * Infer schema from connections and propagate data types
   */
  public static inferSchemaFromConnections(
    connections: CanvasConnection[],
    nodes: CanvasNode[]
  ): Map<string, Array<{ name: string; dataType: PostgreSQLDataType }>> {
    const schemaMap = new Map<string, Array<{ name: string; dataType: PostgreSQLDataType }>>();
    
    connections.forEach(connection => {
      const sourceNode = nodes.find(n => n.id === connection.sourceNodeId);
      const targetNode = nodes.find(n => n.id === connection.targetNodeId);
      
      if (sourceNode && targetNode) {
        // Propagate schema from source to target
        const sourceSchema = this.extractNodeSchema(sourceNode);
        const targetSchema = this.extractNodeSchema(targetNode);
        
        // Apply schema mappings from connection
        if (connection.dataFlow.schemaMappings) {
          const propagatedSchema = this.propagateSchemaThroughMappings(
            sourceSchema,
            targetSchema,
            connection.dataFlow.schemaMappings
          );
          
          schemaMap.set(targetNode.id, propagatedSchema);
        }
      }
    });
    
    return schemaMap;
  }
  
  /**
   * Extract schema from node metadata
   */
  public static extractNodeSchema(node: CanvasNode): Array<{ name: string; dataType: PostgreSQLDataType }> {
    const columns: Array<{ name: string; dataType: PostgreSQLDataType }> = [];
    
    if (node.metadata?.tableMapping?.columns) {
      node.metadata.tableMapping.columns.forEach(col => {
        columns.push({
          name: col.name,
          dataType: col.dataType
        });
      });
    }
    
    return columns;
  }
  
  /**
   * Propagate schema through mappings with data type inference
   */
  public static propagateSchemaThroughMappings(
    sourceSchema: Array<{ name: string; dataType: PostgreSQLDataType }>,
    targetSchema: Array<{ name: string; dataType: PostgreSQLDataType }>,
    mappings: SchemaMapping[]
  ): Array<{ name: string; dataType: PostgreSQLDataType }> {
    const propagated: Array<{ name: string; dataType: PostgreSQLDataType }> = [];
    
    mappings.forEach(mapping => {
      const sourceCol = sourceSchema.find(col => col.name === mapping.sourceColumn);
      
      if (sourceCol) {
        // Determine target data type
        let targetDataType = mapping.dataTypeConversion?.to;
        
        if (!targetDataType) {
          // Try to find in target schema
          const targetCol = targetSchema.find(col => col.name === mapping.targetColumn);
          targetDataType = targetCol?.dataType || sourceCol.dataType;
        }
        
        propagated.push({
          name: mapping.targetColumn,
          dataType: targetDataType
        });
      }
    });
    
    return propagated;
  }
  
  /**
   * Preserve constraints during schema transformation
   */
  public static preserveConstraints(
    sourceConstraints: {
      primaryKey?: string[];
      uniqueConstraints?: Array<{ name: string; columns: string[] }>;
      foreignKeys?: Array<{
        column: string;
        referencedTable: string;
        referencedColumn: string;
      }>;
    },
    mappings: SchemaMapping[]
  ): {
    primaryKey?: string[];
    uniqueConstraints?: Array<{ name: string; columns: string[] }>;
    foreignKeys?: Array<{
      column: string;
      referencedTable: string;
      referencedColumn: string;
    }>;
  } {
    const preservedConstraints = {
      primaryKey: [] as string[],
      uniqueConstraints: [] as Array<{ name: string; columns: string[] }>,
      foreignKeys: [] as Array<{
        column: string;
        referencedTable: string;
        referencedColumn: string;
      }>
    };
    
    // Preserve primary key
    if (sourceConstraints.primaryKey) {
      sourceConstraints.primaryKey.forEach(pkColumn => {
        const mapping = mappings.find(m => m.sourceColumn === pkColumn);
        if (mapping) {
          preservedConstraints.primaryKey.push(mapping.targetColumn);
        }
      });
    }
    
    // Preserve unique constraints
    if (sourceConstraints.uniqueConstraints) {
      sourceConstraints.uniqueConstraints.forEach(constraint => {
        const mappedColumns: string[] = [];
        
        constraint.columns.forEach(sourceCol => {
          const mapping = mappings.find(m => m.sourceColumn === sourceCol);
          if (mapping) {
            mappedColumns.push(mapping.targetColumn);
          }
        });
        
        if (mappedColumns.length > 0) {
          preservedConstraints.uniqueConstraints.push({
            name: `${constraint.name}_mapped`,
            columns: mappedColumns
          });
        }
      });
    }
    
    // Preserve foreign keys
    if (sourceConstraints.foreignKeys) {
      sourceConstraints.foreignKeys.forEach(fk => {
        const mapping = mappings.find(m => m.sourceColumn === fk.column);
        if (mapping) {
          preservedConstraints.foreignKeys.push({
            column: mapping.targetColumn,
            referencedTable: fk.referencedTable,
            referencedColumn: fk.referencedColumn
          });
        }
      });
    }
    
    return preservedConstraints;
  }
  
  /**
   * Validate schema compatibility between source and target
   */
  public static validateSchemaCompatibility(
    sourceSchema: Array<{ name: string; dataType: PostgreSQLDataType }>,
    targetSchema: Array<{ name: string; dataType: PostgreSQLDataType }>,
    mappings: SchemaMapping[]
  ): { valid: boolean; errors: string[]; warnings: string[] } {
    const errors: string[] = [];
    const warnings: string[] = [];
    
    mappings.forEach(mapping => {
      const sourceCol = sourceSchema.find(col => col.name === mapping.sourceColumn);
      const targetCol = targetSchema.find(col => col.name === mapping.targetColumn);
      
      if (!sourceCol) {
        errors.push(`Source column "${mapping.sourceColumn}" not found`);
        return;
      }
      
      if (targetCol) {
        // Check data type compatibility
        const targetType = mapping.dataTypeConversion?.to || targetCol.dataType;
        const isCompatible = this.isDataTypeCompatible(sourceCol.dataType, targetType);
        
        if (!isCompatible) {
          warnings.push(`Data type conversion from ${sourceCol.dataType} to ${targetType} may lose precision`);
        }
      }
    });
    
    // Check for unmapped required columns
    targetSchema.forEach(targetCol => {
      const isMapped = mappings.some(m => m.targetColumn === targetCol.name);
      
      if (!isMapped && !targetCol.dataType.includes('NULL')) {
        warnings.push(`Target column "${targetCol.name}" has no mapping`);
      }
    });
    
    return {
      valid: errors.length === 0,
      errors,
      warnings
    };
  }
  
  /**
   * Check data type compatibility
   */
  private static isDataTypeCompatible(
    sourceType: PostgreSQLDataType,
    targetType: PostgreSQLDataType
  ): boolean {
    // PostgreSQL type compatibility matrix
    const compatibilityMatrix: Record<string, string[]> = {
      [PostgreSQLDataType.SMALLINT]: [
        PostgreSQLDataType.INTEGER,
        PostgreSQLDataType.BIGINT,
        PostgreSQLDataType.NUMERIC,
        PostgreSQLDataType.DECIMAL,
        PostgreSQLDataType.REAL,
        PostgreSQLDataType.DOUBLE_PRECISION,
        PostgreSQLDataType.VARCHAR,
        PostgreSQLDataType.TEXT
      ],
      [PostgreSQLDataType.INTEGER]: [
        PostgreSQLDataType.BIGINT,
        PostgreSQLDataType.NUMERIC,
        PostgreSQLDataType.DECIMAL,
        PostgreSQLDataType.REAL,
        PostgreSQLDataType.DOUBLE_PRECISION,
        PostgreSQLDataType.VARCHAR,
        PostgreSQLDataType.TEXT
      ],
      [PostgreSQLDataType.VARCHAR]: [
        PostgreSQLDataType.TEXT,
        PostgreSQLDataType.CHAR,
        PostgreSQLDataType.INTEGER,
        PostgreSQLDataType.NUMERIC,
        PostgreSQLDataType.DATE,
        PostgreSQLDataType.TIMESTAMP
      ],
      [PostgreSQLDataType.DATE]: [
        PostgreSQLDataType.TIMESTAMP,
        PostgreSQLDataType.TIMESTAMPTZ,
        PostgreSQLDataType.VARCHAR,
        PostgreSQLDataType.TEXT
      ]
    };
    
    if (sourceType === targetType) return true;
    
    return compatibilityMatrix[sourceType]?.includes(targetType) || false;
  }
}