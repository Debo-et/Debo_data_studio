// tests/helpers/buildNode.ts
import { 
    CanvasConnection,
  CanvasNode, 
  ConnectionStatus, 
  PostgreSQLDataType, 
  SchemaMapping 
} from '../../types/pipeline-types';
import { NodeType } from '../../types/unified-pipeline.types';

export interface MockColumn {
  name: string;
  dataType: PostgreSQLDataType;
  nullable?: boolean;
}

// Helper to create a node that matches the unified shape expected by generators
function buildUnifiedNode(
  id: string,
  name: string,
  type: NodeType,
  metadata: any
): CanvasNode {
  return {
    id,
    name,
    type,
    position: { x: 0, y: 0 },
    size: { width: 200, height: 100 },
    metadata,
  } as CanvasNode; // type assertion because our metadata includes extra fields
}

export function buildInputNode(
  id: string,
  name: string,
  tableName: string,
  columns: MockColumn[]
): CanvasNode {
  return buildUnifiedNode(id, name, NodeType.INPUT, {
    tableMapping: {
      schema: 'public',
      name: tableName,
      columns: columns.map(c => ({
        name: c.name,
        dataType: c.dataType,
        nullable: c.nullable ?? true,
      })),
    },
    configuration: {
      type: 'INPUT',
      config: {
        version: '1.0',
        sourceType: 'postgresql',
        sourceDetails: { tableName },
        pushdown: { enabled: false },
        schema: {
          id: `${id}_schema`,
          name: `${name}_schema`,
          fields: columns.map(c => ({
            id: `${id}_${c.name}`,
            name: c.name,
            type: c.dataType,
            nullable: true,
            isKey: false,
          })),
          isTemporary: false,
          isMaterialized: false,
        },
        sqlGeneration: {
          fromClause: tableName,
          alias: name,
          isTemporary: false,
          estimatedRowCount: 1000,
          parallelizable: true,
        },
        compilerMetadata: {
          lastModified: new Date().toISOString(),
          sourceValidated: true,
          warnings: [],
        },
      },
    },
    schemas: {
      output: {
        id: `${id}_output`,
        name: `${name}_output`,
        fields: columns.map(c => ({
          id: `${id}_${c.name}`,
          name: c.name,
          type: c.dataType,
          nullable: true,
          isKey: false,
        })),
        isTemporary: false,
        isMaterialized: false,
      },
    },
  });
}

export function buildOutputNode(id: string, name: string, targetTable: string): CanvasNode {
  return buildUnifiedNode(id, name, NodeType.OUTPUT, {
    targetTableName: targetTable,
    configuration: {
      type: 'OUTPUT',
      config: {
        version: '1.0',
        targetType: 'postgresql',
        targetDetails: { tableName: targetTable, mode: 'APPEND' },
        writeOptions: { batchSize: 1000, truncateFirst: false, createTable: true },
        schemaMapping: [],
        sqlGeneration: {
          insertStatement: '',
          requiresTransaction: true,
          parallelizable: true,
          batchOptimized: true,
        },
        compilerMetadata: {
          lastModified: new Date().toISOString(),
          targetValidated: true,
          warnings: [],
        },
      },
    },
  });
}

export function buildFilterNode(
  id: string,
  name: string,
  condition: string,
  operation: 'INCLUDE' | 'EXCLUDE' = 'INCLUDE'
): CanvasNode {
  return buildUnifiedNode(id, name, NodeType.FILTER_ROW, {
    filterConfig: { condition, operation },
  });
}

export function buildMapNode(
  id: string,
  name: string,
  mappings: Array<{ sourceColumn: string; targetColumn: string; transformation?: string }>
): CanvasNode {
  const schemaMappings: SchemaMapping[] = mappings.map(m => ({
    sourceColumn: m.sourceColumn,
    targetColumn: m.targetColumn,
    transformation: m.transformation,
    isRequired: true,
  }));

  return buildUnifiedNode(id, name, NodeType.MAP, {
    schemaMappings,
    outputSchema: {
      id: `${id}_output`,
      name: `${name}_output`,
      fields: mappings.map(m => ({
        id: `${id}_${m.targetColumn}`,
        name: m.targetColumn,
        type: 'VARCHAR',
        nullable: true,
        isKey: false,
      })),
      isTemporary: false,
      isMaterialized: false,
    },
  });
}

export function buildJoinNode(
  id: string,
  name: string,
  joinType: 'INNER' | 'LEFT' | 'RIGHT' = 'INNER',
  condition: string
): CanvasNode {
  return buildUnifiedNode(id, name, NodeType.JOIN, {
    joinConfig: { type: joinType, condition },
  });
}

export function buildAggregateNode(
  id: string,
  name: string,
  groupBy: string[],
  aggregates: Array<{ function: 'SUM' | 'AVG' | 'COUNT'; column: string; alias: string }>
): CanvasNode {
  return buildUnifiedNode(id, name, NodeType.AGGREGATE_ROW, {
    aggregationConfig: {
      groupBy,
      aggregates: aggregates.map(a => ({
        function: a.function,
        column: a.column,
        alias: a.alias,
      })),
    },
  });
}

export function buildSortNode(
  id: string,
  name: string,
  columns: Array<{ column: string; direction: 'ASC' | 'DESC'; nullsFirst?: boolean }>
): CanvasNode {
  return buildUnifiedNode(id, name, NodeType.SORT_ROW, {
    sortConfig: { columns },
  });
}

export function buildEdge(
  id: string,
  sourceId: string,
  targetId: string,
  schemaMappings?: Array<{ sourceColumn: string; targetColumn: string; isRequired?: boolean }>
): CanvasConnection {
  const mappings: SchemaMapping[] = (schemaMappings || []).map(m => ({
    sourceColumn: m.sourceColumn,
    targetColumn: m.targetColumn,
    isRequired: m.isRequired ?? true,
  }));

  return {
    id,
    sourceNodeId: sourceId,
    targetNodeId: targetId,
    sourcePortId: 'out',
    targetPortId: 'in',
    status: ConnectionStatus.VALID,
    dataFlow: {
      schemaMappings: mappings,
    },
  };
}