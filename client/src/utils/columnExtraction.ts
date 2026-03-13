// src/utils/columnExtraction.ts - FIXED VERSION
// Now correctly extracts columns from UnifiedCanvasNode metadata.schemas

import { Node, Edge } from 'reactflow';

export interface Column {
  name: string;
  type?: string;
  dataType?: string;
}

export interface MapEditorPayload {
  nodeId: string;
  inputColumns: Column[];
  outputColumns: Column[];
}

/**
 * Enhanced column extraction that works with UnifiedCanvasNode metadata
 */
export const extractColumnsFromNodeData = (nodeData: any): Column[] => {
  if (!nodeData) {
    console.log('❌ No node data provided');
    return [];
  }

  // Debug: Log what we're working with
  console.log('🔍 Extracting columns from node:', {
    id: nodeData.componentKey || nodeData.id || 'unknown',
    componentType: nodeData.componentType,
    hasMetadata: !!nodeData.metadata,
    metadataKeys: nodeData.metadata ? Object.keys(nodeData.metadata) : [],
  });

  let columns: any[] = [];

  // ------------------------------------------------------------
  // PRIORITY 1: UNIFIED METADATA MODEL – UnifiedCanvasNode
  // Columns are stored in metadata.schemas
  // ------------------------------------------------------------
  if (nodeData.metadata?.schemas) {
    console.log('📊 Checking unified metadata.schemas');
    const schemas = nodeData.metadata.schemas;

    // For INPUT and TRANSFORM nodes: use output schema
    if (nodeData.componentType === 'INPUT' || nodeData.componentType === 'TRANSFORM') {
      if (schemas.output?.fields && Array.isArray(schemas.output.fields)) {
        columns = schemas.output.fields;
        console.log('📥 Using OUTPUT schema fields for INPUT/TRANSFORM node:', columns.length);
      }
    }
    // For OUTPUT nodes: use input schema (first one)
    else if (nodeData.componentType === 'OUTPUT') {
      if (schemas.input && Array.isArray(schemas.input)) {
        const inputSchema = schemas.input[0];
        if (inputSchema?.fields && Array.isArray(inputSchema.fields)) {
          columns = inputSchema.fields;
          console.log('📤 Using INPUT schema fields for OUTPUT node:', columns.length);
        }
      }
    }

    // Fallback: if componentType is missing or we still have no columns,
    // try any available schema (output first, then input)
    if (columns.length === 0) {
      if (schemas.output?.fields && Array.isArray(schemas.output.fields)) {
        columns = schemas.output.fields;
        console.log('📥 Using OUTPUT schema fields (fallback):', columns.length);
      } else if (schemas.input && Array.isArray(schemas.input)) {
        const inputSchema = schemas.input[0];
        if (inputSchema?.fields && Array.isArray(inputSchema.fields)) {
          columns = inputSchema.fields;
          console.log('📤 Using INPUT schema fields (fallback):', columns.length);
        }
      }
    }
  }

  // ------------------------------------------------------------
  // PRIORITY 2: Columns directly in metadata (legacy format)
  // ------------------------------------------------------------
  if (columns.length === 0 && nodeData.metadata?.columns && Array.isArray(nodeData.metadata.columns)) {
    columns = nodeData.metadata.columns;
    console.log('📊 Found columns in metadata.columns:', columns.length);
  }

  // ------------------------------------------------------------
  // PRIORITY 3: Direct columns in node data
  // ------------------------------------------------------------
  if (columns.length === 0 && nodeData.columns && Array.isArray(nodeData.columns)) {
    columns = nodeData.columns;
    console.log('📊 Found direct columns:', columns.length);
  }

  // ------------------------------------------------------------
  // PRIORITY 4: Configuration schema (legacy)
  // ------------------------------------------------------------
  if (columns.length === 0 && nodeData.configuration?.schema?.fields) {
    columns = nodeData.configuration.schema.fields;
    console.log('📊 Found schema in configuration:', columns.length);
  }

  // ------------------------------------------------------------
  // PRIORITY 5: Legacy schema property
  // ------------------------------------------------------------
  if (columns.length === 0 && nodeData.schema && Array.isArray(nodeData.schema)) {
    columns = nodeData.schema;
    console.log('📊 Found legacy schema property:', columns.length);
  }

  // ------------------------------------------------------------
  // PRIORITY 6: Fields property
  // ------------------------------------------------------------
  if (columns.length === 0 && nodeData.fields && Array.isArray(nodeData.fields)) {
    columns = nodeData.fields;
    console.log('📊 Found fields property:', columns.length);
  }

  // ------------------------------------------------------------
  // Convert found columns to Column[] format and deduplicate
  // ------------------------------------------------------------
  if (columns.length > 0) {
    const uniqueColumns = new Map<string, Column>();

    columns.forEach((col: any, index: number) => {
      // Extract column name and type (support various object shapes)
      let columnName = '';
      let columnType = 'STRING';

      if (typeof col === 'string') {
        columnName = col;
        columnType = 'STRING';
      } else if (col.name) {
        columnName = col.name;
        columnType = col.type || col.dataType || 'STRING';
      } else if (col.fieldName || col.columnName) {
        columnName = col.fieldName || col.columnName;
        columnType = col.type || col.dataType || 'STRING';
      } else if (col.id) {
        columnName = col.id;
        columnType = col.type || 'STRING';
      } else {
        columnName = `Column_${index + 1}`;
        columnType = 'STRING';
      }

      // Normalize type for better UI compatibility
      const normalizedType = columnType.toUpperCase()
        .replace('VARCHAR', 'STRING')
        .replace('CHAR', 'STRING')
        .replace('TEXT', 'STRING')
        .replace('INTEGER', 'INT')
        .replace('BIGINT', 'INT')
        .replace('DECIMAL', 'FLOAT')
        .replace('NUMERIC', 'FLOAT')
        .replace('FLOAT', 'FLOAT')
        .replace('DOUBLE', 'FLOAT');

      // Deduplicate by column name
      if (columnName && !uniqueColumns.has(columnName)) {
        uniqueColumns.set(columnName, {
          name: columnName,
          type: normalizedType,
          dataType: columnType,
        });
      }
    });

    console.log(`✅ Converted ${columns.length} columns to ${uniqueColumns.size} unique columns`);
    return Array.from(uniqueColumns.values());
  }

  console.log('⚠️ No columns found in any location');
  return [];
};

/**
 * Gets connected columns for a tMap node with deduplication
 * Works with live React Flow connections
 */
export const getConnectedColumns = (
  nodeId: string,
  nodes: Node[],
  edges: Edge[]
): MapEditorPayload => {
  console.log('🔗 Getting connected columns for node:', nodeId);
  console.log('   Total nodes:', nodes.length);
  console.log('   Total edges:', edges.length);

  const inputColumns: Column[] = [];
  const outputColumns: Column[] = [];
  const seenInputNames = new Set<string>();
  const seenOutputNames = new Set<string>();

  // Incoming connections (input to tMap)
  const incomingEdges = edges.filter(edge => edge.target === nodeId);
  console.log('   Incoming edges:', incomingEdges.length);

  incomingEdges.forEach((edge, idx) => {
    const sourceNode = nodes.find(n => n.id === edge.source);
    if (sourceNode?.data) {
      console.log(`   📥 [${idx + 1}] Processing incoming edge from:`, {
        id: sourceNode.id,
        label: sourceNode.data?.label || sourceNode.data?.name,
        componentType: sourceNode.data?.componentType,
        componentKey: sourceNode.data?.componentKey,
      });

      const columns = extractColumnsFromNodeData(sourceNode.data);
      console.log(`     Extracted ${columns.length} columns`);

      if (columns.length > 0) {
        console.log('     Column names:', columns.map(c => c.name));
      }

      // Deduplicate and add input columns
      columns.forEach(col => {
        if (col.name && !seenInputNames.has(col.name)) {
          seenInputNames.add(col.name);
          inputColumns.push(col);
        }
      });
    }
  });

  // Outgoing connections (tMap to output)
  const outgoingEdges = edges.filter(edge => edge.source === nodeId);
  console.log('   Outgoing edges:', outgoingEdges.length);

  outgoingEdges.forEach((edge, idx) => {
    const targetNode = nodes.find(n => n.id === edge.target);
    if (targetNode?.data) {
      console.log(`   📤 [${idx + 1}] Processing outgoing edge to:`, {
        id: targetNode.id,
        label: targetNode.data?.label || targetNode.data?.name,
        componentType: targetNode.data?.componentType,
        componentKey: targetNode.data?.componentKey,
      });

      const columns = extractColumnsFromNodeData(targetNode.data);
      console.log(`     Extracted ${columns.length} columns`);

      if (columns.length > 0) {
        console.log('     Column names:', columns.map(c => c.name));
      }

      // Deduplicate and add output columns
      columns.forEach(col => {
        if (col.name && !seenOutputNames.has(col.name)) {
          seenOutputNames.add(col.name);
          outputColumns.push(col);
        }
      });
    }
  });

  console.log('✅ Final column counts:', {
    inputColumns: inputColumns.length,
    outputColumns: outputColumns.length,
    inputNames: inputColumns.map(c => c.name),
    outputNames: outputColumns.map(c => c.name),
  });

  return {
    nodeId,
    inputColumns: inputColumns.sort((a, b) => a.name.localeCompare(b.name)),
    outputColumns: outputColumns.sort((a, b) => a.name.localeCompare(b.name)),
  };
};

/**
 * Direct extraction from node for debugging
 */
export const extractColumnsDirectly = (node: Node): Column[] => {
  console.log('🔍 Direct column extraction for node:', {
    id: node.id,
    type: node.type,
    dataKeys: Object.keys(node.data || {}),
  });
  return extractColumnsFromNodeData(node.data);
};