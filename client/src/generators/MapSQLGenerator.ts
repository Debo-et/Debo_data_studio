// src/generators/MapSQLGenerator.ts

import { BaseSQLGenerator, SQLGenerationContext, GeneratedSQLFragment, SQLGenerationError } from './BaseSQLGenerator';
import { SchemaMapping, TransformationRule, PostgreSQLDataType, SortConfig } from '../types/pipeline-types';
import { UnifiedCanvasNode, UnifiedCanvasConnection, MapComponentConfiguration } from '@/types/unified-pipeline.types';
import { globalLogger } from '../utils/Logger';

// Import MapEditor types for canvas integration
interface ColumnDefinition {
  id: string;
  name: string;
  type: string;
  expression?: string;
  isKey?: boolean;
}

interface TableDefinition {
  id: string;
  name: string;
  type?: 'input' | 'output' | 'lookup' | 'reject';
  columns: ColumnDefinition[];
}

interface Wire {
  id: string;
  sourceTableId: string;
  sourceColumnId: string;
  targetTableId: string;
  targetColumnId: string;
  transformation?: string;
  defaultValue?: any;
}

/**
 * Canvas mapping context for SQL generation
 * Extended to accept an optional node (for output schema and type casting)
 */
export interface CanvasMappingContext {
  sourceTables: TableDefinition[];
  targetTables: TableDefinition[];
  wires: Wire[];
  variables: any[];
  nodeId: string;
  nodeName: string;
  node?: UnifiedCanvasNode;   // <-- added to support output schema
}

/**
 * PostgreSQL MAP SQL Generator with Canvas Integration
 * Handles column mapping, transformations, expression evaluation, and canvas-based SQL generation
 */
export class MapSQLGenerator extends BaseSQLGenerator {
  // ==================== CONSTRUCTOR WITH LOGGING ====================
  constructor(options?: any) {
    super(options);
    globalLogger.debug(`[MapSQLGenerator] Constructor called with options:`, options);
  }

  // ==================== TEMPLATE METHOD IMPLEMENTATIONS ====================

  protected generateSelectStatement(context: SQLGenerationContext): GeneratedSQLFragment {
    globalLogger.debug(`[MapSQLGenerator] generateSelectStatement called for node ${context.node.id} (${context.node.name})`, {
      hasUpstreamSchema: !!context.upstreamSchema,
      upstreamSchemaLength: context.upstreamSchema?.length,
      hasConnection: !!context.connection,
      connectionSourceId: context.connection?.sourceNodeId,
    });

    const { node, connection, upstreamSchema } = context;
    
    // Extract mappings from node metadata (supports MapComponentConfiguration)
    const mappings = this.extractSchemaMappings(node, connection);
    const transformationRules = node.metadata?.transformationRules || [];

    globalLogger.debug(`[MapSQLGenerator] Extracted ${mappings.length} mappings and ${transformationRules.length} transformation rules`);

    if (mappings.length === 0) {
      globalLogger.warn(`[MapSQLGenerator] No mappings found for node ${node.id}, falling back to SELECT *`);
      return this.generateFallbackSelect(context);
    }

    // Validate mappings against upstream schema
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    this.validateMappings(upstreamSchema || [], mappings, errors, warnings);
    if (errors.length > 0) {
      globalLogger.warn(`[MapSQLGenerator] Validation errors: ${errors.map(e => e.message).join(', ')}`);
    }
    if (warnings.length > 0) {
      globalLogger.warn(`[MapSQLGenerator] Validation warnings: ${warnings.join(', ')}`);
    }

    // Determine source table/CTE reference
    let sourceRef = 'source_table';
    if (connection && connection.sourceNodeId) {
      sourceRef = this.sanitizeIdentifier(connection.sourceNodeId);
      globalLogger.debug(`[MapSQLGenerator] Using source reference from connection: ${sourceRef}`);
    } else {
      warnings.push('No incoming connection found for tMap node. Using placeholder source.');
      globalLogger.warn(`[MapSQLGenerator] No incoming connection, using placeholder source: ${sourceRef}`);
    }

    // Build SELECT clause with proper type casting and transformations
    const selectColumns = this.generateMappedColumns(
      mappings,
      transformationRules,
      node,
      upstreamSchema || []
    );

    const sql = `SELECT ${selectColumns} FROM ${sourceRef}`;
    globalLogger.debug(`[MapSQLGenerator] Generated SELECT SQL: ${sql.substring(0, 200)}...`);

    return {
      sql,
      dependencies: this.extractSourceDependencies(mappings),
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'map_select',
        lineCount: 1,
        outputColumns: mappings.map(m => m.targetColumn)
      }
    };
  }

  protected generateJoinConditions(_context: SQLGenerationContext): GeneratedSQLFragment {
    // MAP nodes typically don't have JOIN conditions
    return this.createEmptyFragment('join_conditions');
  }

  protected generateWhereClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    const filterRules = (node.metadata?.transformationRules || [])
      .filter((rule: { type: string; }) => rule.type === 'filter');
    
    if (filterRules.length === 0) {
      return this.createEmptyFragment('where_clause');
    }

    const whereClause = this.buildFilterConditions(filterRules);
    globalLogger.debug(`[MapSQLGenerator] Generated WHERE clause: ${whereClause}`);
    
    return {
      sql: `WHERE ${whereClause}`,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'map_where',
        lineCount: 1
      }
    };
  }

  protected generateHavingClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // MAP nodes typically don't use HAVING
    return this.createEmptyFragment('having_clause');
  }

  protected generateOrderByClause(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    if (node.metadata?.sortConfig) {
      globalLogger.debug(`[MapSQLGenerator] Generating ORDER BY from sortConfig`, node.metadata.sortConfig);
      return this.generateOrderByFromSortConfig(node.metadata.sortConfig);
    }
    
    return this.createEmptyFragment('order_by_clause');
  }

  protected generateGroupByClause(_context: SQLGenerationContext): GeneratedSQLFragment {
    // MAP nodes typically don't use GROUP BY
    return this.createEmptyFragment('group_by_clause');
  }

  // ==================== CANVAS INTEGRATION METHODS ====================

  /**
   * Generate SQL from canvas mapping configuration
   * FIXED: Supports simple direct mapping, type casting, transformations, multiple wires to same target.
   */
  public generateSQLFromCanvasMapping(context: CanvasMappingContext): GeneratedSQLFragment {
    globalLogger.debug(`[MapSQLGenerator] generateSQLFromCanvasMapping called`, {
      nodeId: context.nodeId,
      nodeName: context.nodeName,
      sourceTablesCount: context.sourceTables.length,
      targetTablesCount: context.targetTables.length,
      wiresCount: context.wires.length,
      hasNode: !!context.node,
    });

    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    
    // Extract primary source and target tables
    const primarySourceTable = context.sourceTables.find(t => t.type === 'input');
    const primaryTargetTable = context.targetTables.find(t => t.type === 'output');
    
    globalLogger.debug(`[MapSQLGenerator] Primary source table: ${primarySourceTable?.name || 'none'}, primary target table: ${primaryTargetTable?.name || 'none'}`);
    
    if (!primarySourceTable || !primaryTargetTable) {
      errors.push({
        code: 'INVALID_MAPPING_CONFIG',
        message: 'Missing primary source or target table',
        severity: 'ERROR',
        field: 'tables'
      });
      globalLogger.error(`[MapSQLGenerator] Missing primary source/target table`);
      return this.errorFragment('canvas_mapping', errors, warnings);
    }
    
    // Validate mapping configuration
    const validation = this.validateCanvasMapping(context);
    if (!validation.isValid) {
      validation.errors.forEach(error => {
        errors.push({
          code: 'VALIDATION_ERROR',
          message: error,
          severity: 'ERROR',
          field: 'mappings'
        });
      });
      warnings.push(...validation.warnings);
      globalLogger.warn(`[MapSQLGenerator] Canvas validation failed`, { errors: validation.errors, warnings: validation.warnings });
    } else {
      warnings.push(...validation.warnings);
      globalLogger.debug(`[MapSQLGenerator] Canvas validation passed, warnings: ${validation.warnings.length}`);
    }
    
    // Group wires by target column to handle multiple wires to same target
    const targetToWiresMap = new Map<string, Wire[]>();
    for (const wire of context.wires) {
      const targetColId = wire.targetColumnId;
      if (!targetToWiresMap.has(targetColId)) {
        targetToWiresMap.set(targetColId, []);
      }
      targetToWiresMap.get(targetColId)!.push(wire);
    }
    globalLogger.debug(`[MapSQLGenerator] Grouped wires into ${targetToWiresMap.size} target column groups`);

    // Build explicit mappings from grouped wires
    const explicitMappings: SchemaMapping[] = [];
    for (const [targetColId, wiresForTarget] of targetToWiresMap.entries()) {
      const targetColumn = primaryTargetTable.columns.find(c => c.id === targetColId);
      if (!targetColumn) {
        globalLogger.debug(`[MapSQLGenerator] Target column ${targetColId} not found in primary target table`);
        continue;
      }

      // If multiple wires to same target, combine into a CONCAT expression
      if (wiresForTarget.length > 1) {
        globalLogger.debug(`[MapSQLGenerator] Multiple wires (${wiresForTarget.length}) for target column ${targetColumn.name}, using CONCAT`);
        const sourceExpressions: string[] = [];
        let defaultValue: any = undefined;
        for (const wire of wiresForTarget) {
          const sourceColumn = this.findSourceColumn(wire, context.sourceTables);
          if (sourceColumn) {
            sourceExpressions.push(this.sanitizeIdentifier(sourceColumn.name));
          }
          if (wire.defaultValue !== undefined) defaultValue = wire.defaultValue;
        }
        const combinedExpr = sourceExpressions.length > 0
          ? `CONCAT(${sourceExpressions.join(', ')})`
          : '';
        explicitMappings.push({
          sourceColumn: combinedExpr,
          targetColumn: targetColumn.name,
          transformation: undefined,
          dataTypeConversion: undefined,
          isRequired: true,
          defaultValue: defaultValue
        });
      } else {
        const wire = wiresForTarget[0];
        const sourceColumn = this.findSourceColumn(wire, context.sourceTables);
        if (sourceColumn) {
          let expression: string;
          if (wire.transformation) {
            // Replace {column} placeholders with actual column names
            expression = this.replaceColumnPlaceholders(wire.transformation, context.sourceTables);
            globalLogger.debug(`[MapSQLGenerator] Wire ${wire.id} has transformation: ${wire.transformation} -> ${expression}`);
          } else {
            // Simple direct mapping: use unqualified column name (no table prefix)
            expression = this.sanitizeIdentifier(sourceColumn.name);
            globalLogger.debug(`[MapSQLGenerator] Direct mapping: ${sourceColumn.name} -> ${targetColumn.name}`);
          }
          explicitMappings.push({
            sourceColumn: expression,
            targetColumn: targetColumn.name,
            transformation: undefined,
            dataTypeConversion: undefined,
            isRequired: true,
            defaultValue: wire.defaultValue
          });
        } else {
          globalLogger.warn(`[MapSQLGenerator] Source column not found for wire ${wire.id}`);
        }
      }
    }

    // Generate default positional mappings only if there is at least one wire
    const mappedTargetNames = new Set(explicitMappings.map(m => m.targetColumn));
    let defaultMappings: SchemaMapping[] = [];
    if (context.wires.length > 0) {
      defaultMappings = this.generateDefaultPositionalMappings(
        primarySourceTable,
        primaryTargetTable,
        mappedTargetNames
      );
      globalLogger.debug(`[MapSQLGenerator] Generated ${defaultMappings.length} default positional mappings`);
    }
    
    const allMappings = [...explicitMappings, ...defaultMappings];
    globalLogger.debug(`[MapSQLGenerator] Total mappings: ${allMappings.length} (explicit: ${explicitMappings.length}, default: ${defaultMappings.length})`);
    
    if (allMappings.length === 0) {
      warnings.push('No column mappings found. Using SELECT * fallback.');
      globalLogger.warn(`[MapSQLGenerator] No mappings, using fallback SELECT *`);
      return {
        sql: `SELECT * FROM ${this.sanitizeIdentifier(primarySourceTable.name)}`,
        dependencies: [primarySourceTable.name],
        parameters: new Map(),
        errors,
        warnings,
        metadata: {
          generatedAt: new Date().toISOString(),
          fragmentType: 'canvas_mapping_fallback',
          lineCount: 1
        }
      };
    }
    
    // Convert source columns to required format
    const sourceColumns = primarySourceTable.columns.map(col => ({
      name: col.name,
      dataType: this.stringToPostgreSQLDataType(col.type)
    }));
    
    // Extract transformation rules from variables
    const transformationRules = this.extractTransformationRulesFromVariables(context.variables);
    globalLogger.debug(`[MapSQLGenerator] Extracted ${transformationRules.length} transformation rules from variables`);
    
    // Build SELECT clause with proper type casting (using output schema from context.node)
    const selectColumns = this.buildSelectClauseFromMappings(
      allMappings,
      sourceColumns,
      transformationRules,
      context.node   // <-- pass the map node (contains output schema)
    );
    
    const sql = `SELECT ${selectColumns} FROM ${this.sanitizeIdentifier(primarySourceTable.name)}`;
    globalLogger.debug(`[MapSQLGenerator] Final SQL from canvas mapping: ${sql.substring(0, 300)}...`);
    
    return {
      sql,
      dependencies: [...allMappings.map(() => primarySourceTable.name), context.nodeId],
      parameters: new Map(),
      errors: [...errors],
      warnings: [...warnings, ...validation.warnings],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'canvas_mapping',
        lineCount: sql.split('\n').length,
        nodeId: context.nodeId,
        nodeName: context.nodeName,
        mappingStats: {
          totalMappings: allMappings.length,
          explicitMappings: explicitMappings.length,
          defaultMappings: defaultMappings.length,
          sourceColumns: primarySourceTable.columns.length,
          targetColumns: primaryTargetTable.columns.length
        }
      }
    };
  }

  /**
   * Generate optimized INSERT INTO ... SELECT statement for PostgreSQL
   */
  public generateInsertSelectSQL(
    sourceTable: string,
    targetTable: string,
    mappings: SchemaMapping[],
    transformationRules: TransformationRule[] = [],
    options: {
      batchSize?: number;
      onConflict?: 'DO_NOTHING' | 'DO_UPDATE';
      conflictColumns?: string[];
      updateColumns?: string[];
      whereClause?: string;
      orderBy?: string;
      distinct?: boolean;
    } = {}
  ): GeneratedSQLFragment {
    globalLogger.debug(`[MapSQLGenerator] generateInsertSelectSQL called`, {
      sourceTable,
      targetTable,
      mappingsCount: mappings.length,
      transformationRulesCount: transformationRules.length,
      options
    });

    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    
    if (mappings.length === 0) {
      errors.push({
        code: 'NO_MAPPINGS',
        message: 'No column mappings provided',
        severity: 'ERROR',
        field: 'mappings'
      });
      globalLogger.error(`[MapSQLGenerator] No mappings provided`);
      return this.errorFragment('insert_select', errors, warnings);
    }
    
    // Generate the SELECT clause with transformations
    const sourceColumns = mappings.map(m => ({
      name: m.sourceColumn.split('.').pop() || m.sourceColumn,
      dataType: PostgreSQLDataType.VARCHAR // Default, actual type not critical here
    }));
    
    const mappingResult = this.generateMappingSQL(
      sourceColumns,
      mappings,
      transformationRules,
      {
        preserveNulls: true,
        useCoalesce: true,
        parameterize: false
      }
    );
    
    if (mappingResult.errors.length > 0) {
      globalLogger.error(`[MapSQLGenerator] Mapping SQL generation failed`, mappingResult.errors);
      return mappingResult;
    }
    
    // Extract the SELECT part from the mapping SQL
    const selectMatch = mappingResult.sql.match(/SELECT\s+(.+?)\s+FROM\s+(\w+)/is);
    if (!selectMatch) {
      errors.push({
        code: 'SQL_GENERATION_FAILED',
        message: 'Failed to extract SELECT clause from mapping SQL',
        severity: 'ERROR',
        field: 'sql'
      });
      globalLogger.error(`[MapSQLGenerator] Failed to extract SELECT clause from mapping SQL: ${mappingResult.sql}`);
      return this.errorFragment('insert_select', errors, warnings);
    }
    
    const selectClause = selectMatch[1];
    const fromTable = selectMatch[2] || sourceTable;
    
    // Build column list for INSERT
    const columnList = mappings.map(m => this.sanitizeIdentifier(m.targetColumn)).join(', ');
    
    // Build INSERT INTO ... SELECT statement
    let sql = `INSERT INTO ${this.sanitizeIdentifier(targetTable)} (${columnList})\n`;
    
    if (options.distinct) {
      sql += `SELECT DISTINCT ${selectClause}\n`;
    } else {
      sql += `SELECT ${selectClause}\n`;
    }
    
    sql += `FROM ${this.sanitizeIdentifier(fromTable)}`;
    
    // Add WHERE clause if specified
    if (options.whereClause) {
      sql += `\nWHERE ${options.whereClause}`;
    }
    
    // Add ORDER BY if specified
    if (options.orderBy) {
      sql += `\nORDER BY ${options.orderBy}`;
    }
    
    // Add ON CONFLICT clause if specified
    if (options.onConflict && options.conflictColumns && options.conflictColumns.length > 0) {
      const conflictColumns = options.conflictColumns.map(c => this.sanitizeIdentifier(c)).join(', ');
      sql += `\nON CONFLICT (${conflictColumns})`;
      
      if (options.onConflict === 'DO_UPDATE' && options.updateColumns && options.updateColumns.length > 0) {
        const updateClauses = options.updateColumns.map(col => {
          return `${this.sanitizeIdentifier(col)} = EXCLUDED.${this.sanitizeIdentifier(col)}`;
        }).join(', ');
        
        sql += ` DO UPDATE SET ${updateClauses}`;
      } else {
        sql += ` DO NOTHING`;
      }
    }
    
    // Add LIMIT for batching if specified
    if (options.batchSize) {
      sql += `\nLIMIT ${options.batchSize}`;
    }
    
    // Add performance hints
    if (mappings.length > 20) {
      warnings.push('Consider adding indexes on join/where columns for large datasets');
    }
    
    globalLogger.debug(`[MapSQLGenerator] Generated INSERT SELECT SQL: ${sql.substring(0, 200)}...`);
    
    return {
      sql,
      dependencies: [sourceTable, targetTable],
      parameters: new Map(),
      errors,
      warnings: [...warnings, ...mappingResult.warnings],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'insert_select',
        lineCount: sql.split('\n').length,
        tableMapping: {
          source: sourceTable,
          target: targetTable,
          mappedColumns: mappings.length,
          operation: 'INSERT_SELECT',
          hasConflictResolution: !!options.onConflict
        }
      }
    };
  }

  /**
   * Generate INSERT INTO ... SELECT from canvas mapping
   */
  public generateInsertSelectFromCanvasMapping(
    context: CanvasMappingContext,
    options: {
      batchSize?: number;
      onConflict?: 'DO_NOTHING' | 'DO_UPDATE';
      conflictColumns?: string[];
      updateColumns?: string[];
    } = {}
  ): GeneratedSQLFragment {
    globalLogger.debug(`[MapSQLGenerator] generateInsertSelectFromCanvasMapping called`, { options });
    
    const primarySourceTable = context.sourceTables.find(t => t.type === 'input');
    const primaryTargetTable = context.targetTables.find(t => t.type === 'output');
    
    if (!primarySourceTable || !primaryTargetTable) {
      globalLogger.error(`[MapSQLGenerator] Missing source or target table for canvas insert select`);
      return this.errorFragment('canvas_insert_select', [{
        code: 'INVALID_CONFIG',
        message: 'Missing source or target table',
        severity: 'ERROR',
        field: 'tables'
      }], []);
    }
    
    // Extract mappings
    const mappings = this.extractMappingsFromWires(
      context.wires,
      context.sourceTables,
      context.targetTables
    );
    globalLogger.debug(`[MapSQLGenerator] Extracted ${mappings.length} mappings from wires`);
    
    // Apply default positional mapping
    const defaultMappings = this.generateDefaultPositionalMappings(
      primarySourceTable,
      primaryTargetTable,
      new Set(mappings.map(m => m.targetColumn))
    );
    
    const allMappings = [...mappings, ...defaultMappings];
    globalLogger.debug(`[MapSQLGenerator] Total mappings after adding defaults: ${allMappings.length}`);
    
    // Extract transformation rules
    const transformationRules = this.extractTransformationRulesFromVariables(context.variables);
    
    // Generate INSERT INTO ... SELECT
    return this.generateInsertSelectSQL(
      primarySourceTable.name,
      primaryTargetTable.name,
      allMappings,
      transformationRules,
      options
    );
  }

  /**
   * Generate complete ETL pipeline SQL with multiple stages
   */
  public generateETLPipelineSQL(
    stages: Array<{
      stage: string;
      sourceTable: string;
      targetTable: string;
      mappings: SchemaMapping[];
      transformationRules: TransformationRule[];
      options?: any;
    }>
  ): GeneratedSQLFragment {
    globalLogger.debug(`[MapSQLGenerator] generateETLPipelineSQL called with ${stages.length} stages`);
    
    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];
    const allSQL: string[] = [];
    const dependencies: string[] = [];
    
    // Add transaction start
    allSQL.push('BEGIN;');
    allSQL.push('');
    
    stages.forEach((stageConfig, index) => {
      globalLogger.debug(`[MapSQLGenerator] Generating stage ${index + 1}: ${stageConfig.stage}`);
      const stageResult = this.generateInsertSelectSQL(
        stageConfig.sourceTable,
        stageConfig.targetTable,
        stageConfig.mappings,
        stageConfig.transformationRules,
        stageConfig.options || {}
      );
      
      if (stageResult.errors.length > 0) {
        errors.push(...stageResult.errors.map(err => ({
          ...err,
          message: `Stage ${index + 1} (${stageConfig.stage}): ${err.message}`
        })));
        globalLogger.error(`[MapSQLGenerator] Stage ${index + 1} failed`, stageResult.errors);
      }
      
      if (stageResult.warnings.length > 0) {
        warnings.push(...stageResult.warnings.map(w => `Stage ${index + 1}: ${w}`));
        globalLogger.warn(`[MapSQLGenerator] Stage ${index + 1} warnings`, stageResult.warnings);
      }
      
      allSQL.push(`-- Stage ${index + 1}: ${stageConfig.stage}`);
      allSQL.push(stageResult.sql + ';');
      allSQL.push(''); // Empty line for readability
      
      dependencies.push(...stageResult.dependencies);
    });
    
    // Add transaction commit
    allSQL.push('COMMIT;');
    
    if (errors.length > 0) {
      return this.errorFragment('etl_pipeline', errors, warnings);
    }
    
    return {
      sql: allSQL.join('\n'),
      dependencies: Array.from(new Set(dependencies)),
      parameters: new Map(),
      errors: [],
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'etl_pipeline',
        lineCount: allSQL.length,
        stageCount: stages.length,
        usesTransactions: true
      }
    };
  }

  // ==================== MAP-SPECIFIC METHODS ====================

  /**
   * Generate complete mapping SQL with transformations
   * FIXED: Now defaults useCoalesce to true
   */
  public generateMappingSQL(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    mappings: SchemaMapping[],
    transformationRules: TransformationRule[] = [],
    options: {
      preserveNulls?: boolean;
      useCoalesce?: boolean;
      parameterize?: boolean;
    } = {}
  ): GeneratedSQLFragment {
    globalLogger.debug(`[MapSQLGenerator] generateMappingSQL called`, {
      sourceColumnsCount: sourceColumns.length,
      mappingsCount: mappings.length,
      transformationRulesCount: transformationRules.length,
      options
    });

    const errors: SQLGenerationError[] = [];
    const warnings: string[] = [];

    // Merge default options (useCoalesce true by default)
    const mergedOptions = {
      preserveNulls: true,
      useCoalesce: true,
      parameterize: false,
      ...options
    };

    // Validate mappings
    this.validateMappings(sourceColumns, mappings, errors, warnings);

    if (errors.length > 0) {
      globalLogger.error(`[MapSQLGenerator] Mapping validation failed`, errors);
      return this.errorFragment('mapping_generation', errors, warnings);
    }

    // Generate column expressions
    const columnExpressions = this.generateColumnExpressions(
      sourceColumns,
      mappings,
      transformationRules,
      mergedOptions
    );
    globalLogger.debug(`[MapSQLGenerator] Generated ${columnExpressions.length} column expressions`);

    // Apply conditional logic
    const transformedExpressions = this.applyConditionalLogic(
      columnExpressions,
      transformationRules
    );

    // Build final SQL
    const sql = this.buildMappingSelect(
      transformedExpressions,
      this.determineSourceTable(mappings)
    );
    globalLogger.debug(`[MapSQLGenerator] Generated mapping SQL: ${sql.substring(0, 200)}...`);

    // Add performance hints
    const performanceHints = this.generateMappingPerformanceHints(mappings, transformationRules);
    warnings.push(...performanceHints);

    return {
      sql,
      dependencies: this.extractDependenciesFromMappings(mappings),
      parameters: this.extractParameters(transformationRules),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'complete_mapping',
        lineCount: sql.split('\n').length
      }
    };
  }

  /**
   * Generate CASE statements for conditional mapping
   */
  public generateCaseStatements(
    mappings: SchemaMapping[],
    transformationRules: TransformationRule[]
  ): string[] {
    globalLogger.debug(`[MapSQLGenerator] generateCaseStatements called`, {
      mappingsCount: mappings.length,
      transformationRulesCount: transformationRules.length
    });

    const caseStatements: string[] = [];

    mappings.forEach(mapping => {
      const matchingRules = transformationRules.filter((rule: TransformationRule) => 
        rule.params?.targetColumn === mapping.targetColumn
      );

      if (matchingRules.length > 0) {
        const caseStatement = this.buildCaseStatement(
          mapping.sourceColumn,
          mapping.targetColumn,
          matchingRules,
          mapping.transformation,
          mapping.defaultValue
        );
        
        if (caseStatement) {
          caseStatements.push(caseStatement);
          globalLogger.debug(`[MapSQLGenerator] Generated CASE statement for ${mapping.targetColumn}: ${caseStatement.substring(0, 100)}...`);
        }
      }
    });

    return caseStatements;
  }

  /**
   * Generate expression evaluation for arithmetic and string functions
   */
  public generateExpressionEvaluation(
    expression: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): { sql: string; errors: string[] } {
    globalLogger.debug(`[MapSQLGenerator] generateExpressionEvaluation called`, { expression, sourceColumnsCount: sourceColumns.length });
    
    const errors: string[] = [];

    // Validate expression syntax
    const validationResult = this.validateExpression(expression, sourceColumns);
    if (!validationResult.valid) {
      errors.push(...validationResult.errors);
      globalLogger.warn(`[MapSQLGenerator] Expression validation failed`, { expression, errors });
      return { sql: '', errors };
    }

    // Transform expression to PostgreSQL syntax
    let sql = this.transformToPostgreSQL(expression);

    // Optimize expression
    sql = this.optimizeExpression(sql);

    // Add type casting if needed
    const inferredType = this.inferExpressionType(expression, sourceColumns);
    if (inferredType) {
      sql = this.addTypeCasting(sql, inferredType);
    }

    globalLogger.debug(`[MapSQLGenerator] Expression evaluation result: ${sql}`);
    return { sql, errors };
  }

  // ==================== CANVAS UTILITY METHODS ====================

  /**
   * Extract schema mappings from canvas wires
   */
  private extractMappingsFromWires(
    wires: Wire[],
    sourceTables: TableDefinition[],
    targetTables: TableDefinition[]
  ): SchemaMapping[] {
    globalLogger.debug(`[MapSQLGenerator] extractMappingsFromWires called with ${wires.length} wires`);
    
    const mappings: SchemaMapping[] = [];
    
    wires.forEach(wire => {
      const sourceTable = sourceTables.find(t => t.id === wire.sourceTableId);
      const sourceColumn = sourceTable?.columns.find(c => c.id === wire.sourceColumnId);
      const targetTable = targetTables.find(t => t.id === wire.targetTableId);
      const targetColumn = targetTable?.columns.find(c => c.id === wire.targetColumnId);
      
      if (sourceColumn && targetColumn) {
        mappings.push({
          sourceColumn: `${sourceTable?.name}.${sourceColumn.name}`,
          targetColumn: targetColumn.name,
          transformation: wire.transformation || undefined,
          dataTypeConversion: this.determineDataTypeConversion(sourceColumn.type, targetColumn.type),
          isRequired: true,
          defaultValue: undefined
        });
        globalLogger.debug(`[MapSQLGenerator] Mapped ${sourceTable?.name}.${sourceColumn.name} -> ${targetColumn.name}`);
      } else {
        globalLogger.warn(`[MapSQLGenerator] Could not resolve mapping for wire ${wire.id}: sourceColumn=${wire.sourceColumnId}, targetColumn=${wire.targetColumnId}`);
      }
    });
    
    return mappings;
  }

  /**
   * Generate default positional mappings for unmapped columns
   */
  private generateDefaultPositionalMappings(
    sourceTable: TableDefinition,
    targetTable: TableDefinition,
    alreadyMappedTargetNames: Set<string>
  ): SchemaMapping[] {
    globalLogger.debug(`[MapSQLGenerator] generateDefaultPositionalMappings called`, {
      sourceTable: sourceTable.name,
      targetTable: targetTable.name,
      alreadyMappedCount: alreadyMappedTargetNames.size
    });

    const defaultMappings: SchemaMapping[] = [];
    
    // Get unmapped target columns
    const unmappedTargetColumns = targetTable.columns
      .filter(col => !alreadyMappedTargetNames.has(col.name))
      .map(col => col.name);
    
    // Get source columns (unfiltered, we map by position)
    const sourceColumns = sourceTable.columns.map(col => col.name);
    
    // Map by position
    const maxPosition = Math.min(sourceColumns.length, unmappedTargetColumns.length);
    globalLogger.debug(`[MapSQLGenerator] Positional mapping: source columns=${sourceColumns.length}, unmapped targets=${unmappedTargetColumns.length}, maxPosition=${maxPosition}`);
    
    for (let i = 0; i < maxPosition; i++) {
      const sourceColName = sourceColumns[i];
      const targetColName = unmappedTargetColumns[i];
      const sourceCol = sourceTable.columns.find(c => c.name === sourceColName);
      const targetCol = targetTable.columns.find(c => c.name === targetColName);
      
      if (sourceCol && targetCol) {
        defaultMappings.push({
          sourceColumn: sourceColName,   // unqualified column name
          targetColumn: targetColName,
          dataTypeConversion: this.determineDataTypeConversion(sourceCol.type, targetCol.type),
          isRequired: true,
          defaultValue: undefined
        });
        globalLogger.debug(`[MapSQLGenerator] Positional mapping ${i}: ${sourceColName} -> ${targetColName}`);
      }
    }
    
    return defaultMappings;
  }

  /**
   * Determine if data type conversion is needed
   */
  private determineDataTypeConversion(
    sourceType: string,
    targetType: string
  ): { from: PostgreSQLDataType; to: PostgreSQLDataType; params?: Record<string, any> } | undefined {
    const sourcePGType = this.stringToPostgreSQLDataType(sourceType);
    const targetPGType = this.stringToPostgreSQLDataType(targetType);
    
    if (sourcePGType !== targetPGType) {
      globalLogger.debug(`[MapSQLGenerator] Data type conversion needed: ${sourceType} (${sourcePGType}) -> ${targetType} (${targetPGType})`);
      return {
        from: sourcePGType,
        to: targetPGType,
        params: {}
      };
    }
    
    return undefined;
  }

  /**
   * Convert string type to PostgreSQLDataType
   */
  private stringToPostgreSQLDataType(type: string): PostgreSQLDataType {
    const typeUpper = type.toUpperCase().trim();
    
    // Map common type names
    const typeMap: Record<string, PostgreSQLDataType> = {
      'INT': PostgreSQLDataType.INTEGER,
      'INTEGER': PostgreSQLDataType.INTEGER,
      'BIGINT': PostgreSQLDataType.BIGINT,
      'SMALLINT': PostgreSQLDataType.SMALLINT,
      'NUMBER': PostgreSQLDataType.NUMERIC,
      'NUMERIC': PostgreSQLDataType.NUMERIC,
      'DECIMAL': PostgreSQLDataType.DECIMAL,
      'FLOAT': PostgreSQLDataType.REAL,
      'DOUBLE': PostgreSQLDataType.DOUBLE_PRECISION,
      'REAL': PostgreSQLDataType.REAL,
      'VARCHAR': PostgreSQLDataType.VARCHAR,
      'CHAR': PostgreSQLDataType.CHAR,
      'STRING': PostgreSQLDataType.TEXT,
      'TEXT': PostgreSQLDataType.TEXT,
      'DATE': PostgreSQLDataType.DATE,
      'TIMESTAMP': PostgreSQLDataType.TIMESTAMP,
      'DATETIME': PostgreSQLDataType.TIMESTAMP,
      'BOOLEAN': PostgreSQLDataType.BOOLEAN,
      'BOOL': PostgreSQLDataType.BOOLEAN,
      'JSON': PostgreSQLDataType.JSON,
      'JSONB': PostgreSQLDataType.JSONB,
      'UUID': PostgreSQLDataType.UUID,
      'SERIAL': PostgreSQLDataType.SERIAL,
      'BIGSERIAL': PostgreSQLDataType.BIGSERIAL
    };
    
    const result = typeMap[typeUpper] || PostgreSQLDataType.VARCHAR;
    globalLogger.debug(`[MapSQLGenerator] stringToPostgreSQLDataType: ${type} -> ${result}`);
    return result;
  }

  /**
   * Extract transformation rules from canvas variables
   */
  private extractTransformationRulesFromVariables(variables: any[]): TransformationRule[] {
    globalLogger.debug(`[MapSQLGenerator] extractTransformationRulesFromVariables called with ${variables.length} variables`);
    
    const rules: TransformationRule[] = [];
    
    variables.forEach((variable, index) => {
      if (variable.expression && variable.name) {
        // Try to parse expression to determine rule type
        if (variable.expression.includes('+') || variable.expression.includes('-') || 
            variable.expression.includes('*') || variable.expression.includes('/')) {
          rules.push({
            id: `var_${index}`,
            type: 'arithmetic',
            params: {
              expression: variable.expression,
              variableName: variable.name
            },
            order: index + 1
          });
          globalLogger.debug(`[MapSQLGenerator] Extracted arithmetic rule from variable ${variable.name}: ${variable.expression}`);
        } else if (variable.expression.includes('UPPER') || variable.expression.includes('LOWER') ||
                   variable.expression.includes('TRIM') || variable.expression.includes('CONCAT')) {
          rules.push({
            id: `var_${index}`,
            type: 'string',
            params: {
              expression: variable.expression,
              variableName: variable.name
            },
            order: index + 1
          });
          globalLogger.debug(`[MapSQLGenerator] Extracted string rule from variable ${variable.name}: ${variable.expression}`);
        } else if (variable.expression.includes('EXTRACT') || variable.expression.includes('DATE')) {
          rules.push({
            id: `var_${index}`,
            type: 'date',
            params: {
              expression: variable.expression,
              variableName: variable.name
            },
            order: index + 1
          });
          globalLogger.debug(`[MapSQLGenerator] Extracted date rule from variable ${variable.name}: ${variable.expression}`);
        }
      }
    });
    
    return rules;
  }

  /**
   * Validate canvas mapping configuration
   */
  public validateCanvasMapping(context: CanvasMappingContext): {
    isValid: boolean;
    errors: string[];
    warnings: string[];
    suggestions: string[];
  } {
    globalLogger.debug(`[MapSQLGenerator] validateCanvasMapping called`, {
      sourceTablesCount: context.sourceTables.length,
      targetTablesCount: context.targetTables.length,
      wiresCount: context.wires.length
    });

    const errors: string[] = [];
    const warnings: string[] = [];
    const suggestions: string[] = [];
    
    // Check if we have at least one source and one target table
    if (context.sourceTables.length === 0) {
      errors.push('No source tables defined');
    }
    
    if (context.targetTables.length === 0) {
      errors.push('No target tables defined');
    }
    
    // Check for primary source and target
    const primarySource = context.sourceTables.find(t => t.type === 'input');
    const primaryTarget = context.targetTables.find(t => t.type === 'output');
    
    if (!primarySource) {
      warnings.push('No primary input table found');
    }
    
    if (!primaryTarget) {
      warnings.push('No primary output table found');
    }
    
    if (primarySource && primaryTarget) {
      // Check for unmapped required columns
      const mappedColumns = context.wires
        .filter(w => w.targetTableId === primaryTarget.id)
        .map(w => {
          const targetCol = primaryTarget.columns.find(c => c.id === w.targetColumnId);
          return targetCol?.name;
        })
        .filter(Boolean);
      
      const unmappedColumns = primaryTarget.columns
        .filter(col => !mappedColumns.includes(col.name))
        .map(col => col.name);
      
      if (unmappedColumns.length > 0) {
        warnings.push(`Output table "${primaryTarget.name}" has unmapped columns: ${unmappedColumns.join(', ')}`);
        suggestions.push(`Consider adding default mappings for unmapped columns in ${primaryTarget.name}`);
      }
      
      // Check data type compatibility
      context.wires.forEach(wire => {
        const sourceTable = context.sourceTables.find(t => t.id === wire.sourceTableId);
        const sourceColumn = sourceTable?.columns.find(c => c.id === wire.sourceColumnId);
        const targetTable = context.targetTables.find(t => t.id === wire.targetTableId);
        const targetColumn = targetTable?.columns.find(c => c.id === wire.targetColumnId);
        
        if (sourceColumn && targetColumn) {
          const sourceType = this.stringToPostgreSQLDataType(sourceColumn.type);
          const targetType = this.stringToPostgreSQLDataType(targetColumn.type);
          
          if (!this.validateDataTypeCompatibility(sourceType, targetType)) {
            warnings.push(`Data type mismatch: ${sourceColumn.name} (${sourceColumn.type}) -> ${targetColumn.name} (${targetColumn.type})`);
            suggestions.push(`Add explicit type conversion for ${sourceColumn.name} to ${targetColumn.name}`);
          }
        }
      });
      
      // Check for duplicate mappings
      const targetColumnMap = new Map<string, number>();
      context.wires.forEach(wire => {
        const targetTable = context.targetTables.find(t => t.id === wire.targetTableId);
        const targetColumn = targetTable?.columns.find(c => c.id === wire.targetColumnId);
        if (targetColumn) {
          const count = targetColumnMap.get(targetColumn.name) || 0;
          targetColumnMap.set(targetColumn.name, count + 1);
        }
      });
      
      targetColumnMap.forEach((count, column) => {
        if (count > 1) {
          warnings.push(`Column "${column}" has multiple source mappings`);
        }
      });
    }
    
    globalLogger.debug(`[MapSQLGenerator] Validation result: isValid=${errors.length === 0}, errors=${errors.length}, warnings=${warnings.length}`);
    
    return {
      isValid: errors.length === 0,
      errors,
      warnings,
      suggestions
    };
  }

  // ==================== TRANSFORMATION METHODS ====================

  private generateColumnExpressions(
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    mappings: SchemaMapping[],
    transformationRules: TransformationRule[],
    options: { preserveNulls?: boolean; useCoalesce?: boolean; parameterize?: boolean }
  ): Array<{ expression: string; alias: string }> {
    globalLogger.debug(`[MapSQLGenerator] generateColumnExpressions called`, {
      sourceColumnsCount: sourceColumns.length,
      mappingsCount: mappings.length,
      transformationRulesCount: transformationRules.length,
      options
    });

    const expressions: Array<{ expression: string; alias: string }> = [];

    mappings.forEach(mapping => {
      const expression = this.buildColumnExpression(
        mapping,
        sourceColumns,
        transformationRules,
        options
      );

      expressions.push({
        expression,
        alias: mapping.targetColumn
      });
      globalLogger.debug(`[MapSQLGenerator] Column expression: ${expression} AS ${mapping.targetColumn}`);
    });

    // Handle unmapped source columns
    if (options.preserveNulls) {
      sourceColumns.forEach(sourceCol => {
        const isMapped = mappings.some(m => m.sourceColumn === sourceCol.name);
        if (!isMapped) {
          expressions.push({
            expression: this.sanitizeIdentifier(sourceCol.name),
            alias: sourceCol.name
          });
          globalLogger.debug(`[MapSQLGenerator] Preserved unmapped column: ${sourceCol.name}`);
        }
      });
    }

    return expressions;
  }

  private buildColumnExpression(
    mapping: SchemaMapping,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    transformationRules: TransformationRule[],
    options: { useCoalesce?: boolean; parameterize?: boolean }
  ): string {
    globalLogger.debug(`[MapSQLGenerator] buildColumnExpression for mapping ${mapping.sourceColumn} -> ${mapping.targetColumn}`);
    
    // Base expression – use sanitized qualified identifier to handle column names with spaces
    let expression = this.sanitizeQualifiedIdentifier(mapping.sourceColumn);

    // Apply data type conversion if needed
    if (mapping.dataTypeConversion) {
      const sourceCol = sourceColumns.find(c => c.name === mapping.sourceColumn.split('.').pop());
      if (sourceCol) {
        const before = expression;
        expression = this.applyDataTypeConversion(
          expression,
          sourceCol.dataType,
          mapping.dataTypeConversion.to,
          mapping.dataTypeConversion.params
        );
        globalLogger.debug(`[MapSQLGenerator] Applied data type conversion: ${before} -> ${expression}`);
      }
    }

    // Apply transformation if specified
    if (mapping.transformation) {
      const before = expression;
      expression = this.applyTransformation(
        expression,
        mapping.transformation,
        sourceColumns
      );
      globalLogger.debug(`[MapSQLGenerator] Applied transformation: ${before} -> ${expression}`);
    }

    // Apply COALESCE for NULL handling (if enabled)
    if (options.useCoalesce && mapping.defaultValue !== undefined) {
      const before = expression;
      expression = `COALESCE(${expression}, ${this.sanitizeValue(mapping.defaultValue)})`;
      globalLogger.debug(`[MapSQLGenerator] Applied COALESCE: ${before} -> ${expression}`);
    }

    // Apply transformation rules
    const rules = transformationRules.filter((r: TransformationRule) => 
      r.params?.targetColumn === mapping.targetColumn
    );
    
    if (rules.length > 0) {
      const before = expression;
      expression = this.applyTransformationRules(expression, rules);
      globalLogger.debug(`[MapSQLGenerator] Applied ${rules.length} transformation rules: ${before} -> ${expression}`);
    }

    // Parameterize if requested
    if (options.parameterize && mapping.defaultValue !== undefined) {
      expression = this.parameterizeExpression(expression, mapping.targetColumn);
    }

    return expression;
  }

  private applyDataTypeConversion(
    expression: string,
    fromType: PostgreSQLDataType,
    toType: PostgreSQLDataType,
    _params?: Record<string, any>
  ): string {
    // Handle PostgreSQL type casting
    if (fromType === toType) {
      return expression;
    }

    const castOperator = '::';
    
    // Special handling for common conversions
    switch (`${fromType}->${toType}`) {
      case `${PostgreSQLDataType.VARCHAR}->${PostgreSQLDataType.INTEGER}`:
        return `(${expression})${castOperator}integer`;
      
      case `${PostgreSQLDataType.INTEGER}->${PostgreSQLDataType.VARCHAR}`:
        return `(${expression})${castOperator}varchar`;
      
      case `${PostgreSQLDataType.DATE}->${PostgreSQLDataType.TIMESTAMP}`:
        return `(${expression})${castOperator}timestamp`;
      
      case `${PostgreSQLDataType.TIMESTAMP}->${PostgreSQLDataType.DATE}`:
        return `(${expression})${castOperator}date`;
      
      case `${PostgreSQLDataType.NUMERIC}->${PostgreSQLDataType.DOUBLE_PRECISION}`:
        return `(${expression})${castOperator}double precision`;
      
      default:
        return `(${expression})${castOperator}${toType.toLowerCase()}`;
    }
  }

  private applyTransformation(
    expression: string,
    transformation: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): string {
    // Replace column references in transformation
    let transformed = transformation;
    
    sourceColumns.forEach(col => {
      const regex = new RegExp(`\\b${col.name}\\b`, 'g');
      transformed = transformed.replace(regex, this.sanitizeIdentifier(col.name));
    });

    // Replace the source column reference with the transformation
    if (transformed.includes('?')) {
      // Use PostgreSQL positional parameters
      return transformed.replace('?', expression);
    } else if (transformed.includes('$1')) {
      // Use PostgreSQL numbered parameters
      return transformed.replace('$1', expression);
    } else {
      // Assume direct substitution
      return transformed;
    }
  }

  private applyTransformationRules(
    expression: string,
    rules: TransformationRule[]
  ): string {
    // Sort rules by order
    const sortedRules = [...rules].sort((a, b) => a.order - b.order);
    
    let result = expression;
    
    sortedRules.forEach((rule: TransformationRule) => {
      switch (rule.type) {
        case 'arithmetic':
          result = this.applyArithmeticRule(result, rule.params);
          break;
        case 'string':
          result = this.applyStringRule(result, rule.params);
          break;
        case 'date':
          result = this.applyDateRule(result, rule.params);
          break;
        case 'conditional':
          result = this.applyConditionalRule(result, rule);
          break;
      }
    });
    
    return result;
  }

  private applyArithmeticRule(expression: string, params: any): string {
    const operator = params.operator || '+';
    const operand = params.operand || 0;
    
    switch (operator) {
      case '+': return `(${expression} + ${operand})`;
      case '-': return `(${expression} - ${operand})`;
      case '*': return `(${expression} * ${operand})`;
      case '/': return `(${expression} / ${operand})`;
      case '%': return `(${expression} % ${operand})`;
      case 'power': return `POWER(${expression}, ${operand})`;
      case 'sqrt': return `SQRT(${expression})`;
      default: return expression;
    }
  }

  private applyStringRule(expression: string, params: any): string {
    const operation = params.operation || 'upper';
    
    switch (operation) {
      case 'upper': return `UPPER(${expression})`;
      case 'lower': return `LOWER(${expression})`;
      case 'trim': return `TRIM(${expression})`;
      case 'ltrim': return `LTRIM(${expression})`;
      case 'rtrim': return `RTRIM(${expression})`;
      case 'concat': return `CONCAT(${expression}, '${params.suffix || ''}')`;
      case 'substring': 
        return `SUBSTRING(${expression} FROM ${params.start || 1} FOR ${params.length || 1})`;
      case 'replace':
        return `REPLACE(${expression}, '${params.from || ''}', '${params.to || ''}')`;
      case 'length': return `LENGTH(${expression})`;
      default: return expression;
    }
  }

  private applyDateRule(expression: string, params: any): string {
    const operation = params.operation || 'extract';
    
    switch (operation) {
      case 'extract':
        const part = params.part || 'year';
        return `EXTRACT(${part.toUpperCase()} FROM ${expression})`;
      case 'add':
        const interval = params.interval || '1 day';
        return `${expression} + INTERVAL '${interval}'`;
      case 'subtract':
        const interval2 = params.interval || '1 day';
        return `${expression} - INTERVAL '${interval2}'`;
      case 'format':
        const format = params.format || 'YYYY-MM-DD';
        return `TO_CHAR(${expression}, '${format}')`;
      default: return expression;
    }
  }

  private applyConditionalRule(expression: string, rule: TransformationRule): string {
    if (!rule.condition) {
      return expression;
    }

    const condition = this.sanitizeCondition(rule.condition);
    const trueValue = rule.params?.trueValue || expression;
    const falseValue = rule.params?.falseValue || expression;
    
    return `CASE WHEN ${condition} THEN ${trueValue} ELSE ${falseValue} END`;
  }

  private buildCaseStatement(
    sourceColumn: string,
    targetColumn: string,
    rules: TransformationRule[],
    baseTransformation?: string,
    defaultValue?: string
  ): string | null {
    if (rules.length === 0 && !baseTransformation) {
      return null;
    }

    const caseParts: string[] = [];
    
    // Add WHEN conditions from rules
    rules.forEach((rule: TransformationRule) => {
      if (rule.condition) {
        const condition = this.sanitizeCondition(rule.condition);
        const value = rule.params?.value || this.sanitizeQualifiedIdentifier(sourceColumn);
        caseParts.push(`WHEN ${condition} THEN ${value}`);
      }
    });
    
    // Add ELSE clause
    let elseClause = defaultValue 
      ? this.sanitizeValue(defaultValue)
      : baseTransformation 
        ? baseTransformation.replace('?', this.sanitizeQualifiedIdentifier(sourceColumn))
        : this.sanitizeQualifiedIdentifier(sourceColumn);
    
    return `CASE ${caseParts.join(' ')} ELSE ${elseClause} END AS ${this.sanitizeIdentifier(targetColumn)}`;
  }

  // ==================== VALIDATION AND OPTIMIZATION ====================

  private validateMappings(
  sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
  mappings: SchemaMapping[],
  errors: SQLGenerationError[],
  warnings: string[]
): void {
  const sourceColumnNames = new Set(sourceColumns.map(c => c.name));

  mappings.forEach(mapping => {
    // Extract the column name from qualified identifier
    const sourceColumnSimple = mapping.sourceColumn.split('.').pop() || mapping.sourceColumn;

    // Skip existence check if the sourceColumn is an expression
    if (!this.isExpression(mapping.sourceColumn) && !sourceColumnNames.has(sourceColumnSimple)) {
      errors.push({
        code: 'SOURCE_COLUMN_NOT_FOUND',
        message: `Source column "${sourceColumnSimple}" not found`,
        severity: 'ERROR',
        field: 'sourceColumn'
      });
      globalLogger.warn(`[MapSQLGenerator] Source column not found: ${sourceColumnSimple}`);
    }

    // Validate data type conversions
    if (mapping.dataTypeConversion) {
      const sourceCol = sourceColumns.find(c => c.name === sourceColumnSimple);
      if (sourceCol) {
        const isCompatible = this.validateDataTypeCompatibility(
          sourceCol.dataType,
          mapping.dataTypeConversion.to
        );

        if (!isCompatible) {
          warnings.push(`Data type conversion from ${sourceCol.dataType} to ${mapping.dataTypeConversion.to} may lose precision`);
          globalLogger.warn(`[MapSQLGenerator] Incompatible type conversion: ${sourceCol.dataType} -> ${mapping.dataTypeConversion.to}`);
        }
      }
    }

    // Validate transformation syntax
    if (mapping.transformation) {
      const validation = this.validateExpression(mapping.transformation, sourceColumns);
      if (!validation.valid) {
        errors.push({
          code: 'INVALID_TRANSFORMATION',
          message: `Invalid transformation for column "${mapping.targetColumn}": ${validation.errors[0]}`,
          severity: 'ERROR',
          field: 'transformation'
        });
        globalLogger.warn(`[MapSQLGenerator] Invalid transformation for ${mapping.targetColumn}: ${validation.errors[0]}`);
      }
    }
  });

  // Check for duplicate target columns
  const targetColumns = mappings.map(m => m.targetColumn);
  const duplicates = targetColumns.filter((col, index) => targetColumns.indexOf(col) !== index);

  duplicates.forEach(dup => {
    warnings.push(`Duplicate target column: "${dup}"`);
    globalLogger.warn(`[MapSQLGenerator] Duplicate target column: ${dup}`);
  });
}

  private validateExpression(
    expression: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): { valid: boolean; errors: string[] } {
    const errors: string[] = [];
    
    // Check for invalid characters
    if (/[;\\]/g.test(expression)) {
      errors.push('Expression contains invalid characters');
    }
    
    // Check for balanced parentheses
    const openParens = (expression.match(/\(/g) || []).length;
    const closeParens = (expression.match(/\)/g) || []).length;
    
    if (openParens !== closeParens) {
      errors.push('Unbalanced parentheses in expression');
    }
    
    // Check for valid column references
    const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match: RegExpExecArray | null;
    
    while ((match = columnPattern.exec(expression)) !== null) {
      const columnName = match[1];
      
      // Skip SQL keywords and functions
      if (this.reservedKeywords.has(columnName.toUpperCase())) {
        continue;
      }
      
      // Check if it's a known column
      if (!sourceColumns.some(c => c.name === columnName)) {
        errors.push(`Unknown column reference: "${columnName}"`);
      }
    }
    
    return {
      valid: errors.length === 0,
      errors
    };
  }

  private validateDataTypeCompatibility(
    fromType: PostgreSQLDataType,
    toType: PostgreSQLDataType
  ): boolean {
    // Simple compatibility matrix
    const compatibleConversions: Record<string, string[]> = {
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
      ],
      [PostgreSQLDataType.NUMERIC]: [
        PostgreSQLDataType.DECIMAL,
        PostgreSQLDataType.REAL,
        PostgreSQLDataType.DOUBLE_PRECISION,
        PostgreSQLDataType.VARCHAR,
        PostgreSQLDataType.TEXT
      ]
    };
    
    if (fromType === toType) return true;
    
    return compatibleConversions[fromType]?.includes(toType) || false;
  }

  private transformToPostgreSQL(expression: string): string {
    return expression
      .replace(/\bISNULL\(/gi, 'COALESCE(')
      .replace(/\bLEN\(/gi, 'LENGTH(')
      .replace(/\bSUBSTR\(/gi, 'SUBSTRING(')
      .replace(/\bGETDATE\(\)/gi, 'CURRENT_TIMESTAMP')
      .replace(/\bCONVERT\(/gi, 'CAST(')
      .replace(/\bTOP\s+(\d+)/gi, 'LIMIT $1')
      .replace(/\+/g, '||'); // String concatenation in PostgreSQL
  }

  private optimizeExpression(expression: string): string {
    return expression
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\bLIKE\s+'%(.+)%'/gi, "ILIKE '%$1%'")
      .replace(/\s+AND\s+/gi, ' AND ')
      .replace(/\s+OR\s+/gi, ' OR ')
      .replace(/\(\(/g, '(')
      .replace(/\)\)/g, ')');
  }

  private inferExpressionType(
    expression: string,
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): PostgreSQLDataType | null {
    // Simple type inference based on operations
    if (expression.includes('+') || expression.includes('-') || 
        expression.includes('*') || expression.includes('/')) {
      return PostgreSQLDataType.NUMERIC;
    }
    
    if (expression.includes('||') || expression.includes('CONCAT')) {
      return PostgreSQLDataType.VARCHAR;
    }
    
    if (expression.includes('EXTRACT') || expression.includes('TO_CHAR')) {
      return PostgreSQLDataType.VARCHAR;
    }
    
    // Check column references
    const columnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    let match: RegExpExecArray | null;
    
    while ((match = columnPattern.exec(expression)) !== null) {
      const columnName = match[1];
      const sourceCol = sourceColumns.find(c => c.name === columnName);
      if (sourceCol) {
        return sourceCol.dataType;
      }
    }
    
    return null;
  }

  private addTypeCasting(expression: string, targetType: PostgreSQLDataType): string {
    return `(${expression})::${targetType.toLowerCase()}`;
  }

  private generateMappingPerformanceHints(
    mappings: SchemaMapping[],
    transformationRules: TransformationRule[]
  ): string[] {
    const hints: string[] = [];

    // Check for expensive transformations
    transformationRules.forEach((rule: TransformationRule) => {
      if (rule.type === 'conditional' && rule.condition?.includes('LIKE')) {
        hints.push(`CASE statement with LIKE may be expensive; consider adding index on source column`);
      }
      
      if (rule.type === 'string' && rule.params?.operation === 'substring') {
        hints.push(`SUBSTRING operations may benefit from functional indexes`);
      }
    });

    // Check for data type conversions
    mappings.forEach(mapping => {
      if (mapping.dataTypeConversion) {
        hints.push(`Data type conversion ${mapping.dataTypeConversion.from}->${mapping.dataTypeConversion.to} may impact performance`);
      }
    });

    // Suggest bulk operations
    if (mappings.length > 20) {
      hints.push('Consider materializing complex mappings with a temporary table');
    }

    return hints;
  }

  // ==================== HELPER METHODS ====================

  private extractSchemaMappings(
    node: UnifiedCanvasNode,
    connection?: UnifiedCanvasConnection
  ): SchemaMapping[] {
    globalLogger.debug(`[MapSQLGenerator] extractSchemaMappings called for node ${node.id}`);
    
    const mappings: SchemaMapping[] = [];
    
    // From node.metadata.configuration (MAP component)
    const config = node.metadata?.configuration;
    if (config && config.type === 'MAP') {
      const mapConfig = (config as { type: 'MAP'; config: MapComponentConfiguration }).config;
      if (mapConfig.transformations && Array.isArray(mapConfig.transformations)) {
        globalLogger.debug(`[MapSQLGenerator] Found ${mapConfig.transformations.length} transformations in MAP configuration`);
        for (const t of mapConfig.transformations) {
          mappings.push({
            sourceColumn: t.sourceField,
            targetColumn: t.targetField,
            transformation: t.expression !== t.sourceField ? t.expression : undefined,
            dataTypeConversion: undefined,
            isRequired: true,
            defaultValue: t.defaultValue,
          });
        }
      }
    }
    
    // From node.metadata.schemaMappings (legacy)
    if (node.metadata?.schemaMappings && Array.isArray(node.metadata.schemaMappings)) {
      globalLogger.debug(`[MapSQLGenerator] Found ${node.metadata.schemaMappings.length} legacy schema mappings`);
      mappings.push(...node.metadata.schemaMappings);
    }
    
    // From connection
    if (connection?.dataFlow?.schemaMappings) {
      globalLogger.debug(`[MapSQLGenerator] Found ${connection.dataFlow.schemaMappings.length} connection schema mappings`);
      mappings.push(...connection.dataFlow.schemaMappings);
    }
    
    globalLogger.debug(`[MapSQLGenerator] Total extracted mappings: ${mappings.length}`);
    return mappings;
  }

  /**
   * Generate mapped columns with explicit type casting using output schema.
   */
  private generateMappedColumns(
    mappings: SchemaMapping[],
    transformationRules: TransformationRule[],
    node: UnifiedCanvasNode,
    upstreamSchema: Array<{ name: string; dataType: PostgreSQLDataType }>
  ): string {
    globalLogger.debug(`[MapSQLGenerator] generateMappedColumns called`, {
      mappingsCount: mappings.length,
      transformationRulesCount: transformationRules.length,
      upstreamSchemaLength: upstreamSchema.length,
      hasOutputSchema: !!node.metadata?.schemas?.output?.fields
    });

    // Build a map of target column -> expected PostgreSQL type
    const targetTypeMap = new Map<string, string>();
    const outputFields = node.metadata?.schemas?.output?.fields || [];
    for (const field of outputFields) {
      const pgType = this.mapDataTypeToPostgresType(field.type, field.length, field.precision, field.scale);
      targetTypeMap.set(field.name, pgType);
      globalLogger.debug(`[MapSQLGenerator] Target column ${field.name} expects type ${pgType}`);
    }

    // Helper to get source column type from a simple column name
    const getSourceType = (expr: string): PostgreSQLDataType | null => {
      const simpleName = expr.split('.').pop() || expr;
      const sourceCol = upstreamSchema.find(c => c.name === simpleName);
      if (sourceCol) {
        globalLogger.debug(`[MapSQLGenerator] Source column ${simpleName} has type ${sourceCol.dataType}`);
      }
      return sourceCol ? sourceCol.dataType : null;
    };

    const columns: string[] = [];

    for (const mapping of mappings) {
      let expression = this.sanitizeQualifiedIdentifier(mapping.sourceColumn);
      globalLogger.debug(`[MapSQLGenerator] Processing mapping: ${mapping.sourceColumn} -> ${mapping.targetColumn}, initial expression: ${expression}`);

      // Apply transformation if present
      if (mapping.transformation) {
        expression = this.replaceColumnPlaceholders(mapping.transformation, []);
        globalLogger.debug(`[MapSQLGenerator] Applied transformation: ${mapping.transformation} -> ${expression}`);
      }

      // Apply default value handling (COALESCE)
      if (mapping.defaultValue && !mapping.transformation) {
        expression = `COALESCE(${expression}, ${this.sanitizeValue(mapping.defaultValue)})`;
        globalLogger.debug(`[MapSQLGenerator] Applied COALESCE with default value ${mapping.defaultValue}`);
      }

      // Apply additional transformation rules (case, arithmetic, etc.)
      const rules = transformationRules.filter(r => r.params?.targetColumn === mapping.targetColumn);
      if (rules.length > 0) {
        const before = expression;
        expression = this.applyTransformationRules(expression, rules);
        globalLogger.debug(`[MapSQLGenerator] Applied ${rules.length} transformation rules: ${before} -> ${expression}`);
      }

      // Conditional type cast – only if target type differs from source type
      const targetType = targetTypeMap.get(mapping.targetColumn);
      if (targetType && !expression.toLowerCase().includes('::')) {
        const sourceType = getSourceType(mapping.sourceColumn);
        if (sourceType && sourceType.toLowerCase() !== targetType.toLowerCase()) {
          const before = expression;
          expression = `(${expression})::${targetType}`;
          globalLogger.debug(`[MapSQLGenerator] Type cast applied: ${before} -> ${expression} (${sourceType} -> ${targetType})`);
        } else {
          globalLogger.debug(`[MapSQLGenerator] No type cast needed: source type ${sourceType}, target type ${targetType}`);
        }
      }

      columns.push(`${expression} AS ${this.sanitizeIdentifier(mapping.targetColumn)}`);
    }

    const result = columns.join(', ');
    globalLogger.debug(`[MapSQLGenerator] Final mapped columns: ${result.substring(0, 300)}...`);
    return result;
  }

  private mapDataTypeToPostgresType(dataType: string, _length?: number, precision?: number, scale?: number): string {
    const type = dataType.toUpperCase();
    switch (type) {
      case 'STRING':
        return 'TEXT';
      case 'INTEGER':
        return 'INTEGER';
      case 'BIGINT':
        return 'BIGINT';
      case 'DECIMAL':
        if (precision !== undefined) {
          return `DECIMAL(${precision},${scale !== undefined ? scale : 0})`;
        }
        return 'DECIMAL';
      case 'BOOLEAN':
        return 'BOOLEAN';
      case 'DATE':
        return 'DATE';
      case 'TIMESTAMP':
        return 'TIMESTAMP';
      case 'BINARY':
        return 'BYTEA';
      default:
        return 'TEXT';
    }
  }

  private buildSelectClauseFromMappings(
    mappings: SchemaMapping[],
    sourceColumns: Array<{ name: string; dataType: PostgreSQLDataType }>,
    transformationRules: TransformationRule[],
    mapNode?: UnifiedCanvasNode
  ): string {
    globalLogger.debug(`[MapSQLGenerator] buildSelectClauseFromMappings called`, {
      mappingsCount: mappings.length,
      sourceColumnsCount: sourceColumns.length,
      transformationRulesCount: transformationRules.length,
      hasMapNode: !!mapNode
    });

    // Build target type map from output schema if available
    const targetTypeMap = new Map<string, string>();
    if (mapNode?.metadata?.schemas?.output?.fields) {
      for (const field of mapNode.metadata.schemas.output.fields) {
        const pgType = this.mapDataTypeToPostgresType(field.type, field.length, field.precision, field.scale);
        targetTypeMap.set(field.name, pgType);
        globalLogger.debug(`[MapSQLGenerator] Target column ${field.name} expects type ${pgType} (from output schema)`);
      }
    } else {
      globalLogger.debug(`[MapSQLGenerator] No output schema found on map node, type casting will be limited`);
    }

    // Helper to get source column type from a simple column name
    const getSourceType = (expr: string): PostgreSQLDataType | null => {
      const simpleName = expr.split('.').pop() || expr;
      const sourceCol = sourceColumns.find(c => c.name === simpleName);
      return sourceCol ? sourceCol.dataType : null;
    };

    // Helper to check if an expression is a simple column reference (no functions, no operators)
    const isSimpleColumnRef = (expr: string): boolean => {
      return /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/.test(expr);
    };

    const columns: string[] = [];

    for (const mapping of mappings) {
      let expression = mapping.sourceColumn; // may be expression or column name
      globalLogger.debug(`[MapSQLGenerator] Building column: source=${mapping.sourceColumn}, target=${mapping.targetColumn}`);

      // Apply transformation if present
      if (mapping.transformation) {
        const before = expression;
        expression = this.replaceColumnPlaceholders(mapping.transformation, []);
        globalLogger.debug(`[MapSQLGenerator] Applied transformation: ${before} -> ${expression}`);
      }

      // Apply default value (COALESCE)
      if (mapping.defaultValue !== undefined && !mapping.transformation) {
        expression = `COALESCE(${expression}, ${this.sanitizeValue(mapping.defaultValue)})`;
        globalLogger.debug(`[MapSQLGenerator] Applied COALESCE with default ${mapping.defaultValue}`);
      }

      // Apply transformation rules (CASE, arithmetic, etc.)
      const rules = transformationRules.filter(r => r.params?.targetColumn === mapping.targetColumn);
      if (rules.length) {
        const before = expression;
        expression = this.applyTransformationRules(expression, rules);
        globalLogger.debug(`[MapSQLGenerator] Applied ${rules.length} transformation rules: ${before} -> ${expression}`);
      }

      // ----- Conditional type casting -----
      const targetType = targetTypeMap.get(mapping.targetColumn);
      if (targetType && !expression.toLowerCase().includes('::')) {
        const sourceType = isSimpleColumnRef(mapping.sourceColumn) ? getSourceType(mapping.sourceColumn) : null;
        if (sourceType !== null) {
          const sourcePgTypeStr = sourceType.toLowerCase();
          const targetPgTypeStr = targetType.toLowerCase();
          if (sourcePgTypeStr !== targetPgTypeStr) {
            const before = expression;
            expression = `(${expression})::${targetType}`;
            globalLogger.debug(`[MapSQLGenerator] Type cast applied: ${before} -> ${expression} (${sourcePgTypeStr} -> ${targetPgTypeStr})`);
          } else {
            globalLogger.debug(`[MapSQLGenerator] No type cast needed: source type ${sourcePgTypeStr} matches target type ${targetPgTypeStr}`);
          }
        } else {
          // For complex expressions, we assume they already produce the correct type.
          // Do not add automatic cast – the expression must be written to yield the desired type.
          globalLogger.debug(`[MapSQLGenerator] Skipping type cast for complex expression: ${expression}`);
        }
      }

      columns.push(`${expression} AS ${this.sanitizeIdentifier(mapping.targetColumn)}`);
    }

    const result = columns.join(', ');
    globalLogger.debug(`[MapSQLGenerator] Final SELECT clause: ${result.substring(0, 300)}...`);
    return result;
  }

  private extractSourceDependencies(mappings: SchemaMapping[]): string[] {
    const sources = new Set<string>();
    
    mappings.forEach(mapping => {
      // Extract table name from source column (assuming format "table.column")
      const parts = mapping.sourceColumn.split('.');
      if (parts.length === 2) {
        sources.add(parts[0]);
      }
    });
    
    return Array.from(sources);
  }

  private extractDependenciesFromMappings(mappings: SchemaMapping[]): string[] {
    return this.extractSourceDependencies(mappings);
  }

  private extractParameters(transformationRules: TransformationRule[]): Map<string, any> {
    const parameters = new Map<string, any>();
    
    transformationRules.forEach((rule: TransformationRule, index) => {
      if (rule.params) {
        Object.entries(rule.params).forEach(([key, value]) => {
          parameters.set(`rule_${index}_${key}`, value);
        });
      }
    });
    
    return parameters;
  }

  private determineSourceTable(mappings: SchemaMapping[]): string {
    if (mappings.length === 0) {
      return 'source_table';
    }
    
    // Try to extract common table prefix
    const firstSource = mappings[0].sourceColumn;
    const parts = firstSource.split('.');
    
    if (parts.length === 2) {
      return parts[0];
    }
    
    return 'source_table';
  }

  private buildMappingSelect(
    expressions: Array<{ expression: string; alias: string }>,
    sourceTable: string
  ): string {
    const columnList = expressions.map(exp => 
      `${exp.expression} AS ${this.sanitizeIdentifier(exp.alias)}`
    ).join(',\n    ');
    
    return `SELECT\n    ${columnList}\nFROM ${this.sanitizeIdentifier(sourceTable)}`;
  }

  private applyConditionalLogic(
    expressions: Array<{ expression: string; alias: string }>,
    transformationRules: TransformationRule[]
  ): Array<{ expression: string; alias: string }> {
    return expressions.map(exp => {
      const rules = transformationRules.filter((rule: TransformationRule) => 
        rule.params?.targetColumn === exp.alias && rule.type === 'conditional'
      );
      
      if (rules.length > 0) {
        const caseStatement = this.buildCaseStatementForRules(
          exp.expression,
          exp.alias,
          rules
        );
        
        if (caseStatement) {
          return { expression: caseStatement, alias: exp.alias };
        }
      }
      
      return exp;
    });
  }

  private buildCaseStatementForRules(
    baseExpression: string,
    _targetColumn: string,
    rules: TransformationRule[]
  ): string | null {
    const sortedRules = [...rules].sort((a, b) => a.order - b.order);
    const caseParts: string[] = [];
    
    sortedRules.forEach((rule: TransformationRule) => {
      if (rule.condition) {
        const condition = this.sanitizeCondition(rule.condition);
        const value = rule.params?.value || baseExpression;
        caseParts.push(`WHEN ${condition} THEN ${value}`);
      }
    });
    
    if (caseParts.length === 0) {
      return null;
    }
    
    return `CASE ${caseParts.join(' ')} ELSE ${baseExpression} END`;
  }

  private parameterizeExpression(expression: string, parameterName: string): string {
    return expression.replace(/\?/g, `$${parameterName}`);
  }

  private sanitizeCondition(condition: string): string {
    return condition
      .replace(/\s*=\s*NULL\b/gi, ' IS NULL')
      .replace(/\s*<>\s*NULL\b/gi, ' IS NOT NULL')
      .replace(/\[/g, '"')
      .replace(/\]/g, '"');
  }

  private buildFilterConditions(filterRules: TransformationRule[]): string {
    const conditions = filterRules.map(rule => {
      if (rule.condition) {
        return this.sanitizeCondition(rule.condition);
      }
      return '';
    }).filter(Boolean);
    
    return conditions.join(' AND ');
  }

  private generateOrderByFromSortConfig(sortConfig: SortConfig): GeneratedSQLFragment {
    const orderByClauses = sortConfig.columns.map(col => {
      const parts = [this.sanitizeIdentifier(col.column), col.direction];
      if (col.nullsFirst !== undefined) {
        parts.push(col.nullsFirst ? 'NULLS FIRST' : 'NULLS LAST');
      }
      return parts.join(' ');
    });

    let sql = `ORDER BY ${orderByClauses.join(', ')}`;
    
    if (sortConfig.limit) {
      sql += `\nLIMIT ${sortConfig.limit}`;
    }
    
    if (sortConfig.offset) {
      sql += `\nOFFSET ${sortConfig.offset}`;
    }

    return {
      sql,
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'order_by',
        lineCount: sql.split('\n').length
      }
    };
  }

  private generateFallbackSelect(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node } = context;
    
    return {
      sql: `SELECT * FROM ${this.sanitizeIdentifier(node.name.toLowerCase().replace(/\s+/g, '_'))}`,
      dependencies: [node.name],
      parameters: new Map(),
      errors: [],
      warnings: ['No mapping configuration found, using fallback SELECT'],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType: 'fallback_select',
        lineCount: 1
      }
    };
  }

  // ==================== FIXED: BaseSQLGenerator compliance ====================
  // The base class expects an emptyFragment() method with no arguments.
  // We provide that and keep the parameterized version as a private helper.
  protected emptyFragment(): GeneratedSQLFragment {
    return this.createEmptyFragment('empty');
  }

  private createEmptyFragment(fragmentType: string): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0
      }
    };
  }

  // errorFragment remains as is – it is not abstract in BaseSQLGenerator.
  protected errorFragment(fragmentType: string, errors: SQLGenerationError[], warnings: string[] = []): GeneratedSQLFragment {
    return {
      sql: '',
      dependencies: [],
      parameters: new Map(),
      errors,
      warnings,
      metadata: {
        generatedAt: new Date().toISOString(),
        fragmentType,
        lineCount: 0
      }
    };
  }

  // ==================== CANVAS-SPECIFIC HELPER METHODS ====================

  public generateSQLPreview(context: CanvasMappingContext): {
    sql: string;
    isValid: boolean;
    errors: string[];
    warnings: string[];
    hasDefaultMappings: boolean;
  } {
    globalLogger.debug(`[MapSQLGenerator] generateSQLPreview called for node ${context.nodeId}`);
    
    const validation = this.validateCanvasMapping(context);
    
    if (!validation.isValid) {
      return {
        sql: '',
        isValid: false,
        errors: validation.errors,
        warnings: validation.warnings,
        hasDefaultMappings: false
      };
    }
    
    const primarySourceTable = context.sourceTables.find(t => t.type === 'input');
    const primaryTargetTable = context.targetTables.find(t => t.type === 'output');
    
    if (!primarySourceTable || !primaryTargetTable) {
      return {
        sql: '',
        isValid: false,
        errors: ['Missing primary source or target table'],
        warnings: [],
        hasDefaultMappings: false
      };
    }
    
    // Extract mappings
    const mappings = this.extractMappingsFromWires(
      context.wires,
      context.sourceTables,
      context.targetTables
    );
    
    // Generate default mappings
    const defaultMappings = this.generateDefaultPositionalMappings(
      primarySourceTable,
      primaryTargetTable,
      new Set(mappings.map(m => m.targetColumn))
    );
    
    const hasDefaultMappings = defaultMappings.length > 0;
    const allMappings = [...mappings, ...defaultMappings];
    
    if (allMappings.length === 0) {
      return {
        sql: `SELECT * FROM ${primarySourceTable.name}`,
        isValid: true,
        errors: [],
        warnings: ['No explicit mappings found'],
        hasDefaultMappings: false
      };
    }
    
    // Generate simple SQL preview
    const columnList = allMappings.map(mapping => {
      const sourcePart = mapping.sourceColumn.split('.').pop();
      return `${this.sanitizeIdentifier(sourcePart || '')} AS ${this.sanitizeIdentifier(mapping.targetColumn)}`;
    }).join(', ');
    
    const sql = `INSERT INTO ${primaryTargetTable.name} (${allMappings.map(m => m.targetColumn).join(', ')})
SELECT ${columnList}
FROM ${primarySourceTable.name}`;
    
    return {
      sql,
      isValid: true,
      errors: [],
      warnings: validation.warnings,
      hasDefaultMappings
    };
  }

  public getMappingStatistics(context: CanvasMappingContext): {
    totalSourceColumns: number;
    totalTargetColumns: number;
    mappedColumns: number;
    unmappedColumns: number;
    defaultMappings: number;
    mappingPercentage: number;
  } {
    const primarySourceTable = context.sourceTables.find(t => t.type === 'input');
    const primaryTargetTable = context.targetTables.find(t => t.type === 'output');
    
    if (!primarySourceTable || !primaryTargetTable) {
      return {
        totalSourceColumns: 0,
        totalTargetColumns: 0,
        mappedColumns: 0,
        unmappedColumns: 0,
        defaultMappings: 0,
        mappingPercentage: 0
      };
    }
    
    const totalSourceColumns = primarySourceTable.columns.length;
    const totalTargetColumns = primaryTargetTable.columns.length;
    
    // Get explicit mappings
    const mappedTargetColumns = new Set(
      context.wires
        .filter(w => w.targetTableId === primaryTargetTable.id)
        .map(w => {
          const targetCol = primaryTargetTable.columns.find(c => c.id === w.targetColumnId);
          return targetCol?.name;
        })
        .filter(Boolean)
    );
    
    const mappedColumns = mappedTargetColumns.size;
    const unmappedColumns = totalTargetColumns - mappedColumns;
    
    // Calculate potential default mappings
    const defaultMappings = Math.min(
      totalSourceColumns - mappedColumns, // Available source columns
      unmappedColumns // Available target columns
    );
    
    const mappingPercentage = totalTargetColumns > 0 
      ? Math.round((mappedColumns / totalTargetColumns) * 100) 
      : 0;
    
    return {
      totalSourceColumns,
      totalTargetColumns,
      mappedColumns,
      unmappedColumns,
      defaultMappings,
      mappingPercentage
    };
  }

  public generateMappingSuggestions(
    sourceColumns: string[],
    targetColumns: string[],
    existingMappings: Array<{ source: string; target: string }>
  ): Array<{ source: string; target: string; confidence: number; reason: string }> {
    const suggestions: Array<{ source: string; target: string; confidence: number; reason: string }> = [];
    
    // Get already mapped columns
    const mappedSources = new Set(existingMappings.map(m => m.source));
    const mappedTargets = new Set(existingMappings.map(m => m.target));
    
    // Find unmapped columns
    const unmappedSources = sourceColumns.filter(col => !mappedSources.has(col));
    const unmappedTargets = targetColumns.filter(col => !mappedTargets.has(col));
    
    // Generate suggestions based on name similarity
    unmappedSources.forEach(source => {
      unmappedTargets.forEach(target => {
        const similarity = this.calculateNameSimilarity(source, target);
        if (similarity > 0.3) { // Threshold for suggestion
          suggestions.push({
            source,
            target,
            confidence: similarity,
            reason: 'Name similarity'
          });
        }
      });
    });
    
    // Sort by confidence
    suggestions.sort((a, b) => b.confidence - a.confidence);
    
    // Add positional suggestions for remaining columns
    const maxPositional = Math.min(
      unmappedSources.length - suggestions.filter(s => unmappedSources.includes(s.source)).length,
      unmappedTargets.length - suggestions.filter(s => unmappedTargets.includes(s.target)).length
    );
    
    for (let i = 0; i < maxPositional; i++) {
      const sourceIndex = i;
      const targetIndex = i;
      
      if (sourceIndex < unmappedSources.length && targetIndex < unmappedTargets.length) {
        const source = unmappedSources[sourceIndex];
        const target = unmappedTargets[targetIndex];
        
        // Check if this pair is already in suggestions
        if (!suggestions.some(s => s.source === source && s.target === target)) {
          suggestions.push({
            source,
            target,
            confidence: 0.5, // Medium confidence for positional
            reason: 'Positional mapping'
          });
        }
      }
    }
    
    return suggestions;
  }

  private calculateNameSimilarity(name1: string, name2: string): number {
    const lower1 = name1.toLowerCase();
    const lower2 = name2.toLowerCase();
    
    // Exact match
    if (lower1 === lower2) return 1.0;
    
    // Contains match
    if (lower1.includes(lower2) || lower2.includes(lower1)) return 0.8;
    
    // Common patterns
    const patterns = [
      { pattern: /id$/i, weight: 0.9 },
      { pattern: /name$/i, weight: 0.9 },
      { pattern: /date$/i, weight: 0.9 },
      { pattern: /time$/i, weight: 0.9 },
      { pattern: /amount$/i, weight: 0.9 },
      { pattern: /total$/i, weight: 0.9 },
      { pattern: /price$/i, weight: 0.9 },
      { pattern: /cost$/i, weight: 0.9 },
      { pattern: /code$/i, weight: 0.8 },
      { pattern: /num$/i, weight: 0.8 },
      { pattern: /desc$/i, weight: 0.8 },
      { pattern: /addr$/i, weight: 0.8 }
    ];
    
    for (const pattern of patterns) {
      if (pattern.pattern.test(lower1) && pattern.pattern.test(lower2)) {
        return pattern.weight;
      }
    }
    
    // Levenshtein distance for other cases
    const distance = this.levenshteinDistance(lower1, lower2);
    const maxLength = Math.max(lower1.length, lower2.length);
    return 1 - (distance / maxLength);
  }

  private levenshteinDistance(s1: string, s2: string): number {
    const m = s1.length;
    const n = s2.length;
    const dp: number[][] = Array(m + 1).fill(0).map(() => Array(n + 1).fill(0));
    
    for (let i = 0; i <= m; i++) dp[i][0] = i;
    for (let j = 0; j <= n; j++) dp[0][j] = j;
    
    for (let i = 1; i <= m; i++) {
      for (let j = 1; j <= n; j++) {
        if (s1[i - 1] === s2[j - 1]) {
          dp[i][j] = dp[i - 1][j - 1];
        } else {
          dp[i][j] = Math.min(
            dp[i - 1][j] + 1, // deletion
            dp[i][j - 1] + 1, // insertion
            dp[i - 1][j - 1] + 1 // substitution
          );
        }
      }
    }
    
    return dp[m][n];
  }

  // ==================== NEW HELPER METHODS FOR FIXES ====================

  private findSourceColumn(wire: Wire, sourceTables: TableDefinition[]): ColumnDefinition | undefined {
    const sourceTable = sourceTables.find(t => t.id === wire.sourceTableId);
    return sourceTable?.columns.find(c => c.id === wire.sourceColumnId);
  }

  private replaceColumnPlaceholders(expression: string, sourceTables: TableDefinition[]): string {
    let result = expression;
    const allColumns = sourceTables.flatMap(t => t.columns.map(c => c.name));
    for (const col of allColumns) {
      const placeholder = new RegExp(`\\{${col}\\}`, 'g');
      result = result.replace(placeholder, this.sanitizeIdentifier(col));
    }
    return result;
  }

  // ==================== SANITIZATION ====================

  private sanitizeQualifiedIdentifier(qualifiedName: string): string {
    const parts = qualifiedName.split('.');
    if (parts.length === 1) {
      // Unqualified column name
      return this.sanitizeIdentifier(parts[0]);
    }
    // Qualified: table.column (or more parts)
    const tablePart = parts[0];
    const columnPart = parts.slice(1).join('.'); // handle multiple dots (rare)
    // Table part is likely a CTE name – already safe, but we sanitize for consistency
    // Column part must be sanitized
    return `${this.sanitizeIdentifier(tablePart)}.${this.sanitizeIdentifier(columnPart)}`;
  }

private isExpression(value: string): boolean {
  // A simple column identifier may be:
  // - unqualified: [a-zA-Z_][a-zA-Z0-9_]*
  // - qualified:   table.[a-zA-Z_][a-zA-Z0-9_]*
  // Anything else (operators, spaces, functions, literals) is an expression.
  const simpleIdentifierPattern = /^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$/;
  return !simpleIdentifierPattern.test(value);
}

}