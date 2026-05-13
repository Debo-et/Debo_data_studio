// src/utils/node-validators/index.ts

import {
  CanvasNode,
  NodeType,
  PortType,
  JoinConfig,
  FilterConfig,
  AggregationConfig,
  SortConfig,
  MatchGroupConfig,
  MapEditorConfig} from '../../types/pipeline-types';

// ==================== ERROR TYPES ====================

export enum ValidationSeverity {
  ERROR = 'error',
  WARNING = 'warning',
  INFO = 'info'
}

export interface ValidationIssue {
  code: string;
  message: string;
  severity: ValidationSeverity;
  field?: string;
  suggestion?: string;
  postgresHint?: string;
  sqlExample?: string;
}

export interface NodeValidationResult {
  isValid: boolean;
  nodeId: string;
  nodeType: NodeType;
  issues: ValidationIssue[];
  suggestions: string[];
  postgresCompatibility: {
    compatible: boolean;
    issues: string[];
    requiredExtensions: string[];
  };
  metadata: {
    validatedAt: string;
    validatorVersion: string;
  };
}

// ==================== BASE VALIDATOR ====================

export interface INodeValidator {
  readonly nodeType: NodeType;
  validate(node: CanvasNode): NodeValidationResult;
  getRequiredPorts(): { input: number; output: number };
  suggestFixes(issues: ValidationIssue[]): string[];
}

export abstract class BaseNodeValidator implements INodeValidator {
  abstract readonly nodeType: NodeType;

  validate(node: CanvasNode): NodeValidationResult {
    const issues: ValidationIssue[] = [];
    
    // Common validation for all nodes
    issues.push(...this.validatePorts(node));
    issues.push(...this.validateMetadata(node));
    issues.push(...this.validatePostgresCompatibility(node));
    
    // Node-specific validation
    issues.push(...this.validateSpecific(node));
    
    const isValid = !issues.some(issue => issue.severity === ValidationSeverity.ERROR);
    
    return {
      isValid,
      nodeId: node.id,
      nodeType: this.nodeType,
      issues,
      suggestions: this.suggestFixes(issues),
      postgresCompatibility: this.checkPostgresCompatibility(issues),
      metadata: {
        validatedAt: new Date().toISOString(),
        validatorVersion: '1.0.0'
      }
    };
  }

  abstract getRequiredPorts(): { input: number; output: number };
  abstract validateSpecific(node: CanvasNode): ValidationIssue[];

  protected validatePorts(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const required = this.getRequiredPorts();

    const inputPorts = node.connectionPorts?.filter(p => p.type === PortType.INPUT) || [];
    const outputPorts = node.connectionPorts?.filter(p => p.type === PortType.OUTPUT) || [];

    if (inputPorts.length < required.input) {
      issues.push({
        code: 'PORT_INPUT_MISSING',
        message: `Node requires ${required.input} input ports, found ${inputPorts.length}`,
        severity: ValidationSeverity.ERROR,
        suggestion: `Add ${required.input - inputPorts.length} input ports to the node`,
        postgresHint: 'Each input port corresponds to a data source in PostgreSQL queries'
      });
    }

    if (outputPorts.length < required.output) {
      issues.push({
        code: 'PORT_OUTPUT_MISSING',
        message: `Node requires ${required.output} output ports, found ${outputPorts.length}`,
        severity: ValidationSeverity.ERROR,
        suggestion: `Add ${required.output - outputPorts.length} output ports to the node`
      });
    }

    // Validate port configurations
    node.connectionPorts?.forEach((port, index) => {
      if (!port.dataType && port.type === PortType.OUTPUT) {
        issues.push({
          code: 'PORT_DATATYPE_MISSING',
          message: `Output port ${port.id} has no data type specified`,
          severity: ValidationSeverity.WARNING,
          field: `ports[${index}].dataType`,
          suggestion: 'Set a PostgreSQL data type for proper SQL generation',
          postgresHint: 'Explicit data types improve query optimization'
        });
      }
    });

    return issues;
  }

  protected validateMetadata(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];

    if (!node.metadata) {
      issues.push({
        code: 'METADATA_MISSING',
        message: 'Node has no metadata configuration',
        severity: ValidationSeverity.WARNING,
        suggestion: 'Configure node properties to enable SQL generation'
      });
      return issues;
    }

    // Validate common metadata fields
    if (node.metadata.description && node.metadata.description.length > 1000) {
      issues.push({
        code: 'DESCRIPTION_TOO_LONG',
        message: 'Description exceeds 1000 characters',
        severity: ValidationSeverity.WARNING,
        field: 'metadata.description',
        suggestion: 'Keep description under 1000 characters'
      });
    }

    return issues;
  }

  protected validatePostgresCompatibility(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];

    // Check for PostgreSQL reserved keywords in names
    const postgresReserved = [
      'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER',
      'TABLE', 'VIEW', 'INDEX', 'SEQUENCE', 'TRIGGER', 'FUNCTION', 'PROCEDURE',
      'WHERE', 'GROUP', 'ORDER', 'BY', 'HAVING', 'JOIN', 'LEFT', 'RIGHT',
      'INNER', 'OUTER', 'CROSS', 'UNION', 'INTERSECT', 'EXCEPT', 'DISTINCT'
    ];

    const nodeNameUpper = node.name.toUpperCase();
    if (postgresReserved.includes(nodeNameUpper)) {
      issues.push({
        code: 'POSTGRES_RESERVED_WORD',
        message: `Node name '${node.name}' is a PostgreSQL reserved keyword`,
        severity: ValidationSeverity.ERROR,
        field: 'name',
        suggestion: `Rename node to avoid conflicts (e.g., '${node.name}_node')`,
        postgresHint: 'Using reserved keywords requires quoting with double quotes',
        sqlExample: `-- Instead of: CREATE TABLE ${node.name} (...)\n-- Use: CREATE TABLE "${node.name}" (...)`
      });
    }

    return issues;
  }

  protected checkPostgresCompatibility(issues: ValidationIssue[]): {
    compatible: boolean;
    issues: string[];
    requiredExtensions: string[];
  } {
    const postgresIssues = issues
      .filter(issue => issue.severity === ValidationSeverity.ERROR)
      .map(issue => issue.message);

    // Determine required PostgreSQL extensions based on node type
    const requiredExtensions: string[] = [];

    if (this.nodeType === NodeType.JSON || this.nodeType === NodeType.JSONB) {
      requiredExtensions.push('pgcrypto');
    }

    return {
      compatible: postgresIssues.length === 0,
      issues: postgresIssues,
      requiredExtensions
    };
  }

  suggestFixes(issues: ValidationIssue[]): string[] {
    return issues
      .filter(issue => issue.suggestion)
      .map(issue => issue.suggestion!);
  }

  protected validateJoinCondition(condition: string): ValidationIssue[] {
    const issues: ValidationIssue[] = [];

    if (!condition.trim()) {
      issues.push({
        code: 'JOIN_CONDITION_EMPTY',
        message: 'Join condition is empty',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.joinConfig.condition',
        suggestion: 'Specify a join condition (e.g., "a.id = b.id")',
        sqlExample: '-- Example: ON users.id = orders.user_id'
      });
    }

    // Check for common join condition patterns
    const validPatterns = [
      /^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s*=\s*[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$/,
      /^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s*(?:=|!=|>|<|>=|<=)\s*[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$/,
      /^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s+IN\s+\(.*\)$/,
      /^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\s+BETWEEN\s+.*\s+AND\s+.*$/
    ];

    if (!validPatterns.some(pattern => pattern.test(condition))) {
      issues.push({
        code: 'JOIN_CONDITION_INVALID',
        message: 'Join condition may be malformed',
        severity: ValidationSeverity.WARNING,
        field: 'metadata.joinConfig.condition',
        suggestion: 'Use format: "table1.column = table2.column"',
        postgresHint: 'PostgreSQL supports complex join conditions with AND/OR'
      });
    }

    return issues;
  }

  protected validateAggregateFunction(func: string, column: string): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const validFunctions = ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'STRING_AGG'];

    if (!validFunctions.includes(func.toUpperCase())) {
      issues.push({
        code: 'AGGREGATE_FUNCTION_INVALID',
        message: `Invalid aggregate function: ${func}`,
        severity: ValidationSeverity.ERROR,
        field: 'metadata.aggregationConfig.aggregates',
        suggestion: `Use one of: ${validFunctions.join(', ')}`,
        sqlExample: `-- Example: SELECT ${func.toUpperCase()}(${column}) FROM table`
      });
    }

    return issues;
  }

  protected validateFilterExpression(expression: string): ValidationIssue[] {
    const issues: ValidationIssue[] = [];

    if (!expression.trim()) {
      issues.push({
        code: 'FILTER_EXPRESSION_EMPTY',
        message: 'Filter expression is empty',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.filterConfig.condition',
        suggestion: 'Provide a valid WHERE clause condition',
        sqlExample: '-- Example: age > 18 AND status = "active"'
      });
    }

    // Check for common SQL injection patterns
    const dangerousPatterns = [
      /DROP\s+TABLE/i,
      /DELETE\s+FROM/i,
      /UPDATE\s+.+\s+SET/i,
      /INSERT\s+INTO/i,
      /;\s*--/,
      /UNION\s+SELECT/i,
      /EXEC\s*\(/i,
      /xp_cmdshell/i
    ];

    if (dangerousPatterns.some(pattern => pattern.test(expression))) {
      issues.push({
        code: 'FILTER_EXPRESSION_DANGEROUS',
        message: 'Filter expression contains potentially dangerous SQL',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.filterConfig.condition',
        suggestion: 'Use parameterized queries or safe expressions only',
        postgresHint: 'Consider using prepared statements with $1, $2 placeholders'
      });
    }

    return issues;
  }
}

// ==================== VALIDATOR DECORATOR ====================

const validatorRegistry = new Map<NodeType, new () => BaseNodeValidator>();

export function RegisterValidator(nodeType: NodeType) {
  return function <T extends new () => BaseNodeValidator>(constructor: T) {
    validatorRegistry.set(nodeType, constructor);
    return constructor;
  };
}

// ==================== SPECIALIZED VALIDATORS ====================

@RegisterValidator(NodeType.JOIN)
export class JoinNodeValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.JOIN;

  getRequiredPorts() {
    return { input: 2, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const config = node.metadata?.joinConfig as JoinConfig | undefined;

    if (!config) {
      issues.push({
        code: 'JOIN_CONFIG_MISSING',
        message: 'Join configuration is missing',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.joinConfig',
        suggestion: 'Configure join type and condition'
      });
      return issues;
    }

    // Validate join type
    const validJoinTypes = ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'];
    if (!validJoinTypes.includes(config.type)) {
      issues.push({
        code: 'JOIN_TYPE_INVALID',
        message: `Invalid join type: ${config.type}`,
        severity: ValidationSeverity.ERROR,
        field: 'metadata.joinConfig.type',
        suggestion: `Use one of: ${validJoinTypes.join(', ')}`,
        sqlExample: `-- Example: SELECT * FROM table1 ${config.type} JOIN table2 ON condition`
      });
    }

    // Validate join condition
    if (config.type !== 'CROSS') {
      issues.push(...this.validateJoinCondition(config.condition));
    }

    // Validate table aliases
    if (config.leftAlias && !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.leftAlias)) {
      issues.push({
        code: 'ALIAS_INVALID',
        message: `Invalid left table alias: ${config.leftAlias}`,
        severity: ValidationSeverity.WARNING,
        field: 'metadata.joinConfig.leftAlias',
        suggestion: 'Use alphanumeric characters and underscores only',
        postgresHint: 'Table aliases in PostgreSQL must follow identifier rules'
      });
    }

    if (config.rightAlias && !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.rightAlias)) {
      issues.push({
        code: 'ALIAS_INVALID',
        message: `Invalid right table alias: ${config.rightAlias}`,
        severity: ValidationSeverity.WARNING,
        field: 'metadata.joinConfig.rightAlias',
        suggestion: 'Use alphanumeric characters and underscores only'
      });
    }

    return issues;
  }
}

@RegisterValidator(NodeType.MAP)
export class MapNodeValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.MAP;

  getRequiredPorts() {
    return { input: 1, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const config = node.metadata?.mapEditorConfig as MapEditorConfig | undefined;

    if (!config) {
      issues.push({
        code: 'MAP_CONFIG_MISSING',
        message: 'Map configuration is missing',
        severity: ValidationSeverity.WARNING,
        suggestion: 'Double-click the node to configure mappings'
      });
      return issues;
    }

    // Validate source tables
    if (!config.sourceTables || config.sourceTables.length === 0) {
      issues.push({
        code: 'MAP_SOURCES_MISSING',
        message: 'No source tables configured',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.mapEditorConfig.sourceTables',
        suggestion: 'Add at least one source table for mapping'
      });
    }

    // Validate target tables
    if (!config.targetTables || config.targetTables.length === 0) {
      issues.push({
        code: 'MAP_TARGETS_MISSING',
        message: 'No target tables configured',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.mapEditorConfig.targetTables',
        suggestion: 'Add at least one target table for mapping'
      });
    }

    // Validate column mappings
    if (!config.columnMappings || config.columnMappings.length === 0) {
      issues.push({
        code: 'MAP_MAPPINGS_MISSING',
        message: 'No column mappings configured',
        severity: ValidationSeverity.WARNING,
        suggestion: 'Create mappings between source and target columns'
      });
    } else {
      config.columnMappings.forEach((mapping, index) => {
        if (!mapping.sourceColumn || !mapping.targetColumn) {
          issues.push({
            code: 'MAPPING_INCOMPLETE',
            message: `Mapping ${index + 1} has missing source or target column`,
            severity: ValidationSeverity.ERROR,
            field: `metadata.mapEditorConfig.columnMappings[${index}]`,
            suggestion: 'Specify both source and target column names'
          });
        }

        // Validate transformation syntax
        if (mapping.transformation) {
          const transformationIssues = this.validateTransformation(mapping.transformation);
          issues.push(...transformationIssues.map(issue => ({
            ...issue,
            field: `metadata.mapEditorConfig.columnMappings[${index}].transformation`
          })));
        }
      });
    }

    return issues;
  }

  private validateTransformation(expression: string): ValidationIssue[] {
    const issues: ValidationIssue[] = [];

    // Check for common SQL functions
    const validFunctions = [
      'COALESCE', 'NULLIF', 'CAST', 'CONVERT', 'TRIM', 'UPPER', 'LOWER',
      'SUBSTRING', 'CONCAT', 'ROUND', 'FLOOR', 'CEILING', 'ABS', 'EXTRACT'
    ];

    // Simple validation - could be enhanced with a proper SQL parser
    const functionMatch = expression.match(/^[A-Z_]+\(/);
    if (functionMatch) {
      const funcName = functionMatch[0].slice(0, -1);
      if (!validFunctions.includes(funcName) && !funcName.startsWith('TO_')) {
        issues.push({
          code: 'TRANSFORMATION_FUNCTION_UNKNOWN',
          message: `Unknown function in transformation: ${funcName}`,
          severity: ValidationSeverity.WARNING,
          suggestion: `Use standard SQL functions like: ${validFunctions.slice(0, 5).join(', ')}`,
          sqlExample: `-- Example: COALESCE(${expression.replace(/^[A-Z_]+\(/, '').replace(/\)$/, '')}, 'default')`
        });
      }
    }

    return issues;
  }
}

@RegisterValidator(NodeType.AGGREGATE_ROW)
export class AggregateRowValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.AGGREGATE_ROW;

  getRequiredPorts() {
    return { input: 1, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const config = node.metadata?.aggregationConfig as AggregationConfig | undefined;

    if (!config) {
      issues.push({
        code: 'AGGREGATE_CONFIG_MISSING',
        message: 'Aggregation configuration is missing',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.aggregationConfig',
        suggestion: 'Configure GROUP BY columns and aggregate functions'
      });
      return issues;
    }

    // Validate GROUP BY columns
    if (!config.groupBy || config.groupBy.length === 0) {
      issues.push({
        code: 'GROUPBY_MISSING',
        message: 'No GROUP BY columns specified',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.aggregationConfig.groupBy',
        suggestion: 'Specify at least one column for grouping',
        postgresHint: 'PostgreSQL requires GROUP BY for aggregate functions unless using window functions'
      });
    }

    // Validate aggregate functions
    if (!config.aggregates || config.aggregates.length === 0) {
      issues.push({
        code: 'AGGREGATES_MISSING',
        message: 'No aggregate functions specified',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.aggregationConfig.aggregates',
        suggestion: 'Add at least one aggregate function (SUM, AVG, COUNT, etc.)'
      });
    } else {
      config.aggregates.forEach((agg, index) => {
        issues.push(...this.validateAggregateFunction(agg.function, agg.column));
        
        // Validate alias
        if (!agg.alias || !/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(agg.alias)) {
          issues.push({
            code: 'AGGREGATE_ALIAS_INVALID',
            message: `Invalid alias for aggregate: ${agg.alias || 'missing'}`,
            severity: ValidationSeverity.WARNING,
            field: `metadata.aggregationConfig.aggregates[${index}].alias`,
            suggestion: 'Use a valid SQL alias (alphanumeric, starting with letter)',
            sqlExample: `-- Example: SELECT ${agg.function}(${agg.column}) AS ${agg.alias || 'result'} FROM table`
          });
        }
      });
    }

    // Validate HAVING clause syntax if present
    if (config.having) {
      const havingIssues = this.validateFilterExpression(config.having);
      issues.push(...havingIssues.map(issue => ({
        ...issue,
        field: 'metadata.aggregationConfig.having'
      })));
    }

    return issues;
  }
}

@RegisterValidator(NodeType.FILTER_ROW)
export class FilterRowValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.FILTER_ROW;

  getRequiredPorts() {
    return { input: 1, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const config = node.metadata?.filterConfig as FilterConfig | undefined;

    if (!config) {
      issues.push({
        code: 'FILTER_CONFIG_MISSING',
        message: 'Filter configuration is missing',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.filterConfig',
        suggestion: 'Configure filter condition and operation'
      });
      return issues;
    }

    // Validate condition
    issues.push(...this.validateFilterExpression(config.condition));

    // Validate operation
    if (!['INCLUDE', 'EXCLUDE'].includes(config.operation)) {
      issues.push({
        code: 'FILTER_OPERATION_INVALID',
        message: `Invalid filter operation: ${config.operation}`,
        severity: ValidationSeverity.ERROR,
        field: 'metadata.filterConfig.operation',
        suggestion: 'Use either "INCLUDE" or "EXCLUDE"',
        sqlExample: `-- INCLUDE: WHERE ${config.condition}\n-- EXCLUDE: WHERE NOT (${config.condition})`
      });
    }

    // Validate parameters for prepared statements
    if (config.parameters) {
      Object.entries(config.parameters).forEach(([key, value]) => {
        if (typeof value === 'string' && value.includes(';')) {
          issues.push({
            code: 'PARAMETER_UNSAFE',
            message: `Parameter ${key} contains semicolon`,
            severity: ValidationSeverity.WARNING,
            field: `metadata.filterConfig.parameters.${key}`,
            suggestion: 'Avoid special characters in parameter values',
            postgresHint: 'Use parameterized queries to prevent SQL injection'
          });
        }
      });
    }

    return issues;
  }
}

@RegisterValidator(NodeType.SORT_ROW)
export class SortRowValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.SORT_ROW;

  getRequiredPorts() {
    return { input: 1, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const config = node.metadata?.sortConfig as SortConfig | undefined;

    if (!config) {
      issues.push({
        code: 'SORT_CONFIG_MISSING',
        message: 'Sort configuration is missing',
        severity: ValidationSeverity.WARNING,
        suggestion: 'Configure sort columns and direction'
      });
      return issues;
    }

    // Validate sort columns
    if (!config.columns || config.columns.length === 0) {
      issues.push({
        code: 'SORT_COLUMNS_MISSING',
        message: 'No sort columns specified',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.sortConfig.columns',
        suggestion: 'Specify at least one column to sort by'
      });
    } else {
      config.columns.forEach((col, index) => {
        if (!col.column) {
          issues.push({
            code: 'SORT_COLUMN_EMPTY',
            message: `Sort column ${index + 1} has no column name`,
            severity: ValidationSeverity.ERROR,
            field: `metadata.sortConfig.columns[${index}].column`,
            suggestion: 'Specify a column name to sort by'
          });
        }

        if (col.direction && !['ASC', 'DESC'].includes(col.direction)) {
          issues.push({
            code: 'SORT_DIRECTION_INVALID',
            message: `Invalid sort direction: ${col.direction}`,
            severity: ValidationSeverity.ERROR,
            field: `metadata.sortConfig.columns[${index}].direction`,
            suggestion: 'Use either "ASC" (ascending) or "DESC" (descending)',
            sqlExample: `-- Example: ORDER BY ${col.column} ${col.direction || 'ASC'}`
          });
        }
      });
    }

    // Validate LIMIT and OFFSET
    if (config.limit !== undefined && config.limit < 0) {
      issues.push({
        code: 'LIMIT_INVALID',
        message: `Invalid LIMIT value: ${config.limit}`,
        severity: ValidationSeverity.ERROR,
        field: 'metadata.sortConfig.limit',
        suggestion: 'LIMIT must be a positive integer or zero',
        postgresHint: 'Use LIMIT 0 for no rows, or NULL for unlimited'
      });
    }

    if (config.offset !== undefined && config.offset < 0) {
      issues.push({
        code: 'OFFSET_INVALID',
        message: `Invalid OFFSET value: ${config.offset}`,
        severity: ValidationSeverity.ERROR,
        field: 'metadata.sortConfig.offset',
        suggestion: 'OFFSET must be a non-negative integer'
      });
    }

    return issues;
  }
}

@RegisterValidator(NodeType.MATCH_GROUP)
export class MatchGroupValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.MATCH_GROUP;

  getRequiredPorts() {
    return { input: 1, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    const config = node.metadata?.matchGroupConfig as MatchGroupConfig | undefined;

    if (!config) {
      issues.push({
        code: 'MATCHGROUP_CONFIG_MISSING',
        message: 'Match Group configuration is missing',
        severity: ValidationSeverity.WARNING,
        suggestion: 'Double-click the node to configure match group wizard'
      });
      return issues;
    }

    // Validate input flow
    if (!config.inputFlow) {
      issues.push({
        code: 'INPUT_FLOW_MISSING',
        message: 'Input flow not specified',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.matchGroupConfig.inputFlow',
        suggestion: 'Select an input flow for match processing'
      });
    }

    // Validate schema columns
    if (!config.schemaColumns || config.schemaColumns.length === 0) {
      issues.push({
        code: 'SCHEMA_COLUMNS_MISSING',
        message: 'No schema columns defined',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.matchGroupConfig.schemaColumns',
        suggestion: 'Define columns for match processing'
      });
    }

    // Validate grouping keys
    if (!config.groupingKeys || config.groupingKeys.length === 0) {
      issues.push({
        code: 'GROUPING_KEYS_MISSING',
        message: 'No grouping keys specified',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.matchGroupConfig.groupingKeys',
        suggestion: 'Specify at least one column for grouping records',
        postgresHint: 'GROUP BY clause is required for match group operations'
      });
    } else {
      // Verify grouping keys exist in schema columns
      config.groupingKeys.forEach(key => {
        if (!config.schemaColumns.some(col => col.name === key)) {
          issues.push({
            code: 'GROUPING_KEY_NOT_FOUND',
            message: `Grouping key '${key}' not found in schema columns`,
            severity: ValidationSeverity.ERROR,
            field: 'metadata.matchGroupConfig.groupingKeys',
            suggestion: `Add '${key}' to schema columns or choose existing column`
          });
        }
      });
    }

    // Validate survivorship rules
    if (!config.survivorshipRules || config.survivorshipRules.length === 0) {
      issues.push({
        code: 'SURVIVORSHIP_RULES_MISSING',
        message: 'No survivorship rules defined',
        severity: ValidationSeverity.WARNING,
        suggestion: 'Define rules for selecting surviving records'
      });
    } else {
      config.survivorshipRules.forEach((rule, index) => {
        const validRules = ['MIN', 'MAX', 'FIRST', 'LAST', 'CONCAT', 'SUM', 'AVG'];
        if (!validRules.includes(rule.rule)) {
          issues.push({
            code: 'SURVIVORSHIP_RULE_INVALID',
            message: `Invalid survivorship rule: ${rule.rule}`,
            severity: ValidationSeverity.ERROR,
            field: `metadata.matchGroupConfig.survivorshipRules[${index}].rule`,
            suggestion: `Use one of: ${validRules.join(', ')}`,
            sqlExample: `-- Example: SELECT ${rule.rule}(${rule.column}) FROM ...`
          });
        }

        // Verify column exists
        if (!config.schemaColumns.some(col => col.name === rule.column)) {
          issues.push({
            code: 'SURVIVORSHIP_COLUMN_NOT_FOUND',
            message: `Survivorship column '${rule.column}' not found in schema`,
            severity: ValidationSeverity.ERROR,
            field: `metadata.matchGroupConfig.survivorshipRules[${index}].column`,
            suggestion: `Add '${rule.column}' to schema columns or choose existing column`
          });
        }
      });
    }

    // Validate output table name
    if (!config.outputTableName) {
      issues.push({
        code: 'OUTPUT_TABLE_MISSING',
        message: 'Output table name is required',
        severity: ValidationSeverity.ERROR,
        field: 'metadata.matchGroupConfig.outputTableName',
        suggestion: 'Specify a name for the output table'
      });
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.outputTableName)) {
      issues.push({
        code: 'OUTPUT_TABLE_INVALID',
        message: `Invalid output table name: ${config.outputTableName}`,
        severity: ValidationSeverity.ERROR,
        field: 'metadata.matchGroupConfig.outputTableName',
        suggestion: 'Use alphanumeric characters and underscores only',
        postgresHint: 'Table names must start with a letter or underscore'
      });
    }

    // Validate deduplication strategy
    if (!['KEEP_FIRST', 'KEEP_LAST', 'KEEP_ALL', 'MERGE'].includes(config.deduplication)) {
      issues.push({
        code: 'DEDUPLICATION_INVALID',
        message: `Invalid deduplication strategy: ${config.deduplication}`,
        severity: ValidationSeverity.WARNING,
        field: 'metadata.matchGroupConfig.deduplication',
        suggestion: 'Use KEEP_FIRST, KEEP_LAST, KEEP_ALL, or MERGE',
        sqlExample: '-- KEEP_FIRST: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1'
      });
    }

    return issues;
  }
}

// ==================== ADDITIONAL VALIDATORS ====================

@RegisterValidator(NodeType.DENORMALIZE)
export class DenormalizeValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.DENORMALIZE;
  getRequiredPorts() { return { input: 1, output: 1 }; }
  validateSpecific(_node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    // Validate denormalization key and columns
    return issues;
  }
}

@RegisterValidator(NodeType.NORMALIZE)
export class NormalizeValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.NORMALIZE;
  getRequiredPorts() { return { input: 1, output: 1 }; }
  validateSpecific(_node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    // Validate normalization rules
    return issues;
  }
}

@RegisterValidator(NodeType.REPLACE)
export class ReplaceValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.REPLACE;
  getRequiredPorts() { return { input: 1, output: 1 }; }
  validateSpecific(_node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    // Validate find/replace patterns
    return issues;
  }
}

@RegisterValidator(NodeType.CONVERT_TYPE)
export class ConvertTypeValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.CONVERT_TYPE;
  getRequiredPorts() { return { input: 1, output: 1 }; }
  validateSpecific(_node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    // Validate type conversion rules
    return issues;
  }
}

@RegisterValidator(NodeType.FILTER_COLUMNS)
export class FilterColumnsValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.FILTER_COLUMNS;
  getRequiredPorts() { return { input: 1, output: 1 }; }
  validateSpecific(_node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];
    // Validate column selection list
    return issues;
  }
}

// ==================== FACTORY CLASS ====================

export class NodeValidatorFactory {
  private static instance: NodeValidatorFactory;
  private validators = new Map<NodeType, INodeValidator>();
  private defaultValidator: INodeValidator;

  private constructor() {
    // Auto-discover and register validators
    this.autoDiscoverValidators();
    this.defaultValidator = new DefaultNodeValidator();
  }

  static getInstance(): NodeValidatorFactory {
    if (!NodeValidatorFactory.instance) {
      NodeValidatorFactory.instance = new NodeValidatorFactory();
    }
    return NodeValidatorFactory.instance;
  }

  private autoDiscoverValidators(): void {
    // In a real implementation, this would use reflection or module loading
    // For now, we'll manually register the ones we've defined above
    const validatorClasses = [
      JoinNodeValidator,
      MapNodeValidator,
      AggregateRowValidator,
      FilterRowValidator,
      SortRowValidator,
      MatchGroupValidator,
      DenormalizeValidator,
      NormalizeValidator,
      ReplaceValidator,
      ConvertTypeValidator,
      FilterColumnsValidator
    ];

    validatorClasses.forEach(ValidatorClass => {
      const instance = new ValidatorClass();
      this.validators.set(instance.nodeType, instance);
    });
  }

  getValidator(nodeType: NodeType): INodeValidator {
    // Return cached validator or default
    const validator = this.validators.get(nodeType);
    if (validator) {
      return validator;
    }

    console.warn(`No specialized validator found for node type: ${nodeType}, using default`);
    return this.defaultValidator;
  }

  validateNode(node: CanvasNode): NodeValidationResult {
    const validator = this.getValidator(node.type as NodeType);
    return validator.validate(node);
  }

  validateNodes(nodes: CanvasNode[]): Map<string, NodeValidationResult> {
    const results = new Map<string, NodeValidationResult>();
    
    nodes.forEach(node => {
      const result = this.validateNode(node);
      results.set(node.id, result);
    });

    return results;
  }

  getValidationSummary(results: Map<string, NodeValidationResult>): {
    total: number;
    valid: number;
    warnings: number;
    errors: number;
    postgresCompatible: number;
    bySeverity: Record<ValidationSeverity, number>;
  } {
    let valid = 0;
    let warnings = 0;
    let errors = 0;
    let postgresCompatible = 0;

    const bySeverity: Record<ValidationSeverity, number> = {
      [ValidationSeverity.ERROR]: 0,
      [ValidationSeverity.WARNING]: 0,
      [ValidationSeverity.INFO]: 0
    };

    results.forEach(result => {
      if (result.isValid) valid++;
      if (result.postgresCompatibility.compatible) postgresCompatible++;
      
      result.issues.forEach(issue => {
        bySeverity[issue.severity]++;
      });

      if (result.issues.some(i => i.severity === ValidationSeverity.ERROR)) errors++;
      if (result.issues.some(i => i.severity === ValidationSeverity.WARNING)) warnings++;
    });

    return {
      total: results.size,
      valid,
      warnings,
      errors,
      postgresCompatible,
      bySeverity
    };
  }

  registerValidator(nodeType: NodeType, validator: INodeValidator): void {
    this.validators.set(nodeType, validator);
  }

  getRegisteredValidators(): NodeType[] {
    return Array.from(this.validators.keys());
  }
}

// ==================== DEFAULT VALIDATOR ====================

class DefaultNodeValidator extends BaseNodeValidator {
  readonly nodeType = NodeType.UNKNOWN;

  getRequiredPorts() {
    return { input: 1, output: 1 };
  }

  validateSpecific(node: CanvasNode): ValidationIssue[] {
    const issues: ValidationIssue[] = [];

    // Generic validation for unknown node types
    issues.push({
      code: 'NODE_TYPE_UNKNOWN',
      message: `No specialized validator for node type: ${node.type}`,
      severity: ValidationSeverity.WARNING,
      suggestion: 'Consider using a standard node type or contact support'
    });

    // Check for basic configuration
    if (!node.name || node.name.trim() === '') {
      issues.push({
        code: 'NODE_NAME_MISSING',
        message: 'Node has no name',
        severity: ValidationSeverity.ERROR,
        field: 'name',
        suggestion: 'Provide a descriptive name for the node'
      });
    }

    // Check for reasonable position
    if (node.position.x < 0 || node.position.y < 0) {
      issues.push({
        code: 'NODE_POSITION_INVALID',
        message: 'Node position is negative',
        severity: ValidationSeverity.WARNING,
        field: 'position',
        suggestion: 'Position nodes within visible canvas area'
      });
    }

    return issues;
  }
}

// ==================== UTILITY FUNCTIONS ====================

export function validateNodeWithSuggestions(node: CanvasNode): {
  result: NodeValidationResult;
  fixes: string[];
  sqlReady: boolean;
} {
  const factory = NodeValidatorFactory.getInstance();
  const result = factory.validateNode(node);
  
  const fixes = result.suggestions;
  const sqlReady = result.isValid && result.postgresCompatibility.compatible;

  return { result, fixes, sqlReady };
}

export function batchValidatePipeline(nodes: CanvasNode[]): {
  results: Map<string, NodeValidationResult>;
  summary: ReturnType<NodeValidatorFactory['getValidationSummary']>;
  pipelineReady: boolean;
  blockingIssues: ValidationIssue[];
} {
  const factory = NodeValidatorFactory.getInstance();
  const results = factory.validateNodes(nodes);
  const summary = factory.getValidationSummary(results);
  
  const blockingIssues: ValidationIssue[] = [];
  results.forEach(result => {
    if (!result.isValid) {
      blockingIssues.push(...result.issues.filter(i => i.severity === ValidationSeverity.ERROR));
    }
  });

  const pipelineReady = summary.valid === summary.total && summary.postgresCompatible === summary.total;

  return { results, summary, pipelineReady, blockingIssues };
}