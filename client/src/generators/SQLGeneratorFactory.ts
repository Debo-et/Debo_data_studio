// src/generators/SQLGeneratorFactory.ts

import { BaseSQLGenerator } from './BaseSQLGenerator';
import { NodeType } from '../types/pipeline-types';
import { MapSQLGenerator } from './MapSQLGenerator';
import { FilterSQLGenerator } from './FilterSQLGenerator';
import { AggregateSQLGenerator } from './AggregateSQLGenerator';
import { InputSQLGenerator } from './InputSQLGenerator';
import { OutputSQLGenerator } from './OutputSQLGenerator';

// Dedicated generator imports
import { JoinSQLGenerator } from './JoinSQLGenerator';
import { SortSQLGenerator } from './SortSQLGenerator';
import { ConvertTypeSQLGenerator } from './ConvertTypeSQLGenerator';
import { ReplaceSQLGenerator } from './ReplaceSQLGenerator';
import { ExtractDelimitedSQLGenerator } from './ExtractDelimitedSQLGenerator';
import { ExtractJSONSQLGenerator } from './ExtractJSONSQLGenerator';
import { ExtractXMLSQLGenerator } from './ExtractXMLSQLGenerator';
import { NormalizeNumberSQLGenerator } from './NormalizeNumberSQLGenerator';
import {
  ReplicateSQLGenerator,
  UniteSQLGenerator,
  FlowToIterateSQLGenerator,
  IterateToFlowSQLGenerator,
  FlowMergeSQLGenerator,
  FlowMeterSQLGenerator,
  FlowMeterCatcherSQLGenerator,
} from './FlowControlSQLGenerator';
import { UniqueRowSQLGenerator } from './UniqueRowSQLGenerator';
import { SplitRowSQLGenerator } from './SplitRowSQLGenerator';
import { PivotSQLGenerator } from './PivotSQLGenerator';
import { DenormalizeSQLGenerator } from './DenormalizeSQLGenerator';
import { ExtractRegexSQLGenerator } from './ExtractRegexSQLGenerator';
import { ParseRecordSetSQLGenerator } from './ParseRecordSetSQLGenerator';
import { SampleRowSQLGenerator } from './SampleRowSQLGenerator';
import { DataMaskingSQLGenerator } from './DataMaskingSQLGenerator';
import { RowGeneratorSQLGenerator } from './RowGeneratorSQLGenerator';
import { LookupSQLGenerator } from './LookupSQLGenerator';
import { CacheInSQLGenerator, CacheOutSQLGenerator } from './CacheSQLGenerator';
import { SchemaComplianceSQLGenerator } from './SchemaComplianceSQLGenerator';
import { AssertSQLGenerator } from './AssertSQLGenerator';
import { AddCRCSQLGenerator } from './AddCRCSQLGenerator';
import { FileLookupSQLGenerator } from './FileLookupSQLGenerator';
import { MatchGroupSQLGenerator } from './MatchGroupSQLGenerator';
import { RecordMatchingSQLGenerator } from './RecordMatchingSQLGenerator';
import { StandardizeRowSQLGenerator } from './StandardizeRowSQLGenerator';
import { UnpivotSQLGenerator } from './UnpivotSQLGenerator';

import { globalLogger } from '../utils/Logger';

// ----------------------------------------------------------------------
// Inline fallback generators (no dedicated file yet)
// ----------------------------------------------------------------------

import { SQLGenerationContext, GeneratedSQLFragment } from './BaseSQLGenerator';

// Replace List (CASE based mapping)
// Replace List (CASE based mapping and REGEXP_REPLACE chain)
class ReplaceListSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel, upstreamSchema } = context;
    const indent = '  '.repeat(indentLevel);
    const config = node.metadata?.replaceListConfig || {};
    const column = config.column;
    const pairs = config.pairs || [];
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    const allColumns = upstreamSchema || [];

    const selectParts = allColumns.map(col => {
      if (col.name === column) {
        // Check if any pair is a regex replacement
        const hasRegex = pairs.some((p: any) => p.regex === true);

        if (hasRegex) {
          // Build a chain of REGEXP_REPLACE calls (regex pairs only)
          let expr = this.sanitizeIdentifier(column);
          for (const pair of pairs) {
            if (pair.regex) {
              const search = this.escapeString(pair.search);
              const replace = this.escapeString(pair.replace);
              expr = `REGEXP_REPLACE(${expr}, '${search}', '${replace}', 'g')`;
            }
            // Non‑regex pairs in a regex context are ignored – they could be added as REPLACE() if needed.
          }
          return `${expr} AS ${this.sanitizeIdentifier(column)}`;
        } else {
          // Existing behaviour for exact‑value mappings (CASE expression)
          const cases = pairs.map((p: any) => {
            const search = this.escapeString(p.search);
            const replace = this.escapeString(p.replace);
            return `WHEN '${search}' THEN '${replace}'`;
          }).join(' ');
          const caseExpr = `CASE ${this.sanitizeIdentifier(column)} ${cases} ELSE ${this.sanitizeIdentifier(column)} END`;
          return `${caseExpr} AS ${this.sanitizeIdentifier(column)}`;
        }
      }
      return this.sanitizeIdentifier(col.name);
    });

    const sql = `${indent}SELECT\n${indent}  ${selectParts.join(`,\n${indent}  `)}\n${indent}FROM ${sourceTable}`;
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'replaceList', lineCount: sql.split('\n').length },
    };
  }
}

// Normalize (numeric string with separators)
class NormalizeSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel, upstreamSchema } = context;
    const indent = '  '.repeat(indentLevel);
    const config = node.metadata?.normalizeConfig || {};
    const sourceColumn = config.sourceColumn;
    const decimalSep = this.escapeString(config.decimalSeparator || '.');
    const groupingSep = this.escapeString(config.groupingSeparator || ',');
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';
    const allColumns = upstreamSchema || [];

    const selectParts = allColumns.map(col => {
      if (col.name === sourceColumn) {
        const expr = `REPLACE(REPLACE(${this.sanitizeIdentifier(col.name)}, '${groupingSep}', ''), '${decimalSep}', '.')::DECIMAL`;
        return `${expr} AS ${this.sanitizeIdentifier(col.name)}`;
      }
      return this.sanitizeIdentifier(col.name);
    });

    const sql = `${indent}SELECT\n${indent}  ${selectParts.join(`,\n${indent}  `)}\n${indent}FROM ${sourceTable}`;
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'normalize', lineCount: sql.split('\n').length },
    };
  }
}

// Filter Columns (column selection)
class FilterColumnsSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const included = node.metadata?.filterColumnsConfig?.includedColumns || [];
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';

    const columns = included.map((col: string) => this.sanitizeIdentifier(col)).join(', ');
    const sql = `${indent}SELECT ${columns} FROM ${sourceTable}`;
    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'filterColumns', lineCount: 1 },
    };
  }
}

// Conditional Split (branch filtering)
// src/generators/SQLGeneratorFactory.ts (excerpt – ConditionalSplitSQLGenerator)

class ConditionalSplitSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
  const { node, connection, indentLevel, activeOutputPort } = context;
  const indent = '  '.repeat(indentLevel);
  const config = node.metadata?.conditionalSplitConfig || {};
  const conditions: Array<{ condition: string; outputPort: string }> = config.conditions || [];

  // Determine the source table
  const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'source';

  let whereClause = '';

  // Check if the active output port is 'default' (else branch)
  if (activeOutputPort === 'default') {
    // Build negation of all explicit conditions
    const explicitConditions = conditions
      .filter(c => c.condition && c.condition.trim() !== '')
      .map(c => `(${c.condition})`);
    if (explicitConditions.length > 0) {
      const combined = explicitConditions.join(' OR ');
      whereClause = `NOT (${combined})`;
    }
  } else {
    // Find the condition matching the active output port
    const matchedCondition = conditions.find(c => c.outputPort === activeOutputPort)?.condition;
    if (matchedCondition) {
      whereClause = matchedCondition;
    }
  }

  let sql = `${indent}SELECT * FROM ${sourceTable}`;
  if (whereClause) {
    sql += `\n${indent}WHERE ${whereClause}`;
  }

  return {
    sql,
    dependencies: connection ? [connection.sourceNodeId] : [],
    parameters: new Map(),
    errors: [],
    warnings: [],
    metadata: {
      generatedAt: new Date().toISOString(),
      fragmentType: 'conditionalSplit',
      lineCount: sql.split('\n').length,
    },
  };
}
}
// Default fallback
class DefaultSQLGenerator extends BaseSQLGenerator {
  protected generateSelectStatement(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateJoinConditions(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateWhereClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateHavingClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateOrderByClause(): GeneratedSQLFragment { return this.emptyFragment(); }
  protected generateGroupByClause(): GeneratedSQLFragment { return this.emptyFragment(); }

  generateSQL(context: SQLGenerationContext): GeneratedSQLFragment {
    globalLogger.warn(`[DefaultSQLGenerator] No specific generator for node type ${context.node.type}, using fallback`);
    const { node, connection, indentLevel } = context;
    const indent = '  '.repeat(indentLevel);
    const sourceTable = connection ? this.sanitizeIdentifier(connection.sourceNodeId) : 'unknown_source';
    const sql = `${indent}SELECT * FROM ${sourceTable}`;

    return {
      sql,
      dependencies: connection ? [connection.sourceNodeId] : [],
      parameters: new Map(),
      errors: [],
      warnings: [`Using default SQL generator for node type ${node.type}`],
      metadata: { generatedAt: new Date().toISOString(), fragmentType: 'default', lineCount: 1 },
    };
  }
}

// ----------------------------------------------------------------------
// Factory
// ----------------------------------------------------------------------
export class SQLGeneratorFactory {
  static createGenerator(type: string, options?: any): BaseSQLGenerator | undefined {
    globalLogger.debug(`[SQLGeneratorFactory] Creating generator for type: ${type}`, { options });
    let generator: BaseSQLGenerator | undefined;

    switch (type) {
      case NodeType.MAP:
        generator = new MapSQLGenerator(options);
        break;
      case NodeType.FILTER_ROW:
        generator = new FilterSQLGenerator(options);
        break;
      case NodeType.JOIN:
        generator = new JoinSQLGenerator(options);
        break;
      case NodeType.AGGREGATE_ROW:
        generator = new AggregateSQLGenerator(options);
        break;
      case NodeType.SORT_ROW:
        generator = new SortSQLGenerator(options);
        break;
      case NodeType.CONVERT_TYPE:
        generator = new ConvertTypeSQLGenerator(options);
        break;
      case NodeType.REPLACE:
        generator = new ReplaceSQLGenerator(options);
        break;
      case NodeType.REPLACE_LIST:
        generator = new ReplaceListSQLGenerator(options);
        break;
      case NodeType.EXTRACT_DELIMITED_FIELDS:
        generator = new ExtractDelimitedSQLGenerator(options);
        break;
      case NodeType.EXTRACT_JSON_FIELDS:
        generator = new ExtractJSONSQLGenerator(options);
        break;
      case NodeType.EXTRACT_XML_FIELD:
        generator = new ExtractXMLSQLGenerator(options);
        break;
      case NodeType.NORMALIZE:
        generator = new NormalizeSQLGenerator(options);
        break;
      case NodeType.NORMALIZE_NUMBER:
        generator = new NormalizeNumberSQLGenerator(options);
        break;
      case NodeType.REPLICATE:
        generator = new ReplicateSQLGenerator(options);
        break;
      case NodeType.UNIQ_ROW:
        generator = new UniqueRowSQLGenerator(options);
        break;
      case NodeType.SPLIT_ROW:
        generator = new SplitRowSQLGenerator(options);
        break;
      case NodeType.PIVOT_TO_COLUMNS_DELIMITED:
        generator = new PivotSQLGenerator(options);
        break;
      case NodeType.DENORMALIZE:
        generator = new DenormalizeSQLGenerator(options);
        break;
      case NodeType.EXTRACT_REGEX_FIELDS:
        generator = new ExtractRegexSQLGenerator(options);
        break;
      case NodeType.PARSE_RECORD_SET:
        generator = new ParseRecordSetSQLGenerator(options);
        break;
      case NodeType.SAMPLE_ROW:
        generator = new SampleRowSQLGenerator(options);
        break;
      case NodeType.DATA_MASKING:
        generator = new DataMaskingSQLGenerator(options);
        break;
      case NodeType.ROW_GENERATOR:
        generator = new RowGeneratorSQLGenerator(options);
        break;
      case NodeType.LOOKUP:
        generator = new LookupSQLGenerator(options);
        break;
      case NodeType.CACHE_IN:
        generator = new CacheInSQLGenerator(options);
        break;
      case NodeType.CACHE_OUT:
        generator = new CacheOutSQLGenerator(options);
        break;
      case NodeType.SCHEMA_COMPLIANCE_CHECK:
        generator = new SchemaComplianceSQLGenerator(options);
        break;
      case NodeType.FILTER_COLUMNS:
        generator = new FilterColumnsSQLGenerator(options);
        break;
      case NodeType.CONDITIONAL_SPLIT:
        generator = new ConditionalSplitSQLGenerator(options);
        break;
      case NodeType.UNITE:
        generator = new UniteSQLGenerator(options);
        break;
      case NodeType.FLOW_TO_ITERATE:
        generator = new FlowToIterateSQLGenerator(options);
        break;
      case NodeType.ITERATE_TO_FLOW:
        generator = new IterateToFlowSQLGenerator(options);
        break;
      case NodeType.FLOW_MERGE:
        generator = new FlowMergeSQLGenerator(options);
        break;
      case NodeType.FLOW_METER:
        generator = new FlowMeterSQLGenerator(options);
        break;
      case NodeType.FLOW_METER_CATCHER:
        generator = new FlowMeterCatcherSQLGenerator(options);
        break;
      case NodeType.ASSERT:
        generator = new AssertSQLGenerator(options);
        break;
      case NodeType.ADD_CRC:
        generator = new AddCRCSQLGenerator(options);
        break;
      case NodeType.FILE_LOOKUP:
        generator = new FileLookupSQLGenerator(options);
        break;
      case NodeType.MATCH_GROUP:
        generator = new MatchGroupSQLGenerator(options);
        break;
      case NodeType.RECORD_MATCHING:
        generator = new RecordMatchingSQLGenerator(options);
        break;
      case NodeType.STANDARDIZE_ROW:
        generator = new StandardizeRowSQLGenerator(options);
        break;
      case NodeType.UNPIVOT:
        generator = new UnpivotSQLGenerator(options);
        break;
      case NodeType.INPUT:
        generator = new InputSQLGenerator(options);
        break;
      case NodeType.OUTPUT:
        generator = new OutputSQLGenerator(options);
        break;
      default:
        generator = new DefaultSQLGenerator(options);
        globalLogger.warn(`[SQLGeneratorFactory] No explicit generator for type ${type}, using DefaultSQLGenerator`);
    }

    globalLogger.debug(`[SQLGeneratorFactory] Created generator: ${generator?.constructor.name}`);
    return generator;
  }
}