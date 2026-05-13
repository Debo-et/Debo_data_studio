// src/services/sql-generator.service.ts
import { Node, Edge } from 'reactflow';
import { FlowNodeMeta } from '../types/metadata';
import { databaseApi } from './database-api.service';

export interface SQLGenerationResult {
  success: boolean;
  sql: string;
  messages: string[];
  warnings: string[];
  errors: string[];
  scriptPath?: string;
  executionPlan?: ExecutionPlan;
}

export interface ExecutionPlan {
  steps: ExecutionStep[];
  totalNodes: number;
  totalConnections: number;
  estimatedRows: number;
}

export interface ExecutionStep {
  step: number;
  nodeId: string;
  nodeType: string;
  nodeName: string;
  sqlFragment: string;
  description: string;
}

export class SQLGeneratorService {
  private visitedNodes: Set<string> = new Set();
  private sqlFragments: Map<string, string> = new Map();
  private nodeAliases: Map<string, string> = new Map();
  private stepCounter: number = 1;

  /**
   * Generate PostgreSQL SQL from canvas graph
   */
  public generateSQLFromGraph(
    nodes: Node[],
    edges: Edge[],
    connectionId?: string
  ): SQLGenerationResult {
    this.visitedNodes.clear();
    this.sqlFragments.clear();
    this.nodeAliases.clear();
    this.stepCounter = 1;

    const result: SQLGenerationResult = {
      success: false,
      sql: '',
      messages: [],
      warnings: [],
      errors: []
    };

    try {
      // 1. Find start nodes (input sources)
      const inputNodes = this.findInputNodes(nodes);
      
      if (inputNodes.length === 0) {
        result.errors.push('No input data source found. Add at least one input component.');
        return result;
      }

      // 2. Find output nodes
      const outputNodes = this.findOutputNodes(nodes);
      
      if (outputNodes.length === 0) {
        result.errors.push('No output destination found. Add at least one output component.');
        return result;
      }

      // 3. Build execution plan
      const executionPlan = this.buildExecutionPlan(inputNodes, outputNodes, nodes, edges);
      result.executionPlan = executionPlan;

      // 4. Generate SQL for each path
      const sqlScripts: string[] = [];
      
      for (const inputNode of inputNodes) {
        const sql = this.generatePathSQL(inputNode, nodes, edges, connectionId);
        if (sql) {
          sqlScripts.push(sql);
        }
      }

      if (sqlScripts.length === 0) {
        result.errors.push('Failed to generate SQL from any input paths.');
        return result;
      }

      // 5. Combine SQL scripts
      result.sql = this.combineSQLScripts(sqlScripts);
      result.messages.push(`Generated SQL script with ${executionPlan.steps.length} steps`);
      result.messages.push(`Input sources: ${inputNodes.length}, Output destinations: ${outputNodes.length}`);
      
      // 6. Validate SQL syntax (basic)
      const validation = this.validateSQLSyntax(result.sql);
      if (!validation.valid) {
        result.warnings.push(...validation.warnings);
      }

      result.success = true;
      return result;

    } catch (error: any) {
      result.errors.push(`SQL generation failed: ${error.message}`);
      console.error('SQL generation error:', error);
      return result;
    }
  }

  /**
   * Execute generated SQL against PostgreSQL
   */
  public async executeSQL(
    sql: string,
    connectionId: string
  ): Promise<{
    success: boolean;
    results?: any[];
    rowCount?: number;
    executionTime?: number;
    messages: string[];
    errors: string[];
  }> {
    const startTime = Date.now();
    
    try {
      console.log('🚀 Executing generated SQL:', sql.substring(0, 200) + '...');

      // Split SQL into individual statements
      const statements = this.splitSQLStatements(sql);
      
      const results: any[] = [];
      let totalRowsAffected = 0;

      for (const statement of statements) {
        if (!statement.trim()) continue;

        const result = await databaseApi.executeQuery(connectionId, statement);
        
        if (result.success && result.result) {
          results.push(result.result);
          totalRowsAffected += result.result.rowCount || 0;
        } else {
          return {
            success: false,
            messages: [],
            errors: [`Failed to execute statement: ${result.error}`]
          };
        }
      }

      const executionTime = Date.now() - startTime;

      return {
        success: true,
        results,
        rowCount: totalRowsAffected,
        executionTime,
        messages: [
          `✅ SQL execution completed in ${executionTime}ms`,
          `📊 ${totalRowsAffected} total rows affected`,
          `📝 ${statements.length} statements executed`
        ],
        errors: []
      };

    } catch (error: any) {
      return {
        success: false,
        messages: [],
        errors: [`SQL execution failed: ${error.message}`]
      };
    }
  }

  /**
   * Find all input nodes in the graph
   */
  private findInputNodes(nodes: Node[]): Node[] {
    return nodes.filter(node => {
      const data = node.data as FlowNodeMeta;
      return data.componentType === 'INPUT' && 
             data.configuration.type === 'INPUT' &&
             data.componentKey?.toLowerCase().includes('input');
    });
  }

  /**
   * Find all output nodes in the graph
   */
  private findOutputNodes(nodes: Node[]): Node[] {
    return nodes.filter(node => {
      const data = node.data as FlowNodeMeta;
      return data.componentType === 'OUTPUT' && 
             data.configuration.type === 'OUTPUT' &&
             data.componentKey?.toLowerCase().includes('output');
    });
  }

  /**
   * Build execution plan from graph
   */
  private buildExecutionPlan(
    inputNodes: Node[],
    outputNodes: Node[],
    allNodes: Node[],
    edges: Edge[]
  ): ExecutionPlan {
    const steps: ExecutionStep[] = [];
    let totalRowsEstimate = 1000; // Default estimate

    // For each input node, trace path to outputs
    for (const inputNode of inputNodes) {
      const pathSteps = this.tracePath(inputNode, outputNodes, allNodes, edges);
      steps.push(...pathSteps);
    }

    // Estimate total rows based on nodes
    totalRowsEstimate = this.estimateRowCount(allNodes);

    return {
      steps: steps.sort((a, b) => a.step - b.step),
      totalNodes: allNodes.length,
      totalConnections: edges.length,
      estimatedRows: totalRowsEstimate
    };
  }

  /**
   * Trace path from input to output
   */
  private tracePath(
    startNode: Node,
    outputNodes: Node[],
    allNodes: Node[],
    edges: Edge[]
  ): ExecutionStep[] {
    const steps: ExecutionStep[] = [];
    const visited = new Set<string>();
    
    const traverse = (node: Node, step: number): void => {
      if (visited.has(node.id)) return;
      visited.add(node.id);

      const data = node.data as FlowNodeMeta;
      
      // Create step for this node
      const stepInfo: ExecutionStep = {
        step: this.stepCounter++,
        nodeId: node.id,
        nodeType: data.componentKey || 'unknown',
        nodeName: data.label || `Node_${node.id.slice(0, 8)}`,
        sqlFragment: this.generateNodeSQL(data),
        description: this.getNodeDescription(data)
      };
      steps.push(stepInfo);

      // Check if this is an output node
      const isOutput = outputNodes.some(out => out.id === node.id);
      if (isOutput) return;

      // Find outgoing edges
      const outgoingEdges = edges.filter(edge => edge.source === node.id);
      
      for (const edge of outgoingEdges) {
        const targetNode = allNodes.find(n => n.id === edge.target);
        if (targetNode) {
          traverse(targetNode, step + 1);
        }
      }
    };

    traverse(startNode, 1);
    return steps;
  }

  /**
   * Generate SQL fragment for a node
   */
  private generateNodeSQL(nodeMeta: FlowNodeMeta): string {
    switch (nodeMeta.componentKey) {
      case 'tFileInputDelimited':
      case 'tFileInputXML':
      case 'tFileInputExcel':
      case 'tFileInputJson':
        return this.generateInputSQL(nodeMeta);
      
      case 'tMap':
        return this.generateMapSQL(nodeMeta);
      
      case 'tJoin':
        return this.generateJoinSQL(nodeMeta);
      
      case 'tFilterRow':
        return this.generateFilterSQL(nodeMeta);
      
      case 'tAggregateRow':
        return this.generateAggregateSQL(nodeMeta);
      
      case 'tSortRow':
        return this.generateSortSQL(nodeMeta);
      
      case 'tDatabaseOutput':
      case 'tFileOutputDelimited':
        return this.generateOutputSQL(nodeMeta);
      
      default:
        return `-- Unsupported component: ${nodeMeta.componentKey}`;
    }
  }

  /**
   * Generate SQL for input components
   */
  private generateInputSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'INPUT') return '';
    
    const config = nodeMeta.configuration.config;
    const schema = config.schema;
    const fields = schema?.fields || [];
    
    // Generate column list
    
    // Generate COPY statement for file inputs
    if (nodeMeta.componentKey.includes('FileInput')) {
      const fileName = config.sourceDetails?.filePath || 'data.csv';
      return `-- Reading from file: ${fileName}
COPY ${this.getNodeAlias(nodeMeta)} (${fields.map(f => f.name).join(', ')})
FROM '${fileName}'
DELIMITER ',' 
CSV HEADER;`;
    }
    
    // For database inputs
    const tableName = config.sourceDetails?.tableName || 'source_table';
    return `-- Reading from table: ${tableName}
SELECT ${fields.map(f => f.name).join(', ')}
FROM ${tableName} AS ${this.getNodeAlias(nodeMeta)}`;
  }

  /**
   * Generate SQL for tMap transformations
   */
  private generateMapSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'MAP') return '';
    
    const config = nodeMeta.configuration.config;
    const transformations = config.transformations || [];
    
    if (transformations.length === 0) {
      return '-- No transformations defined in tMap';
    }
    
    const fragments = transformations.map(trans => {
      return `  ${trans.expression || trans.sourceField} AS ${trans.targetField}`;
    });
    
    return `-- Mapping transformations
${fragments.join(',\n')}`;
  }

  /**
   * Generate SQL for tJoin
   */
  private generateJoinSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'JOIN') return '';
    
    const config = nodeMeta.configuration.config;
    const joinType = config.joinType || 'INNER';
    
    const joinConditions = (config.joinConditions || []).map(condition => {
      return `${condition.leftTable || 'table1'}.${condition.leftField} = ${condition.rightTable || 'table2'}.${condition.rightField}`;
    }).join(' AND ');
    
    return `-- ${joinType} JOIN
${joinConditions}`;
  }

  /**
   * Generate SQL for tFilterRow
   */
  private generateFilterSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'FILTER') return '';
    
    const config = nodeMeta.configuration.config;
    const conditions = config.filterConditions || [];
    
    if (conditions.length === 0) {
      return '-- No filter conditions';
    }
    
    const whereClause = conditions.map(condition => {
      const operator = condition.operator || '=';
      const value = condition.value !== undefined ? condition.value : 'NULL';
      return `${condition.field} ${operator} ${value}`;
    }).join(` ${config.filterLogic || 'AND'} `);
    
    return `-- Filter conditions
WHERE ${whereClause}`;
  }

  /**
   * Generate SQL for tAggregateRow
   */
  private generateAggregateSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'AGGREGATE') return '';
    
    const config = nodeMeta.configuration.config;
    const groupBy = config.groupByFields || [];
    const aggregates = config.aggregateFunctions || [];
    
    let sql = '';
    
    if (groupBy.length > 0) {
      sql += `GROUP BY ${groupBy.join(', ')}\n`;
    }
    
    if (aggregates.length > 0) {
      const aggFragments = aggregates.map(agg => {
        const distinct = agg.distinct ? 'DISTINCT ' : '';
        return `${agg.function}(${distinct}${agg.field}) AS ${agg.alias}`;
      });
      sql += aggFragments.join(',\n');
    }
    
    return sql || '-- No aggregation defined';
  }

  /**
   * Generate SQL for tSortRow
   */
  private generateSortSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'SORT') return '';
    
    const config = nodeMeta.configuration.config;
    const sortFields = config.sortFields || [];
    
    if (sortFields.length === 0) {
      return '-- No sort fields defined';
    }
    
    const orderBy = sortFields.map(field => {
      return `${field.field} ${field.direction || 'ASC'}${field.nullsFirst ? ' NULLS FIRST' : ''}`;
    }).join(', ');
    
    return `ORDER BY ${orderBy}`;
  }

  /**
   * Generate SQL for output components
   */
  private generateOutputSQL(nodeMeta: FlowNodeMeta): string {
    if (nodeMeta.configuration.type !== 'OUTPUT') return '';
    
    const config = nodeMeta.configuration.config;
    const tableName = config.targetDetails?.tableName || 'output_table';
    const mode = config.targetDetails?.mode || 'APPEND';
    
    if (mode === 'OVERWRITE') {
      return `-- Output to: ${tableName} (OVERWRITE mode)
TRUNCATE TABLE ${tableName};

INSERT INTO ${tableName} (...columns...)
SELECT ...columns...;`;
    }
    
    return `-- Output to: ${tableName} (${mode} mode)
INSERT INTO ${tableName} (...columns...)
SELECT ...columns...;`;
  }

  /**
   * Generate SQL for a complete path
   */
  private generatePathSQL(
    inputNode: Node,
    allNodes: Node[],
    edges: Edge[],
    _connectionId?: string
  ): string {
    const inputMeta = inputNode.data as FlowNodeMeta;
    const inputAlias = this.getNodeAlias(inputMeta);
    
    // Start building SQL
    let sql = `-- ========================================
-- Generated SQL for: ${inputMeta.label || inputNode.id}
-- ========================================
\n`;

    // Add input source
    const inputSQL = this.generateNodeSQL(inputMeta);
    sql += `${inputSQL}\n\n`;

    // Find connected nodes and add their SQL
    const connectedNodes = this.getConnectedNodes(inputNode, allNodes, edges, 'output');
    
    for (const node of connectedNodes) {
      const nodeMeta = node.data as FlowNodeMeta;
      const nodeSQL = this.generateNodeSQL(nodeMeta);
      
      if (nodeSQL && !nodeSQL.startsWith('-- Unsupported')) {
        sql += `${nodeSQL}\n`;
      }
    }

    // Find output nodes connected to this path
    const outputNodes = this.findConnectedOutputs(inputNode, allNodes, edges);
    
    for (const outputNode of outputNodes) {
      const outputMeta = outputNode.data as FlowNodeMeta;
      const outputSQL = this.generateOutputInsert(outputMeta, inputAlias);
      sql += `\n${outputSQL}`;
    }

    return sql;
  }

  /**
   * Generate INSERT statement for output
   */
  private generateOutputInsert(outputMeta: FlowNodeMeta, sourceAlias: string): string {
    if (outputMeta.configuration.type !== 'OUTPUT') return '';
    
    const config = outputMeta.configuration.config;
    const tableName = config.targetDetails?.tableName || 'output_table';
    const mappings = config.schemaMapping || [];
    
    if (mappings.length === 0) {
      return `-- No column mappings defined for output
-- INSERT INTO ${tableName} SELECT * FROM ${sourceAlias};`;
    }
    
    const sourceColumns = mappings.map(m => m.sourceField).join(', ');
    const targetColumns = mappings.map(m => m.targetField).join(', ');
    
    return `-- Inserting into: ${tableName}
INSERT INTO ${tableName} (${targetColumns})
SELECT ${sourceColumns}
FROM ${sourceAlias};`;
  }

  /**
   * Combine multiple SQL scripts
   */
  private combineSQLScripts(scripts: string[]): string {
    const header = `-- ========================================
-- AUTO-GENERATED POSTGRESQL SCRIPT
-- Generated: ${new Date().toISOString()}
-- Total scripts: ${scripts.length}
-- ========================================
\n`;

    const combined = scripts.join('\n\n-- ========================================\n\n');
    
    return header + combined;
  }

  /**
   * Validate SQL syntax (basic)
   */
  private validateSQLSyntax(sql: string): { valid: boolean; warnings: string[] } {
    const warnings: string[] = [];
    
    // Check for SELECT without FROM (basic check)
    const selectRegex = /SELECT\s+.+?(?=FROM|WHERE|GROUP|ORDER|$)/gi;
    const matches = sql.match(selectRegex);
    
    if (matches) {
      matches.forEach(match => {
        if (!match.includes('FROM') && !match.includes('@')) {
          warnings.push('Possible missing FROM clause in SELECT statement');
        }
      });
    }
    
    // Check for unmatched parentheses
    const openParens = (sql.match(/\(/g) || []).length;
    const closeParens = (sql.match(/\)/g) || []).length;
    
    if (openParens !== closeParens) {
      warnings.push(`Unmatched parentheses: ${openParens} opening vs ${closeParens} closing`);
    }
    
    return {
      valid: warnings.length === 0,
      warnings
    };
  }

  /**
   * Split SQL into individual statements
   */
  private splitSQLStatements(sql: string): string[] {
    // Split by semicolon, but not inside parentheses or quotes
    const statements: string[] = [];
    let current = '';
    let inParens = 0;
    let inQuotes = false;
    let quoteChar = '';
    
    for (let i = 0; i < sql.length; i++) {
      const char = sql[i];
      
      if (char === "'" || char === '"') {
        if (!inQuotes) {
          inQuotes = true;
          quoteChar = char;
        } else if (char === quoteChar && sql[i - 1] !== '\\') {
          inQuotes = false;
        }
      } else if (!inQuotes) {
        if (char === '(') inParens++;
        if (char === ')') inParens--;
      }
      
      current += char;
      
      if (char === ';' && !inQuotes && inParens === 0) {
        statements.push(current.trim());
        current = '';
      }
    }
    
    if (current.trim()) {
      statements.push(current.trim());
    }
    
    return statements;
  }

  /**
   * Get unique alias for node
   */
  private getNodeAlias(nodeMeta: FlowNodeMeta): string {
    if (this.nodeAliases.has(nodeMeta.compilerMetadata.nodeId)) {
      return this.nodeAliases.get(nodeMeta.compilerMetadata.nodeId)!;
    }
    
    const alias = nodeMeta.codegen?.alias || 
                 `t${this.nodeAliases.size + 1}_${nodeMeta.componentKey.replace(/[^a-zA-Z0-9]/g, '_')}`;
    
    this.nodeAliases.set(nodeMeta.compilerMetadata.nodeId, alias);
    return alias;
  }

  /**
   * Get connected nodes in direction
   */
  private getConnectedNodes(
    node: Node,
    allNodes: Node[],
    edges: Edge[],
    direction: 'input' | 'output'
  ): Node[] {
    const edgeIds = direction === 'output' 
      ? edges.filter(e => e.source === node.id).map(e => e.target)
      : edges.filter(e => e.target === node.id).map(e => e.source);
    
    return allNodes.filter(n => edgeIds.includes(n.id));
  }

  /**
   * Find output nodes connected to path
   */
  private findConnectedOutputs(
    startNode: Node,
    allNodes: Node[],
    edges: Edge[]
  ): Node[] {
    const outputs: Node[] = [];
    const visited = new Set<string>();
    
    const traverse = (node: Node): void => {
      if (visited.has(node.id)) return;
      visited.add(node.id);
      
      const data = node.data as FlowNodeMeta;
      if (data.componentType === 'OUTPUT') {
        outputs.push(node);
        return;
      }
      
      // Find outgoing edges
      const outgoingEdges = edges.filter(edge => edge.source === node.id);
      for (const edge of outgoingEdges) {
        const targetNode = allNodes.find(n => n.id === edge.target);
        if (targetNode) {
          traverse(targetNode);
        }
      }
    };
    
    traverse(startNode);
    return outputs;
  }

  /**
   * Get description for node
   */
  private getNodeDescription(nodeMeta: FlowNodeMeta): string {
    switch (nodeMeta.componentKey) {
      case 'tFileInputDelimited': return 'Read delimited file';
      case 'tFileInputXML': return 'Read XML file';
      case 'tFileInputExcel': return 'Read Excel file';
      case 'tMap': return 'Transform and map data';
      case 'tJoin': return 'Join multiple data sources';
      case 'tFilterRow': return 'Filter rows based on conditions';
      case 'tAggregateRow': return 'Aggregate and group data';
      case 'tSortRow': return 'Sort data';
      case 'tDatabaseOutput': return 'Write to database table';
      default: return 'Process data';
    }
  }

  /**
   * Estimate row count based on nodes
   */
  private estimateRowCount(nodes: Node[]): number {
    // Simple estimation based on node types
    let estimate = 1000;
    
    nodes.forEach(node => {
      const data = node.data as FlowNodeMeta;
      
      if (data.componentKey === 'tFilterRow') {
        estimate *= 0.5; // Filter reduces rows
      } else if (data.componentKey === 'tJoin') {
        estimate *= 2; // Join may increase rows
      } else if (data.componentKey === 'tAggregateRow') {
        estimate *= 0.1; // Aggregation reduces rows
      }
    });
    
    return Math.max(100, Math.min(estimate, 1000000));
  }


  /**
   * Save SQL script to file
   */
  public async saveSQLScript(sql: string, jobName: string): Promise<string> {
    try {
      // Create a blob and download link
      const blob = new Blob([sql], { type: 'application/sql' });
      const url = URL.createObjectURL(blob);
      
      const filename = `generated_${jobName.replace(/[^a-zA-Z0-9]/g, '_')}_${Date.now()}.sql`;
      
      // Trigger download
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      
      return filename;
    } catch (error) {
      console.error('Failed to save SQL script:', error);
      return '';
    }
  }
}

// Export singleton instance
export const sqlGenerator = new SQLGeneratorService();