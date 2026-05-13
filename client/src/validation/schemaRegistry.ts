// src/validation/schemaRegistry.ts

import {
  NodeSchema,
  ConnectionRule} from './types';

/**
 * Schema Registry for node type validation rules
 */
export class SchemaRegistry {
  private schemas: Map<string, NodeSchema> = new Map();
  private connectionRules: Map<string, ConnectionRule> = new Map();
  private typeHierarchy: Map<string, string[]> = new Map();

  /**
   * Register a node schema
   */
  registerSchema(schema: NodeSchema): void {
    this.schemas.set(schema.nodeType, schema);
    
    // Build type hierarchy for inheritance
    if (schema.nodeType.includes(':')) {
      const [parent, child] = schema.nodeType.split(':');
      if (!this.typeHierarchy.has(parent)) {
        this.typeHierarchy.set(parent, []);
      }
      this.typeHierarchy.get(parent)!.push(child);
    }
  }

  /**
   * Register multiple schemas
   */
  registerSchemas(schemas: NodeSchema[]): void {
    schemas.forEach(schema => this.registerSchema(schema));
  }

  /**
   * Get schema for node type
   */
  getSchema(nodeType: string): NodeSchema | undefined {
    // Try exact match first
    let schema = this.schemas.get(nodeType);
    
    // Try parent type if not found
    if (!schema && nodeType.includes(':')) {
      const parent = nodeType.split(':')[0];
      schema = this.schemas.get(parent);
    }
    
    return schema;
  }

  /**
   * Check if schema exists for node type
   */
  hasSchema(nodeType: string): boolean {
    return this.schemas.has(nodeType) || 
           (nodeType.includes(':') && this.schemas.has(nodeType.split(':')[0]));
  }

  /**
   * Register a connection rule
   */
  registerConnectionRule(rule: ConnectionRule): void {
    const key = `${rule.sourceType}:${rule.targetType}`;
    this.connectionRules.set(key, rule);
  }

  /**
   * Check if connection is allowed between node types
   */
  isConnectionAllowed(sourceType: string, targetType: string): {
    allowed: boolean;
    rule?: ConnectionRule;
  } {
    // Check exact type match
    const exactKey = `${sourceType}:${targetType}`;
    const exactRule = this.connectionRules.get(exactKey);
    
    if (exactRule) {
      return { allowed: exactRule.allowed, rule: exactRule };
    }
    
    // Check parent types for hierarchical types
    if (sourceType.includes(':') || targetType.includes(':')) {
      const sourceParent = sourceType.includes(':') ? sourceType.split(':')[0] : sourceType;
      const targetParent = targetType.includes(':') ? targetType.split(':')[0] : targetType;
      const parentKey = `${sourceParent}:${targetParent}`;
      const parentRule = this.connectionRules.get(parentKey);
      
      if (parentRule) {
        return { allowed: parentRule.allowed, rule: parentRule };
      }
    }
    
    // Default: allow if no rule exists (configurable)
    return { allowed: true };
  }

  /**
   * Get all schemas
   */
  getAllSchemas(): NodeSchema[] {
    return Array.from(this.schemas.values());
  }

  /**
   * Get all connection rules
   */
  getAllConnectionRules(): ConnectionRule[] {
    return Array.from(this.connectionRules.values());
  }

  /**
   * Clear all schemas and rules
   */
  clear(): void {
    this.schemas.clear();
    this.connectionRules.clear();
    this.typeHierarchy.clear();
  }

  /**
   * Validate schema consistency
   */
  validateSchemaConsistency(): Array<{
    type: 'error' | 'warning';
    message: string;
    schemaId?: string;
  }> {
    const issues: Array<{
      type: 'error' | 'warning';
      message: string;
      schemaId?: string;
    }> = [];

    // Check for circular references in allowed types
    this.schemas.forEach(schema => {
      // Check that allowed source/target types exist
      schema.allowedSourceTypes.forEach(sourceType => {
        if (!this.hasSchema(sourceType)) {
          issues.push({
            type: 'warning',
            message: `Schema "${schema.nodeType}" references unknown source type: ${sourceType}`,
            schemaId: schema.id
          });
        }
      });

      schema.allowedTargetTypes.forEach(targetType => {
        if (!this.hasSchema(targetType)) {
          issues.push({
            type: 'warning',
            message: `Schema "${schema.nodeType}" references unknown target type: ${targetType}`,
            schemaId: schema.id
          });
        }
      });

      // Check for self-references if cycles not allowed
      if (!schema.allowsCycles && 
          (schema.allowedSourceTypes.includes(schema.nodeType) || 
           schema.allowedTargetTypes.includes(schema.nodeType))) {
        issues.push({
          type: 'error',
          message: `Schema "${schema.nodeType}" disallows cycles but references itself`,
          schemaId: schema.id
        });
      }
      
      // Check ETL consistency
      if (schema.etlCategory) {
        // Validate that ETL category is valid
        const validETLCategories = ['source', 'sink', 'processing', 'merge', 'branching'];
        if (!validETLCategories.includes(schema.etlCategory)) {
          issues.push({
            type: 'error',
            message: `Schema "${schema.nodeType}" has invalid ETL category: ${schema.etlCategory}`,
            schemaId: schema.id
          });
        }
        
        // Check that source components don't have incoming connections
        if (schema.etlCategory === 'source' && schema.maxIncomingConnections !== 0) {
          issues.push({
            type: 'warning',
            message: `Source component "${schema.nodeType}" should not accept incoming connections`,
            schemaId: schema.id
          });
        }
        
        // Check that sink components don't have outgoing connections
        if (schema.etlCategory === 'sink' && schema.maxOutgoingConnections !== 0) {
          issues.push({
            type: 'warning',
            message: `Sink component "${schema.nodeType}" should not have outgoing connections`,
            schemaId: schema.id
          });
        }
      }
    });

    return issues;
  }
  
  /**
   * Get ETL classification for a node type
   */
  getETLClassification(nodeType: string): {
    etlCategory: 'source' | 'sink' | 'processing' | 'merge' | 'branching' | 'unknown';
    schema?: NodeSchema;
  } {
    const schema = this.getSchema(nodeType);
    
    if (!schema) {
      return { etlCategory: 'unknown' };
    }
    
    return {
      etlCategory: schema.etlCategory || 'unknown',
      schema
    };
  }
  
  /**
   * Check if a component can accept multiple inputs
   */
  canAcceptMultipleInputs(nodeType: string): boolean {
    const schema = this.getSchema(nodeType);
    
    if (!schema) {
      return false;
    }
    
    // Check ETL category
    if (schema.etlCategory === 'merge' || schema.etlCategory === 'branching') {
      return true;
    }
    
    // Check component metadata
    if (schema.componentMetadata?.isMultiInput || 
        schema.componentMetadata?.allowsUnlimitedInputs ||
        schema.componentMetadata?.allowsLookupInputs) {
      return true;
    }
    
    // Check max incoming connections
    if (schema.maxIncomingConnections === null || schema.maxIncomingConnections > 1) {
      return true;
    }
    
    return false;
  }
  
  /**
   * Check if a component can have multiple outputs
   */
  canHaveMultipleOutputs(nodeType: string): boolean {
    const schema = this.getSchema(nodeType);
    
    if (!schema) {
      return false;
    }
    
    // Check ETL category
    if (schema.etlCategory === 'branching') {
      return true;
    }
    
    // Check component metadata
    if (schema.componentMetadata?.allowsMultipleMainOutputs) {
      return true;
    }
    
    // Check max outgoing connections
    if (schema.maxOutgoingConnections === null || schema.maxOutgoingConnections > 1) {
      return true;
    }
    
    return false;
  }
  
  /**
   * Get all schemas by ETL category
   */
  getSchemasByETLCategory(category: string): NodeSchema[] {
    return Array.from(this.schemas.values()).filter(schema => 
      schema.etlCategory === category
    );
  }
  
  /**
   * Get ETL connection rules
   */
  getETLConnectionRules(): ConnectionRule[] {
    return Array.from(this.connectionRules.values()).filter(rule => 
      // Include rules that are explicitly set to true or false for ETL validation
      rule.allowed === false || 
      (rule.sourceType.includes('t') && rule.targetType.includes('t')) // ETL components typically start with 't'
    );
  }
  
  /**
   * Check if connection is ETL-compliant
   */
  isETLConnectionAllowed(sourceType: string, targetType: string): {
    allowed: boolean;
    reason?: string;
    rule?: ConnectionRule;
  } {
    const sourceSchema = this.getSchema(sourceType);
    const targetSchema = this.getSchema(targetType);
    
    // If either schema doesn't exist, we can't validate ETL rules
    if (!sourceSchema || !targetSchema) {
      return { allowed: true }; // Default to allowed
    }
    
    const sourceCategory = sourceSchema.etlCategory;
    const targetCategory = targetSchema.etlCategory;
    
    // If either component doesn't have an ETL category, we can't validate
    if (!sourceCategory || !targetCategory) {
      return { allowed: true }; // Default to allowed
    }
    
    // Prohibited ETL connections
    if (sourceCategory === 'source' && targetCategory === 'source') {
      return {
        allowed: false,
        reason: 'Source components cannot connect to other source components',
        rule: { sourceType, targetType, allowed: false }
      };
    }
    
    if (sourceCategory === 'sink' && targetCategory === 'sink') {
      return {
        allowed: false,
        reason: 'Sink components cannot connect to other sink components',
        rule: { sourceType, targetType, allowed: false }
      };
    }
    
    // Check allowed connections based on ETL categories
    const allowedTargets: Record<string, string[]> = {
      'source': ['processing', 'merge', 'branching'],
      'processing': ['processing', 'merge', 'branching', 'sink'],
      'merge': ['processing', 'merge', 'branching', 'sink'],
      'branching': ['processing', 'merge', 'branching', 'sink'],
      'sink': [] // Sinks cannot connect to anything
    };
    
    const allowed = allowedTargets[sourceCategory]?.includes(targetCategory) || false;
    
    if (!allowed) {
      return {
        allowed: false,
        reason: `ETL rules prohibit ${sourceCategory} → ${targetCategory} connections`,
        rule: { sourceType, targetType, allowed: false }
      };
    }
    
    // Also check the regular connection rules
    return this.isConnectionAllowed(sourceType, targetType);
  }
}

/**
 * Default schemas for common node types - ENHANCED WITH ETL
 */
export const DefaultSchemas: NodeSchema[] = [
  // ==================== DATA SOURCE COMPONENTS ====================
  {
    id: 'schema:excel',
    nodeType: 'excel',
    displayName: 'Excel File',
    category: 'input',
    allowsCycles: false,
    maxIncomingConnections: 0, // Sources typically don't have inputs
    maxOutgoingConnections: 5,
    allowedSourceTypes: [],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tjoin', 'tmatchgroup', 'tnormalize', 'tdenormalize', 'tfileoutputdelimited'],
    etlCategory: 'source',
    etlConnectionRules: {
      allowedSourceCategories: [],
      allowedTargetCategories: ['processing', 'merge', 'branching']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 0,
      outputPortCount: 1
    }
  },
  {
    id: 'schema:database',
    nodeType: 'database',
    displayName: 'Database Table',
    category: 'input',
    allowsCycles: false,
    maxIncomingConnections: 0,
    maxOutgoingConnections: 5,
    allowedSourceTypes: [],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tjoin', 'tmatchgroup', 'tnormalize', 'tdenormalize', 'tfileoutputdelimited'],
    etlCategory: 'source',
    etlConnectionRules: {
      allowedSourceCategories: [],
      allowedTargetCategories: ['processing', 'merge', 'branching']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 0,
      outputPortCount: 1
    }
  },
  {
    id: 'schema:csv',
    nodeType: 'csv',
    displayName: 'CSV File',
    category: 'input',
    allowsCycles: false,
    maxIncomingConnections: 0,
    maxOutgoingConnections: 5,
    allowedSourceTypes: [],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tjoin', 'tmatchgroup', 'tnormalize', 'tdenormalize', 'tfileoutputdelimited'],
    etlCategory: 'source',
    etlConnectionRules: {
      allowedSourceCategories: [],
      allowedTargetCategories: ['processing', 'merge', 'branching']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 0,
      outputPortCount: 1
    }
  },
  {
    id: 'schema:delimited',
    nodeType: 'delimited',
    displayName: 'Delimited File',
    category: 'input',
    allowsCycles: false,
    maxIncomingConnections: 0,
    maxOutgoingConnections: 5,
    allowedSourceTypes: [],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tjoin', 'tmatchgroup', 'tnormalize', 'tdenormalize', 'tfileoutputdelimited'],
    etlCategory: 'source',
    etlConnectionRules: {
      allowedSourceCategories: [],
      allowedTargetCategories: ['processing', 'merge', 'branching']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 0,
      outputPortCount: 1
    }
  },
  
  // ==================== PROCESSING COMPONENTS ====================
  {
    id: 'schema:tsortrow',
    nodeType: 'tsortrow',
    displayName: 'tSortRow',
    category: 'processing',
    allowsCycles: false,
    maxIncomingConnections: 1, // Processing: 1 main input
    maxOutgoingConnections: 1, // Processing: 1 main output
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tjoin'],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tjoin', 'tmatchgroup', 'tfileoutputdelimited', 'tmysqloutput'],
    etlCategory: 'processing',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 1,
      outputPortCount: 1
    }
  },
  {
    id: 'schema:tfilterrow',
    nodeType: 'tfilterrow',
    displayName: 'tFilterRow',
    category: 'processing',
    allowsCycles: false,
    maxIncomingConnections: 1,
    maxOutgoingConnections: 2, // Can have reject output
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tsortrow', 'tjoin'],
    allowedTargetTypes: ['tmap', 'tsortrow', 'tjoin', 'tmatchgroup', 'tfileoutputdelimited'],
    etlCategory: 'processing',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 1,
      outputPortCount: 2
    }
  },
  {
    id: 'schema:taggregaterow',
    nodeType: 'taggregaterow',
    displayName: 'tAggregateRow',
    category: 'processing',
    allowsCycles: false,
    maxIncomingConnections: 1,
    maxOutgoingConnections: 1,
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tjoin'],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tjoin', 'tmatchgroup', 'tfileoutputdelimited'],
    etlCategory: 'processing',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink']
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 1,
      outputPortCount: 1
    }
  },
  
  // ==================== MULTI-INPUT / MERGE COMPONENTS ====================
  {
    id: 'schema:tmap',
    nodeType: 'tmap',
    displayName: 'tMap',
    category: 'transform',
    allowsCycles: false,
    maxIncomingConnections: null, // Unlimited inputs (1 main + N lookups)
    maxOutgoingConnections: null, // Multiple outputs allowed
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tfilterrow', 'tsortrow', 'tjoin', 'tunite'],
    allowedTargetTypes: ['tfilterrow', 'tsortrow', 'tjoin', 'tmatchgroup', 'tfileoutputdelimited', 'tmysqloutput'],
    etlCategory: 'merge',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink'],
      requiresMergeNodeForMultipleInputs: false, // tMap itself is a merge node
      allowsUnlimitedInputs: true
    },
    etlPorts: {
      mainInput: {
        id: 'main-input',
        minConnections: 1,
        maxConnections: 1,
        required: true
      },
      mainOutput: {
        id: 'main-output',
        minConnections: 0,
        maxConnections: null,
        allowsFanOut: false
      },
      lookupInputs: [
        {
          id: 'lookup-1',
          maxConnections: 1
        }
      ]
    },
    componentMetadata: {
      isMultiInput: true,
      isBranching: false,
      isMerge: true,
      allowsLookupInputs: true,
      allowsUnlimitedInputs: true,
      inputPortCount: null, // Variable
      outputPortCount: null // Variable
    }
  },
  {
    id: 'schema:tjoin',
    nodeType: 'tjoin',
    displayName: 'tJoin',
    category: 'transform',
    allowsCycles: false,
    maxIncomingConnections: 2, // Exactly 2 inputs (1 main + 1 lookup)
    maxOutgoingConnections: 1,
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow'],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tsortrow', 'tmatchgroup', 'tfileoutputdelimited'],
    etlCategory: 'merge',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink'],
      exactInputCount: 2,
      requiresMergeNodeForMultipleInputs: false // tJoin itself is a merge node
    },
    componentMetadata: {
      isMultiInput: true,
      isBranching: false,
      isMerge: true,
      requiresExactInputs: 2, // Exactly 2 inputs (1 main + 1 lookup)
      inputPortCount: 2,
      outputPortCount: 1
    }
  },
  {
    id: 'schema:tunite',
    nodeType: 'tunite',
    displayName: 'tUnite',
    category: 'transform',
    allowsCycles: false,
    maxIncomingConnections: null, // Unlimited inputs
    maxOutgoingConnections: 1,
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin'],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tsortrow', 'tmatchgroup', 'tfileoutputdelimited'],
    etlCategory: 'merge',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink'],
      allowsUnlimitedInputs: true,
      requiresMergeNodeForMultipleInputs: false // tUnite itself is a merge node
    },
    componentMetadata: {
      isMultiInput: true,
      isBranching: false,
      isMerge: true,
      allowsUnlimitedInputs: true,
      inputPortCount: null, // Variable
      outputPortCount: 1
    }
  },
  {
    id: 'schema:tflowmerge',
    nodeType: 'tflowmerge',
    displayName: 'tFlowMerge',
    category: 'transform',
    allowsCycles: false,
    maxIncomingConnections: null, // Unlimited inputs
    maxOutgoingConnections: 1,
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin'],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tsortrow', 'tmatchgroup', 'tfileoutputdelimited'],
    etlCategory: 'merge',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink'],
      allowsUnlimitedInputs: true
    },
    componentMetadata: {
      isMultiInput: true,
      isBranching: false,
      isMerge: true,
      allowsUnlimitedInputs: true,
      inputPortCount: null,
      outputPortCount: 1
    }
  },
  {
    id: 'schema:tmatchgroup',
    nodeType: 'tmatchgroup',
    displayName: 'tMatchGroup',
    category: 'match',
    allowsCycles: false,
    maxIncomingConnections: null, // Unlimited inputs
    maxOutgoingConnections: 1,
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin'],
    allowedTargetTypes: ['tfileoutputdelimited', 'tmysqloutput'], // Match groups should only connect to outputs
    etlCategory: 'merge',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['sink'], // Only sinks
      allowsUnlimitedInputs: true
    },
    componentMetadata: {
      isMultiInput: true,
      isBranching: false,
      isMerge: true,
      allowsUnlimitedInputs: true,
      inputPortCount: null,
      outputPortCount: 1
    }
  },
  
  // ==================== BRANCHING COMPONENTS ====================
  {
    id: 'schema:treplicate',
    nodeType: 'treplicate',
    displayName: 'tReplicate',
    category: 'transform',
    allowsCycles: false,
    maxIncomingConnections: 1,
    maxOutgoingConnections: null, // Unlimited outputs
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin'],
    allowedTargetTypes: ['tmap', 'tfilterrow', 'tsortrow', 'tjoin', 'tfileoutputdelimited', 'tmysqloutput'],
    etlCategory: 'branching',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: ['processing', 'merge', 'branching', 'sink'],
      requiresBranchingNodeForFanOut: false, // tReplicate itself is a branching node
      allowsMultipleMainOutputs: true
    },
    etlPorts: {
      mainInput: {
        id: 'input',
        minConnections: 1,
        maxConnections: 1,
        required: true
      },
      mainOutput: {
        id: 'output',
        minConnections: 2, // At least 2 outputs for branching
        maxConnections: null,
        allowsFanOut: true
      }
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: true,
      isMerge: false,
      allowsMultipleMainOutputs: true,
      inputPortCount: 1,
      outputPortCount: null // Variable
    }
  },
  
  // ==================== DATA SINK COMPONENTS ====================
  {
    id: 'schema:tfileoutputdelimited',
    nodeType: 'tfileoutputdelimited',
    displayName: 'tFileOutputDelimited',
    category: 'output',
    allowsCycles: false,
    maxIncomingConnections: 5,
    maxOutgoingConnections: 0, // Sinks cannot have outputs
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin', 'tmatchgroup'],
    allowedTargetTypes: [],
    etlCategory: 'sink',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: [] // Sinks cannot connect to anything
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 1,
      outputPortCount: 0
    }
  },
  {
    id: 'schema:tmysqloutput',
    nodeType: 'tmysqloutput',
    displayName: 'tMysqlOutput',
    category: 'output',
    allowsCycles: false,
    maxIncomingConnections: 5,
    maxOutgoingConnections: 0, // Sinks cannot have outputs
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin', 'tmatchgroup'],
    allowedTargetTypes: [],
    etlCategory: 'sink',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: [] // Sinks cannot connect to anything
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 1,
      outputPortCount: 0
    }
  },
  {
    id: 'schema:output',
    nodeType: 'output',
    displayName: 'Output',
    category: 'output',
    allowsCycles: false,
    maxIncomingConnections: 5,
    maxOutgoingConnections: 0,
    allowedSourceTypes: ['excel', 'database', 'csv', 'delimited', 'tmap', 'tfilterrow', 'tsortrow', 'tjoin', 'tmatchgroup'],
    allowedTargetTypes: [],
    etlCategory: 'sink',
    etlConnectionRules: {
      allowedSourceCategories: ['source', 'processing', 'merge', 'branching'],
      allowedTargetCategories: [] // Sinks cannot connect to anything
    },
    componentMetadata: {
      isMultiInput: false,
      isBranching: false,
      isMerge: false,
      inputPortCount: 1,
      outputPortCount: 0
    }
  }
];

/**
 * Default connection rules - ENHANCED WITH ETL
 */
export const DefaultConnectionRules: ConnectionRule[] = [
  // ==================== PROHIBITED ETL CONNECTIONS ====================
  
  // ❌ Source → Source (Invalid)
  { sourceType: 'excel', targetType: 'excel', allowed: false },
  { sourceType: 'database', targetType: 'database', allowed: false },
  { sourceType: 'csv', targetType: 'csv', allowed: false },
  { sourceType: 'delimited', targetType: 'delimited', allowed: false },
  { sourceType: 'tfileinputdelimited', targetType: 'tfileinputdelimited', allowed: false },
  { sourceType: 'tfileinputxml', targetType: 'tfileinputxml', allowed: false },
  { sourceType: 'tfileinputjson', targetType: 'tfileinputjson', allowed: false },
  
  // ❌ Sink → Sink (Invalid)
  { sourceType: 'tfileoutputdelimited', targetType: 'tfileoutputdelimited', allowed: false },
  { sourceType: 'tmysqloutput', targetType: 'tmysqloutput', allowed: false },
  { sourceType: 'toracleoutput', targetType: 'toracleoutput', allowed: false },
  { sourceType: 'output', targetType: 'output', allowed: false },
  
  // ❌ Sinks can't have outgoing connections
  { sourceType: 'tfileoutputdelimited', targetType: '*', allowed: false },
  { sourceType: 'tmysqloutput', targetType: '*', allowed: false },
  { sourceType: 'output', targetType: '*', allowed: false },
  
  // ==================== ETL-SPECIFIC RULES ====================
  
  // Data sources can connect to processing or merge components
  { sourceType: 'excel', targetType: 'tmap', allowed: true },
  { sourceType: 'excel', targetType: 'tjoin', allowed: true },
  { sourceType: 'excel', targetType: 'tunite', allowed: true },
  { sourceType: 'excel', targetType: 'tsortrow', allowed: true },
  { sourceType: 'excel', targetType: 'tfilterrow', allowed: true },
  
  // Processing components can chain
  { sourceType: 'tsortrow', targetType: 'tfilterrow', allowed: true },
  { sourceType: 'tfilterrow', targetType: 'tjoin', allowed: true },
  { sourceType: 'tfilterrow', targetType: 'tmap', allowed: true },
  
  // Merge components can connect to sinks
  { sourceType: 'tmap', targetType: 'tfileoutputdelimited', allowed: true },
  { sourceType: 'tmap', targetType: 'tmysqloutput', allowed: true },
  { sourceType: 'tjoin', targetType: 'tfileoutputdelimited', allowed: true },
  { sourceType: 'tunite', targetType: 'tfileoutputdelimited', allowed: true },
  { sourceType: 'tflowmerge', targetType: 'tfileoutputdelimited', allowed: true },
  
  // Branching components can connect to multiple destinations
  { sourceType: 'treplicate', targetType: 'tmap', allowed: true },
  { sourceType: 'treplicate', targetType: 'tfilterrow', allowed: true },
  { sourceType: 'treplicate', targetType: 'tfileoutputdelimited', allowed: true },
  { sourceType: 'treplicate', targetType: 'tmysqloutput', allowed: true },
  
  // Match groups should only connect to outputs
  { sourceType: 'tmatchgroup', targetType: 'tmap', allowed: false },
  { sourceType: 'tmatchgroup', targetType: 'tfilterrow', allowed: false },
  { sourceType: 'tmatchgroup', targetType: 'tjoin', allowed: false },
  { sourceType: 'tmatchgroup', targetType: 'tfileoutputdelimited', allowed: true },
  { sourceType: 'tmatchgroup', targetType: 'tmysqloutput', allowed: true },
  
  // ==================== COMPONENT-SPECIFIC RULES ====================
  
  // tJoin requires exactly 2 inputs
  { sourceType: 'tjoin', targetType: 'tjoin', allowed: false }, // Can't chain tJoins
  
  // tReplicate can have multiple outputs
  { sourceType: 'treplicate', targetType: 'treplicate', allowed: false }, // Can't chain tReplicates
  
  // Processing components typically have 1 input
  { sourceType: 'tsortrow', targetType: 'tsortrow', allowed: false }, // Can't chain same processing component
  
  // ==================== PORT-SPECIFIC RULES ====================
  
  // Output ports can only connect to input ports (handled elsewhere)
  // Input ports can only accept one connection (handled elsewhere)
];