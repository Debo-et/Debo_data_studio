// src/utils/etlValidationUtils.ts

import { CanvasNode } from '../utils/canvasUtils';

/**
 * Interface for canvas connections used in ETL validation
 */
export interface EnhancedCanvasConnection {
  id: string;
  sourceNodeId: string;
  targetNodeId: string;
  sourcePortId?: string;
  targetPortId?: string;
  metadata?: any;
}

/**
 * Validates ETL-specific connection rules
 */
export class ETLValidationUtils {
  /**
   * Validate if a connection between two ETL components is allowed
   */
  static validateETLConnection(
    sourceNode: CanvasNode,
    targetNode: CanvasNode,
    existingConnections: EnhancedCanvasConnection[]
  ): { isValid: boolean; errors: string[]; warnings: string[] } {
    const errors: string[] = [];
    const warnings: string[] = [];
    
    const sourceClass = this.getClassifyETLComponent(sourceNode);
    const targetClass = this.getClassifyETLComponent(targetNode);
    
    // Prohibited connections
    if (sourceClass === 'source' && targetClass === 'source') {
      errors.push('Source components cannot connect to other source components');
    }
    
    if (sourceClass === 'sink' && targetClass === 'sink') {
      errors.push('Sink components cannot connect to other sink components');
    }
    
    if (sourceClass === 'output' && targetClass === 'output') {
      errors.push('Output ports cannot connect to output ports');
    }
    
    if (sourceClass === 'input' && targetClass === 'input') {
      errors.push('Input ports cannot connect to input ports');
    }
    
    // Validate allowed connections
    const allowed = this.isConnectionAllowed(sourceClass, targetClass);
    if (!allowed) {
      errors.push(`Connection from ${sourceClass} to ${targetClass} is not allowed by ETL rules`);
    }
    
    // Validate fan-out rules
    if (!this.isBranchingComponent(sourceNode)) {
      const sourceOutputs = existingConnections.filter(
        conn => conn.sourceNodeId === sourceNode.id
      ).length;
      
      if (sourceOutputs >= 1) {
        errors.push('Fan-out without tReplicate is forbidden. Use tReplicate for multiple outputs');
      }
    }
    
    // Validate multi-input rules
    if (!this.isMergeComponent(targetNode)) {
      const targetInputs = existingConnections.filter(
        conn => conn.targetNodeId === targetNode.id
      ).length;
      
      if (targetInputs >= 1) {
        errors.push('Multiple main inputs into a non-merge component is forbidden');
      }
    }
    
    // Validate tJoin specific rules
    if (targetNode.type.toLowerCase().includes('tjoin')) {
      const joinInputs = existingConnections.filter(
        conn => conn.targetNodeId === targetNode.id
      ).length;
      
      if (joinInputs >= 2) {
        errors.push('tJoin can only accept exactly 2 inputs (1 main + 1 lookup)');
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }
  
  /**
   * Classify ETL component
   */
  static getClassifyETLComponent(node: CanvasNode): string {
    const type = node.type.toLowerCase();
    
    // Data Source Components
    const sources = [
      'excel', 'database', 'csv', 'xml', 'json', 'delimited',
      'mysql', 'oracle', 'webservice', 'ldif', 'regex'
    ];
    
    // Processing Components
    const processing = [
      'tsortrow', 'tfilterrow', 'taggregaterow', 'tnormalize',
      'tconverttype', 'textract', 'tparse', 'tsplit', 'tpivot',
      'tsample', 'tuniq', 'tunique', 'tmatch'
    ];
    
    // Multi-Input / Merge Components
    const merge = [
      'tmap', 'tjoin', 'tunite', 'tflowmerge', 'tmatchgroup'
    ];
    
    // Branching Components
    const branching = [
      'treplicate'
    ];
    
    // Data Sink Components
    const sink = [
      'output', 'tfileoutputdelimited', 'tmysqloutput', 'tfileoutput'
    ];
    
    if (sources.some(s => type.includes(s))) return 'source';
    if (processing.some(p => type.includes(p))) return 'processing';
    if (merge.some(m => type.includes(m))) return 'merge';
    if (branching.some(b => type.includes(b))) return 'branching';
    if (sink.some(s => type.includes(s))) return 'sink';
    
    return 'unknown';
  }
  
  /**
   * Check if component is a merge component
   */
  static isMergeComponent(node: CanvasNode): boolean {
    const mergeTypes = ['tmap', 'tjoin', 'tunite', 'tflowmerge', 'tmatchgroup'];
    return mergeTypes.some(type => node.type.toLowerCase().includes(type));
  }
  
  /**
   * Check if component is a branching component
   */
  static isBranchingComponent(node: CanvasNode): boolean {
    return node.type.toLowerCase().includes('treplicate');
  }
  
  /**
   * Check if connection is allowed by ETL rules
   */
  static isConnectionAllowed(sourceClass: string, targetClass: string): boolean {
    const allowedConnections: Record<string, string[]> = {
      'source': ['processing', 'merge'],           // Source → Processing, Merge
      'processing': ['processing', 'merge', 'sink'], // Processing → Processing, Merge, Sink
      'merge': ['processing', 'merge', 'sink'],     // Merge → Processing, Merge, Sink
      'branching': ['processing', 'merge', 'sink'], // Branching → Processing, Merge, Sink
      'sink': []                                   // Sink cannot connect to anything
    };
    
    return allowedConnections[sourceClass]?.includes(targetClass) || false;
  }
  
  /**
   * Validate component-specific input/output counts
   */
  static validateComponentPorts(
    node: CanvasNode,
    connections: EnhancedCanvasConnection[]
  ): { isValid: boolean; errors: string[] } {
    const errors: string[] = [];
    const incoming = connections.filter(c => c.targetNodeId === node.id);
    const outgoing = connections.filter(c => c.sourceNodeId === node.id);
    
    const componentClass = this.getClassifyETLComponent(node);
    
    switch (componentClass) {
      case 'source':
        // Sources can have 0-1 inputs, 1+ outputs
        if (incoming.length > 1) {
          errors.push('Source components cannot have multiple inputs');
        }
        if (outgoing.length === 0) {
          errors.push('Source components must have at least one output');
        }
        break;
        
      case 'processing':
        // Processing: 1 input, 1 output (default)
        if (incoming.length !== 1) {
          errors.push('Processing components must have exactly 1 input');
        }
        if (outgoing.length === 0) {
          errors.push('Processing components must have at least 1 output');
        }
        break;
        
      case 'merge':
        // Merge components: N inputs, 1 output
        const mergeType = node.type.toLowerCase();
        
        if (mergeType.includes('tjoin')) {
          // tJoin: exactly 2 inputs
          if (incoming.length > 2) {
            errors.push('tJoin components must have exactly 2 inputs');
          }
        } else if (mergeType.includes('tmap')) {
          // tMap: 1 main + N lookup inputs
          if (incoming.length < 1) {
            errors.push('tMap components must have at least 1 main input');
          }
        } else {
          // Other merge: at least 1 input
          if (incoming.length < 1) {
            errors.push('Merge components must have at least 1 input');
          }
        }
        
        if (outgoing.length === 0) {
          errors.push('Merge components must have at least 1 output');
        }
        break;
        
      case 'branching':
        // Branching: 1 input, N outputs
        if (incoming.length !== 1) {
          errors.push('Branching components must have exactly 1 input');
        }
        if (outgoing.length < 2) {
          errors.push('Branching components must have at least 2 outputs');
        }
        break;
        
      case 'sink':
        // Sink: 1 input, 0 outputs
        if (incoming.length !== 1) {
          errors.push('Sink components must have exactly 1 input');
        }
        if (outgoing.length > 0) {
          errors.push('Sink components cannot have outputs');
        }
        break;
    }
    
    return {
      isValid: errors.length === 0,
      errors
    };
  }
}