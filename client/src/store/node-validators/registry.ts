// src/utils/node-validators/registry.ts
/**
 * Auto-discovery registry for node validators
 */

import { NodeType } from '../../types/pipeline-types';
import { INodeValidator } from './index';

// Dynamic import pattern for auto-discovery

// This would be populated by a build step or module loader
export const VALIDATOR_REGISTRY: Map<NodeType, new () => INodeValidator> = new Map();

export function registerValidator(nodeType: NodeType, constructor: new () => INodeValidator): void {
  VALIDATOR_REGISTRY.set(nodeType, constructor);
}

// FIXED: Changed return type to properly handle null constructor
export function getValidatorConstructor(nodeType: NodeType): (new () => INodeValidator) | null {
  return VALIDATOR_REGISTRY.get(nodeType) || null;
}

export function getAllRegisteredTypes(): NodeType[] {
  return Array.from(VALIDATOR_REGISTRY.keys());
}