// src/utils/node-validators/decorators.ts
/**
 * Enhanced decorators for validator registration
 */

import { NodeType } from '../../types/pipeline-types';
import { INodeValidator } from './index';

export function ValidatorConfig(options: {
  nodeType: NodeType;
  description?: string;
  category?: string;
  version?: string;
}) {
  return function <T extends new () => INodeValidator>(constructor: T) {
    // Add metadata to constructor
    Object.defineProperty(constructor, 'validatorConfig', {
      value: options,
      writable: false,
      enumerable: true
    });

    // Auto-register with factory
    const factory = (globalThis as any).__nodeValidatorFactory;
    if (factory && typeof factory.registerValidator === 'function') {
      const instance = new constructor();
      factory.registerValidator(options.nodeType, instance);
    }

    return constructor;
  };
}

// Usage example:
// @ValidatorConfig({
//   nodeType: NodeType.JOIN,
//   description: 'Validates JOIN node configurations',
//   category: 'Data Transformation',
//   version: '1.0.0'
// })
// export class JoinNodeValidator extends BaseNodeValidator { ... }