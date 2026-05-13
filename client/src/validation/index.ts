// src/validation/index.ts
export * from './types';
export * from './validationEngine';
export * from './validationResult';
export * from './validationRules';
export * from './schemaRegistry';

// Export the createDefaultValidationEngine function
export function createDefaultValidationEngine(): import('./validationEngine').ValidationEngine {
  const { SchemaRegistry } = require('./schemaRegistry');
  
  // Create a schema registry instance
  const schemaRegistry = new SchemaRegistry();
  
  // Create and return validation engine with default config
  return new (require('./validationEngine').ValidationEngine)({
    schemaRegistry,
    mode: 'strict',
    enableCaching: true,
    enableETLValidation: true,
    etlMode: 'strict'
  });
}

// Also export ValidationLevel enum
export { ValidationLevel } from './types';