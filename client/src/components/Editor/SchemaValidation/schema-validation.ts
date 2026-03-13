// types/schema-validation.ts
export type DataType = 
  | 'string' | 'number' | 'integer' | 'boolean' 
  | 'date' | 'datetime' | 'timestamp' | 'email'
  | 'phone' | 'url' | 'uuid' | 'object' | 'array' | 'any';

export type ValidationRuleType = 
  | 'required' | 'type' | 'format' | 'range' 
  | 'regex' | 'unique' | 'custom' | 'enum' | 'length';

export interface SchemaField {
  name: string;
  type: DataType;
  nullable: boolean;
  format?: string; // e.g., 'date-time', 'email', 'phone'
  description?: string;
  metadata?: Record<string, any>;
  arrayItemType?: DataType; // For array types
  objectSchema?: SchemaField[]; // For object types
}

export interface ValidationRule {
  id: string;
  field: string;
  rule: ValidationRuleType;
  parameters?: any;
  message?: string;
  enabled: boolean;
  severity: 'error' | 'warning';
}

export interface SchemaValidationConfig {
  expectedSchema: SchemaField[];
  validationRules: ValidationRule[];
  strictMode: boolean;
  treatWarningsAsErrors: boolean;
  errorThreshold: number; // 0-1
  schemaName?: string;
  version?: string;
}

export interface SchemaTemplate {
  id: string;
  name: string;
  description: string;
  schema: SchemaField[];
  rules: ValidationRule[];
  category: 'standard' | 'industry' | 'custom';
}