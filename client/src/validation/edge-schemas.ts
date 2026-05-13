// src/validation/edge-schemas.ts
import { z } from 'zod';

// Base schema for all edge configurations
export const BaseEdgeConfigSchema = z.object({
  description: z.string().optional(),
  isActive: z.boolean().default(true),
  metadata: z.record(z.string(), z.any()).optional()
});

// Join edge schema
export const JoinEdgeConfigSchema = BaseEdgeConfigSchema.extend({
  joinType: z.enum(['INNER', 'LEFT_OUTER', 'RIGHT_OUTER', 'FULL_OUTER', 'CROSS']),
  joinConditions: z.array(
    z.object({
      id: z.string(),
      leftField: z.string().min(1, "Left field is required"),
      rightField: z.string().min(1, "Right field is required"),
      operator: z.enum(['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN']),
      value: z.union([z.string(), z.number()]).optional(),
      isActive: z.boolean().default(true)
    })
  ).min(1, "At least one join condition is required"),
  joinAlias: z.string().optional(),
  enableJoinHints: z.boolean().default(false),
  joinHint: z.string().optional()
});

// Filter edge schema
export const FilterEdgeConfigSchema = BaseEdgeConfigSchema.extend({
  filterConditions: z.array(
    z.object({
      id: z.string(),
      field: z.string().min(1, "Field name is required"),
      operator: z.enum(['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN', 'IS_NULL', 'NOT_NULL']),
      value: z.union([z.string(), z.number(), z.boolean()]).optional(),
      valueType: z.enum(['CONSTANT', 'FIELD', 'PARAMETER']).default('CONSTANT'),
      isActive: z.boolean().default(true),
      logicGroup: z.number().optional()
    })
  ).min(1, "At least one filter condition is required"),
  filterLogic: z.enum(['AND', 'OR']).default('AND'),
  nullHandling: z.enum(['INCLUDE', 'EXCLUDE', 'TREAT_AS_FALSE']).default('INCLUDE')
});

// Mapping edge schema (for tMap connections)
export const MappingEdgeConfigSchema = BaseEdgeConfigSchema.extend({
  mappingId: z.string(),
  fieldMappings: z.array(
    z.object({
      id: z.string(),
      sourceField: z.string().min(1, "Source field is required"),
      targetField: z.string().min(1, "Target field is required"),
      transformation: z.string().optional(),
      defaultValue: z.string().optional(),
      isRequired: z.boolean().default(false),
      validationRules: z.array(z.string()).optional()
    })
  ),
  preserveUnmappedFields: z.boolean().default(false),
  strictMapping: z.boolean().default(false)
});

// Lookup edge schema
export const LookupEdgeConfigSchema = BaseEdgeConfigSchema.extend({
  lookupKeyFields: z.array(z.string()).min(1, "At least one lookup key field is required"),
  lookupReturnFields: z.array(z.string()).min(1, "At least one return field is required"),
  lookupType: z.enum(['SIMPLE', 'RANGE', 'MULTIPLE']).default('SIMPLE'),
  lookupCacheSize: z.number().min(0).default(1000),
  lookupFailOnMissing: z.boolean().default(false),
  defaultValueStrategy: z.enum(['NULL', 'DEFAULT', 'ERROR']).default('NULL')
});

// Iterate edge schema (for flow control)
export const IterateEdgeConfigSchema = BaseEdgeConfigSchema.extend({
  iterationVariable: z.string().min(1, "Iteration variable name is required"),
  collectionField: z.string().min(1, "Collection field is required"),
  iterationType: z.enum(['FOR_EACH', 'WHILE', 'DO_WHILE']).default('FOR_EACH'),
  maxIterations: z.number().optional(),
  breakCondition: z.string().optional()
});

// Flow edge schema (generic data flow)
export const FlowEdgeConfigSchema = BaseEdgeConfigSchema.extend({
  dataFlowOrder: z.number().min(1),
  isConditional: z.boolean().default(false),
  conditionExpression: z.string().optional(),
  batchSize: z.number().optional(),
  parallelExecution: z.boolean().default(false)
});

// Union type for all edge configs
export const EdgeConfigSchema = z.discriminatedUnion('relationType', [
  z.object({ relationType: z.literal('FLOW'), config: FlowEdgeConfigSchema }),
  z.object({ relationType: z.literal('JOIN'), config: JoinEdgeConfigSchema }),
  z.object({ relationType: z.literal('FILTER'), config: FilterEdgeConfigSchema }),
  z.object({ relationType: z.literal('MAPPING'), config: MappingEdgeConfigSchema }),
  z.object({ relationType: z.literal('LOOKUP'), config: LookupEdgeConfigSchema }),
  z.object({ relationType: z.literal('ITERATE'), config: IterateEdgeConfigSchema })
]);

// Validation function with proper type handling
export const validateEdgeConfig = (edgeData: unknown): {
  isValid: boolean;
  data: z.infer<typeof EdgeConfigSchema> | null;
  errors: string[];
} => {
  try {
    const result = EdgeConfigSchema.safeParse(edgeData);
    if (result.success) {
      return {
        isValid: true,
        data: result.data,
        errors: []
      };
    } else {
      // In Zod v3+, errors are in the `issues` property, not `errors`
      return {
        isValid: false,
        data: null,
        errors: result.error.issues.map(e => 
          e.path.length > 0 
            ? `${e.path.join('.')}: ${e.message}` 
            : e.message
        )
      };
    }
  } catch (error) {
    return {
      isValid: false,
      data: null,
      errors: ['Invalid edge configuration structure']
    };
  }
};

// Type export for convenience
export type EdgeConfig = z.infer<typeof EdgeConfigSchema>;
export type FlowEdgeConfig = z.infer<typeof FlowEdgeConfigSchema>;
export type JoinEdgeConfig = z.infer<typeof JoinEdgeConfigSchema>;
export type FilterEdgeConfig = z.infer<typeof FilterEdgeConfigSchema>;
export type MappingEdgeConfig = z.infer<typeof MappingEdgeConfigSchema>;
export type LookupEdgeConfig = z.infer<typeof LookupEdgeConfigSchema>;
export type IterateEdgeConfig = z.infer<typeof IterateEdgeConfigSchema>;