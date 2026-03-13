// joinUtils.ts
import { JoinConfiguration, InputSchemas } from './JoinConfigEditor';

export const validateConfiguration = (
  config: JoinConfiguration,
  schemas: InputSchemas
): { isValid: boolean; errors: Record<string, string> } => {
  const errors: Record<string, string> = {};

  // Validate key arrays have same length
  if (config.leftKeys.length !== config.rightKeys.length) {
    errors.keyLength = 'Left and right key arrays must have the same length';
  }

  // Validate keys exist in respective schemas
  config.leftKeys.forEach((key, index) => {
    if (!schemas.left.some(field => field.name === key)) {
      errors[`leftKey_${index}`] = `Key "${key}" not found in left input schema`;
    }
  });

  config.rightKeys.forEach((key, index) => {
    if (!schemas.right.some(field => field.name === key)) {
      errors[`rightKey_${index}`] = `Key "${key}" not found in right input schema`;
    }
  });

  // Validate cross join doesn't have keys
  if (config.joinType === 'CROSS' && (config.leftKeys.length > 0 || config.rightKeys.length > 0)) {
    errors.crossJoinKeys = 'Cross join should not have key pairs defined';
  }

  // Validate other join types have at least one key pair
  if (config.joinType !== 'CROSS' && config.leftKeys.length === 0) {
    errors.noKeys = `${config.joinType} join requires at least one key pair`;
  }

  // Validate prefix configuration
  if (config.prefixAliases) {
    if (!config.prefixLeft?.trim() && !config.prefixRight?.trim()) {
      errors.prefixes = 'Prefix aliases are enabled but both prefixes are empty';
    }
  }

  return {
    isValid: Object.keys(errors).length === 0,
    errors
  };
};

export const exportConfiguration = (config: JoinConfiguration): string => {
  return JSON.stringify({
    ...config,
    _metadata: {
      exportDate: new Date().toISOString(),
      version: '1.0.0'
    }
  }, null, 2);
};

export const importConfiguration = (
  jsonString: string,
  defaultConfig: JoinConfiguration
): JoinConfiguration => {
  try {
    const parsed = JSON.parse(jsonString);
    
    // Validate required fields
    const requiredFields = ['joinType', 'leftKeys', 'rightKeys'];
    for (const field of requiredFields) {
      if (!(field in parsed)) {
        throw new Error(`Missing required field: ${field}`);
      }
    }

    // Merge with defaults for optional fields
    return {
      ...defaultConfig,
      ...parsed
    };
  } catch (error) {
    throw new Error(`Invalid configuration JSON: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
};

export const generateSampleConfiguration = (): JoinConfiguration => ({
  joinType: 'INNER',
  leftKeys: ['id', 'customer_id'],
  rightKeys: ['id', 'user_id'],
  filterExpression: "left.status = 'active' AND right.type = 'premium'",
  prefixAliases: true,
  prefixLeft: 'l_',
  prefixRight: 'r_',
  nullEquality: true
});

export const getFieldTypeIcon = (type: string): string => {
  const icons: Record<string, string> = {
    string: 'ABC',
    number: '123',
    boolean: '✓',
    date: '📅',
    timestamp: '⏰',
    array: '[]',
    object: '{}'
  };
  return icons[type] || '?';
};