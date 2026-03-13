// components/RuleBuilder.tsx
import React, { useState } from 'react';
import { ValidationRule, ValidationRuleType, SchemaField } from './schema-validation';
import { 
  Card, CardContent, 
  Button, Input, Label, Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
  Switch, Badge} from '../../ui';
import RuleParameterEditor from './RuleParameterEditor';

interface RuleBuilderProps {
  rules: ValidationRule[];
  schema: SchemaField[];
  onChange: (rules: ValidationRule[]) => void;
  selectedField?: string | null;
}

const RULE_TYPES: Array<{ value: ValidationRuleType; label: string; description: string }> = [
  { value: 'required', label: 'Required', description: 'Field must be present' },
  { value: 'type', label: 'Type', description: 'Validate data type' },
  { value: 'format', label: 'Format', description: 'Validate specific format' },
  { value: 'range', label: 'Range', description: 'Numeric range validation' },
  { value: 'regex', label: 'Regex', description: 'Regular expression pattern' },
  { value: 'length', label: 'Length', description: 'String length constraints' },
  { value: 'enum', label: 'Enum', description: 'Allowed values list' },
  { value: 'unique', label: 'Unique', description: 'Ensure unique values' },
  { value: 'custom', label: 'Custom', description: 'Custom validation logic' },
];

const RuleBuilder: React.FC<RuleBuilderProps> = ({ 
  rules, 
  schema, 
  onChange,
  selectedField 
}) => {
  const [editingRule, setEditingRule] = useState<string | null>(null);

  const handleAddRule = () => {
    const newRule: ValidationRule = {
      id: `rule_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      field: selectedField || schema[0]?.name || '',
      rule: 'required',
      message: '',
      enabled: true,
      severity: 'error'
    };
    onChange([...rules, newRule]);
    setEditingRule(newRule.id);
  };

  const handleUpdateRule = (ruleId: string, updates: Partial<ValidationRule>) => {
    const updated = rules.map(rule => 
      rule.id === ruleId ? { ...rule, ...updates } : rule
    );
    onChange(updated);
  };

  const handleRemoveRule = (ruleId: string) => {
    const updated = rules.filter(rule => rule.id !== ruleId);
    onChange(updated);
  };


  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-semibold">Validation Rules</h3>
        <Button onClick={handleAddRule} variant="outline" size="sm">
          + Add Rule
        </Button>
      </div>

      <div className="space-y-3">
        {rules.map((rule) => (
          <Card key={rule.id} className="overflow-hidden">
            <CardContent className="p-4">
              <div className="flex justify-between items-start mb-3">
                <div className="flex-1">
                  <div className="flex items-center space-x-2 mb-2">
                    <Badge variant={rule.severity === 'error' ? 'destructive' : 'secondary'}>
                      {rule.severity}
                    </Badge>
                    <Badge variant="outline">
                      {rule.enabled ? 'Enabled' : 'Disabled'}
                    </Badge>
                  </div>
                  
                  <div className="grid grid-cols-3 gap-3">
                    <div>
                      <Label className="text-xs text-gray-500">Field</Label>
                      <Select
                        value={rule.field}
                        onValueChange={(value) => 
                          handleUpdateRule(rule.id, { field: value })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {schema.map(field => (
                            <SelectItem key={field.name} value={field.name}>
                              {field.name} ({field.type})
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <Label className="text-xs text-gray-500">Rule Type</Label>
                      <Select
                        value={rule.rule}
                        onValueChange={(value: ValidationRuleType) => 
                          handleUpdateRule(rule.id, { rule: value })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {RULE_TYPES.map(type => (
                            <SelectItem key={type.value} value={type.value}>
                              {type.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div>
                      <Label className="text-xs text-gray-500">Severity</Label>
                      <Select
                        value={rule.severity}
                        onValueChange={(value: 'error' | 'warning') => 
                          handleUpdateRule(rule.id, { severity: value })
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="error">Error</SelectItem>
                          <SelectItem value="warning">Warning</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </div>

                <div className="flex space-x-2 ml-4">
                  <Switch
                    checked={rule.enabled}
                    onCheckedChange={(checked) => 
                      handleUpdateRule(rule.id, { enabled: checked })
                    }
                  />
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleRemoveRule(rule.id)}
                  >
                    Remove
                  </Button>
                </div>
              </div>

              {editingRule === rule.id && (
                <div className="mt-4 border-t pt-4">
                  <RuleParameterEditor
                    rule={rule}
                    onChange={(updates) => handleUpdateRule(rule.id, updates)}
                  />
                </div>
              )}

              <div className="mt-3">
                <Input
                  value={rule.message || ''}
                  onChange={(e) => 
                    handleUpdateRule(rule.id, { message: e.target.value })
                  }
                  placeholder="Error message (optional)"
                />
              </div>

              <div className="mt-3 flex justify-end">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => 
                    setEditingRule(editingRule === rule.id ? null : rule.id)
                  }
                >
                  {editingRule === rule.id ? 'Hide Details' : 'Edit Details'}
                </Button>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {rules.length === 0 && (
        <div className="text-center py-8 text-gray-500">
          No rules defined. Add rules to validate your data.
        </div>
      )}
    </div>
  );
};

export default RuleBuilder;