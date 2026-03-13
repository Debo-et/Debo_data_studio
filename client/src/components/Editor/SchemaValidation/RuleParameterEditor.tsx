import React from 'react';
import { ValidationRule } from './schema-validation';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Textarea } from '../../ui/textarea';
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from '../../ui/select';

interface RuleParameterEditorProps {
  rule: ValidationRule;
  onChange: (updates: Partial<ValidationRule>) => void;
}

const RuleParameterEditor: React.FC<RuleParameterEditorProps> = ({ rule, onChange }) => {
  const renderParameterInputs = () => {
    switch (rule.rule) {
      case 'range':
        return (
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Minimum Value</Label>
                <Input
                  type="number"
                  value={rule.parameters?.min || ''}
                  onChange={(e) =>
                    onChange({
                      parameters: { ...rule.parameters, min: e.target.value }
                    })
                  }
                />
              </div>
              <div>
                <Label>Maximum Value</Label>
                <Input
                  type="number"
                  value={rule.parameters?.max || ''}
                  onChange={(e) =>
                    onChange({
                      parameters: { ...rule.parameters, max: e.target.value }
                    })
                  }
                />
              </div>
            </div>
            <div className="flex items-center space-x-2">
              <input
                type="checkbox"
                id="inclusive"
                checked={rule.parameters?.inclusive ?? true}
                onChange={(e) =>
                  onChange({
                    parameters: { ...rule.parameters, inclusive: e.target.checked }
                  })
                }
              />
              <Label htmlFor="inclusive">Inclusive bounds</Label>
            </div>
          </div>
        );

      case 'regex':
        return (
          <div className="space-y-3">
            <div>
              <Label>Regular Expression Pattern</Label>
              <Input
                value={rule.parameters?.pattern || ''}
                onChange={(e) =>
                  onChange({
                    parameters: { ...rule.parameters, pattern: e.target.value }
                  })
                }
                placeholder="^[a-zA-Z0-9]+$"
                className="font-mono"
              />
            </div>
            <div className="flex space-x-3">
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="caseSensitive"
                  checked={rule.parameters?.caseSensitive ?? true}
                  onChange={(e) =>
                    onChange({
                      parameters: { ...rule.parameters, caseSensitive: e.target.checked }
                    })
                  }
                />
                <Label htmlFor="caseSensitive">Case sensitive</Label>
              </div>
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  id="global"
                  checked={rule.parameters?.global ?? false}
                  onChange={(e) =>
                    onChange({
                      parameters: { ...rule.parameters, global: e.target.checked }
                    })
                  }
                />
                <Label htmlFor="global">Global match</Label>
              </div>
            </div>
            {rule.parameters?.pattern && (
              <div className="mt-2 p-2 bg-gray-50 rounded">
                <Label>Test Regex</Label>
                <Input
                  placeholder="Test input"
                  onChange={(e) => {
                    const regex = new RegExp(
                      rule.parameters.pattern,
                      `${rule.parameters.caseSensitive ? '' : 'i'}${rule.parameters.global ? 'g' : ''}`
                    );
                    console.log('Test result:', regex.test(e.target.value));
                  }}
                />
              </div>
            )}
          </div>
        );

      case 'format':
        return (
          <div className="space-y-3">
            <div>
              <Label>Format Type</Label>
              <Select
                value={rule.parameters?.format || 'custom'}
                onValueChange={(value) =>
                  onChange({
                    parameters: { ...rule.parameters, format: value }
                  })
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="email">Email Address</SelectItem>
                  <SelectItem value="date">Date (YYYY-MM-DD)</SelectItem>
                  <SelectItem value="date-time">ISO 8601 DateTime</SelectItem>
                  <SelectItem value="phone">Phone Number</SelectItem>
                  <SelectItem value="url">URL</SelectItem>
                  <SelectItem value="uuid">UUID</SelectItem>
                  <SelectItem value="custom">Custom Format</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {rule.parameters?.format === 'custom' && (
              <div>
                <Label>Custom Format Pattern</Label>
                <Input
                  value={rule.parameters?.customPattern || ''}
                  onChange={(e) =>
                    onChange({
                      parameters: { 
                        ...rule.parameters, 
                        customPattern: e.target.value 
                      }
                    })
                  }
                  placeholder="e.g., ^\\d{3}-\\d{2}-\\d{4}$"
                />
              </div>
            )}
          </div>
        );

      case 'length':
        return (
          <div className="grid grid-cols-2 gap-3">
            <div>
              <Label>Min Length</Label>
              <Input
                type="number"
                min="0"
                value={rule.parameters?.minLength || ''}
                onChange={(e) =>
                  onChange({
                    parameters: { ...rule.parameters, minLength: parseInt(e.target.value) }
                  })
                }
              />
            </div>
            <div>
              <Label>Max Length</Label>
              <Input
                type="number"
                min="0"
                value={rule.parameters?.maxLength || ''}
                onChange={(e) =>
                  onChange({
                    parameters: { ...rule.parameters, maxLength: parseInt(e.target.value) }
                  })
                }
              />
            </div>
          </div>
        );

      case 'enum':
        return (
          <div className="space-y-3">
            <Label>Allowed Values (comma-separated)</Label>
            <Textarea
              value={rule.parameters?.values?.join(', ') || ''}
              onChange={(e) =>
                onChange({
                  parameters: { 
                    ...rule.parameters, 
                    values: e.target.value.split(',').map(v => v.trim()).filter(v => v)
                  }
                })
              }
              placeholder="value1, value2, value3"
              rows={3}
            />
          </div>
        );

      case 'custom':
        return (
          <div className="space-y-3">
            <Label>Custom JavaScript Expression</Label>
            <Textarea
              value={rule.parameters?.expression || ''}
              onChange={(e) =>
                onChange({
                  parameters: { ...rule.parameters, expression: e.target.value }
                })
              }
              placeholder="return value > 0 && value < 100;"
              className="font-mono text-sm"
              rows={4}
            />
            <div className="text-sm text-gray-500">
              Use <code className="bg-gray-100 px-1">value</code> for field value
            </div>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="space-y-4">
      <div>
        <Label>Error Message Template</Label>
        <Input
          value={rule.message || ''}
          onChange={(e) => onChange({ message: e.target.value })}
          placeholder="e.g., '{field}' must be a valid {rule}"
        />
        <p className="text-xs text-gray-500 mt-1">
          Available placeholders: {'{field}'}, {'{value}'}, {'{rule}'}, {'{parameter}'}
        </p>
      </div>
      {renderParameterInputs()}
    </div>
  );
};

export default RuleParameterEditor;