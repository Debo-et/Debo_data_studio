import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../../ui/Button';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Textarea } from '../../ui/textarea';
import { X, Plus, Save, RotateCcw } from 'lucide-react';
import { CanvasNodeData, SimpleColumn } from '../../../pages/canvas.types';
import { DataMaskingComponentConfiguration, DataMaskingRule, SchemaDefinition, DataType } from '../../../types/unified-pipeline.types';

interface DataMaskingEditorProps {
  nodeId: string;
  nodeMetadata: CanvasNodeData;
  inputColumns: SimpleColumn[];
  initialConfig?: DataMaskingComponentConfiguration;
  onClose: () => void;
  onSave: (config: DataMaskingComponentConfiguration) => void;
}

const DEFAULT_RULE: Omit<DataMaskingRule, 'id' | 'position'> = {
  column: '',
  maskingType: 'REPLACE',
  parameters: {
    replaceValue: '***',
    randomType: 'STRING',
    randomLength: 10,
    hashAlgorithm: 'SHA256',
    customExpression: '',
  },
};

const generateId = () => `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const DataMaskingEditor: React.FC<DataMaskingEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  const [rules, setRules] = useState<DataMaskingRule[]>(() => {
    if (initialConfig?.rules) {
      return initialConfig.rules.map((rule, idx) => ({ ...rule, position: idx }));
    }
    return [];
  });

  // Helper to generate output schema from input columns and rules
  const generateOutputSchema = (rules: DataMaskingRule[]): SchemaDefinition => {
    const fields = inputColumns.map((col, idx) => {
      const rule = rules.find(r => r.column === col.name);
      return {
        id: `${nodeId}_${col.name}_${idx}`,
        name: col.name,
        type: col.type as DataType || 'STRING',
        nullable: true,
        isKey: false,
        description: rule ? `Masked with ${rule.maskingType}` : undefined,
      };
    });
    return {
      id: `${nodeId}_output_schema`,
      name: `${nodeMetadata.name || nodeId} Output Schema`,
      fields,
      isTemporary: false,
      isMaterialized: false,
    };
  };

  const handleAddRule = () => {
    const newRule: DataMaskingRule = {
      ...DEFAULT_RULE,
      id: generateId(),
      position: rules.length,
    };
    setRules([...rules, newRule]);
  };

  const handleRemoveRule = (id: string) => {
    setRules(rules.filter(r => r.id !== id));
  };

  const handleRuleChange = (id: string, updates: Partial<DataMaskingRule>) => {
    setRules(rules.map(r => (r.id === id ? { ...r, ...updates } : r)));
  };

  const handleSave = () => {
    const outputSchema = generateOutputSchema(rules);
    const config: DataMaskingComponentConfiguration = {
      version: '1.0',
      rules: rules.map((r, idx) => ({ ...r, position: idx })),
      outputSchema,
      sqlGeneration: {
        selectExpressions: [], // will be filled by compiler
        estimatedRowMultiplier: 1.0,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'data-masking-editor',
        ruleCount: rules.length,
        validationStatus: rules.length > 0 ? 'VALID' : 'WARNING',
        warnings: rules.length === 0 ? ['No masking rules defined'] : [],
        dependencies: [],
        compiledSql: undefined,
      },
    };
    onSave(config);
  };

  const handleReset = () => {
    setRules([]);
  };

  const renderParameters = (rule: DataMaskingRule) => {
    switch (rule.maskingType) {
      case 'REPLACE':
        return (
          <div className="flex items-center space-x-2">
            <Label className="text-xs text-gray-500">Replace with:</Label>
            <Input
              value={rule.parameters?.replaceValue || ''}
              onChange={(e) => handleRuleChange(rule.id, { parameters: { ...rule.parameters, replaceValue: e.target.value } })}
              className="w-32 h-8 text-sm"
              placeholder="***"
            />
          </div>
        );
      case 'RANDOM':
        return (
          <div className="flex items-center space-x-2">
            <Select
              value={rule.parameters?.randomType || 'STRING'}
              onValueChange={(val) => handleRuleChange(rule.id, { parameters: { ...rule.parameters, randomType: val as any } })}
            >
              <SelectTrigger className="w-28 h-8 text-sm">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="STRING">String</SelectItem>
                <SelectItem value="NUMBER">Number</SelectItem>
                <SelectItem value="UUID">UUID</SelectItem>
              </SelectContent>
            </Select>
            {rule.parameters?.randomType !== 'UUID' && (
              <Input
                type="number"
                value={rule.parameters?.randomLength || 10}
                onChange={(e) => handleRuleChange(rule.id, { parameters: { ...rule.parameters, randomLength: parseInt(e.target.value) } })}
                className="w-20 h-8 text-sm"
                min={1}
                max={100}
              />
            )}
          </div>
        );
      case 'HASH':
        return (
          <Select
            value={rule.parameters?.hashAlgorithm || 'SHA256'}
            onValueChange={(val) => handleRuleChange(rule.id, { parameters: { ...rule.parameters, hashAlgorithm: val as any } })}
          >
            <SelectTrigger className="w-32 h-8 text-sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="MD5">MD5</SelectItem>
              <SelectItem value="SHA1">SHA1</SelectItem>
              <SelectItem value="SHA256">SHA256</SelectItem>
              <SelectItem value="SHA512">SHA512</SelectItem>
            </SelectContent>
          </Select>
        );
      case 'CUSTOM':
        return (
          <Textarea
            value={rule.parameters?.customExpression || ''}
            onChange={(e) => handleRuleChange(rule.id, { parameters: { ...rule.parameters, customExpression: e.target.value } })}
            className="w-64 h-20 text-sm font-mono"
            placeholder="SQL expression (e.g., CONCAT(SUBSTRING(column,1,3), '***'))"
          />
        );
      default:
        return <span className="text-xs text-gray-400">No parameters</span>;
    }
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-5xl h-[80vh] flex flex-col"
      >
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-purple-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🔒</span>
              Data Masking Editor
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Define masking rules for columns in{' '}
              <span className="font-semibold text-purple-600">{nodeMetadata.name || nodeId}</span>
              <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {inputColumns.length} input columns • {rules.length} rules
              </span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            title="Close"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Main Content */}
        <div className="flex-1 overflow-auto p-6">
          <div className="mb-4 flex justify-between items-center">
            <h3 className="text-lg font-semibold text-gray-800">Masking Rules</h3>
            <Button variant="outline" size="sm" onClick={handleAddRule}>
              <Plus className="w-4 h-4 mr-2" />
              Add Rule
            </Button>
          </div>

          {rules.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              <p className="text-sm">No masking rules defined.</p>
              <p className="text-xs">Click "Add Rule" to start masking columns.</p>
            </div>
          ) : (
            <div className="border rounded-lg overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Masking Type</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Parameters</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider w-16">Actions</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {rules.map((rule) => (
                    <tr key={rule.id}>
                      <td className="px-4 py-2">
                        <Select
                          value={rule.column}
                          onValueChange={(val) => handleRuleChange(rule.id, { column: val })}
                        >
                          <SelectTrigger className="w-48 h-8 text-sm">
                            <SelectValue placeholder="Select column" />
                          </SelectTrigger>
                          <SelectContent>
                            {inputColumns.map(col => (
                              <SelectItem key={col.name} value={col.name}>{col.name}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </td>
                      <td className="px-4 py-2">
                        <Select
                          value={rule.maskingType}
                          onValueChange={(val) => handleRuleChange(rule.id, { maskingType: val as any })}
                        >
                          <SelectTrigger className="w-32 h-8 text-sm">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="REPLACE">Replace</SelectItem>
                            <SelectItem value="RANDOM">Random</SelectItem>
                            <SelectItem value="NULLIFY">Nullify</SelectItem>
                            <SelectItem value="HASH">Hash</SelectItem>
                            <SelectItem value="EMAIL">Email</SelectItem>
                            <SelectItem value="CREDIT_CARD">Credit Card</SelectItem>
                            <SelectItem value="PHONE">Phone</SelectItem>
                            <SelectItem value="SSN">SSN</SelectItem>
                            <SelectItem value="CUSTOM">Custom Expression</SelectItem>
                          </SelectContent>
                        </Select>
                      </td>
                      <td className="px-4 py-2">
                        {renderParameters(rule)}
                      </td>
                      <td className="px-4 py-2 text-center">
                        <button
                          onClick={() => handleRemoveRule(rule.id)}
                          className="text-red-500 hover:text-red-700"
                          title="Remove rule"
                        >
                          <X className="w-4 h-4" />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t bg-gray-50">
          <div className="text-xs text-gray-500">
            Masking rules will be applied in order. Each column can be masked only once.
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={handleReset}>
              <RotateCcw className="w-4 h-4 mr-2" />
              Reset All
            </Button>
            <Button variant="default" onClick={handleSave} className="bg-purple-600 hover:bg-purple-700">
              <Save className="w-4 h-4 mr-2" />
              Save Configuration
            </Button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default DataMaskingEditor;