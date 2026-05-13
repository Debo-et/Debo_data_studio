// src/components/Editor/PivotToColumnsDelimitedEditor.tsx
import React, { useState, useEffect } from 'react';
import { X, AlertCircle, Check } from 'lucide-react';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Button } from '../../ui/Button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../ui/select';
import { Switch } from '../../ui/switch';
import { Checkbox } from '../../ui/checkbox';
import { UnifiedCanvasNode } from '../../../types/unified-pipeline.types';

// Local SimpleColumn definition (matches the one used in Canvas.tsx)
interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// This configuration type should eventually be imported from unified-pipeline.types
// after adding it there. For now, we define it here.
export interface PivotToColumnsDelimitedConfiguration {
  version: string;
  sourceColumn: string;
  delimiter: string;
  keyValueSeparator: string;
  columnGeneration: 'fromKeys' | 'fixedList';
  fixedColumns?: string[];
  missingKeyHandling: 'omit' | 'null' | 'default';
  defaultValue?: string;
  valueType: 'string' | 'integer' | 'decimal' | 'date' | 'boolean';
  columnPrefix?: string;
  trimWhitespace: boolean;
  caseSensitiveKeys: boolean;
  errorHandling: 'fail' | 'skip' | 'setNull';
  parallelization: boolean;
  batchSize?: number;
  compilerMetadata?: {
    lastModified: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
  };
}

interface PivotToColumnsDelimitedEditorProps {
  nodeId: string;
  nodeMetadata: UnifiedCanvasNode;
  inputColumns: SimpleColumn[];
  initialConfig?: PivotToColumnsDelimitedConfiguration;
  onClose: () => void;
  onSave: (config: PivotToColumnsDelimitedConfiguration) => void;
}

export const PivotToColumnsDelimitedEditor: React.FC<PivotToColumnsDelimitedEditorProps> = ({
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // Default configuration
  const defaultConfig: PivotToColumnsDelimitedConfiguration = {
    version: '1.0',
    sourceColumn: inputColumns[0]?.name || '',
    delimiter: ',',
    keyValueSeparator: ':',
    columnGeneration: 'fromKeys',
    fixedColumns: [],
    missingKeyHandling: 'null',
    defaultValue: '',
    valueType: 'string',
    columnPrefix: '',
    trimWhitespace: true,
    caseSensitiveKeys: false,
    errorHandling: 'fail',
    parallelization: false,
    batchSize: 1000,
    compilerMetadata: {
      lastModified: new Date().toISOString(),
      validationStatus: 'VALID',
      warnings: [],
    },
  };

  const [config, setConfig] = useState<PivotToColumnsDelimitedConfiguration>(
    initialConfig || defaultConfig
  );
  const [errors, setErrors] = useState<string[]>([]);
  const [warnings, setWarnings] = useState<string[]>([]);

  // Validate config on changes
  useEffect(() => {
    const newErrors: string[] = [];
    const newWarnings: string[] = [];

    if (!config.sourceColumn) {
      newErrors.push('Source column is required.');
    } else if (!inputColumns.some(c => c.name === config.sourceColumn)) {
      newErrors.push(`Source column "${config.sourceColumn}" does not exist in input schema.`);
    }

    if (!config.delimiter) {
      newErrors.push('Pair delimiter cannot be empty.');
    }
    if (!config.keyValueSeparator) {
      newErrors.push('Key-value separator cannot be empty.');
    }

    if (config.columnGeneration === 'fixedList') {
      if (!config.fixedColumns || config.fixedColumns.length === 0) {
        newWarnings.push('No fixed columns defined. Output will have no columns.');
      }
    }

    if (config.missingKeyHandling === 'default' && !config.defaultValue) {
      newWarnings.push('Default value is empty. Missing keys will be set to empty string.');
    }

    setErrors(newErrors);
    setWarnings(newWarnings);
  }, [config, inputColumns]);

  const handleChange = <K extends keyof PivotToColumnsDelimitedConfiguration>(
    key: K,
    value: PivotToColumnsDelimitedConfiguration[K]
  ) => {
    setConfig(prev => ({ ...prev, [key]: value }));
  };

  const handleAddFixedColumn = () => {
    const newColumn = `col_${(config.fixedColumns?.length || 0) + 1}`;
    setConfig(prev => ({
      ...prev,
      fixedColumns: [...(prev.fixedColumns || []), newColumn],
    }));
  };

  const handleRemoveFixedColumn = (index: number) => {
    setConfig(prev => ({
      ...prev,
      fixedColumns: prev.fixedColumns?.filter((_, i) => i !== index),
    }));
  };

  const handleFixedColumnChange = (index: number, value: string) => {
    const updated = [...(config.fixedColumns || [])];
    updated[index] = value;
    setConfig(prev => ({ ...prev, fixedColumns: updated }));
  };

  const handleSave = () => {
    if (errors.length > 0) return;
    // Update compiler metadata
    const savedConfig: PivotToColumnsDelimitedConfiguration = {
      ...config,
      compilerMetadata: {
        ...config.compilerMetadata,
        lastModified: new Date().toISOString(),
        validationStatus: errors.length === 0 ? 'VALID' : 'ERROR',
        warnings: warnings.length > 0 ? warnings : undefined,
      },
    };
    onSave(savedConfig);
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <div className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">📊</span>
              Pivot to Columns (Delimited)
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Configure how delimited key‑value pairs are pivoted into columns.
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Validation warnings/errors */}
          {(errors.length > 0 || warnings.length > 0) && (
            <div className="mb-4 space-y-2">
              {errors.map((err, i) => (
                <div key={`err-${i}`} className="bg-red-50 text-red-800 border border-red-200 rounded-lg p-3 flex items-start">
                  <AlertCircle className="w-5 h-5 mr-2 flex-shrink-0 mt-0.5" />
                  <span className="text-sm">{err}</span>
                </div>
              ))}
              {warnings.map((warn, i) => (
                <div key={`warn-${i}`} className="bg-yellow-50 text-yellow-800 border border-yellow-200 rounded-lg p-3 flex items-start">
                  <AlertCircle className="w-5 h-5 mr-2 flex-shrink-0 mt-0.5" />
                  <span className="text-sm">{warn}</span>
                </div>
              ))}
            </div>
          )}

          <div className="space-y-6">
            {/* Source Column */}
            <div className="space-y-2">
              <Label htmlFor="sourceColumn">Source Column *</Label>
              <Select
                value={config.sourceColumn}
                onValueChange={(val) => handleChange('sourceColumn', val)}
              >
                <SelectTrigger id="sourceColumn" className="bg-gray-50">
                  <SelectValue placeholder="Select a column" />
                </SelectTrigger>
                <SelectContent>
                  {inputColumns.map(col => (
                    <SelectItem key={col.name} value={col.name}>
                      {col.name} {col.type && <span className="text-gray-500 ml-2">({col.type})</span>}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Delimiters (two inputs side by side) */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="delimiter">Pair Delimiter</Label>
                <Input
                  id="delimiter"
                  value={config.delimiter}
                  onChange={(e) => handleChange('delimiter', e.target.value)}
                  placeholder="e.g., ,"
                  className="bg-gray-50"
                />
                <p className="text-xs text-gray-500">Separates key‑value pairs</p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="keyValueSeparator">Key‑Value Separator</Label>
                <Input
                  id="keyValueSeparator"
                  value={config.keyValueSeparator}
                  onChange={(e) => handleChange('keyValueSeparator', e.target.value)}
                  placeholder="e.g., :"
                  className="bg-gray-50"
                />
                <p className="text-xs text-gray-500">Separates key from value</p>
              </div>
            </div>

            {/* Column Generation */}
            <div className="space-y-2">
              <Label>Column Generation Method</Label>
              <div className="flex space-x-4">
                <label className="flex items-center space-x-2">
                  <input
                    type="radio"
                    name="columnGeneration"
                    value="fromKeys"
                    checked={config.columnGeneration === 'fromKeys'}
                    onChange={() => handleChange('columnGeneration', 'fromKeys')}
                  />
                  <span>From keys in data (dynamic)</span>
                </label>
                <label className="flex items-center space-x-2">
                  <input
                    type="radio"
                    name="columnGeneration"
                    value="fixedList"
                    checked={config.columnGeneration === 'fixedList'}
                    onChange={() => handleChange('columnGeneration', 'fixedList')}
                  />
                  <span>Fixed list of columns</span>
                </label>
              </div>
            </div>

            {/* Fixed columns list (conditional) */}
            {config.columnGeneration === 'fixedList' && (
              <div className="space-y-2 border rounded-lg p-4 bg-gray-50">
                <div className="flex items-center justify-between">
                  <Label>Fixed Columns</Label>
                  <Button variant="outline" size="sm" onClick={handleAddFixedColumn}>
                    Add Column
                  </Button>
                </div>
                {config.fixedColumns && config.fixedColumns.length > 0 ? (
                  <div className="space-y-2">
                    {config.fixedColumns.map((col, idx) => (
                      <div key={idx} className="flex items-center space-x-2">
                        <Input
                          value={col}
                          onChange={(e) => handleFixedColumnChange(idx, e.target.value)}
                          className="flex-1 bg-white"
                          placeholder="Column name"
                        />
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleRemoveFixedColumn(idx)}
                          className="text-red-500 hover:text-red-700"
                        >
                          ✕
                        </Button>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-gray-500">No fixed columns defined.</p>
                )}
              </div>
            )}

            {/* Missing Key Handling */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="missingKeyHandling">Missing Key Handling</Label>
                <Select
                  value={config.missingKeyHandling}
                  onValueChange={(val: any) => handleChange('missingKeyHandling', val)}
                >
                  <SelectTrigger id="missingKeyHandling" className="bg-gray-50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="omit">Omit column</SelectItem>
                    <SelectItem value="null">Set NULL</SelectItem>
                    <SelectItem value="default">Use default value</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {config.missingKeyHandling === 'default' && (
                <div className="space-y-2">
                  <Label htmlFor="defaultValue">Default Value</Label>
                  <Input
                    id="defaultValue"
                    value={config.defaultValue || ''}
                    onChange={(e) => handleChange('defaultValue', e.target.value)}
                    className="bg-gray-50"
                  />
                </div>
              )}
            </div>

            {/* Value Type & Prefix */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="valueType">Value Data Type</Label>
                <Select
                  value={config.valueType}
                  onValueChange={(val: any) => handleChange('valueType', val)}
                >
                  <SelectTrigger id="valueType" className="bg-gray-50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="string">String</SelectItem>
                    <SelectItem value="integer">Integer</SelectItem>
                    <SelectItem value="decimal">Decimal</SelectItem>
                    <SelectItem value="date">Date</SelectItem>
                    <SelectItem value="boolean">Boolean</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label htmlFor="columnPrefix">Column Prefix (optional)</Label>
                <Input
                  id="columnPrefix"
                  value={config.columnPrefix || ''}
                  onChange={(e) => handleChange('columnPrefix', e.target.value)}
                  placeholder="e.g., pivot_"
                  className="bg-gray-50"
                />
              </div>
            </div>

            {/* Data cleaning options */}
            <div className="space-y-3 border rounded-lg p-4 bg-gray-50">
              <h3 className="font-medium">Data Cleaning</h3>
              <div className="flex items-center space-x-6">
                <label className="flex items-center space-x-2">
                  <Checkbox
                    checked={config.trimWhitespace}
                    onChange={(e) => handleChange('trimWhitespace', e.target.checked)}
                  />
                  <span className="text-sm">Trim whitespace</span>
                </label>
                <label className="flex items-center space-x-2">
                  <Checkbox
                    checked={config.caseSensitiveKeys}
                    onChange={(e) => handleChange('caseSensitiveKeys', e.target.checked)}
                  />
                  <span className="text-sm">Case‑sensitive keys</span>
                </label>
              </div>
            </div>

            {/* Advanced Section */}
            <details className="border rounded-lg p-4">
              <summary className="font-medium cursor-pointer">Advanced Options</summary>
              <div className="mt-4 space-y-4">
                {/* Error handling */}
                <div className="space-y-2">
                  <Label htmlFor="errorHandling">On error</Label>
                  <Select
                    value={config.errorHandling}
                    onValueChange={(val: any) => handleChange('errorHandling', val)}
                  >
                    <SelectTrigger id="errorHandling" className="bg-gray-50">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="fail">Fail job</SelectItem>
                      <SelectItem value="skip">Skip row</SelectItem>
                      <SelectItem value="setNull">Set NULL</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                {/* Parallel execution */}
                <div className="flex items-center justify-between">
                  <Label htmlFor="parallelization">Parallel execution</Label>
                  <Switch
                    id="parallelization"
                    checked={config.parallelization}
                    onCheckedChange={(checked) => handleChange('parallelization', checked)}
                  />
                </div>

                {config.parallelization && (
                  <div className="space-y-2">
                    <Label htmlFor="batchSize">Batch size</Label>
                    <Input
                      id="batchSize"
                      type="number"
                      min="1"
                      value={config.batchSize || 1000}
                      onChange={(e) => handleChange('batchSize', parseInt(e.target.value) || 1000)}
                      className="bg-gray-50"
                    />
                  </div>
                )}

                {/* Read-only metadata */}
                {config.compilerMetadata && (
                  <div className="text-xs text-gray-500 border-t pt-2 mt-2">
                    <p>Last modified: {config.compilerMetadata.lastModified}</p>
                    {config.compilerMetadata.warnings && config.compilerMetadata.warnings.length > 0 && (
                      <p className="text-yellow-600">Warnings: {config.compilerMetadata.warnings.join(', ')}</p>
                    )}
                  </div>
                )}
              </div>
            </details>
          </div>
        </div>

        {/* Footer */}
        <div className="flex justify-end items-center p-6 border-t bg-gray-50 space-x-3">
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button
            onClick={handleSave}
            disabled={errors.length > 0}
            className="bg-blue-600 hover:bg-blue-700 text-white"
          >
            <Check className="w-4 h-4 mr-2" />
            Save Configuration
          </Button>
        </div>
      </div>
    </div>
  );
};