import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Label } from '../../ui/label';
import { Input } from '../../ui/input';
import { Button } from '../../ui/Button';
import { Switch } from '../../ui/switch';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { SampleRowComponentConfiguration } from '../../../types/unified-pipeline.types';
import { SimpleColumn } from '../../../pages/canvas.types';

interface SampleRowEditorProps {
  nodeId: string;
  nodeName: string;
  inputColumns: SimpleColumn[];
  initialConfig?: SampleRowComponentConfiguration;
  onClose: () => void;
  onSave: (config: SampleRowComponentConfiguration) => void;
}

const SampleRowEditor: React.FC<SampleRowEditorProps> = ({
  nodeId,
  nodeName,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // Default config
  const defaultConfig: SampleRowComponentConfiguration = {
    version: '1.0',
    samplingMethod: 'firstRows',
    sampleValue: 10,
    randomSeed: 42,
    ignoreEmptyRows: false,
    includeHeader: true,
    outputSchema: {
      id: `${nodeId}_output_schema`,
      name: `${nodeName} Output Schema`,
      fields: inputColumns.map((col, idx) => ({
        id: `${nodeId}_${col.name}_${idx}`,
        name: col.name,
        type: col.type as any || 'STRING',
        nullable: true,
        isKey: false,
      })),
      isTemporary: false,
      isMaterialized: false,
    },
    sqlGeneration: {
      estimatedRowMultiplier: 0.1,
    },
    compilerMetadata: {
      lastModified: new Date().toISOString(),
      createdBy: 'canvas',
      validationStatus: 'VALID',
      warnings: [],
      dependencies: [],
    },
  };

  const [formData, setFormData] = useState<SampleRowComponentConfiguration>(() => {
    if (initialConfig) return initialConfig;
    return defaultConfig;
  });

  // Helper to update specific fields
  const updateField = <K extends keyof SampleRowComponentConfiguration>(
    key: K,
    value: SampleRowComponentConfiguration[K]
  ) => {
    setFormData(prev => ({ ...prev, [key]: value }));
  };

  const handleSave = () => {
    // Ensure metadata is updated
    const updatedConfig: SampleRowComponentConfiguration = {
      ...formData,
      compilerMetadata: {
        ...formData.compilerMetadata,
        lastModified: new Date().toISOString(),
      },
    };
    onSave(updatedConfig);
  };

  const renderSamplingMethodUI = () => {
    switch (formData.samplingMethod) {
      case 'firstRows':
        return (
          <div className="space-y-2">
            <Label htmlFor="sampleValue">Number of rows</Label>
            <Input
              id="sampleValue"
              type="number"
              min={1}
              value={formData.sampleValue}
              onChange={(e) => updateField('sampleValue', parseInt(e.target.value) || 1)}
              className="bg-gray-800 border-gray-700 text-white"
            />
            <p className="text-xs text-gray-400">
              The first N rows of the input will be kept.
            </p>
          </div>
        );
      case 'percentage':
        return (
          <div className="space-y-2">
            <Label htmlFor="sampleValue">Percentage (%)</Label>
            <Input
              id="sampleValue"
              type="number"
              min={0.1}
              max={100}
              step={0.1}
              value={formData.sampleValue}
              onChange={(e) => updateField('sampleValue', parseFloat(e.target.value))}
              className="bg-gray-800 border-gray-700 text-white"
            />
            <p className="text-xs text-gray-400">
              Approximately this percentage of rows will be kept.
            </p>
          </div>
        );
      case 'random':
        return (
          <>
            <div className="space-y-2">
              <Label htmlFor="sampleValue">Sample size (rows)</Label>
              <Input
                id="sampleValue"
                type="number"
                min={1}
                value={formData.sampleValue}
                onChange={(e) => updateField('sampleValue', parseInt(e.target.value) || 1)}
                className="bg-gray-800 border-gray-700 text-white"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="randomSeed">Random seed (optional)</Label>
              <Input
                id="randomSeed"
                type="number"
                value={formData.randomSeed ?? ''}
                onChange={(e) => updateField('randomSeed', e.target.value ? parseInt(e.target.value) : undefined)}
                className="bg-gray-800 border-gray-700 text-white"
                placeholder="Leave blank for random"
              />
              <p className="text-xs text-gray-400">
                A seed ensures reproducible sampling.
              </p>
            </div>
          </>
        );
      default:
        return null;
    }
  };

  return (
    <div className="fixed inset-0 z-[10000] bg-black bg-opacity-80 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: -20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 20 }}
        transition={{ duration: 0.2 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-lg font-bold text-gray-800 flex items-center">
              <span className="mr-2">📊</span>
              Sample Row Configuration
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold text-blue-600">{nodeName}</span>
              <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {inputColumns.length} input columns
              </span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            title="Close"
          >
            ✕
          </button>
        </div>

        {/* Main content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Sampling Method */}
          <Card className="border-gray-200">
            <CardHeader>
              <CardTitle className="text-sm font-medium">Sampling Method</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label>Method</Label>
                <Select
                  value={formData.samplingMethod}
                  onValueChange={(val: 'firstRows' | 'percentage' | 'random') =>
                    updateField('samplingMethod', val)
                  }
                >
                  <SelectTrigger className="bg-gray-800 border-gray-700 text-white">
                    <SelectValue placeholder="Select sampling method" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="firstRows">First N rows</SelectItem>
                    <SelectItem value="percentage">Percentage of rows</SelectItem>
                    <SelectItem value="random">Random sampling</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {renderSamplingMethodUI()}
            </CardContent>
          </Card>

          {/* Advanced Options */}
          <Card className="border-gray-200">
            <CardHeader>
              <CardTitle className="text-sm font-medium">Advanced Options</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center justify-between">
                <Label htmlFor="ignoreEmptyRows" className="text-sm">
                  Ignore empty rows
                </Label>
                <Switch
                  id="ignoreEmptyRows"
                  checked={formData.ignoreEmptyRows}
                  onCheckedChange={(checked) => updateField('ignoreEmptyRows', checked)}
                />
              </div>
              <div className="flex items-center justify-between">
                <Label htmlFor="includeHeader" className="text-sm">
                  Include header row (if present)
                </Label>
                <Switch
                  id="includeHeader"
                  checked={formData.includeHeader}
                  onCheckedChange={(checked) => updateField('includeHeader', checked)}
                />
              </div>
            </CardContent>
          </Card>

          {/* Input Schema Preview */}
          <Card className="border-gray-200">
            <CardHeader>
              <CardTitle className="text-sm font-medium">Input Schema</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="max-h-48 overflow-y-auto border rounded">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="px-3 py-2 text-left">Column</th>
                      <th className="px-3 py-2 text-left">Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {inputColumns.map((col, idx) => (
                      <tr key={idx} className="border-t">
                        <td className="px-3 py-1 font-mono">{col.name}</td>
                        <td className="px-3 py-1 text-gray-500">{col.type || 'string'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="text-xs text-gray-500 mt-2">
                Output schema will inherit the same columns.
              </p>
            </CardContent>
          </Card>

          {/* Validation messages */}
          {formData.compilerMetadata.warnings && formData.compilerMetadata.warnings.length > 0 && (
            <Card className="border-yellow-200 bg-yellow-50">
              <CardContent className="p-3">
                <div className="text-sm text-yellow-800">
                  ⚠️ Warnings:
                  <ul className="list-disc list-inside text-xs mt-1">
                    {formData.compilerMetadata.warnings.map((w, i) => (
                      <li key={i}>{w}</li>
                    ))}
                  </ul>
                </div>
              </CardContent>
            </Card>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-4 border-t bg-gray-50">
          <div className="text-xs text-gray-600">
            Sampling will be applied at runtime.
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button variant="default" onClick={handleSave}>
              Save Configuration
            </Button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default SampleRowEditor;