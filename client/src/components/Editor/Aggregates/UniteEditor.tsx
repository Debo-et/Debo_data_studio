import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { 
  Card, 
  CardContent, 
  CardHeader, 
  CardTitle,
  CardDescription 
} from '../../ui/card';
import { Label } from '../../ui/label';
import { RadioGroup, RadioGroupItem } from '../../ui/radio-group';
import { Checkbox } from '../../ui/checkbox';
import { Input } from '../../ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Button } from '../../ui/Button';
import { AlertCircle, Info, GitMerge } from 'lucide-react';
import { DataType } from '../../../types/metadata';
import { UniteComponentConfiguration } from '../../../types/unified-pipeline.types';

// Helper to map string to DataType

interface InputSchema {
  id: string;
  name: string;
  fields: Array<{ name: string; type: string; nullable: boolean }>;
}

interface UniteEditorProps {
  nodeId: string;
  nodeMetadata: any; // UnifiedCanvasNode
  inputSchemas: InputSchema[];
  initialConfig?: UniteComponentConfiguration;
  onClose: () => void;
  onSave: (config: UniteComponentConfiguration) => void;
}

export const UniteEditor: React.FC<UniteEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputSchemas,
  initialConfig,
  onClose,
  onSave
}) => {
  // State
  const [unionMode, setUnionMode] = useState<'ALL' | 'DISTINCT'>(
    initialConfig?.unionMode || 'ALL'
  );
  const [addSourceColumn, setAddSourceColumn] = useState(
    initialConfig?.addSourceColumn || false
  );
  const [sourceColumnName, setSourceColumnName] = useState(
    initialConfig?.sourceColumnName || 'source_flow'
  );
  const [sourceColumnType, setSourceColumnType] = useState<DataType>(
    initialConfig?.sourceColumnType || 'STRING'
  );
  const [schemaHandling, setSchemaHandling] = useState<'strict' | 'flexible'>(
    initialConfig?.schemaHandling || 'strict'
  );

  // Validation warnings
  const [warnings, setWarnings] = useState<string[]>([]);

  // Check schema compatibility
  useEffect(() => {
    const newWarnings: string[] = [];

    if (inputSchemas.length === 0) {
      newWarnings.push('No input flows connected. tUnite requires at least one input.');
    }

    if (schemaHandling === 'strict' && inputSchemas.length > 1) {
      // Compare all schemas
      const firstSchema = inputSchemas[0];
      for (let i = 1; i < inputSchemas.length; i++) {
        const other = inputSchemas[i];
        // Compare field names and types
        const firstFields = firstSchema.fields.map(f => `${f.name}:${f.type}`);
        const otherFields = other.fields.map(f => `${f.name}:${f.type}`);
        if (firstFields.join(',') !== otherFields.join(',')) {
          newWarnings.push(
            `Schema mismatch between "${firstSchema.name}" and "${other.name}". ` +
            'With strict mode, all input schemas must be identical.'
          );
          break;
        }
      }
    }

    if (addSourceColumn && sourceColumnName.trim() === '') {
      newWarnings.push('Source column name cannot be empty.');
    } else if (addSourceColumn) {
      // Check for duplicate column names with existing fields
      const allFieldNames = new Set<string>();
      inputSchemas.forEach(schema => {
        schema.fields.forEach(f => allFieldNames.add(f.name));
      });
      if (allFieldNames.has(sourceColumnName)) {
        newWarnings.push(
          `Source column name "${sourceColumnName}" conflicts with an existing column.`
        );
      }
    }

    setWarnings(newWarnings);
  }, [inputSchemas, schemaHandling, addSourceColumn, sourceColumnName]);

  // Build output schema preview
  const outputFields = () => {
    if (inputSchemas.length === 0) return [];
    if (schemaHandling === 'strict') {
      return inputSchemas[0].fields.map(f => f.name);
    } else {
      // Union of all field names
      const fieldSet = new Set<string>();
      inputSchemas.forEach(schema => {
        schema.fields.forEach(f => fieldSet.add(f.name));
      });
      return Array.from(fieldSet);
    }
  };

  const handleSave = () => {
    if (warnings.some(w => w.includes('must') || w.includes('cannot'))) {
      // Block save if critical errors
      alert('Please fix configuration errors before saving.');
      return;
    }

    const config: UniteComponentConfiguration = {
      version: '1.0',
      unionMode,
      addSourceColumn,
      sourceColumnName: addSourceColumn ? sourceColumnName : undefined,
      sourceColumnType: addSourceColumn ? sourceColumnType : undefined,
      schemaHandling,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'unite-editor',
        validationStatus: warnings.length > 0 ? 'WARNING' : 'VALID',
        warnings: warnings,
        dependencies: inputSchemas.map(s => s.id)
      }
    };
    onSave(config);
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b bg-gradient-to-r from-indigo-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <GitMerge className="mr-2 text-indigo-600" />
              Unite Configuration
              <span className="ml-2 text-xs bg-indigo-100 text-indigo-800 px-2 py-0.5 rounded">
                tUnite
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold">{nodeMetadata?.name || nodeId}</span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Input Flows Summary */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Input Flows ({inputSchemas.length})</CardTitle>
              <CardDescription>
                Flows connected to this unite component
              </CardDescription>
            </CardHeader>
            <CardContent>
              {inputSchemas.length === 0 ? (
                <div className="text-amber-600 bg-amber-50 p-3 rounded flex items-start">
                  <AlertCircle className="h-5 w-5 mr-2 flex-shrink-0 mt-0.5" />
                  <span className="text-sm">No input flows connected. Please connect at least one source.</span>
                </div>
              ) : (
                <div className="space-y-3">
                  {inputSchemas.map((schema) => (
                    <div key={schema.id} className="border rounded p-3 bg-gray-50">
                      <div className="flex items-center justify-between mb-2">
                        <span className="font-medium">{schema.name}</span>
                        <span className="text-xs text-gray-500">{schema.fields.length} columns</span>
                      </div>
                      <div className="text-xs text-gray-600 grid grid-cols-3 gap-1">
                        {schema.fields.slice(0, 6).map(f => (
                          <span key={f.name} className="truncate" title={`${f.name} (${f.type})`}>
                            {f.name} <span className="text-gray-400">({f.type})</span>
                          </span>
                        ))}
                        {schema.fields.length > 6 && (
                          <span className="text-gray-400">+{schema.fields.length - 6} more</span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Union Settings */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Union Settings</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <Label className="text-sm font-medium mb-2 block">Union Mode</Label>
                <RadioGroup
                  value={unionMode}
                  onValueChange={(val: 'ALL' | 'DISTINCT') => setUnionMode(val)}
                  className="flex space-x-6"
                >
                  <div className="flex items-center space-x-2">
                    <RadioGroupItem value="ALL" id="all" />
                    <Label htmlFor="all" className="text-sm">Include duplicates (ALL)</Label>
                  </div>
                  <div className="flex items-center space-x-2">
                    <RadioGroupItem value="DISTINCT" id="distinct" />
                    <Label htmlFor="distinct" className="text-sm">Remove duplicates (DISTINCT)</Label>
                  </div>
                </RadioGroup>
              </div>

              <div>
                <Label className="text-sm font-medium mb-2 block">Schema Handling</Label>
                <RadioGroup
                  value={schemaHandling}
                  onValueChange={(val: 'strict' | 'flexible') => setSchemaHandling(val)}
                  className="flex space-x-6"
                >
                  <div className="flex items-center space-x-2">
                    <RadioGroupItem value="strict" id="strict" />
                    <Label htmlFor="strict" className="text-sm">Strict – all schemas must match</Label>
                  </div>
                  <div className="flex items-center space-x-2">
                    <RadioGroupItem value="flexible" id="flexible" />
                    <Label htmlFor="flexible" className="text-sm">Flexible – union of all columns</Label>
                  </div>
                </RadioGroup>
                {schemaHandling === 'flexible' && (
                  <div className="mt-2 text-xs text-gray-500 bg-blue-50 p-2 rounded flex items-start">
                    <Info className="h-4 w-4 mr-1 text-blue-500 flex-shrink-0 mt-0.5" />
                    <span>
                      Columns missing in some flows will be filled with NULL. Output schema will be the union of all columns.
                    </span>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Source Identifier */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Source Identifier</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center space-x-2">
                <Checkbox
                  id="addSource"
                  checked={addSourceColumn}
                  onChange={(e) => setAddSourceColumn(e.target.checked)}
                />
                <Label htmlFor="addSource" className="text-sm font-medium">
                  Add a column identifying the source flow
                </Label>
              </div>

              {addSourceColumn && (
                <div className="ml-6 grid grid-cols-2 gap-4 mt-2">
                  <div>
                    <Label htmlFor="sourceName" className="text-xs">Column Name</Label>
                    <Input
                      id="sourceName"
                      value={sourceColumnName}
                      onChange={(e) => setSourceColumnName(e.target.value)}
                      placeholder="e.g., source_flow"
                      className="mt-1"
                    />
                  </div>
                  <div>
                    <Label htmlFor="sourceType" className="text-xs">Data Type</Label>
                    <Select
                      value={sourceColumnType}
                      onValueChange={(val: DataType) => setSourceColumnType(val)}
                    >
                      <SelectTrigger className="mt-1">
                        <SelectValue placeholder="Select type" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="STRING">STRING</SelectItem>
                        <SelectItem value="INTEGER">INTEGER</SelectItem>
                        <SelectItem value="DECIMAL">DECIMAL</SelectItem>
                        <SelectItem value="BOOLEAN">BOOLEAN</SelectItem>
                        <SelectItem value="DATE">DATE</SelectItem>
                        <SelectItem value="TIMESTAMP">TIMESTAMP</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Output Schema Preview */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Output Schema Preview</CardTitle>
            </CardHeader>
            <CardContent>
              {inputSchemas.length === 0 ? (
                <p className="text-sm text-gray-500">No inputs – output schema empty.</p>
              ) : (
                <div>
                  <div className="text-sm font-medium mb-2">
                    {outputFields().length} columns
                    {addSourceColumn && (
                      <span className="ml-2 text-xs text-indigo-600">
                        +1 source column
                      </span>
                    )}
                  </div>
                  <div className="bg-gray-50 p-3 rounded max-h-32 overflow-y-auto text-xs">
                    <div className="grid grid-cols-3 gap-1">
                      {outputFields().map(field => (
                        <span key={field} className="truncate font-mono">{field}</span>
                      ))}
                      {addSourceColumn && (
                        <span className="truncate font-mono text-indigo-600">
                          {sourceColumnName} ({sourceColumnType})
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Warnings */}
          {warnings.length > 0 && (
            <div className="bg-yellow-50 border border-yellow-200 rounded p-3">
              <h4 className="text-sm font-medium text-yellow-800 flex items-center mb-2">
                <AlertCircle className="h-4 w-4 mr-1" />
                Configuration Warnings
              </h4>
              <ul className="text-xs text-yellow-700 list-disc list-inside space-y-1">
                {warnings.map((w, i) => (
                  <li key={i}>{w}</li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t bg-gray-50">
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button 
            onClick={handleSave}
            disabled={inputSchemas.length === 0 || warnings.some(w => w.includes('cannot'))}
            className="bg-gradient-to-r from-indigo-500 to-indigo-600 hover:from-indigo-600 hover:to-indigo-700"
          >
            Save Configuration
          </Button>
        </div>
      </motion.div>
    </div>
  );
};