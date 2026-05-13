// src/components/Editor/Quality/AddCRCRowEditor.tsx
import React, { useState } from 'react';
import { motion } from 'framer-motion';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '../../ui/card';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Button } from '../../ui/Button';
import { Switch } from '../../ui/switch';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../ui/select';
import { Checkbox } from '../../ui/checkbox';
import {
  AlertCircle,
  Check,
  X,
  Hash,
  Columns,
  Settings,
} from 'lucide-react';
import {
  AddCRCRowComponentConfiguration,
  DataType,
} from '../../../types/unified-pipeline.types';
import { SimpleColumn } from '../../../pages/canvas.types';

// Helper to map string type to DataType enum
const mapStringToDataType = (type: string): DataType => {
  const upper = type.toUpperCase();
  if (upper.includes('INT')) return 'INTEGER';
  if (upper.includes('DEC') || upper.includes('NUM')) return 'DECIMAL';
  if (upper.includes('BOOL')) return 'BOOLEAN';
  if (upper.includes('DATE')) return 'DATE';
  if (upper.includes('TIMESTAMP')) return 'TIMESTAMP';
  return 'STRING';
};

interface AddCRCRowEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: AddCRCRowComponentConfiguration;
  onClose: () => void;
  onSave: (config: AddCRCRowComponentConfiguration) => void;
}

const AddCRCRowEditor: React.FC<AddCRCRowEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // Form state
  const [includedColumns, setIncludedColumns] = useState<string[]>(
    initialConfig?.includedColumns || []
  );
  const [algorithm, setAlgorithm] = useState<'CRC32' | 'CRC16' | 'CRC8'>(
    initialConfig?.algorithm || 'CRC32'
  );
  const [outputColumnName, setOutputColumnName] = useState<string>(
    initialConfig?.outputColumnName || 'crc'
  );
  const [nullHandling, setNullHandling] = useState<
    'SKIP_ROW' | 'USE_DEFAULT' | 'TREAT_AS_EMPTY'
  >(initialConfig?.nullHandling || 'TREAT_AS_EMPTY');
  const [defaultValue, setDefaultValue] = useState<string>(
    initialConfig?.defaultValue || ''
  );
  const [characterEncoding, setCharacterEncoding] = useState<string>(
    initialConfig?.characterEncoding || 'UTF-8'
  );
  const [computeOnWholeRow, setComputeOnWholeRow] = useState<boolean>(
    initialConfig?.computeOnWholeRow || false
  );
  const [columnSeparator, setColumnSeparator] = useState<string>(
    initialConfig?.columnSeparator || ','
  );

  // Derived state
  const existingColumnNames = inputColumns.map(col => col.name);
  const isOutputNameConflicting = existingColumnNames.includes(outputColumnName);
  const isValid = outputColumnName.trim() !== '' && !isOutputNameConflicting;

  // All column names
  const allColumns = inputColumns.map(col => col.name);
  const allSelected = includedColumns.length === allColumns.length;

  const toggleAllColumns = () => {
    if (allSelected) {
      setIncludedColumns([]);
    } else {
      setIncludedColumns([...allColumns]);
    }
  };

  const toggleColumn = (colName: string, checked: boolean) => {
    if (checked) {
      setIncludedColumns(prev => [...prev, colName]);
    } else {
      setIncludedColumns(prev => prev.filter(c => c !== colName));
    }
  };

  const handleSave = () => {
    if (!isValid) return;

    // Map input columns to FieldSchema with correct DataType
    const inputFields = inputColumns.map(col => ({
      id: `${nodeId}_${col.name}`,
      name: col.name,
      type: mapStringToDataType(col.type || 'STRING'),
      nullable: true,
      isKey: false,
      description: `Original column from input`,
    }));

    const crcField = {
      id: `${nodeId}_${outputColumnName}`,
      name: outputColumnName,
      type: 'STRING' as DataType,
      nullable: false,
      isKey: false,
      description: `CRC value computed using ${algorithm}`,
    };

    const outputFields = [...inputFields, crcField];

    const config: AddCRCRowComponentConfiguration = {
      version: '1.0',
      includedColumns,
      algorithm,
      outputColumnName,
      nullHandling,
      ...(nullHandling === 'USE_DEFAULT' && { defaultValue }),
      characterEncoding,
      computeOnWholeRow,
      columnSeparator,
      outputSchema: {
        id: `${nodeId}_output_schema`,
        name: `${nodeMetadata?.name || nodeId} Output Schema`,
        fields: outputFields,
        isTemporary: false,
        isMaterialized: false,
        metadata: {},
      },
      sqlGeneration: {
        canPushDown: false,
        requiresExpression: true,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'add-crc-row-editor',
        validationStatus: isValid ? 'VALID' : 'WARNING',
        warnings: isOutputNameConflicting
          ? [`Output column "${outputColumnName}" conflicts with existing column`]
          : [],
        dependencies: includedColumns,
        compiledSql: undefined,
      },
    };

    onSave(config);
  };

  const showColumnSelection = !computeOnWholeRow;

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-3xl max-h-[90vh] flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50 rounded-t-xl">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <Hash className="mr-2 h-5 w-5 text-blue-600" />
              tAddCRCRow Configuration
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Add a CRC (Cyclic Redundancy Check) value to each row
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
          >
            <X className="h-5 w-5 text-gray-500" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Algorithm and Output Column */}
          <div className="grid grid-cols-2 gap-6">
            <Card className="border-gray-200">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center">
                  <Settings className="h-4 w-4 mr-1" />
                  CRC Algorithm
                </CardTitle>
              </CardHeader>
              <CardContent>
                <Select value={algorithm} onValueChange={(val: any) => setAlgorithm(val)}>
                  <SelectTrigger>
                    <SelectValue placeholder="Select algorithm" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="CRC32">CRC-32 (32-bit)</SelectItem>
                    <SelectItem value="CRC16">CRC-16 (16-bit)</SelectItem>
                    <SelectItem value="CRC8">CRC-8 (8-bit)</SelectItem>
                  </SelectContent>
                </Select>
              </CardContent>
            </Card>

            <Card className="border-gray-200">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium flex items-center">
                  <Columns className="h-4 w-4 mr-1" />
                  Output Column Name
                </CardTitle>
              </CardHeader>
              <CardContent>
                <Input
                  value={outputColumnName}
                  onChange={(e) => setOutputColumnName(e.target.value)}
                  placeholder="e.g., crc"
                  className={isOutputNameConflicting ? 'border-red-500' : ''}
                />
                {isOutputNameConflicting && (
                  <p className="text-xs text-red-500 mt-1 flex items-center">
                    <AlertCircle className="h-3 w-3 mr-1" />
                    Column name already exists in input
                  </p>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Whole Row vs. Column Selection */}
          <Card className="border-gray-200">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">
                Compute Over
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex items-center justify-between">
                <Label htmlFor="computeOnWholeRow">Compute CRC on entire row</Label>
                <Switch
                  id="computeOnWholeRow"
                  checked={computeOnWholeRow}
                  onCheckedChange={setComputeOnWholeRow}
                />
              </div>
              {computeOnWholeRow && (
                <div className="mt-2">
                  <Label htmlFor="columnSeparator">Column Separator</Label>
                  <Input
                    id="columnSeparator"
                    value={columnSeparator}
                    onChange={(e) => setColumnSeparator(e.target.value)}
                    placeholder="e.g., , (comma)"
                    className="mt-1"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Values from all columns will be concatenated using this separator
                    before CRC calculation.
                  </p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Column Selection (if not whole row) */}
          {showColumnSelection && (
            <Card className="border-gray-200">
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm font-medium flex items-center">
                    <Columns className="h-4 w-4 mr-1" />
                    Columns to Include
                  </CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={toggleAllColumns}
                    className="text-xs"
                  >
                    {allSelected ? 'Deselect All' : 'Select All'}
                  </Button>
                </div>
                <CardDescription className="text-xs">
                  Choose which columns to use in CRC calculation
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-2 max-h-48 overflow-y-auto border rounded-md p-2">
                  {inputColumns.map(col => (
                    <div key={col.name} className="flex items-center space-x-2">
                      <Checkbox
                        id={`col-${col.name}`}
                        checked={includedColumns.includes(col.name)}
                        onChange={(e) => toggleColumn(col.name, e.target.checked)}
                      />
                      <Label
                        htmlFor={`col-${col.name}`}
                        className="text-sm cursor-pointer"
                      >
                        {col.name}
                        <span className="ml-1 text-xs text-gray-400">
                          ({col.type || 'string'})
                        </span>
                      </Label>
                    </div>
                  ))}
                </div>
                {includedColumns.length === 0 && !computeOnWholeRow && (
                  <p className="text-xs text-yellow-600 mt-2">
                    No columns selected. CRC will be computed on an empty string.
                  </p>
                )}
              </CardContent>
            </Card>
          )}

          {/* Null Handling */}
          <Card className="border-gray-200">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">
                Null Handling
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <Select value={nullHandling} onValueChange={(val: any) => setNullHandling(val)}>
                <SelectTrigger>
                  <SelectValue placeholder="Select handling" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="SKIP_ROW">Skip row (no output)</SelectItem>
                  <SelectItem value="USE_DEFAULT">Use default value</SelectItem>
                  <SelectItem value="TREAT_AS_EMPTY">Treat as empty string</SelectItem>
                </SelectContent>
              </Select>

              {nullHandling === 'USE_DEFAULT' && (
                <div>
                  <Label htmlFor="defaultValue">Default Value</Label>
                  <Input
                    id="defaultValue"
                    value={defaultValue}
                    onChange={(e) => setDefaultValue(e.target.value)}
                    placeholder="e.g., 0 or empty string"
                    className="mt-1"
                  />
                </div>
              )}

              <div>
                <Label htmlFor="characterEncoding">Character Encoding</Label>
                <Input
                  id="characterEncoding"
                  value={characterEncoding}
                  onChange={(e) => setCharacterEncoding(e.target.value)}
                  placeholder="UTF-8"
                  className="mt-1"
                />
                <p className="text-xs text-gray-500 mt-1">
                  Encoding used to convert strings to bytes before CRC calculation
                </p>
              </div>
            </CardContent>
          </Card>

          {/* Output Schema Preview (informative) */}
          <Card className="border-gray-200 bg-gray-50">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">
                Output Schema Preview
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-xs space-y-1">
                <div className="font-medium text-gray-700">Input columns:</div>
                <div className="ml-2 text-gray-600">
                  {inputColumns.map(col => col.name).join(', ') || '(none)'}
                </div>
                <div className="font-medium text-gray-700 mt-2">Added column:</div>
                <div className="ml-2 text-green-600">
                  {outputColumnName || 'crc'} (STRING) – CRC value
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Footer */}
        <div className="flex justify-end gap-3 p-6 border-t bg-gray-50 rounded-b-xl">
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button
            variant="default"
            onClick={handleSave}
            disabled={!isValid}
            className="bg-blue-600 hover:bg-blue-700"
          >
            <Check className="h-4 w-4 mr-2" />
            Save Configuration
          </Button>
        </div>
      </motion.div>
    </div>
  );
};

export default AddCRCRowEditor;