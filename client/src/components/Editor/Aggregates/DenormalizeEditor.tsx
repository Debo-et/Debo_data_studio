// src/components/Editor/DenormalizeEditor.tsx
import React, { useState, useEffect } from 'react';
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
import { Checkbox } from '../../ui/checkbox';   // fixed import (removed trailing ;)
import { Button } from '../../ui/Button';
import { Badge } from '../../ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../../ui/select';
import { RadioGroup, RadioGroupItem } from '../../ui/radio-group';
import { X, Save, AlertCircle } from 'lucide-react';

// ----------------------------------------------------------------------
// Types (should be imported from unified-pipeline.types.ts later)
// ----------------------------------------------------------------------
export interface DenormalizeComponentConfiguration {
  version: string;
  sourceColumn: string;
  delimiter: string;
  trimValues: boolean;
  treatEmptyAsNull: boolean;
  quoteChar?: string;
  escapeChar?: string;
  outputColumnName: string;
  addRowNumber: boolean;
  rowNumberColumnName?: string;
  keepColumns: string[];
  errorHandling: 'fail' | 'skip' | 'setNull';
  batchSize?: number;
  parallelization: boolean;
  sqlGeneration?: {
    unnestExpression: string;
  };
  compilerMetadata?: {
    lastModified: string;
    createdBy: string;
    validationStatus: 'VALID' | 'WARNING' | 'ERROR';
    warnings?: string[];
  };
}

export interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// ----------------------------------------------------------------------
// Preset delimiters
// ----------------------------------------------------------------------
const DELIMITER_PRESETS = [
  { label: 'Comma', value: ',' },
  { label: 'Pipe', value: '|' },
  { label: 'Tab', value: '\t' },
  { label: 'Space', value: ' ' },
  { label: 'Semicolon', value: ';' },
];

// ----------------------------------------------------------------------
// Main Component
// ----------------------------------------------------------------------
interface DenormalizeEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: DenormalizeComponentConfiguration;
  onClose: () => void;
  onSave: (config: DenormalizeComponentConfiguration) => void;
}

export const DenormalizeEditor: React.FC<DenormalizeEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // --------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------
  const [sourceColumn, setSourceColumn] = useState(
    initialConfig?.sourceColumn || (inputColumns[0]?.name ?? '')
  );
  const [delimiter, setDelimiter] = useState(initialConfig?.delimiter || ',');
  const [trimValues, setTrimValues] = useState(initialConfig?.trimValues ?? true);
  const [treatEmptyAsNull, setTreatEmptyAsNull] = useState(
    initialConfig?.treatEmptyAsNull ?? false
  );
  const [quoteChar, setQuoteChar] = useState(initialConfig?.quoteChar || '');
  const [escapeChar, setEscapeChar] = useState(initialConfig?.escapeChar || '');
  const [outputColumnName, setOutputColumnName] = useState(
    initialConfig?.outputColumnName || 'denormalized_value'
  );
  const [addRowNumber, setAddRowNumber] = useState(
    initialConfig?.addRowNumber ?? false
  );
  const [rowNumberColumnName, setRowNumberColumnName] = useState(
    initialConfig?.rowNumberColumnName || 'row_index'
  );
  const [keepColumns, setKeepColumns] = useState<string[]>(
    initialConfig?.keepColumns || inputColumns.map((c) => c.name)
  );
  const [errorHandling, setErrorHandling] = useState<
    'fail' | 'skip' | 'setNull'
  >(initialConfig?.errorHandling || 'fail');
  const [batchSize, setBatchSize] = useState<number>(
    initialConfig?.batchSize || 1000
  );
  const [parallelization, setParallelization] = useState(
    initialConfig?.parallelization ?? false
  );

  // Validation state
  const [errors, setErrors] = useState<{ [key: string]: string }>({});

  // --------------------------------------------------------------------
  // Derived values
  // --------------------------------------------------------------------
  const keptColumnsCount = keepColumns.length;

  // --------------------------------------------------------------------
  // Validation
  // --------------------------------------------------------------------
  const validate = (): boolean => {
    const newErrors: { [key: string]: string } = {};
    if (!sourceColumn) newErrors.sourceColumn = 'Source column is required';
    if (!delimiter) newErrors.delimiter = 'Delimiter is required';
    if (!outputColumnName) newErrors.outputColumnName = 'Output column name is required';
    if (addRowNumber && !rowNumberColumnName)
      newErrors.rowNumberColumnName = 'Row number column name is required';
    if (batchSize !== undefined && (batchSize < 1 || batchSize > 10000))
      newErrors.batchSize = 'Batch size must be between 1 and 10000';
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  // --------------------------------------------------------------------
  // Handlers
  // --------------------------------------------------------------------
  const handleKeepAll = () => {
    setKeepColumns(inputColumns.map((c) => c.name));
  };

  const handleKeepNone = () => {
    setKeepColumns([]);
  };

  const toggleKeepColumn = (colName: string) => {
    setKeepColumns((prev) =>
      prev.includes(colName)
        ? prev.filter((c) => c !== colName)
        : [...prev, colName]
    );
  };

  const handleSave = () => {
    if (!validate()) return;

    const config: DenormalizeComponentConfiguration = {
      version: '1.0',
      sourceColumn,
      delimiter,
      trimValues,
      treatEmptyAsNull,
      quoteChar: quoteChar || undefined,
      escapeChar: escapeChar || undefined,
      outputColumnName,
      addRowNumber,
      rowNumberColumnName: addRowNumber ? rowNumberColumnName : undefined,
      keepColumns,
      errorHandling,
      batchSize,
      parallelization,
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'denormalize-editor',
        validationStatus: 'VALID',
        warnings: [],
      },
    };
    onSave(config);
  };

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
      if (e.key === 's' && e.ctrlKey) {
        e.preventDefault();
        handleSave();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose, handleSave]);

  // --------------------------------------------------------------------
  // Render
  // --------------------------------------------------------------------
  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-900 rounded-xl shadow-2xl w-full max-w-5xl max-h-[90vh] flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50 dark:from-gray-800 dark:to-gray-900">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🗂️</span>
              tDenormalize Configuration
            </h2>
            <p className="text-sm text-gray-600 dark:text-gray-400 mt-1">
              Node: <span className="font-semibold text-blue-600 dark:text-blue-400">
                {nodeMetadata?.name || nodeId}
              </span>
              <Badge variant="outline" className="ml-3">
                {inputColumns.length} input columns
              </Badge>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-full transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Main content - scrollable */}
        <div className="flex-1 overflow-y-auto p-6">
          <div className="grid grid-cols-2 gap-8">
            {/* Left column: Basic settings */}
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Source & Output</CardTitle>
                  <CardDescription>
                    Select the column to denormalize and name the result.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Source Column */}
                  <div className="space-y-2">
                    <Label htmlFor="sourceColumn">Source Column *</Label>
                    <Select
                      value={sourceColumn}
                      onValueChange={setSourceColumn}
                    >
                      <SelectTrigger id="sourceColumn">
                        <SelectValue placeholder="Select column" />
                      </SelectTrigger>
                      <SelectContent>
                        {inputColumns.map((col) => (
                          <SelectItem key={col.name} value={col.name}>
                            {col.name} {col.type && `(${col.type})`}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    {errors.sourceColumn && (
                      <p className="text-xs text-red-500 flex items-center mt-1">
                        <AlertCircle className="h-3 w-3 mr-1" />
                        {errors.sourceColumn}
                      </p>
                    )}
                  </div>

                  {/* Delimiter */}
                  <div className="space-y-2">
                    <Label htmlFor="delimiter">Delimiter *</Label>
                    <div className="flex gap-2">
                      <Input
                        id="delimiter"
                        value={delimiter}
                        onChange={(e) => setDelimiter(e.target.value)}
                        placeholder="e.g., ,"
                        className="flex-1"
                      />
                      <div className="flex gap-1">
                        {DELIMITER_PRESETS.map((preset) => (
                          <Button
                            key={preset.value}
                            variant="outline"
                            size="sm"
                            onClick={() => setDelimiter(preset.value)}
                            className="text-xs px-2"
                          >
                            {preset.label}
                          </Button>
                        ))}
                      </div>
                    </div>
                    {errors.delimiter && (
                      <p className="text-xs text-red-500 flex items-center mt-1">
                        <AlertCircle className="h-3 w-3 mr-1" />
                        {errors.delimiter}
                      </p>
                    )}
                  </div>

                  {/* Output Column Name */}
                  <div className="space-y-2">
                    <Label htmlFor="outputColumn">Output Column Name *</Label>
                    <Input
                      id="outputColumn"
                      value={outputColumnName}
                      onChange={(e) => setOutputColumnName(e.target.value)}
                      placeholder="denormalized_value"
                    />
                    {errors.outputColumnName && (
                      <p className="text-xs text-red-500 flex items-center mt-1">
                        <AlertCircle className="h-3 w-3 mr-1" />
                        {errors.outputColumnName}
                      </p>
                    )}
                  </div>

                  {/* Keep Columns */}
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <Label>Keep Columns</Label>
                      <div className="space-x-2">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={handleKeepAll}
                          className="text-xs"
                        >
                          Select All
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={handleKeepNone}
                          className="text-xs"
                        >
                          Deselect All
                        </Button>
                      </div>
                    </div>
                    <div className="border rounded p-3 max-h-40 overflow-y-auto space-y-2">
                      {inputColumns.map((col) => (
                        <div key={col.name} className="flex items-center space-x-2">
                          <Checkbox
                            id={`keep-${col.name}`}
                            checked={keepColumns.includes(col.name)}
                            onChange={() => toggleKeepColumn(col.name)}  // changed
                          />
                          <Label
                            htmlFor={`keep-${col.name}`}
                            className="text-sm cursor-pointer"
                          >
                            {col.name} {col.type && <span className="text-gray-500">({col.type})</span>}
                          </Label>
                        </div>
                      ))}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Right column: Advanced options */}
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Parsing Options</CardTitle>
                  <CardDescription>
                    Control how the delimited values are processed.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="trim"
                      checked={trimValues}
                      onChange={(e) => setTrimValues(e.target.checked)}   // changed
                    />
                    <Label htmlFor="trim" className="text-sm">
                      Trim whitespace from values
                    </Label>
                  </div>

                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="treatEmpty"
                      checked={treatEmptyAsNull}
                      onChange={(e) => setTreatEmptyAsNull(e.target.checked)} // changed
                    />
                    <Label htmlFor="treatEmpty" className="text-sm">
                      Treat empty strings as NULL
                    </Label>
                  </div>

                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="quoteChar">Quote Character</Label>
                      <Input
                        id="quoteChar"
                        value={quoteChar}
                        onChange={(e) => setQuoteChar(e.target.value)}
                        placeholder="e.g., \\"
                                                maxLength={1}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="escapeChar">Escape Character</Label>
                      <Input
                        id="escapeChar"
                        value={escapeChar}
                        onChange={(e) => setEscapeChar(e.target.value)}
                        placeholder="e.g., \\"
                        maxLength={1}
                      />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Additional Columns</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="addRowNumber"
                      checked={addRowNumber}
                      onChange={(e) => setAddRowNumber(e.target.checked)}   // changed
                    />
                    <Label htmlFor="addRowNumber" className="text-sm">
                      Add row number column
                    </Label>
                  </div>

                  {addRowNumber && (
                    <div className="space-y-2 pl-6">
                      <Label htmlFor="rowNumberName">Row Number Column Name</Label>
                      <Input
                        id="rowNumberName"
                        value={rowNumberColumnName}
                        onChange={(e) => setRowNumberColumnName(e.target.value)}
                        placeholder="row_index"
                      />
                      {errors.rowNumberColumnName && (
                        <p className="text-xs text-red-500 flex items-center mt-1">
                          <AlertCircle className="h-3 w-3 mr-1" />
                          {errors.rowNumberColumnName}
                        </p>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Execution</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2">
                    <Label>Error Handling</Label>
                    <RadioGroup
                      value={errorHandling}
                      onValueChange={(val: any) => setErrorHandling(val)}
                      className="flex space-x-4"
                    >
                      <div className="flex items-center space-x-2">
                        <RadioGroupItem value="fail" id="fail" />
                        <Label htmlFor="fail">Fail</Label>
                      </div>
                      <div className="flex items-center space-x-2">
                        <RadioGroupItem value="skip" id="skip" />
                        <Label htmlFor="skip">Skip Row</Label>
                      </div>
                      <div className="flex items-center space-x-2">
                        <RadioGroupItem value="setNull" id="setNull" />
                        <Label htmlFor="setNull">Set NULL</Label>
                      </div>
                    </RadioGroup>
                  </div>

                  <div className="space-y-2">
                    <Label htmlFor="batchSize">Batch Size</Label>
                    <Input
                      id="batchSize"
                      type="number"
                      min={1}
                      max={10000}
                      value={batchSize}
                      onChange={(e) => setBatchSize(parseInt(e.target.value) || 1000)}
                    />
                    {errors.batchSize && (
                      <p className="text-xs text-red-500 flex items-center mt-1">
                        <AlertCircle className="h-3 w-3 mr-1" />
                        {errors.batchSize}
                      </p>
                    )}
                  </div>

                  <div className="flex items-center space-x-2">
                    <Checkbox
                      id="parallel"
                      checked={parallelization}
                      onChange={(e) => setParallelization(e.target.checked)} // changed
                    />
                    <Label htmlFor="parallel" className="text-sm">
                      Enable parallel processing
                    </Label>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>

          {/* Schema Preview */}
          <Card className="mt-6">
            <CardHeader>
              <CardTitle className="text-base">Output Schema Preview</CardTitle>
              <CardDescription>
                {keptColumnsCount} kept column(s) + denormalized column
                {addRowNumber ? ' + row number column' : ''}
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded text-sm font-mono border">
                <div className="grid grid-cols-4 gap-2 text-gray-500 border-b pb-2 mb-2">
                  <div>Column Name</div>
                  <div>Source</div>
                  <div>Type</div>
                  <div>Description</div>
                </div>
                {keepColumns.map((colName) => {
                  const col = inputColumns.find((c) => c.name === colName);
                  return (
                    <div key={colName} className="grid grid-cols-4 gap-2">
                      <div className="font-medium">{colName}</div>
                      <div>Kept</div>
                      <div>{col?.type || 'unknown'}</div>
                      <div className="text-gray-500">original column</div>
                    </div>
                  );
                })}
                <div className="grid grid-cols-4 gap-2 text-blue-600 dark:text-blue-400">
                  <div className="font-medium">{outputColumnName}</div>
                  <div>Denormalized</div>
                  <div>string</div>
                  <div>values from {sourceColumn}</div>
                </div>
                {addRowNumber && (
                  <div className="grid grid-cols-4 gap-2 text-green-600 dark:text-green-400">
                    <div className="font-medium">{rowNumberColumnName}</div>
                    <div>Row number</div>
                    <div>integer</div>
                    <div>index within exploded array</div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t bg-gray-50 dark:bg-gray-800">
          <div className="text-xs text-gray-500">
            Press <kbd className="px-1 py-0.5 bg-white border rounded">Ctrl+S</kbd> to save
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button onClick={handleSave} className="bg-blue-600 hover:bg-blue-700">
              <Save className="h-4 w-4 mr-2" />
              Save Configuration
            </Button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};