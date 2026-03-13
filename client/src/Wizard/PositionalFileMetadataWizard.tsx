import React, { useState, useRef, useCallback } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  Table,
  AlertCircle,
  FileText,
  Settings,
  Ruler,
  Plus,
  Trash2,
  Eye
} from 'lucide-react';
import {
  PositionalFileMetadataFormData,
  PositionalFileMetadataWizardProps,
  PositionalColumnDefinition
} from '../types/types';

const PositionalFileMetadataWizard: React.FC<PositionalFileMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<PositionalFileMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    encoding: 'UTF-8',
    schema: [],
    sampleData: [],
    recordLength: 0,
    hasHeaders: false
  });

  const [, setFileContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [previewConfig, setPreviewConfig] = useState({
    previewLines: 10,
    showLineNumbers: true
  });
  const [selectedLine, setSelectedLine] = useState<number | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 5;

  // File encodings
  const encodings = [
    'UTF-8',
    'ISO-8859-1',
    'Windows-1252',
    'ASCII',
    'UTF-16'
  ];

  // Data types for dropdown
  const dataTypes = ['String', 'Integer', 'Decimal', 'Date', 'Boolean'];

  // Handle file selection
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const validExtensions = ['.txt', '.dat', '.prn', '.fixed', '.fw'];
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please select a valid positional file (.txt, .dat, .prn, .fixed, .fw)');
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        setFileContent(content);
        
        // Calculate record length from first few lines
        const lines = content.split('\n').filter(line => line.trim() !== '');
        const recordLength = lines.length > 0 ? Math.max(...lines.slice(0, 10).map(line => line.length)) : 0;

        // Get sample data
        const sampleData = lines.slice(0, previewConfig.previewLines);

        updateFormData({
          file,
          filePath: file.name,
          recordLength,
          sampleData
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to read file: ${err.message}`);
        setIsLoading(false);
      }
    };

    reader.onerror = () => {
      setError('Failed to read file. The file may be corrupted or in an unsupported format.');
      setIsLoading(false);
    };

    reader.readAsText(file, formData.encoding);
  };

  // Parse positional data based on column definitions
  const parsePositionalData = useCallback((data: string[], schema: PositionalColumnDefinition[]): string[][] => {
    return data.map(line => {
      return schema.map(column => {
        // Extract the substring for this column
        // Note: positions are 1-based in the UI, but 0-based in code
        const start = column.start - 1;
        const end = column.start - 1 + column.length;
        
        if (line.length < start) {
          return ''; // Column starts beyond line length
        }
        
        const value = line.substring(start, Math.min(end, line.length));
        return value.trim();
      });
    });
  }, []);

  // Add a new column definition
  const addColumn = () => {
    const newSchema = [...formData.schema];
    const lastColumn = newSchema[newSchema.length - 1];
    const nextStart = lastColumn ? lastColumn.start + lastColumn.length : 1;
    
    newSchema.push({
      name: `Column${newSchema.length + 1}`,
      type: 'String',
      start: nextStart,
      length: 10,
      description: ''
    });
    
    updateFormData({ schema: newSchema });
  };

  // Update column definition
  const updateColumn = (index: number, field: keyof PositionalColumnDefinition, value: any) => {
    const newSchema = [...formData.schema];
    
    if (field === 'start' || field === 'length') {
      const numValue = parseInt(value) || 1;
      newSchema[index] = { ...newSchema[index], [field]: numValue };
      
      // Validate and adjust positions to prevent overlaps
      if (field === 'start' && index > 0) {
        const prevColumn = newSchema[index - 1];
        if (numValue < prevColumn.start + prevColumn.length) {
          newSchema[index].start = prevColumn.start + prevColumn.length;
        }
      }
      
      if (field === 'length' && index < newSchema.length - 1) {
        const nextColumn = newSchema[index + 1];
        if (numValue + newSchema[index].start > nextColumn.start) {
          newSchema[index + 1].start = newSchema[index].start + numValue;
        }
      }
    } else {
      newSchema[index] = { ...newSchema[index], [field]: value };
    }
    
    updateFormData({ schema: newSchema });
  };

  // Remove column definition
  const removeColumn = (index: number) => {
    const newSchema = formData.schema.filter((_, i) => i !== index);
    updateFormData({ schema: newSchema });
  };

  // Auto-detect columns based on data patterns
  const autoDetectColumns = () => {
    if (!formData.sampleData || formData.sampleData.length === 0) return;

    const sampleLines = formData.sampleData.slice(0, 5);
    const lineLength = Math.max(...sampleLines.map(line => line.length));
    
    // Simple algorithm: look for consistent patterns in the first few lines
    const columnBreaks: number[] = [0];
    
    // Analyze each position to find potential column breaks
    for (let pos = 1; pos < lineLength - 1; pos++) {
      let isBreak = true;
      
      // Check if this position consistently has spaces or specific patterns
      for (const line of sampleLines) {
        if (line.length > pos) {
          const char = line[pos];
          const prevChar = line[pos - 1];
          // Consider it a break if we see space surrounded by non-spaces, or consistent pattern changes
          if (!(char === ' ' && prevChar !== ' ') && !(char !== ' ' && prevChar === ' ')) {
            isBreak = false;
            break;
          }
        }
      }
      
      if (isBreak) {
        columnBreaks.push(pos);
      }
    }
    
    columnBreaks.push(lineLength);
    
    // Create column definitions from breaks
    const detectedSchema: PositionalColumnDefinition[] = [];
    for (let i = 0; i < columnBreaks.length - 1; i++) {
      const start = columnBreaks[i] + 1; // Convert to 1-based for UI
      const length = columnBreaks[i + 1] - columnBreaks[i];
      
      detectedSchema.push({
        name: `Column${i + 1}`,
        type: 'String',
        start,
        length,
        description: `Auto-detected column ${i + 1}`
      });
    }
    
    updateFormData({ schema: detectedSchema });
  };

  // Update form data
  const updateFormData = (updates: Partial<PositionalFileMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Update preview configuration
  const updatePreviewConfig = (updates: Partial<typeof previewConfig>) => {
    setPreviewConfig(prev => ({ ...prev, ...updates }));
  };

  const handleNext = () => {
    if (currentStep < totalSteps) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleSave = () => {
    // Validate that we have at least one column before saving
    if (formData.schema.length === 0) {
      setError('Please define at least one column before saving.');
      setCurrentStep(3); // Go back to column definition step
      return;
    }
    
    onSave(formData);
    handleClose();
  };

  const handleClose = () => {
    onClose();
    // Reset state
    setCurrentStep(1);
    setFormData({
      name: '',
      purpose: '',
      description: '',
      file: null,
      filePath: '',
      encoding: 'UTF-8',
      schema: [],
      sampleData: [],
      recordLength: 0,
      hasHeaders: false
    });
    setFileContent('');
    setError(null);
    setSelectedLine(null);
  };


  // Render ruler for visual position reference
  const renderRuler = (length: number) => {
    const rulerNumbers = [];
    const rulerTicks = [];
    
    for (let i = 1; i <= length; i++) {
      rulerNumbers.push(
        <div key={`num-${i}`} className="w-4 text-center text-xs text-gray-500 border-r border-gray-300">
          {i % 5 === 0 ? i : ''}
        </div>
      );
      rulerTicks.push(
        <div key={`tick-${i}`} className="w-4 text-center text-xs text-gray-400 border-r border-gray-200">
          {i % 5 === 0 ? '┼' : '│'}
        </div>
      );
    }
    
    return (
      <div className="bg-gray-50 dark:bg-gray-700 border-b border-gray-200 dark:border-gray-600">
        <div className="flex font-mono text-xs">
          {rulerNumbers}
        </div>
        <div className="flex font-mono text-xs">
          {rulerTicks}
        </div>
      </div>
    );
  };

  // Render data preview with column overlays
  const renderDataPreview = () => {
    if (!formData.sampleData || formData.sampleData.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <Table className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No data available for preview</p>
          </div>
        </div>
      );
    }

    const maxLineLength = Math.max(...formData.sampleData.map(line => line.length));
    const parsedData = formData.schema.length > 0 ? 
      parsePositionalData(formData.sampleData, formData.schema) : [];

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <div className="flex justify-between items-center">
            <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
              Data Preview ({formData.sampleData.length} rows)
            </h4>
            <div className="flex items-center space-x-2">
              <span className="text-xs text-gray-500 dark:text-gray-400">
                Record length: {formData.recordLength} chars
              </span>
            </div>
          </div>
        </div>

        {/* Column overlays visualization */}
        {formData.schema.length > 0 && (
          <div className="bg-blue-50 dark:bg-blue-900/20 border-b border-blue-200 dark:border-blue-800 p-2">
            <div className="flex items-center space-x-2 mb-2">
              <Ruler className="h-4 w-4 text-blue-600" />
              <span className="text-sm font-medium text-blue-700 dark:text-blue-300">
                Column Layout
              </span>
            </div>
            <div className="relative h-8 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded">
              {formData.schema.map((column, index) => {
                const left = `${((column.start - 1) / maxLineLength) * 100}%`;
                const width = `${(column.length / maxLineLength) * 100}%`;
                
                return (
                  <div
                    key={index}
                    className="absolute h-full border-l border-r border-blue-500 bg-blue-100 dark:bg-blue-900/30 opacity-70"
                    style={{ left, width }}
                    title={`${column.name} (${column.start}-${column.start + column.length - 1})`}
                  >
                    <div className="text-xs text-blue-700 dark:text-blue-300 truncate px-1">
                      {column.name}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        <div className="overflow-x-auto max-h-96">
          {/* Ruler */}
          {renderRuler(maxLineLength)}
          
          {/* Data lines */}
          <div className="font-mono text-sm">
            {formData.sampleData.map((line, lineIndex) => (
              <div
                key={lineIndex}
                className={`flex border-b border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer ${
                  selectedLine === lineIndex ? 'bg-blue-50 dark:bg-blue-900/20' : ''
                }`}
                onClick={() => setSelectedLine(lineIndex)}
              >
                {previewConfig.showLineNumbers && (
                  <div className="w-12 bg-gray-50 dark:bg-gray-800 text-right pr-2 py-1 text-xs text-gray-500 border-r border-gray-200 dark:border-gray-600">
                    {lineIndex + 1}
                  </div>
                )}
                <div className="flex-1 min-w-0">
                  <div className="whitespace-pre py-1 px-2">
                    {line}
                    {/* Show end of line if shorter than max length */}
                    {line.length < maxLineLength && (
                      <span className="text-gray-300 dark:text-gray-600">
                        {'·'.repeat(maxLineLength - line.length)}
                      </span>
                    )}
                  </div>
                  
                  {/* Show parsed values when columns are defined */}
                  {formData.schema.length > 0 && parsedData[lineIndex] && (
                    <div className="bg-gray-50 dark:bg-gray-800 border-t border-gray-200 dark:border-gray-600 px-2 py-1 text-xs">
                      <div className="text-gray-600 dark:text-gray-400 mb-1">Parsed:</div>
                      <div className="flex flex-wrap gap-2">
                        {parsedData[lineIndex].map((value, colIndex) => (
                          <div key={colIndex} className="bg-white dark:bg-gray-700 px-2 py-1 rounded border">
                            <span className="font-medium text-gray-700 dark:text-gray-300">
                              {formData.schema[colIndex].name}:
                            </span>
                            <span className="ml-1 text-gray-600 dark:text-gray-400">
                              {value || '<empty>'}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  };

  // Render column definition table
  const renderColumnDefinitions = () => {
    return (
      <div className="space-y-4">
        <div className="flex justify-between items-center">
          <h4 className="text-lg font-medium text-gray-900 dark:text-white">
            Column Definitions
          </h4>
          <div className="flex space-x-2">
            <Button
              variant="outline"
              size="sm"
              onClick={autoDetectColumns}
              disabled={!formData.sampleData.length}
            >
              <Eye className="h-4 w-4 mr-2" />
              Auto-detect
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={addColumn}
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Column
            </Button>
          </div>
        </div>

        {formData.schema.length === 0 ? (
          <div className="text-center py-8 text-gray-500 dark:text-gray-400 border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg">
            <Ruler className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No columns defined. Add columns manually or use auto-detection.</p>
          </div>
        ) : (
          <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 dark:bg-gray-700">
                <tr>
                  <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Column Name</th>
                  <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Start Position</th>
                  <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Length</th>
                  <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Data Type</th>
                  <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Description</th>
                  <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                {formData.schema.map((column, index) => (
                  <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                    <td className="px-4 py-3">
                      <input
                        type="text"
                        value={column.name}
                        onChange={(e) => updateColumn(index, 'name', e.target.value)}
                        className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      />
                    </td>
                    <td className="px-4 py-3">
                      <input
                        type="number"
                        value={column.start}
                        onChange={(e) => updateColumn(index, 'start', e.target.value)}
                        className="w-20 px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                        min="1"
                        max={formData.recordLength}
                      />
                    </td>
                    <td className="px-4 py-3">
                      <input
                        type="number"
                        value={column.length}
                        onChange={(e) => updateColumn(index, 'length', e.target.value)}
                        className="w-20 px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                        min="1"
                        max={formData.recordLength}
                      />
                    </td>
                    <td className="px-4 py-3">
                      <select
                        value={column.type}
                        onChange={(e) => updateColumn(index, 'type', e.target.value)}
                        className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      >
                        {dataTypes.map(type => (
                          <option key={type} value={type}>{type}</option>
                        ))}
                      </select>
                    </td>
                    <td className="px-4 py-3">
                      <input
                        type="text"
                        value={column.description}
                        onChange={(e) => updateColumn(index, 'description', e.target.value)}
                        className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                        placeholder="Optional description"
                      />
                    </td>
                    <td className="px-4 py-3">
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => removeColumn(index)}
                        className="text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300"
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    );
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-7xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Positional File Metadata Wizard
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Step {currentStep} of {totalSteps} - Fixed-width File Format
            </p>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleClose}
            className="h-8 w-8 p-0"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Progress Bar */}
        <div className="px-6 pt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${(currentStep / totalSteps) * 100}%` }}
            />
          </div>
        </div>

        {/* Content */}
        <div className="p-6 max-h-[60vh] overflow-y-auto">
          {currentStep === 1 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                General Properties
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Enter a Name for the metadata entry. Optionally, add a Purpose and Description to clarify its use.
              </p>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => updateFormData({ name: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter metadata name"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Purpose
                  </label>
                  <input
                    type="text"
                    value={formData.purpose}
                    onChange={(e) => updateFormData({ purpose: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter purpose"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Description
                  </label>
                  <textarea
                    value={formData.description}
                    onChange={(e) => updateFormData({ description: e.target.value })}
                    rows={3}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter description"
                  />
                </div>
              </div>
            </div>
          )}

          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                File Selection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Select your positional (fixed-width) file. The application will analyze the file structure and record length.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept=".txt,.dat,.prn,.fixed,.fw"
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Positional File *
                  </label>
                  <div className="flex space-x-2">
                    <input
                      type="text"
                      value={formData.filePath}
                      readOnly
                      className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white bg-gray-50 dark:bg-gray-600"
                      placeholder="No file selected"
                    />
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => fileInputRef.current?.click()}
                      disabled={isLoading}
                    >
                      {isLoading ? (
                        <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-current mr-2" />
                      ) : (
                        <Upload className="h-4 w-4 mr-2" />
                      )}
                      {isLoading ? 'Loading...' : 'Browse'}
                    </Button>
                  </div>
                  {formData.file && (
                    <p className="text-sm text-green-600 dark:text-green-400 mt-1">
                      ✓ File selected: {formData.file.name} ({(formData.file.size / 1024).toFixed(2)} KB)
                    </p>
                  )}
                </div>

                {/* File Encoding */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    File Encoding
                  </label>
                  <select
                    value={formData.encoding}
                    onChange={(e) => updateFormData({ encoding: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                  >
                    {encodings.map(encoding => (
                      <option key={encoding} value={encoding}>{encoding}</option>
                    ))}
                  </select>
                </div>

                {/* Preview Configuration */}
                {formData.file && (
                  <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
                    <div className="flex items-center space-x-2 mb-3">
                      <Settings className="h-4 w-4 text-gray-500" />
                      <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">Preview Configuration</h4>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                          Preview Lines
                        </label>
                        <input
                          type="number"
                          value={previewConfig.previewLines}
                          onChange={(e) => updatePreviewConfig({ previewLines: parseInt(e.target.value) || 10 })}
                          className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                          min="1"
                          max="50"
                        />
                      </div>
                      <div className="flex items-center space-x-2">
                        <input
                          type="checkbox"
                          id="showLineNumbers"
                          checked={previewConfig.showLineNumbers}
                          onChange={(e) => updatePreviewConfig({ showLineNumbers: e.target.checked })}
                          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                        />
                        <label htmlFor="showLineNumbers" className="text-xs text-gray-700 dark:text-gray-300">
                          Show line numbers
                        </label>
                      </div>
                    </div>
                  </div>
                )}

                {error && (
                  <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                    <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                    <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                  </div>
                )}

                {formData.file && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <FileText className="h-4 w-4" />
                      <span className="font-medium">File loaded successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Record length: <strong>{formData.recordLength} characters</strong> • 
                      Sample lines: <strong>{formData.sampleData.length}</strong>
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-6">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Define Column Positions
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Define the column positions and lengths for your fixed-width file. Use the auto-detect feature or manually add columns.
              </p>
              
              {!formData.file ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <FileText className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No file loaded. Please go back to Step 2 and select a file.</p>
                </div>
              ) : (
                <>
                  {renderColumnDefinitions()}
                  {renderDataPreview()}
                </>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Review Schema
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review and verify the parsed data based on your column definitions.
              </p>
              
              {formData.schema.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Table className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No schema defined. Please complete previous steps.</p>
                </div>
              ) : (
                <>
                  {renderDataPreview()}
                  <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md p-4">
                    <div className="flex items-center space-x-2 text-blue-800 dark:text-blue-300">
                      <CheckCircle className="h-4 w-4" />
                      <span className="font-medium">Schema Validation</span>
                    </div>
                    <p className="text-sm text-blue-700 dark:text-blue-400 mt-1">
                      Column definitions cover positions 1 to {formData.schema.reduce((max, col) => Math.max(max, col.start + col.length - 1), 0)} 
                      of {formData.recordLength} total record length.
                    </p>
                  </div>
                </>
              )}
            </div>
          )}

          {currentStep === 5 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your configuration and click Finish to save the metadata to the Repository.
              </p>
              
              {error && (
                <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                  <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                  <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                </div>
              )}
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your positional file metadata configuration is complete and ready to be saved to the repository.
                </p>
              </div>
              
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-4">
                  <h4 className="font-medium text-gray-900 dark:text-white">Configuration Summary</h4>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Name:</span>
                      <span className="text-gray-900 dark:text-white font-medium">{formData.name || 'Not specified'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">File:</span>
                      <span className="text-gray-900 dark:text-white">{formData.filePath || 'No file selected'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Record Length:</span>
                      <span className="text-gray-900 dark:text-white">{formData.recordLength} characters</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Encoding:</span>
                      <span className="text-gray-900 dark:text-white">{formData.encoding}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Columns:</span>
                      <span className="text-gray-900 dark:text-white">{formData.schema.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Sample Rows:</span>
                      <span className="text-gray-900 dark:text-white">{formData.sampleData.length}</span>
                    </div>
                  </div>
                </div>

                {formData.schema.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Column Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.schema.map((column, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <div>
                              <span className="text-gray-600 dark:text-gray-400 font-medium">{column.name}</span>
                              <span className="text-gray-400 text-xs ml-2">
                                ({column.start}-{column.start + column.length - 1})
                              </span>
                            </div>
                            <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded">
                              {column.type}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t border-gray-200 dark:border-gray-700">
          <Button
            variant="outline"
            onClick={currentStep === 1 ? handleClose : handleBack}
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            {currentStep === 1 ? 'Cancel' : 'Back'}
          </Button>
          
          <div className="flex items-center space-x-3">
            <span className="text-sm text-gray-500 dark:text-gray-400">
              Step {currentStep} of {totalSteps}
            </span>
            {currentStep < totalSteps ? (
              <Button 
                onClick={handleNext}
                disabled={
                  (currentStep === 1 && !formData.name.trim()) || // Name required for step 1
                  (currentStep === 2 && !formData.file) // File required for step 2
                  // Step 3 doesn't require columns to proceed to review
                }
              >
                Next
                <ArrowRight className="h-4 w-4 ml-2" />
              </Button>
            ) : (
              <Button onClick={handleSave}>
                <CheckCircle className="h-4 w-4 mr-2" />
                Finish & Save to Repository
              </Button>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default PositionalFileMetadataWizard;