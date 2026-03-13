import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import Papa, { ParseResult } from 'papaparse';
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
  Settings
} from 'lucide-react';
import {
  DelimitedFileMetadataFormData,
  DelimitedFileMetadataWizardProps
} from '../types/types';

const DelimitedFileMetadataWizard: React.FC<DelimitedFileMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<DelimitedFileMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    delimiter: ',',
    hasHeaders: true,
    textQualifier: '"',
    encoding: 'UTF-8',
    schema: [],
    sampleData: []
  });

  const [fileContent, setFileContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [parsingConfig, setParsingConfig] = useState({
    skipEmptyLines: true,
    transformHeader: true,
    previewLines: 10
  });
  const [parseResult, setParseResult] = useState<ParseResult<string[]> | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 5;

  // Common delimiters
  const delimiters = [
    { value: ',', label: 'Comma ( , )' },
    { value: ';', label: 'Semicolon ( ; )' },
    { value: '\t', label: 'Tab ( \\t )' },
    { value: '|', label: 'Pipe ( | )' },
    { value: ':', label: 'Colon ( : )' },
    { value: ' ', label: 'Space (   )' }
  ];

  // Text qualifiers
  const textQualifiers = [
    { value: '"', label: 'Double Quote ( " )' },
    { value: "'", label: 'Single Quote ( \' )' },
    { value: '', label: 'None' }
  ];

  // File encodings
  const encodings = [
    'UTF-8',
    'ISO-8859-1',
    'Windows-1252',
    'ASCII',
    'UTF-16'
  ];

  // Parse delimited file using PapaParse with proper typing
  const parseWithPapaParse = (content: string, config: any): Promise<ParseResult<string[]>> => {
    return new Promise((resolve, reject) => {
      Papa.parse(content, {
        delimiter: config.delimiter,
        header: false, // We'll handle headers manually to get raw data
        quoteChar: config.textQualifier || '"',
        skipEmptyLines: config.skipEmptyLines ? 'greedy' : false,
        complete: (results: ParseResult<string[]>) => {
          resolve(results);
        },
        error: (error: any) => {
          reject(error);
        },
        // For large files, we could use chunking, but for metadata we want the full structure
        chunkSize: 8192
      });
    });
  };

  // Auto-detect delimiter
  const autoDetectDelimiter = (content: string): string => {
    const firstLine = content.split('\n')[0];
    
    // Count occurrences of common delimiters
    const delimiterCounts = delimiters.map(delimiter => ({
      delimiter: delimiter.value,
      count: (firstLine.match(new RegExp(`[${delimiter.value === ' ' ? '\\s' : delimiter.value}]`, 'g')) || []).length
    }));

    // Find the delimiter with the highest count (excluding space if it's too common)
    const bestDelimiter = delimiterCounts
      .filter(d => d.delimiter !== ' ' || d.count > 0)
      .reduce((best, current) => 
        current.count > best.count ? current : best, 
        { delimiter: ',', count: 0 }
      );

    return bestDelimiter.count > 0 ? bestDelimiter.delimiter : ',';
  };

  // Handle file selection and parsing
  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const validExtensions = ['.csv', '.tsv', '.txt', '.dat'];
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please select a valid delimited file (.csv, .tsv, .txt, .dat)');
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = async (e) => {
      try {
        const content = e.target?.result as string;
        setFileContent(content);
        
        // Auto-detect delimiter based on file extension and content
        let detectedDelimiter = ',';
        if (fileExtension === '.tsv') {
          detectedDelimiter = '\t';
        } else {
          detectedDelimiter = autoDetectDelimiter(content);
        }

        // Parse the file with PapaParse
        const result = await parseWithPapaParse(content, {
          delimiter: detectedDelimiter,
          textQualifier: formData.textQualifier,
          skipEmptyLines: parsingConfig.skipEmptyLines
        });

        setParseResult(result);

        // Generate headers from the parsed data
        const rawData = result.data;
        const headers = formData.hasHeaders && rawData.length > 0 
          ? rawData[0].map((cell, index) => cell?.toString().trim() || `Column${index + 1}`)
          : rawData[0]?.map((_, index) => `Column${index + 1}`) || [];

        // Generate schema
        const schema = generateSchemaFromData(headers, rawData);
        
        // Get sample data (first 10 rows, excluding header if needed)
        const sampleStartRow = formData.hasHeaders && rawData.length > 1 ? 1 : 0;
        const sampleData = rawData.slice(sampleStartRow, sampleStartRow + parsingConfig.previewLines);

        updateFormData({
          file,
          filePath: file.name,
          delimiter: detectedDelimiter,
          schema,
          sampleData
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to parse delimited file: ${err.message}`);
        setIsLoading(false);
      }
    };

    reader.onerror = () => {
      setError('Failed to read file. The file may be corrupted or in an unsupported format.');
      setIsLoading(false);
    };

    reader.readAsText(file, formData.encoding);
  };

  // Generate schema from detected headers and data
  const generateSchemaFromData = (headers: string[], data: string[][]): Array<{ 
    name: string; 
    type: string; 
    length?: number;
    sampleValues: string[];
  }> => {
    return headers.map((header, index) => {
      // Analyze column data to infer type (skip header row if hasHeaders is true)
      const startRow = formData.hasHeaders && data.length > 1 ? 1 : 0;
      const columnData = data
        .slice(startRow)
        .map(row => row[index])
        .filter(val => val !== '' && val != null && val !== undefined)
        .map(val => val.toString().trim());

      const inferredType = inferDataTypeFromSample(columnData);
      const sampleValues = columnData.slice(0, 5); // Get first 5 sample values
      
      return {
        name: header,
        type: inferredType,
        length: inferDefaultLength(inferredType, columnData),
        sampleValues
      };
    });
  };

  // Infer data type from sample data
  const inferDataTypeFromSample = (samples: string[]): string => {
    if (samples.length === 0) return 'String';
    
    // Check for dates
    const datePatterns = [
      /^\d{4}-\d{2}-\d{2}$/, // YYYY-MM-DD
      /^\d{2}\/\d{2}\/\d{4}$/, // MM/DD/YYYY
      /^\d{2}-\d{2}-\d{4}$/, // MM-DD-YYYY
      /^\d{4}\/\d{2}\/\d{2}$/ // YYYY/MM/DD
    ];

    const dateCount = samples.filter(val => {
      // Try parsing as date
      const date = new Date(val);
      if (!isNaN(date.getTime())) return true;
      
      // Check against patterns
      return datePatterns.some(pattern => pattern.test(val));
    }).length;
    
    if (dateCount / samples.length > 0.7) return 'Date';
    
    // Check for numbers
    const numberCount = samples.filter(val => {
      if (val.trim() === '') return false;
      const num = parseFloat(val.replace(/[,$%]/g, '')); // Remove common number formatting
      return !isNaN(num) && isFinite(num);
    }).length;
    
    if (numberCount / samples.length > 0.7) {
      // Check if all numbers are integers
      const integerCount = samples.filter(val => {
        if (val.trim() === '') return false;
        const num = parseFloat(val.replace(/[,$%]/g, ''));
        return !isNaN(num) && isFinite(num) && Number.isInteger(num);
      }).length;
      
      return integerCount / numberCount > 0.9 ? 'Integer' : 'Decimal';
    }
    
    // Check for booleans
    const booleanCount = samples.filter(val => 
      ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n', 't', 'f'].includes(val.toLowerCase().trim())
    ).length;
    
    if (booleanCount / samples.length > 0.8) return 'Boolean';
    
    return 'String';
  };

  // Infer default length based on type and data
  const inferDefaultLength = (dataType: string, samples: string[]): number | undefined => {
    switch (dataType) {
      case 'String':
        const maxLength = samples.length > 0 ? 
          Math.max(...samples.map(val => String(val).length)) : 255;
        return Math.min(Math.max(maxLength, 10), 4000);
      case 'Integer':
        return 15;
      case 'Decimal':
        return 20;
      default:
        return undefined;
    }
  };

  // Handle delimiter change
  const handleDelimiterChange = async (newDelimiter: string) => {
    if (!fileContent) return;

    try {
      setIsLoading(true);
      const result = await parseWithPapaParse(fileContent, {
        delimiter: newDelimiter,
        textQualifier: formData.textQualifier,
        skipEmptyLines: parsingConfig.skipEmptyLines
      });

      setParseResult(result);

      const rawData = result.data;
      const headers = formData.hasHeaders && rawData.length > 0 
        ? rawData[0].map((cell, index) => cell?.toString().trim() || `Column${index + 1}`)
        : rawData[0]?.map((_, index) => `Column${index + 1}`) || [];

      const schema = generateSchemaFromData(headers, rawData);
      const sampleStartRow = formData.hasHeaders && rawData.length > 1 ? 1 : 0;
      const sampleData = rawData.slice(sampleStartRow, sampleStartRow + parsingConfig.previewLines);

      updateFormData({
        delimiter: newDelimiter,
        schema,
        sampleData
      });
      
      setIsLoading(false);
    } catch (err: any) {
      setError(`Failed to parse with new delimiter: ${err.message}`);
      setIsLoading(false);
    }
  };

  // Handle headers option change
  const handleHasHeadersChange = async (hasHeaders: boolean) => {
    if (!fileContent || !parseResult) return;

    try {
      const rawData = parseResult.data;
      const headers = hasHeaders && rawData.length > 0 
        ? rawData[0].map((cell, index) => cell?.toString().trim() || `Column${index + 1}`)
        : rawData[0]?.map((_, index) => `Column${index + 1}`) || [];

      const schema = generateSchemaFromData(headers, rawData);
      const sampleStartRow = hasHeaders && rawData.length > 1 ? 1 : 0;
      const sampleData = rawData.slice(sampleStartRow, sampleStartRow + parsingConfig.previewLines);

      updateFormData({
        hasHeaders,
        schema,
        sampleData
      });
    } catch (err: any) {
      setError(`Failed to update headers: ${err.message}`);
    }
  };

  // Update form data
  const updateFormData = (updates: Partial<DelimitedFileMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Update schema field
  const updateSchema = (index: number, field: string, value: string | number) => {
    const newSchema = [...formData.schema];
    newSchema[index] = { ...newSchema[index], [field]: value };
    updateFormData({ schema: newSchema });
  };

  // Update parsing configuration
  const updateParsingConfig = (updates: Partial<typeof parsingConfig>) => {
    setParsingConfig(prev => ({ ...prev, ...updates }));
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
      delimiter: ',',
      hasHeaders: true,
      textQualifier: '"',
      encoding: 'UTF-8',
      schema: [],
      sampleData: []
    });
    setFileContent('');
    setParseResult(null);
    setError(null);
  };

  // Data types for dropdown
  const dataTypes = ['String', 'Integer', 'Decimal', 'Date', 'Boolean'];

  // Render parsing statistics
  const renderParsingStats = () => {
    if (!parseResult) return null;

    return (
      <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-md p-3">
        <div className="grid grid-cols-3 gap-4 text-sm">
          <div>
            <span className="text-blue-700 dark:text-blue-300 font-medium">Total Rows:</span>
            <span className="ml-2 text-blue-900 dark:text-blue-100">{parseResult.data.length}</span>
          </div>
          <div>
            <span className="text-blue-700 dark:text-blue-300 font-medium">Columns:</span>
            <span className="ml-2 text-blue-900 dark:text-blue-100">{formData.schema.length}</span>
          </div>
          <div>
            <span className="text-blue-700 dark:text-blue-300 font-medium">Parsing Errors:</span>
            <span className={`ml-2 ${parseResult.errors.length > 0 ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}`}>
              {parseResult.errors.length}
            </span>
          </div>
        </div>
        {parseResult.errors.length > 0 && (
          <div className="mt-2 text-xs text-orange-700 dark:text-orange-300">
            <strong>Note:</strong> {parseResult.errors.length} parsing errors detected. Some data may not be parsed correctly.
          </div>
        )}
        {parseResult.meta && (
          <div className="mt-2 text-xs text-gray-600 dark:text-gray-400">
            <strong>Delimiter detected:</strong> {parseResult.meta.delimiter}
            {parseResult.meta.linebreak && ` • Linebreak: ${parseResult.meta.linebreak}`}
            {parseResult.meta.aborted && ` • Aborted: ${parseResult.meta.aborted}`}
          </div>
        )}
      </div>
    );
  };

  // Render data preview table
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

    const headers = formData.schema.map(column => column.name);

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <div className="flex justify-between items-center">
            <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
              Data Preview ({formData.sampleData.length} rows)
              {formData.hasHeaders && ` - Showing data rows (excluding headers)`}
            </h4>
            <div className="flex items-center space-x-2">
              <span className="text-xs text-gray-500 dark:text-gray-400">
                Showing first {parsingConfig.previewLines} rows
              </span>
            </div>
          </div>
        </div>
        <div className="overflow-x-auto max-h-96">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                {headers.map((header, index) => (
                  <th key={index} className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300 border-r border-gray-200 dark:border-gray-600 last:border-r-0">
                    <div className="flex items-center space-x-1">
                      <span>{header}</span>
                      <span className="text-xs text-gray-500 font-normal">
                        ({formData.schema[index]?.type})
                      </span>
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.sampleData.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  {row.map((cell, cellIndex) => (
                    <td key={cellIndex} className="px-3 py-2 text-gray-600 dark:text-gray-300 border-r border-gray-200 dark:border-gray-600 last:border-r-0">
                      {cell !== '' && cell != null ? (
                        <div className="max-w-xs truncate" title={cell.toString()}>
                          {cell.toString()}
                        </div>
                      ) : (
                        <span className="text-gray-400 italic text-xs">empty</span>
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
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
              Delimited File Metadata Wizard
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Step {currentStep} of {totalSteps} - Powered by PapaParse
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
                File Selection & Parsing
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Select your delimited file (CSV, TSV, etc.). The application will automatically parse the file using PapaParse and detect the structure.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept=".csv,.tsv,.txt,.dat"
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Delimited File *
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
                      {isLoading ? 'Parsing...' : 'Browse'}
                    </Button>
                  </div>
                  {formData.file && (
                    <p className="text-sm text-green-600 dark:text-green-400 mt-1">
                      ✓ File selected: {formData.file.name} ({(formData.file.size / 1024).toFixed(2)} KB)
                    </p>
                  )}
                </div>

                {/* Parsing Configuration */}
                {formData.file && (
                  <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
                    <div className="flex items-center space-x-2 mb-3">
                      <Settings className="h-4 w-4 text-gray-500" />
                      <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">Parsing Configuration</h4>
                    </div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
                          Preview Lines
                        </label>
                        <input
                          type="number"
                          value={parsingConfig.previewLines}
                          onChange={(e) => updateParsingConfig({ previewLines: parseInt(e.target.value) || 10 })}
                          className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-sm focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                          min="1"
                          max="50"
                        />
                      </div>
                      <div className="flex items-center space-x-2">
                        <input
                          type="checkbox"
                          id="skipEmptyLines"
                          checked={parsingConfig.skipEmptyLines}
                          onChange={(e) => updateParsingConfig({ skipEmptyLines: e.target.checked })}
                          className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                        />
                        <label htmlFor="skipEmptyLines" className="text-xs text-gray-700 dark:text-gray-300">
                          Skip empty lines
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

                {parseResult && renderParsingStats()}

                {formData.schema.length > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <FileText className="h-4 w-4" />
                      <span className="font-medium">File parsed successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Detected {formData.schema.length} columns with {formData.sampleData.length} sample rows.
                      Delimiter: <strong>{delimiters.find(d => d.value === formData.delimiter)?.label || formData.delimiter}</strong>
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                File Format Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Configure the delimited file format settings. Adjust the delimiter, text qualifier, and encoding as needed.
              </p>
              
              {!formData.file ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <FileText className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No file loaded. Please go back to Step 2 and select a file.</p>
                </div>
              ) : (
                <div className="space-y-6">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Delimiter *
                      </label>
                      <select
                        value={formData.delimiter}
                        onChange={(e) => handleDelimiterChange(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                        disabled={isLoading}
                      >
                        {delimiters.map(delimiter => (
                          <option key={delimiter.value} value={delimiter.value}>
                            {delimiter.label}
                          </option>
                        ))}
                      </select>
                    </div>
                    
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Text Qualifier
                      </label>
                      <select
                        value={formData.textQualifier}
                        onChange={(e) => updateFormData({ textQualifier: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      >
                        {textQualifiers.map(qualifier => (
                          <option key={qualifier.value} value={qualifier.value}>
                            {qualifier.label}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>

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
                  
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="hasHeaders"
                      checked={formData.hasHeaders}
                      onChange={(e) => handleHasHeadersChange(e.target.checked)}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <label htmlFor="hasHeaders" className="text-sm text-gray-700 dark:text-gray-300">
                      First row contains column headers
                    </label>
                  </div>
                  
                  {formData.schema.length > 0 && renderDataPreview()}
                </div>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Define Schema
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                The application has inferred the schema from your delimited file data. You can manually adjust column names, data types, and lengths.
              </p>
              
              {formData.schema.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Table className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No schema data available. Please complete previous steps.</p>
                </div>
              ) : (
                <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 dark:bg-gray-700">
                      <tr>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Column Name</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Data Type</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Length</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Sample Values</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                      {formData.schema.map((column, index) => (
                        <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                          <td className="px-4 py-3">
                            <input
                              type="text"
                              value={column.name}
                              onChange={(e) => updateSchema(index, 'name', e.target.value)}
                              className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                            />
                          </td>
                          <td className="px-4 py-3">
                            <select
                              value={column.type}
                              onChange={(e) => updateSchema(index, 'type', e.target.value)}
                              className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                            >
                              {dataTypes.map(type => (
                                <option key={type} value={type}>{type}</option>
                              ))}
                            </select>
                          </td>
                          <td className="px-4 py-3">
                            <input
                              type="number"
                              value={column.length || ''}
                              onChange={(e) => updateSchema(index, 'length', e.target.value ? parseInt(e.target.value) : '')}
                              className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                              placeholder="Auto"
                              min="1"
                              max="4000"
                            />
                          </td>
                          <td className="px-4 py-3">
                            <div className="text-xs text-gray-500 dark:text-gray-400 max-w-xs">
                              <div className="truncate" title={column.sampleValues.join(', ')}>
                                {column.sampleValues.slice(0, 3).map(val => String(val)).join(', ')}
                                {column.sampleValues.length > 3 && '...'}
                              </div>
                              <div className="text-gray-400 mt-1">
                                {column.sampleValues.length} samples
                              </div>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
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
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your delimited file metadata configuration is complete and ready to be saved to the repository.
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
                      <span className="text-gray-500 dark:text-gray-400">Delimiter:</span>
                      <span className="text-gray-900 dark:text-white">
                        {delimiters.find(d => d.value === formData.delimiter)?.label || formData.delimiter}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Has Headers:</span>
                      <span className="text-gray-900 dark:text-white">{formData.hasHeaders ? 'Yes' : 'No'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Text Qualifier:</span>
                      <span className="text-gray-900 dark:text-white">
                        {textQualifiers.find(q => q.value === formData.textQualifier)?.label || formData.textQualifier}
                      </span>
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
                    <h4 className="font-medium text-gray-900 dark:text-white">Schema Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.schema.map((column, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <div>
                              <span className="text-gray-600 dark:text-gray-400 font-medium">{column.name}</span>
                              <span className="text-gray-400 text-xs ml-2">
                                {column.sampleValues.length} samples
                              </span>
                            </div>
                            <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded">
                              {column.type}{column.length ? `(${column.length})` : ''}
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
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && !formData.schema.length) || isLoading}
              >
                {isLoading ? (
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-current mr-2" />
                ) : null}
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

export default DelimitedFileMetadataWizard;