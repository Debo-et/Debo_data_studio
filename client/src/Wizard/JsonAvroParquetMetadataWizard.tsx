// JsonAvroParquetMetadataWizard.tsx (simplified version without avro-js)
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  FileJson,
  AlertCircle,
  Database,
  FileText,
  Braces,
  Binary
} from 'lucide-react';
import {
  JsonAvroParquetMetadataFormData,
  JsonAvroParquetMetadataWizardProps,
  JsonFieldDefinition,
  AvroFieldDefinition,
  ParquetColumnDefinition
} from '../types/types';

const JsonAvroParquetMetadataWizard: React.FC<JsonAvroParquetMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<JsonAvroParquetMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    format: 'json',
    schema: [],
    totalFields: 0,
    recordCount: 0,
    encoding: 'UTF-8',
    compression: 'none',
    sampleData: []
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 4;

  // Supported file extensions
  const supportedExtensions = {
    json: ['.json', '.jsonl'],
    avro: ['.avro', '.avsc'],
    parquet: ['.parquet', '.parq']
  };

  // Parse JSON file and extract schema
  const parseJSONFile = (content: string): { schema: JsonFieldDefinition[], sampleData: any[], recordCount: number } => {
    try {
      let records: any[] = [];
      
      // Try to parse as JSON array first
      try {
        const parsed = JSON.parse(content);
        if (Array.isArray(parsed)) {
          records = parsed.slice(0, 10); // Sample first 10 records
        } else {
          records = [parsed]; // Single object
        }
      } catch {
        // Try to parse as JSONL (JSON Lines)
        const lines = content.split('\n').filter(line => line.trim());
        records = lines.slice(0, 10).map(line => {
          try {
            return JSON.parse(line);
          } catch {
            return null;
          }
        }).filter(record => record !== null);
      }

      if (records.length === 0) {
        throw new Error('No valid JSON records found');
      }

      const schema = inferJSONSchema(records[0], '', 0);
      const sampleData = records.slice(0, 5); // Keep 5 records for sample data

      return {
        schema,
        sampleData,
        recordCount: records.length
      };
    } catch (err: any) {
      throw new Error(`Failed to parse JSON: ${err.message}`);
    }
  };

  // Infer JSON schema recursively
  const inferJSONSchema = (obj: any, path: string = '', level: number = 0): JsonFieldDefinition[] => {
    const fields: JsonFieldDefinition[] = [];
    
    if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
      Object.entries(obj).forEach(([key, value]) => {
        const currentPath = path ? `${path}.${key}` : key;
        const fieldType = inferJSONType(value);
        
        const field: JsonFieldDefinition = {
          name: key,
          type: fieldType,
          path: currentPath,
          level,
          sampleValue: typeof value === 'string' ? value.substring(0, 100) : JSON.stringify(value),
          nullable: value === null || value === undefined,
          description: `Field ${key} of type ${fieldType}`
        };

        fields.push(field);

        // Recursively process nested objects
        if (fieldType === 'object' && value && typeof value === 'object' && !Array.isArray(value)) {
          const nestedFields = inferJSONSchema(value, currentPath, level + 1);
          fields.push(...nestedFields);
        }
      });
    }

    return fields;
  };

  // Infer type from JSON value
  const inferJSONType = (value: any): string => {
    if (value === null) return 'null';
    if (Array.isArray(value)) {
      if (value.length > 0) {
        return `array<${inferJSONType(value[0])}>`;
      }
      return 'array<unknown>';
    }
    if (typeof value === 'object') return 'object';
    if (typeof value === 'string') return 'string';
    if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'number';
    if (typeof value === 'boolean') return 'boolean';
    return 'unknown';
  };

  // Parse Avro file and extract schema (simplified without avro-js)
  const parseAvroFile = async (file: File): Promise<{ schema: AvroFieldDefinition[], sampleData: any[], recordCount: number }> => {
    try {
      // For Avro schema files (.avsc)
      if (file.name.endsWith('.avsc')) {
        const content = await file.text();
        const schema = JSON.parse(content);
        const fields = extractAvroFields(schema, '', 0);
        
        return {
          schema: fields,
          sampleData: [],
          recordCount: 0
        };
      } else {
        // For binary Avro files, we can only extract basic info
        // In a real application, this would be handled by a backend service
        throw new Error('Binary Avro file parsing requires server-side processing. Please use .avsc schema files instead.');
      }
    } catch (err: any) {
      throw new Error(`Failed to parse Avro: ${err.message}`);
    }
  };

  // Extract fields from Avro schema
  const extractAvroFields = (schema: any, path: string = '', level: number = 0): AvroFieldDefinition[] => {
    const fields: AvroFieldDefinition[] = [];
    
    if (schema.type === 'record' && schema.fields) {
      schema.fields.forEach((field: any) => {
        const currentPath = path ? `${path}.${field.name}` : field.name;
        const fieldType = extractAvroType(field.type);
        
        const avroField: AvroFieldDefinition = {
          name: field.name,
          type: fieldType,
          path: currentPath,
          level,
          sampleValue: field.default !== undefined ? JSON.stringify(field.default) : undefined,
          nullable: isAvroTypeNullable(field.type),
          description: field.doc || `Field ${field.name}`,
          logicalType: field.logicalType
        };

        fields.push(avroField);

        // Handle nested records
        if (field.type.type === 'record') {
          const nestedFields = extractAvroFields(field.type, currentPath, level + 1);
          fields.push(...nestedFields);
        } else if (Array.isArray(field.type) && field.type.find((t: any) => t.type === 'record')) {
          const recordType = field.type.find((t: any) => t.type === 'record');
          if (recordType) {
            const nestedFields = extractAvroFields(recordType, currentPath, level + 1);
            fields.push(...nestedFields);
          }
        }
      });
    }

    return fields;
  };

  // Extract Avro type as string
  const extractAvroType = (type: any): string => {
    if (typeof type === 'string') return type;
    if (Array.isArray(type)) {
      return type.map(t => extractAvroType(t)).join(' | ');
    }
    if (type.type) {
      if (type.logicalType) {
        return `${type.type} (${type.logicalType})`;
      }
      return type.type;
    }
    return JSON.stringify(type);
  };

  // Check if Avro type is nullable
  const isAvroTypeNullable = (type: any): boolean => {
    if (Array.isArray(type)) {
      return type.includes('null') || type.some(t => t === 'null');
    }
    return type === 'null';
  };

  // Parse Parquet file and extract schema (simplified)
  const parseParquetFile = async (_file: File): Promise<{ schema: ParquetColumnDefinition[], sampleData: any[], recordCount: number }> => {
    // For Parquet files, we can only extract basic metadata without proper parsing
    // In a real application, this would be handled by a backend service
    const columns: ParquetColumnDefinition[] = [
      {
        name: 'column_1',
        type: 'unknown',
        path: 'column_1',
        level: 0,
        nullable: true,
        description: 'Column extracted from Parquet file'
      }
    ];

    return {
      schema: columns,
      sampleData: [],
      recordCount: 0
    };
  };

  // Handle file selection and parsing
  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type based on selected format
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    const validExtensions = supportedExtensions[formData.format];
    
    if (!validExtensions.includes(fileExtension)) {
      setError(`Please select a valid ${formData.format.toUpperCase()} file: ${validExtensions.join(', ')}`);
      return;
    }

    setError(null);
    setIsLoading(true);
    
    try {
      let result;
      
      switch (formData.format) {
        case 'json':
          const content = await file.text();
          result = parseJSONFile(content);
          break;
        
        case 'avro':
          result = await parseAvroFile(file);
          break;
        
        case 'parquet':
          result = await parseParquetFile(file);
          break;
        
        default:
          throw new Error(`Unsupported format: ${formData.format}`);
      }

      // Update form data with parsed results
      updateFormData({
        file,
        filePath: file.name,
        schema: result.schema,
        totalFields: result.schema.length,
        recordCount: result.recordCount,
        sampleData: result.sampleData
      });
      
    } catch (err: any) {
      setError(`Failed to process ${formData.format} file: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Update form data
  const updateFormData = (updates: Partial<JsonAvroParquetMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
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
      format: 'json',
      schema: [],
      totalFields: 0,
      recordCount: 0,
      encoding: 'UTF-8',
      compression: 'none',
      sampleData: []
    });
    setError(null);
  };

  // Render schema table based on format
  const renderSchemaTable = () => {
    if (formData.schema.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No schema fields defined</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Schema Fields ({formData.schema.length} fields)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Field Name
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Data Type
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Path
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Level
                </th>
                {formData.format === 'avro' && (
                  <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                    Logical Type
                  </th>
                )}
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Nullable
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.schema.map((field: any, index) => (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 font-medium text-gray-700 dark:text-gray-300">
                    {field.name}
                  </td>
                  <td className="px-3 py-2">
                    <span className="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                      {field.type}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-gray-600 dark:text-gray-300 text-xs">
                    <code>{field.path}</code>
                  </td>
                  <td className="px-3 py-2">
                    <span className="text-xs bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300 px-2 py-1 rounded">
                      Level {field.level}
                    </span>
                  </td>
                  {formData.format === 'avro' && (
                    <td className="px-3 py-2 text-gray-600 dark:text-gray-300">
                      {field.logicalType || '-'}
                    </td>
                  )}
                  <td className="px-3 py-2">
                    <span className={`text-xs px-2 py-1 rounded ${
                      field.nullable 
                        ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300' 
                        : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'
                    }`}>
                      {field.nullable ? 'NULL' : 'NOT NULL'}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  // Render sample data preview
  const renderSampleData = () => {
    if (formData.sampleData.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <p>No sample data available</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Sample Data ({formData.sampleData.length} records)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-48">
          <pre className="p-4 text-sm bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300">
            {JSON.stringify(formData.sampleData, null, 2)}
          </pre>
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
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-6xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              JSON/AVRO/Parquet Metadata Wizard
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Step {currentStep} of {totalSteps}
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
                Configure the basic properties for your JSON, AVRO, or Parquet metadata.
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

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      File Format *
                    </label>
                    <select
                      value={formData.format}
                      onChange={(e) => updateFormData({ 
                        format: e.target.value as 'json' | 'avro' | 'parquet',
                        schema: [],
                        file: null,
                        filePath: ''
                      })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="json">JSON</option>
                      <option value="avro">AVRO</option>
                      <option value="parquet">Parquet</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Encoding
                    </label>
                    <select
                      value={formData.encoding}
                      onChange={(e) => updateFormData({ encoding: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="UTF-8">UTF-8</option>
                      <option value="UTF-16">UTF-16</option>
                      <option value="ISO-8859-1">ISO-8859-1</option>
                      <option value="ASCII">ASCII</option>
                    </select>
                  </div>
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
                Select a {formData.format.toUpperCase()} file to parse and extract metadata.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept={supportedExtensions[formData.format].join(',')}
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    {formData.format.toUpperCase()} File *
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

                {error && (
                  <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                    <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                    <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                  </div>
                )}

                {formData.schema.length > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      {formData.format === 'json' && <Braces className="h-4 w-4" />}
                      {formData.format === 'avro' && <FileJson className="h-4 w-4" />}
                      {formData.format === 'parquet' && <Binary className="h-4 w-4" />}
                      <span className="font-medium">
                        {formData.format.toUpperCase()} parsed successfully!
                      </span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Fields: <strong>{formData.schema.length}</strong> | 
                      Records: <strong>{formData.recordCount}</strong>
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-6">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Schema Analysis
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review the extracted schema structure and sample data.
              </p>
              
              {!formData.file ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No file loaded. Please go back to Step 2 and select a file.</p>
                </div>
              ) : (
                <>
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Schema Structure</h4>
                    {renderSchemaTable()}
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Sample Data</h4>
                    {renderSampleData()}
                  </div>
                </>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your metadata configuration and click Finish to save it to the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your {formData.format.toUpperCase()} metadata configuration is complete and ready to be saved.
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
                      <span className="text-gray-500 dark:text-gray-400">Format:</span>
                      <span className="text-gray-900 dark:text-white">{formData.format.toUpperCase()}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Fields:</span>
                      <span className="text-gray-900 dark:text-white">{formData.schema.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Record Count:</span>
                      <span className="text-gray-900 dark:text-white">{formData.recordCount}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Encoding:</span>
                      <span className="text-gray-900 dark:text-white">{formData.encoding}</span>
                    </div>
                  </div>
                </div>

                {formData.schema.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Schema Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.schema.slice(0, 8).map((field: any, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <div>
                              <span className="text-gray-600 dark:text-gray-400 font-medium">{field.name}</span>
                              <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded ml-2">
                                {field.type}
                              </span>
                            </div>
                            <span className={`text-xs px-2 py-1 rounded ${
                              field.nullable 
                                ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300' 
                                : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'
                            }`}>
                              {field.nullable ? 'NULL' : 'NOT NULL'}
                            </span>
                          </div>
                        ))}
                        {formData.schema.length > 8 && (
                          <div className="text-center text-gray-500 dark:text-gray-400 text-xs">
                            ... and {formData.schema.length - 8} more fields
                          </div>
                        )}
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
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && formData.schema.length === 0)}
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

export default JsonAvroParquetMetadataWizard;