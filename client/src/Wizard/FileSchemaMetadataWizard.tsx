// FileSchemaMetadataWizard.tsx
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  Database,
  AlertCircle,
  FileText,
  Plus,
  Trash2
} from 'lucide-react';
import {
  FileSchemaMetadataFormData,
  FileSchemaMetadataWizardProps,
  SchemaFieldDefinition
} from '../types/types';

const FileSchemaMetadataWizard: React.FC<FileSchemaMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<FileSchemaMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    schemaType: 'custom',
    fields: [],
    totalFields: 0,
    version: '1.0',
    encoding: 'UTF-8'
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 4;

  // Supported schema file extensions
  const supportedExtensions = ['.sql', '.ddl', '.json', '.avsc', '.proto', '.xsd', '.yaml', '.yml'];

  // Parse schema file based on type
  const parseSchemaFile = (content: string, schemaType: string): SchemaFieldDefinition[] => {
    
    try {
      switch (schemaType) {
        case 'database':
          // Parse SQL/DDL files
          return parseSQLSchema(content);
        case 'json':
          // Parse JSON Schema
          return parseJSONSchema(content);
        case 'avro':
          // Parse Avro Schema
          return parseAvroSchema(content);
        case 'protobuf':
          // Parse Protobuf Schema
          return parseProtobufSchema(content);
        default:
          // For custom or unknown types, try to extract field information
          return parseGenericSchema(content);
      }
    } catch (err) {
      console.warn('Schema parsing failed, using generic parser:', err);
      return parseGenericSchema(content);
    }
  };

  // Parse SQL/DDL schema
  const parseSQLSchema = (content: string): SchemaFieldDefinition[] => {
    const fields: SchemaFieldDefinition[] = [];
    const lines = content.split('\n');
    
    lines.forEach(line => {
      // Simple pattern matching for CREATE TABLE and column definitions
      const columnMatch = line.match(/(\w+)\s+(\w+)(?:\((\d+)(?:,\s*(\d+))?\))?(\s+NOT NULL)?/i);
      if (columnMatch) {
        const [, name, type, length, scale, nullable] = columnMatch;
        fields.push({
          name: name.trim(),
          type: type.toUpperCase(),
          length: length ? parseInt(length) : undefined,
          scale: scale ? parseInt(scale) : undefined,
          nullable: !nullable,
          description: `Column ${name} of type ${type}`,
          constraints: []
        });
      }
    });
    
    return fields;
  };

  // Parse JSON Schema
  const parseJSONSchema = (content: string): SchemaFieldDefinition[] => {
    const fields: SchemaFieldDefinition[] = [];
    
    try {
      const schema = JSON.parse(content);
      
      if (schema.properties) {
        Object.entries(schema.properties).forEach(([key, value]: [string, any]) => {
          fields.push({
            name: key,
            type: mapJSONTypeToSQL(value.type),
            nullable: !schema.required?.includes(key),
            description: value.description || `Field ${key}`,
            constraints: []
          });
        });
      }
    } catch (err) {
      throw new Error('Invalid JSON schema format');
    }
    
    return fields;
  };

  // Parse Avro Schema
  const parseAvroSchema = (content: string): SchemaFieldDefinition[] => {
    const fields: SchemaFieldDefinition[] = [];
    
    try {
      const schema = JSON.parse(content);
      
      if (schema.fields && Array.isArray(schema.fields)) {
        schema.fields.forEach((field: any) => {
          const fieldType = Array.isArray(field.type) ? field.type[0] : field.type;
          fields.push({
            name: field.name,
            type: mapAvroTypeToSQL(fieldType),
            nullable: Array.isArray(field.type) && field.type.includes('null'),
            description: field.doc || `Field ${field.name}`,
            defaultValue: field.default,
            constraints: []
          });
        });
      }
    } catch (err) {
      throw new Error('Invalid Avro schema format');
    }
    
    return fields;
  };

  // Parse Protobuf Schema
  const parseProtobufSchema = (content: string): SchemaFieldDefinition[] => {
    const fields: SchemaFieldDefinition[] = [];
    const lines = content.split('\n');
    
    lines.forEach(line => {
      const fieldMatch = line.match(/(repeated\s+)?(\w+)\s+(\w+)\s*=\s*(\d+)/);
      if (fieldMatch) {
        const [, repeated, type, name] = fieldMatch;
        fields.push({
          name: name.trim(),
          type: mapProtobufTypeToSQL(type),
          nullable: true, // Protobuf 3 has no required fields
          description: `Field ${name} of type ${type}`,
          constraints: repeated ? ['repeated'] : []
        });
      }
    });
    
    return fields;
  };

  // Generic schema parser
  const parseGenericSchema = (content: string): SchemaFieldDefinition[] => {
    const fields: SchemaFieldDefinition[] = [];
    const lines = content.split('\n');
    let fieldId = 1;
    
    lines.forEach(line => {
      const trimmed = line.trim();
      if (trimmed && !trimmed.startsWith('//') && !trimmed.startsWith('--') && !trimmed.startsWith('#')) {
        // Try to extract field-like patterns
        const words = trimmed.split(/\s+/).filter(word => word.length > 0);
        if (words.length >= 2) {
          fields.push({
            name: words[0] || `field_${fieldId}`,
            type: 'VARCHAR',
            length: 255,
            nullable: true,
            description: `Field extracted from schema: ${trimmed.substring(0, 50)}...`,
            constraints: []
          });
          fieldId++;
        }
      }
    });
    
    return fields;
  };

  // Type mapping functions
  const mapJSONTypeToSQL = (jsonType: string): string => {
    const typeMap: { [key: string]: string } = {
      'string': 'VARCHAR',
      'number': 'DECIMAL',
      'integer': 'INTEGER',
      'boolean': 'BOOLEAN',
      'object': 'JSON',
      'array': 'JSON'
    };
    return typeMap[jsonType] || 'VARCHAR';
  };

  const mapAvroTypeToSQL = (avroType: string | object): string => {
    if (typeof avroType === 'object') {
      return 'JSON';
    }
    
    const typeMap: { [key: string]: string } = {
      'string': 'VARCHAR',
      'int': 'INTEGER',
      'long': 'BIGINT',
      'float': 'FLOAT',
      'double': 'DOUBLE',
      'boolean': 'BOOLEAN',
      'bytes': 'BINARY',
      'null': 'NULL'
    };
    return typeMap[avroType as string] || 'VARCHAR';
  };

  const mapProtobufTypeToSQL = (protoType: string): string => {
    const typeMap: { [key: string]: string } = {
      'string': 'VARCHAR',
      'int32': 'INTEGER',
      'int64': 'BIGINT',
      'float': 'FLOAT',
      'double': 'DOUBLE',
      'bool': 'BOOLEAN',
      'bytes': 'BINARY'
    };
    return typeMap[protoType] || 'VARCHAR';
  };

  // Handle file selection and parsing
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!supportedExtensions.includes(fileExtension)) {
      setError(`Please select a supported schema file: ${supportedExtensions.join(', ')}`);
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        
        // Infer schema type from file extension
        let schemaType: 'database' | 'json' | 'avro' | 'protobuf' | 'custom' = 'custom';
        if (['.sql', '.ddl'].includes(fileExtension)) schemaType = 'database';
        else if (fileExtension === '.json') schemaType = 'json';
        else if (fileExtension === '.avsc') schemaType = 'avro';
        else if (fileExtension === '.proto') schemaType = 'protobuf';
        
        // Parse the schema file
        const fields = parseSchemaFile(content, schemaType);
        
        // Update form data
        updateFormData({
          file,
          filePath: file.name,
          schemaType,
          fields,
          totalFields: fields.length
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to process schema file: ${err.message}`);
        setIsLoading(false);
      }
    };

    reader.onerror = () => {
      setError('Failed to read file. The file may be corrupted or in an unsupported format.');
      setIsLoading(false);
    };

    reader.readAsText(file);
  };

  // Update form data
  const updateFormData = (updates: Partial<FileSchemaMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Update field
  const updateField = (index: number, field: string, value: any) => {
    const newFields = [...formData.fields];
    newFields[index] = { ...newFields[index], [field]: value };
    updateFormData({ fields: newFields });
  };

  // Add new field
  const addField = () => {
    const newField: SchemaFieldDefinition = {
      name: `field_${formData.fields.length + 1}`,
      type: 'VARCHAR',
      length: 255,
      nullable: true,
      description: '',
      constraints: []
    };
    updateFormData({ 
      fields: [...formData.fields, newField],
      totalFields: formData.fields.length + 1
    });
  };

  // Remove field
  const removeField = (index: number) => {
    const newFields = formData.fields.filter((_, i) => i !== index);
    updateFormData({ 
      fields: newFields,
      totalFields: newFields.length
    });
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
      schemaType: 'custom',
      fields: [],
      totalFields: 0,
      version: '1.0',
      encoding: 'UTF-8'
    });
    setError(null);
  };

  // Data types for dropdown
  const sqlDataTypes = [
    'VARCHAR', 'CHAR', 'TEXT', 'INTEGER', 'BIGINT', 'SMALLINT', 
    'DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'DATE', 
    'TIME', 'TIMESTAMP', 'BINARY', 'BLOB', 'CLOB', 'JSON'
  ];

  // Render fields table
  const renderFieldsTable = () => {
    if (formData.fields.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No fields defined</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Schema Fields ({formData.fields.length} fields)
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
                  Length
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Nullable
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.fields.map((field, index) => (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2">
                    <input
                      type="text"
                      value={field.name}
                      onChange={(e) => updateField(index, 'name', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <select
                      value={field.type}
                      onChange={(e) => updateField(index, 'type', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      {sqlDataTypes.map(type => (
                        <option key={type} value={type}>{type}</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="number"
                      value={field.length || ''}
                      onChange={(e) => updateField(index, 'length', e.target.value ? parseInt(e.target.value) : undefined)}
                      className="w-20 px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      placeholder="Length"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="checkbox"
                      checked={field.nullable}
                      onChange={(e) => updateField(index, 'nullable', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeField(index)}
                      className="h-6 w-6 p-0 text-red-600 hover:text-red-800"
                    >
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </td>
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
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-6xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              File Schema Metadata Wizard
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
                Enter basic information about the schema metadata.
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
                    placeholder="Enter schema name"
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
                      Version
                    </label>
                    <input
                      type="text"
                      value={formData.version}
                      onChange={(e) => updateFormData({ version: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      placeholder="1.0"
                    />
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
                Schema File Selection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Select a schema file to parse. Supported formats: {supportedExtensions.join(', ')}
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept={supportedExtensions.join(',')}
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Schema File *
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

                {formData.fields.length > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <Database className="h-4 w-4" />
                      <span className="font-medium">Schema parsed successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Schema Type: <strong>{formData.schemaType}</strong> | 
                      Fields: <strong>{formData.fields.length}</strong>
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Schema Fields Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review and configure the schema fields. You can modify field names, data types, and other properties.
              </p>
              
              <div className="flex justify-between items-center">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Schema Type
                  </label>
                  <select
                    value={formData.schemaType}
                    onChange={(e) => updateFormData({ 
                      schemaType: e.target.value as 'database' | 'json' | 'avro' | 'protobuf' | 'custom' 
                    })}
                    className="w-48 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                  >
                    <option value="database">Database Schema</option>
                    <option value="json">JSON Schema</option>
                    <option value="avro">Avro Schema</option>
                    <option value="protobuf">Protobuf Schema</option>
                    <option value="custom">Custom Schema</option>
                  </select>
                </div>
                
                <Button
                  onClick={addField}
                  variant="outline"
                  className="flex items-center space-x-2"
                >
                  <Plus className="h-4 w-4" />
                  <span>Add Field</span>
                </Button>
              </div>

              {renderFieldsTable()}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your schema metadata configuration and click Finish to save it to the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your schema metadata configuration is complete and ready to be saved to the repository.
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
                      <span className="text-gray-500 dark:text-gray-400">Schema Type:</span>
                      <span className="text-gray-900 dark:text-white">{formData.schemaType}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Fields:</span>
                      <span className="text-gray-900 dark:text-white">{formData.fields.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Version:</span>
                      <span className="text-gray-900 dark:text-white">{formData.version}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Encoding:</span>
                      <span className="text-gray-900 dark:text-white">{formData.encoding}</span>
                    </div>
                  </div>
                </div>

                {formData.fields.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Fields Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.fields.slice(0, 10).map((field, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <div>
                              <span className="text-gray-600 dark:text-gray-400 font-medium">{field.name}</span>
                              <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded ml-2">
                                {field.type}
                              </span>
                            </div>
                            <span className={`text-xs px-2 py-1 rounded ${field.nullable ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300' : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'}`}>
                              {field.nullable ? 'NULL' : 'NOT NULL'}
                            </span>
                          </div>
                        ))}
                        {formData.fields.length > 10 && (
                          <div className="text-center text-gray-500 dark:text-gray-400 text-xs">
                            ... and {formData.fields.length - 10} more fields
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
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && formData.fields.length === 0)}
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

export default FileSchemaMetadataWizard;