// AvroMetadataWizard.tsx (real implementation)
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  AlertCircle,
  FileText,
  Database,
  Braces,
  Layers,
} from 'lucide-react';
import {
  JsonAvroParquetMetadataFormData,
  JsonAvroParquetMetadataWizardProps,
  AvroFieldDefinition,
} from '../types/types';
// Real Avro parsing library
import * as avro from 'avsc';

// -----------------------------------------------------------------
// Avro type helper: flattens a parsed Avro schema into a field list
// -----------------------------------------------------------------
function flattenAvroSchema(
  schemaObj: any,
  path: string = '',
  level: number = 0
): AvroFieldDefinition[] {
  const fields: AvroFieldDefinition[] = [];

  if (schemaObj.type === 'record' && schemaObj.fields) {
    schemaObj.fields.forEach((field: any) => {
      const fieldName = field.name;
      const fullPath = path ? `${path}.${fieldName}` : fieldName;
      const fieldType = field.type;

      const { resolvedType, nullable } = resolveAvroType(fieldType);

      fields.push({
        name: fieldName,
        type: resolvedType,
        path: fullPath,
        level,
        sampleValue: field.default !== undefined ? JSON.stringify(field.default) : undefined,
        description: field.doc || '',
        nullable,
        logicalType: extractLogicalType(fieldType) || undefined,
      });

      // Recurse into nested records
      if (typeof fieldType === 'object' && !Array.isArray(fieldType)) {
        if (fieldType.type === 'record') {
          const nested = flattenAvroSchema(fieldType, fullPath, level + 1);
          fields.push(...nested);
        } else if (fieldType.type === 'array' && typeof fieldType.items === 'object') {
          if (fieldType.items.type === 'record') {
            const nested = flattenAvroSchema(fieldType.items, `${fullPath}[]`, level + 1);
            fields.push(...nested);
          }
        } else if (fieldType.type === 'map' && typeof fieldType.values === 'object') {
          if (fieldType.values.type === 'record') {
            const nested = flattenAvroSchema(fieldType.values, `${fullPath}{key}`, level + 1);
            fields.push(...nested);
          }
        }
      }
    });
  }

  return fields;
}

function resolveAvroType(typeDef: any): { resolvedType: string; nullable: boolean } {
  if (Array.isArray(typeDef)) {
    const nonNull = typeDef.filter((t: any) => t !== 'null' && t !== 'null');
    const nullable = typeDef.some((t: any) => t === 'null' || t === null);
    if (nonNull.length === 0) return { resolvedType: 'null', nullable: true };
    const main = nonNull[0];
    const typeStr = typeof main === 'string' ? main : main?.type || 'unknown';
    return { resolvedType: typeStr, nullable };
  } else if (typeof typeDef === 'string') {
    return { resolvedType: typeDef, nullable: false };
  } else if (typeDef && typeof typeDef === 'object') {
    return { resolvedType: typeDef.type || 'unknown', nullable: false };
  }
  return { resolvedType: 'unknown', nullable: false };
}

function extractLogicalType(typeDef: any): string | undefined {
  if (Array.isArray(typeDef)) {
    const nonNull = typeDef.find((t: any) => t !== 'null' && t !== null);
    return extractLogicalType(nonNull);
  }
  if (typeDef && typeof typeDef === 'object' && typeDef.logicalType) {
    return typeDef.logicalType;
  }
  return undefined;
}

// -----------------------------------------------------------------
// Browser ReadableStream → Node Readable shim (FIXED)
// -----------------------------------------------------------------
/**
 * Converts a browser ReadableStream<Uint8Array> into a Node.js Readable stream.
 * Assumes polyfills for 'stream' and 'buffer' are provided (e.g., by Webpack).
 */
function ReadableStreamToNodeReadable(stream: ReadableStream<Uint8Array>): any {
  const reader = stream.getReader();
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { Readable } = require('stream') as { Readable: any };
  return new Readable({
    read() {
      reader
        .read()
        .then(({ done, value }) => {
          if (done) {
            this.push(null);
          } else {
            this.push(Buffer.from(value));
          }
        })
        .catch((err) => {
          this.destroy(err);
        });
    },
  });
}

// -----------------------------------------------------------------
// Real Avro file reader
// -----------------------------------------------------------------
async function readAvroFile(
  file: File
): Promise<{
  schemaFields: AvroFieldDefinition[];
  recordCount: number;
  sampleRows: any[];
}> {
  const fileBuffer = await file.arrayBuffer();

  // 1) Try to parse as JSON schema (.avsc)
  try {
    const text = await file.text();
    const schema = JSON.parse(text);
    // Avro schema records always have type = "record" and fields
    if (schema && typeof schema === 'object' && schema.type === 'record') {
      return {
        schemaFields: flattenAvroSchema(schema),
        recordCount: 0,
        sampleRows: [],
      };
    }
  } catch {
    // Not JSON → assume binary Avro container
  }

  // 2) Binary Avro (.avro)
  return new Promise((resolve, reject) => {
    try {
      // Use avsc to decode the file and extract schema + sample rows
      const decoder = new avro.streams.BlockDecoder();
      const rows: any[] = [];
      let schemaFields: AvroFieldDefinition[] = [];
      let totalRows = 0;
      let schemaResolved = false;

      decoder.on('metadata', (type) => {
        // Extract schema from the first block's metadata
        if (!schemaResolved) {
          schemaFields = flattenAvroSchema(type.schema);
          schemaResolved = true;
        }
      });

      decoder.on('data', (record) => {
        totalRows++;
        if (rows.length < 10) {
          rows.push(record);
        }
      });

      decoder.on('end', () => {
        if (!schemaResolved) {
          // fallback: try to parse schema from the type if available
          const type = (decoder as any).type;
          if (type) {
            schemaFields = flattenAvroSchema(type.schema);
          }
        }
        resolve({ schemaFields, recordCount: totalRows, sampleRows: rows });
      });

      decoder.on('error', reject);

      // Convert ArrayBuffer to a stream
      const uint8 = new Uint8Array(fileBuffer);
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(uint8);
          controller.close();
        },
      });

      // avsc expects a Node.js readable stream; we adapt to browser
      const readable = ReadableStreamToNodeReadable(stream);
      readable.pipe(decoder);
    } catch (err: any) {
      reject(err);
    }
  });
}

// -----------------------------------------------------------------
// Component
// -----------------------------------------------------------------
const AvroMetadataWizard: React.FC<JsonAvroParquetMetadataWizardProps> = ({
  isOpen,
  onClose,
  onSave,
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [formData, setFormData] = useState<
    JsonAvroParquetMetadataFormData & {
      schemaJson?: any;
    }
  >({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    format: 'avro',
    schema: [],
    totalFields: 0,
    recordCount: 0,
    encoding: 'UTF-8',
    compression: 'none',
    sampleData: [],
    schemaJson: undefined,
  });

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 4;

  // ------------------------------------------------------------------
  // File selection & parsing (now real)
  // ------------------------------------------------------------------
  const handleFileSelect = async (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const validExtensions = ['.avro', '.avsc'];
    const ext = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    if (!validExtensions.includes(ext)) {
      setError('Please select an Avro file (.avro) or Avro schema file (.avsc)');
      return;
    }

    setError(null);
    setIsLoading(true);

    try {
      const { schemaFields, recordCount, sampleRows } = await readAvroFile(file);

      setFormData((prev) => ({
        ...prev,
        file,
        filePath: file.name,
        schema: schemaFields,
        totalFields: schemaFields.length,
        recordCount,
        sampleData: sampleRows, // now real sample data
      }));
    } catch (err: any) {
      setError(`Failed to process Avro file: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const updateFormData = (updates: Partial<typeof formData>) => {
    setFormData((prev) => ({ ...prev, ...updates }));
  };

  // ------------------------------------------------------------------
  // Navigation unchanged
  // ------------------------------------------------------------------
  const handleNext = () => {
    if (currentStep < totalSteps) setCurrentStep(currentStep + 1);
  };
  const handleBack = () => {
    if (currentStep > 1) setCurrentStep(currentStep - 1);
  };
  const handleSave = () => {
    onSave({
      ...formData,
      format: 'avro',
      compression: formData.compression || 'none',
    });
    handleClose();
  };

  const handleClose = () => {
    onClose();
    setCurrentStep(1);
    setFormData({
      name: '',
      purpose: '',
      description: '',
      file: null,
      filePath: '',
      format: 'avro',
      schema: [],
      totalFields: 0,
      recordCount: 0,
      encoding: 'UTF-8',
      compression: 'none',
      sampleData: [],
    });
    setError(null);
  };

  // ------------------------------------------------------------------
  // Render helpers
  // ------------------------------------------------------------------
  const renderSchemaTable = () => {
    if (formData.schema.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No fields detected</p>
          </div>
        </div>
      );
    }

    const avroFields = formData.schema as AvroFieldDefinition[];

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Avro Schema Fields ({avroFields.length} fields)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium">Name</th>
                <th className="px-3 py-2 text-left font-medium">Type</th>
                <th className="px-3 py-2 text-left font-medium">Logical</th>
                <th className="px-3 py-2 text-left font-medium">Nullable</th>
                <th className="px-3 py-2 text-left font-medium">Path</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {avroFields.map((field, idx) => (
                <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 font-medium text-gray-700 dark:text-gray-300">
                    {field.name}
                  </td>
                  <td className="px-3 py-2">
                    <span className="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                      {field.type}
                    </span>
                  </td>
                  <td className="px-3 py-2">
                    {field.logicalType ? (
                      <span className="text-xs bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-200 px-2 py-1 rounded">
                        {field.logicalType}
                      </span>
                    ) : (
                      <span className="text-xs text-gray-400">—</span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className={`text-xs px-2 py-1 rounded ${
                        field.nullable
                          ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300'
                          : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'
                      }`}
                    >
                      {field.nullable ? 'NULL' : 'NOT NULL'}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-xs text-gray-600 dark:text-gray-300">
                    <code>{field.path}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const renderSampleData = () => {
    if (!formData.sampleData || formData.sampleData.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <p className="text-sm text-gray-500 dark:text-gray-400">
            No sample rows available
            {formData.recordCount > 0 && (
              <> · Total rows: <strong>{formData.recordCount.toLocaleString()}</strong></>
            )}
          </p>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium">
            Sample Rows ({Math.min(formData.sampleData.length, 10)} of {formData.recordCount.toLocaleString()})
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

  // ------------------------------------------------------------------
  // Step content
  // ------------------------------------------------------------------
  const renderStepContent = () => {
    switch (currentStep) {
      case 1:
        return (
          <div className="space-y-6">
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              General Properties
            </h3>
            <div className="grid grid-cols-1 gap-4">
              <div>
                <label className="block text-sm font-medium mb-1">
                  Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={formData.name}
                  onChange={(e) => updateFormData({ name: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                  placeholder="e.g., User Activity Log"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Purpose</label>
                <input
                  type="text"
                  value={formData.purpose}
                  onChange={(e) => updateFormData({ purpose: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                  placeholder="Why this Avro data is used"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Description</label>
                <textarea
                  value={formData.description}
                  onChange={(e) => updateFormData({ description: e.target.value })}
                  rows={3}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                  placeholder="Additional details..."
                />
              </div>

              <div className="pt-2 border-t border-gray-200 dark:border-gray-600">
                <h4 className="text-sm font-semibold mb-2 flex items-center gap-2">
                  <Layers className="h-4 w-4" />
                  Format Information
                </h4>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium mb-1">
                      File Format
                    </label>
                    <input
                      type="text"
                      value="Avro"
                      readOnly
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-gray-100 dark:bg-gray-600 dark:text-white cursor-not-allowed"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium mb-1">
                      Encoding
                    </label>
                    <select
                      value={formData.encoding}
                      onChange={(e) => updateFormData({ encoding: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                    >
                      <option value="UTF-8">UTF-8</option>
                      <option value="ISO-8859-1">ISO-8859-1</option>
                      <option value="ASCII">ASCII</option>
                    </select>
                  </div>
                </div>
              </div>
            </div>
          </div>
        );
      case 2:
        return (
          <div className="space-y-4">
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              File Selection
            </h3>
            <p className="text-sm text-gray-600 dark:text-gray-400">
              Upload an Avro binary file (.avro) or an Avro schema file (.avsc).
            </p>
            <input
              type="file"
              ref={fileInputRef}
              onChange={handleFileSelect}
              accept=".avro,.avsc"
              className="hidden"
            />
            <div className="space-y-3">
              <label className="block text-sm font-medium mb-1">
                Avro File <span className="text-red-500">*</span>
              </label>
              <div className="flex space-x-2">
                <input
                  type="text"
                  value={formData.filePath}
                  readOnly
                  className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md bg-gray-50 dark:bg-gray-600 dark:text-white"
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
                  {isLoading ? 'Reading...' : 'Browse'}
                </Button>
              </div>
              {formData.file && (
                <p className="text-sm text-green-600 dark:text-green-400">
                  ✓ {formData.file.name} ({(formData.file.size / 1024).toFixed(0)} KB)
                </p>
              )}
              {error && (
                <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                  <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                  <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                </div>
              )}
              {formData.schema.length > 0 && (
                <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                  <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                    <Braces className="h-4 w-4" />
                    <span className="font-medium">Avro schema loaded successfully</span>
                  </div>
                  <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                    Fields: <strong>{formData.totalFields}</strong>
                    {formData.recordCount > 0 && (
                      <> · Records: <strong>{formData.recordCount.toLocaleString()}</strong></>
                    )}
                  </p>
                </div>
              )}
            </div>
          </div>
        );
      case 3:
        return (
          <div className="space-y-6">
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              Schema Analysis
            </h3>
            <p className="text-sm text-gray-600 dark:text-gray-400">
              Review the inferred Avro schema structure.
            </p>
            {!formData.file ? (
              <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No file loaded. Go back to Step 2 and select a file.</p>
              </div>
            ) : (
              <>
                <div>
                  <h4 className="font-medium mb-2">Field Details</h4>
                  {renderSchemaTable()}
                </div>
                <div>
                  <h4 className="font-medium mb-2">Sample Data</h4>
                  {renderSampleData()}
                </div>
              </>
            )}
          </div>
        );
      case 4:
        return (
          <div className="space-y-6">
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              Summary & Save
            </h3>
            <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
              <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                <CheckCircle className="h-5 w-5" />
                <span className="font-medium">Ready to save</span>
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-3">
                <h4 className="font-medium">General</h4>
                <div className="text-sm space-y-2">
                  <div className="flex justify-between">
                    <span className="text-gray-500">Name</span>
                    <span className="font-medium">{formData.name || '—'}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">File</span>
                    <span>{formData.filePath || '—'}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">Fields</span>
                    <span>{formData.totalFields}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">Records</span>
                    <span>{formData.recordCount.toLocaleString()}</span>
                  </div>
                </div>
              </div>
              <div className="space-y-3">
                <h4 className="font-medium">Format Details</h4>
                <div className="text-sm space-y-2">
                  <div className="flex justify-between">
                    <span className="text-gray-500">Format</span>
                    <span className="font-medium">Avro</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-500">Encoding</span>
                    <span>{formData.encoding}</span>
                  </div>
                </div>
              </div>
            </div>
            {formData.schema.length > 0 && (
              <div>
                <h4 className="font-medium mb-2">Schema Preview</h4>
                <div className="max-h-40 overflow-y-auto border border-gray-200 dark:border-gray-600 rounded-md p-2">
                  <div className="space-y-1 text-sm">
                    {(formData.schema as AvroFieldDefinition[]).slice(0, 6).map((field, idx) => (
                      <div key={idx} className="flex justify-between py-1 border-b border-gray-100 dark:border-gray-700 last:border-0">
                        <span>
                          <span className="font-medium">{field.name}</span>
                          <span className="text-xs ml-2 bg-blue-100 dark:bg-blue-900 rounded px-1">{field.type}</span>
                          {field.logicalType && (
                            <span className="text-xs ml-1 bg-purple-100 dark:bg-purple-900 rounded px-1">{field.logicalType}</span>
                          )}
                        </span>
                        <span className="text-xs">{field.nullable ? 'NULL' : 'NOT NULL'}</span>
                      </div>
                    ))}
                    {formData.schema.length > 6 && (
                      <p className="text-center text-xs text-gray-500">… and {formData.schema.length - 6} more fields</p>
                    )}
                  </div>
                </div>
              </div>
            )}
          </div>
        );
      default:
        return null;
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-6xl max-h-[90vh] overflow-hidden"
      >
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">Avro Metadata Wizard</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">Step {currentStep} of {totalSteps}</p>
          </div>
          <Button variant="ghost" size="sm" onClick={handleClose} className="h-8 w-8 p-0">
            <X className="h-4 w-4" />
          </Button>
        </div>
        <div className="px-6 pt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${(currentStep / totalSteps) * 100}%` }}
            />
          </div>
        </div>
        <div className="p-6 max-h-[60vh] overflow-y-auto">{renderStepContent()}</div>
        <div className="flex items-center justify-between p-6 border-t border-gray-200 dark:border-gray-700">
          <Button variant="outline" onClick={currentStep === 1 ? handleClose : handleBack}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            {currentStep === 1 ? 'Cancel' : 'Back'}
          </Button>
          <div className="flex items-center space-x-3">
            <span className="text-sm text-gray-500 dark:text-gray-400">Step {currentStep} of {totalSteps}</span>
            {currentStep < totalSteps ? (
              <Button
                onClick={handleNext}
                disabled={
                  (currentStep === 1 && !formData.name.trim()) ||
                  (currentStep === 2 && !formData.file) ||
                  (currentStep === 3 && formData.schema.length === 0)
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

export default AvroMetadataWizard;