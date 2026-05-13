// ParquetMetadataWizard.tsx (real implementation using parquet-wasm)
import React, { useState, useRef, useEffect } from 'react';
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
  FileArchive,
  Layers,
} from 'lucide-react';
import {
  JsonAvroParquetMetadataFormData,
  JsonAvroParquetMetadataWizardProps,
} from '../types/types';

// Parquet WebAssembly library
// The default export is the WASM init function
import initWasm, {  } from 'parquet-wasm';

// ---------- WASM initialisation flag (once per app) ----------
let wasmInitialized = false;
async function ensureWasmInitialized(): Promise<void> {
  if (!wasmInitialized) {
    // initWasm loads the .wasm file and sets up internal bindings
    await initWasm();
    wasmInitialized = true;
  }
}

// ----------------------------------------------------------------------
// Parquet‑specific configuration options
// ----------------------------------------------------------------------
const PARQUET_COMPRESSION_CODECS = [
  { value: 'none', label: 'None' },
  { value: 'snappy', label: 'Snappy' },
  { value: 'gzip', label: 'Gzip' },
  { value: 'lzo', label: 'LZO' },
  { value: 'brotli', label: 'Brotli' },
  { value: 'lz4', label: 'LZ4' },
  { value: 'zstd', label: 'Zstandard (ZSTD)' },
];

const DEFAULT_ROW_GROUP_SIZE_MB = 128;
const DEFAULT_DATA_PAGE_SIZE_KB = 1024;


async function fetchParquetMetadata(file: File, sampleCount = 10) {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('sampleCount', String(sampleCount));

  const response = await fetch('http://localhost:3000/api/parquet/metadata', {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Metadata extraction failed: ${response.status} - ${errorText}`);
  }

  const result = await response.json();
  if (result.error) {
    throw new Error(result.error);
  }

  return {
    metadata: {
      schema: result.fields.map((field: any) => ({
        name: field.name,
        type: field.type,
        nullable: field.nullable,
        compression: field.metadata?.compression || '',
        path: field.name,
        level: 0,
        sampleValue: undefined,
        description: '',
      })),
      numRows: result.recordCount,
      rowGroups: result.numRowGroups || 1,
      compressionCodec: 'snappy',   // You can parse from file metadata if needed
      rowGroupSizeBytes: 0,
      dataPageSizeBytes: 0,
    },
    sampleRows: result.sampleRows,
  };
}

// Helper to map Parquet physical/logical types to string

// ----------------------------------------------------------------------
// Component
// ----------------------------------------------------------------------
const ParquetMetadataWizard: React.FC<JsonAvroParquetMetadataWizardProps> = ({
  isOpen,
  onClose,
  onSave,
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [formData, setFormData] = useState<
    JsonAvroParquetMetadataFormData & {
      compressionCodec: string;
      rowGroupSizeMB: number;
      dataPageSizeKB: number;
      dictionaryEncoding: boolean;
      statisticsEnabled: boolean;
      bloomFilterColumns: string[];
    }
  >({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    format: 'parquet',
    schema: [],
    totalFields: 0,
    recordCount: 0,
    encoding: 'UTF-8',
    compression: 'snappy',
    sampleData: [],
    compressionCodec: 'snappy',
    rowGroupSizeMB: DEFAULT_ROW_GROUP_SIZE_MB,
    dataPageSizeKB: DEFAULT_DATA_PAGE_SIZE_KB,
    dictionaryEncoding: true,
    statisticsEnabled: true,
    bloomFilterColumns: [],
  });

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 4;

  // Optionally pre-initialise WASM when the wizard opens (saves a tiny delay later)
  useEffect(() => {
    if (isOpen) {
      ensureWasmInitialized().catch((err) =>
        console.error('Parquet WASM init failed:', err)
      );
    }
  }, [isOpen]);

  // ------------------------------------------------------------------
  // File handling & parsing
  // ------------------------------------------------------------------
  const handleFileSelect = async (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const ext = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    if (ext !== '.parquet') {
      setError('Please select a valid Parquet file (.parquet)');
      return;
    }

    setError(null);
    setIsLoading(true);

    try {
      const { metadata, sampleRows } = await fetchParquetMetadata(file, 5);

      setFormData((prev) => ({
        ...prev,
        file,
        filePath: file.name,
        schema: metadata.schema,
        totalFields: metadata.schema.length,
        recordCount: metadata.numRows,
        compressionCodec: metadata.compressionCodec,
        rowGroupSizeMB: Math.round(
          metadata.rowGroupSizeBytes / (1024 * 1024)
        ),
        dataPageSizeKB: Math.round(metadata.dataPageSizeBytes / 1024),
        sampleData: sampleRows,
      }));
    } catch (err: any) {
      setError(`Failed to process Parquet file: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const updateFormData = (updates: Partial<typeof formData>) => {
    setFormData((prev) => ({ ...prev, ...updates }));
  };

  // ------------------------------------------------------------------
  // Navigation
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
      format: 'parquet',
      compression: formData.compressionCodec,
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
      format: 'parquet',
      schema: [],
      totalFields: 0,
      recordCount: 0,
      encoding: 'UTF-8',
      compression: 'snappy',
      sampleData: [],
      compressionCodec: 'snappy',
      rowGroupSizeMB: DEFAULT_ROW_GROUP_SIZE_MB,
      dataPageSizeKB: DEFAULT_DATA_PAGE_SIZE_KB,
      dictionaryEncoding: true,
      statisticsEnabled: true,
      bloomFilterColumns: [],
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
            <p>No schema fields found</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Parquet Schema ({formData.schema.length} columns)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium">Name</th>
                <th className="px-3 py-2 text-left font-medium">Type</th>
                <th className="px-3 py-2 text-left font-medium">Nullable</th>
                <th className="px-3 py-2 text-left font-medium">Compression</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.schema.map((col: any, idx) => (
                <tr key={idx} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 font-medium text-gray-700 dark:text-gray-300">
                    {col.name}
                  </td>
                  <td className="px-3 py-2">
                    <span className="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                      {col.type}
                    </span>
                  </td>
                  <td className="px-3 py-2">
                    <span
                      className={`text-xs px-2 py-1 rounded ${
                        col.nullable
                          ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300'
                          : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'
                      }`}
                    >
                      {col.nullable ? 'NULL' : 'NOT NULL'}
                    </span>
                  </td>
                  <td className="px-3 py-2 text-xs text-gray-500">
                    {col.compression || '—'}
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
            No sample rows available.
            {formData.recordCount > 0 && (
              <>
                {' '}
                Total rows:{' '}
                <strong>{formData.recordCount.toLocaleString()}</strong>
              </>
            )}
          </p>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium">
            Sample Rows ({Math.min(formData.sampleData.length, 10)} of{' '}
            {formData.recordCount.toLocaleString()})
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
            <p className="text-sm text-gray-600 dark:text-gray-400">
              Basic information and Parquet‑specific storage settings.
            </p>

            {/* ---------- Name Input ---------- */}
            <div className="mb-4">
              <label
                htmlFor="metadata-name"
                className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
              >
                Metadata Name <span className="text-red-500">*</span>
              </label>
              <input
                id="metadata-name"
                type="text"
                value={formData.name}
                onChange={(e) => updateFormData({ name: e.target.value })}
                placeholder="e.g. Sales Transactions Archive"
                className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm
                           bg-white dark:bg-gray-700 text-gray-900 dark:text-white
                           focus:ring-2 focus:ring-blue-500 focus:border-blue-500
                           placeholder-gray-400 dark:placeholder-gray-500"
              />
              {!formData.name.trim() && currentStep === 1 && (
                <p className="mt-1 text-xs text-red-500 dark:text-red-400">
                  A name is required to proceed.
                </p>
              )}
            </div>
            {/* ------------------------------- */}

            <div className="border-t border-gray-200 dark:border-gray-600 pt-4 mt-2">
              <h4 className="text-sm font-semibold mb-4 flex items-center gap-2">
                <FileArchive className="h-4 w-4" />
                Parquet Storage Options
              </h4>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-1">
                    Compression Codec
                  </label>
                  <select
                    value={formData.compressionCodec}
                    onChange={(e) =>
                      updateFormData({ compressionCodec: e.target.value })
                    }
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                  >
                    {PARQUET_COMPRESSION_CODECS.map((c) => (
                      <option key={c.value} value={c.value}>
                        {c.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">
                    Row Group Size (MB)
                  </label>
                  <input
                    type="number"
                    min={8}
                    max={512}
                    value={formData.rowGroupSizeMB}
                    onChange={(e) =>
                      updateFormData({ rowGroupSizeMB: Number(e.target.value) })
                    }
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium mb-1">
                    Data Page Size (KB)
                  </label>
                  <input
                    type="number"
                    min={64}
                    max={2048}
                    value={formData.dataPageSizeKB}
                    onChange={(e) =>
                      updateFormData({
                        dataPageSizeKB: Number(e.target.value),
                      })
                    }
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                  />
                </div>
                <div className="flex flex-col space-y-2">
                  <label className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      checked={formData.dictionaryEncoding}
                      onChange={(e) =>
                        updateFormData({
                          dictionaryEncoding: e.target.checked,
                        })
                      }
                      className="h-4 w-4"
                    />
                    <span className="text-sm">Enable Dictionary Encoding</span>
                  </label>
                  <label className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      checked={formData.statisticsEnabled}
                      onChange={(e) =>
                        updateFormData({
                          statisticsEnabled: e.target.checked,
                        })
                      }
                      className="h-4 w-4"
                    />
                    <span className="text-sm">Collect Column Statistics</span>
                  </label>
                </div>
              </div>
              <div className="mt-3">
                <label className="block text-sm font-medium mb-1">
                  Bloom Filter Columns (comma‑separated)
                </label>
                <input
                  type="text"
                  value={formData.bloomFilterColumns.join(', ')}
                  onChange={(e) => {
                    const cols = e.target.value
                      .split(',')
                      .map((s) => s.trim())
                      .filter(Boolean);
                    updateFormData({ bloomFilterColumns: cols });
                  }}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md dark:bg-gray-700 dark:text-white"
                />
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
              Upload a Parquet file to extract its schema and metadata.
            </p>
            <input
              type="file"
              ref={fileInputRef}
              onChange={handleFileSelect}
              accept=".parquet"
              className="hidden"
            />
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
                ✓ {formData.file.name} (
                {(formData.file.size / 1024).toFixed(0)} KB)
              </p>
            )}
            {error && (
              <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                <span className="text-sm text-red-600 dark:text-red-400">
                  {error}
                </span>
              </div>
            )}
            {formData.schema.length > 0 && (
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <Layers className="h-4 w-4" />
                  <span className="font-medium">
                    Parquet file read successfully
                  </span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Columns: <strong>{formData.totalFields}</strong> · Rows:{' '}
                  <strong>{formData.recordCount.toLocaleString()}</strong>
                </p>
              </div>
            )}
          </div>
        );

      case 3:
        return (
          <div className="space-y-6">
            <h3 className="text-lg font-medium text-gray-900 dark:text-white">
              Schema Analysis
            </h3>
            <p className="text-sm text-gray-600 dark:text-gray-400">
              Inspect the column structure and sample rows.
            </p>
            {!formData.file ? (
              <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No file loaded. Go back to Step 2 and select a file.</p>
              </div>
            ) : (
              <>
                {renderSchemaTable()}
                {renderSampleData()}
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
            <p className="text-sm text-gray-600 dark:text-gray-400">
              Review the Parquet metadata before saving.
            </p>
            <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
              <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                <CheckCircle className="h-5 w-5" />
                <span className="font-medium">Ready to save</span>
              </div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div className="space-y-2 text-sm">
                <h4 className="font-medium">General</h4>
                <div className="flex justify-between">
                  <span>Name</span>
                  <span className="font-medium">
                    {formData.name || '—'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>File</span>
                  <span>{formData.filePath || '—'}</span>
                </div>
                <div className="flex justify-between">
                  <span>Rows</span>
                  <span>{formData.recordCount.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span>Columns</span>
                  <span>{formData.totalFields}</span>
                </div>
              </div>
              <div className="space-y-2 text-sm">
                <h4 className="font-medium">Parquet Configuration</h4>
                <div className="flex justify-between">
                  <span>Compression</span>
                  <span className="font-medium">
                    {formData.compressionCodec}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span>Row Group Size</span>
                  <span>{formData.rowGroupSizeMB} MB</span>
                </div>
                <div className="flex justify-between">
                  <span>Page Size</span>
                  <span>{formData.dataPageSizeKB} KB</span>
                </div>
                <div className="flex justify-between">
                  <span>Dictionary Encoding</span>
                  <span>{formData.dictionaryEncoding ? 'Yes' : 'No'}</span>
                </div>
                <div className="flex justify-between">
                  <span>Statistics</span>
                  <span>
                    {formData.statisticsEnabled ? 'Enabled' : 'Disabled'}
                  </span>
                </div>
              </div>
            </div>
            {formData.schema.length > 0 && (
              <div>
                <h4 className="font-medium mb-2">Schema Preview</h4>
                <div className="max-h-40 overflow-y-auto border border-gray-200 dark:border-gray-600 rounded-md p-2">
                  <div className="space-y-1 text-sm">
                    {formData.schema.slice(0, 6).map((col: any, i) => (
                      <div
                        key={i}
                        className="flex justify-between py-1 border-b border-gray-100 dark:border-gray-700 last:border-0"
                      >
                        <span>
                          <span className="font-medium">{col.name}</span>
                          <span className="text-xs ml-2 bg-blue-100 dark:bg-blue-900 rounded px-1">
                            {col.type}
                          </span>
                        </span>
                        <span className="text-xs">
                          {col.nullable ? 'NULL' : 'NOT NULL'}
                        </span>
                      </div>
                    ))}
                    {formData.schema.length > 6 && (
                      <p className="text-center text-xs text-gray-500">
                        … and {formData.schema.length - 6} more columns
                      </p>
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
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Parquet Metadata Wizard
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

        {/* Progress bar */}
        <div className="px-6 pt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${(currentStep / totalSteps) * 100}%` }}
            />
          </div>
        </div>

        {/* Step content */}
        <div className="p-6 max-h-[60vh] overflow-y-auto">
          {renderStepContent()}
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

export default ParquetMetadataWizard;