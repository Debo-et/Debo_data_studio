// src/components/Editor/FileLookupEditor.tsx
import React, { useState, useEffect, useCallback } from 'react';
import { motion } from 'framer-motion';
import {
  X,
  Upload,
  RefreshCw,
  Settings,
  Database,
  Zap,
  Shield,
  ChevronRight,
  Check,
  AlertCircle,
  Plus,
  Trash2,
  Eye
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Button } from '../../ui/Button';
import { Switch } from '../../ui/switch';
import { Badge } from '../../ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '../../ui/table';
import { Checkbox } from '../../ui/checkbox';

// Simple column interface (from MapEditor)
interface SimpleColumn {
  name: string;
  type?: string;
  id?: string;
}

// Props
interface FileLookupEditorProps {
  nodeId: string;
  nodeMetadata?: any;
  inputColumns: SimpleColumn[];            // columns from upstream
  initialConfig?: any;                     // existing FileLookupComponentConfiguration
  onClose: () => void;
  onSave: (config: any) => void;           // saves the union { type: 'FILE_LOOKUP', config }
}

// File column info from preview
interface FileColumnInfo {
  name: string;
  type: string;
  sample?: string;
}

export const FileLookupEditor: React.FC<FileLookupEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave
}) => {
  // ==================== STATE ====================
  // File source
  const [filePath, setFilePath] = useState(initialConfig?.file?.path || '');
  const [fileFormat, setFileFormat] = useState<'CSV' | 'EXCEL' | 'JSON' | 'PARQUET' | 'AVRO'>(
    initialConfig?.file?.format || 'CSV'
  );
  const [fileOptions, setFileOptions] = useState<Record<string, any>>(
    initialConfig?.file?.options || { delimiter: ',', header: true }
  );

  // Preview
  const [fileColumns, setFileColumns] = useState<FileColumnInfo[]>([]);
  const [isLoadingPreview, setIsLoadingPreview] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  // Key mappings
  const [keyMappings, setKeyMappings] = useState<
    Array<{ inputField: string; fileColumn: string; operator?: string }>
  >(initialConfig?.keyMappings || []);

  // Return fields
  const [returnFields, setReturnFields] = useState<
    Array<{ fileColumn: string; outputName: string; dataType?: string; selected: boolean }>
  >(
    initialConfig?.returnFields?.map((rf: any) => ({
      ...rf,
      selected: true,
    })) ||
      fileColumns.map(col => ({
        fileColumn: col.name,
        outputName: col.name,
        dataType: col.type,
        selected: false,
      }))
  );

  // Caching
  const [cacheEnabled, setCacheEnabled] = useState(initialConfig?.cache?.enabled ?? true);
  const [cacheSize, setCacheSize] = useState(initialConfig?.cache?.size ?? 1000);
  const [cacheTtl, setCacheTtl] = useState(initialConfig?.cache?.ttlSeconds ?? 300);
  const [cacheType, setCacheType] = useState<'LRU' | 'FIFO' | 'NONE'>(
    initialConfig?.cache?.type || 'LRU'
  );

  // Fallback
  const [onMissing, setOnMissing] = useState<'NULL' | 'DEFAULT' | 'FAIL'>(
    initialConfig?.fallback?.onMissing || 'NULL'
  );
  const [defaultValue, setDefaultValue] = useState(initialConfig?.fallback?.defaultValue || '');

  // Error handling
  const [errorHandling, setErrorHandling] = useState<'FAIL' | 'SKIP_ROW' | 'LOG_CONTINUE'>(
    initialConfig?.errorHandling || 'FAIL'
  );

  // Parallelization
  const [parallelEnabled, setParallelEnabled] = useState(
    initialConfig?.parallelization?.enabled ?? false
  );
  const [maxThreads, setMaxThreads] = useState(initialConfig?.parallelization?.maxThreads ?? 4);
  const [batchSize, setBatchSize] = useState(initialConfig?.parallelization?.batchSize ?? 1000);

  // UI tabs
  const [activeTab, setActiveTab] = useState<'file' | 'mapping' | 'cache' | 'advanced'>('file');

  // ==================== EFFECTS ====================
  // When fileColumns change, update returnFields list
  useEffect(() => {
    if (fileColumns.length > 0) {
      setReturnFields(prev => {
        const newReturnFields = fileColumns.map(col => {
          const existing = prev.find(rf => rf.fileColumn === col.name);
          return {
            fileColumn: col.name,
            outputName: existing?.outputName || col.name,
            dataType: existing?.dataType || col.type,
            selected: existing?.selected ?? false,
          };
        });
        return newReturnFields;
      });
    }
  }, [fileColumns]);

  // ==================== HANDLERS ====================
  const handlePreview = useCallback(async () => {
    if (!filePath.trim()) {
      setPreviewError('Please enter a file path');
      return;
    }
    setIsLoadingPreview(true);
    setPreviewError(null);
    try {
      // Simulate API call – replace with actual fetch to backend
      await new Promise(resolve => setTimeout(resolve, 800));
      let mockColumns: FileColumnInfo[] = [];
      if (fileFormat === 'CSV') {
        mockColumns = [
          { name: 'customer_id', type: 'integer', sample: '101' },
          { name: 'name', type: 'string', sample: 'Acme Inc' },
          { name: 'city', type: 'string', sample: 'New York' },
          { name: 'region', type: 'string', sample: 'East' },
          { name: 'segment', type: 'string', sample: 'Enterprise' },
        ];
      } else if (fileFormat === 'EXCEL') {
        mockColumns = [
          { name: 'ID', type: 'integer', sample: '1001' },
          { name: 'Product', type: 'string', sample: 'Widget' },
          { name: 'Price', type: 'decimal', sample: '19.99' },
        ];
      } else {
        mockColumns = [
          { name: 'key', type: 'string', sample: 'abc' },
          { name: 'value', type: 'string', sample: 'xyz' },
        ];
      }
      setFileColumns(mockColumns);
    } catch (err: any) {
      setPreviewError(err.message || 'Failed to load file schema');
    } finally {
      setIsLoadingPreview(false);
    }
  }, [filePath, fileFormat]);

  // Add a key mapping row
  const addKeyMapping = () => {
    setKeyMappings(prev => [
      ...prev,
      { inputField: '', fileColumn: '', operator: '=' }
    ]);
  };

  // Remove key mapping
  const removeKeyMapping = (index: number) => {
    setKeyMappings(prev => prev.filter((_, i) => i !== index));
  };

  // Update key mapping field
  const updateKeyMapping = (index: number, field: 'inputField' | 'fileColumn' | 'operator', value: string) => {
    setKeyMappings(prev =>
      prev.map((item, i) => (i === index ? { ...item, [field]: value } : item))
    );
  };

  // Toggle return field selection
  const toggleReturnField = (fileColumn: string) => {
    setReturnFields(prev =>
      prev.map(f =>
        f.fileColumn === fileColumn ? { ...f, selected: !f.selected } : f
      )
    );
  };

  // Update return field output name
  const updateReturnFieldName = (fileColumn: string, newName: string) => {
    setReturnFields(prev =>
      prev.map(f => (f.fileColumn === fileColumn ? { ...f, outputName: newName } : f))
    );
  };

  // Save configuration
  const handleSave = () => {
    // Filter selected return fields
    const selectedReturnFields = returnFields
      .filter(f => f.selected)
      .map(({ fileColumn, outputName, dataType }) => ({
        fileColumn,
        outputName,
        dataType,
      }));

    // Build configuration object
    const config: any = {
      version: '1.0',
      file: {
        path: filePath,
        format: fileFormat,
        options: fileOptions,
      },
      keyMappings: keyMappings.filter(
        km => km.inputField && km.fileColumn
      ), // only valid ones
      returnFields: selectedReturnFields,
      cache: {
        enabled: cacheEnabled,
        size: cacheSize,
        ttlSeconds: cacheTtl,
        type: cacheType,
      },
      fallback: {
        onMissing,
        defaultValue: onMissing === 'DEFAULT' ? defaultValue : undefined,
      },
      errorHandling,
      parallelization: {
        enabled: parallelEnabled,
        maxThreads,
        batchSize,
      },
      // outputSchema will be built by the canvas using input + return fields
      outputSchema: {
        id: `${nodeId}_output_schema`,
        name: `${nodeMetadata?.name || 'FileLookup'} Output`,
        fields: [], // will be generated later
        isTemporary: false,
        isMaterialized: false,
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        validationStatus: 'VALID',
        warnings: [],
      },
    };

    onSave({ type: 'FILE_LOOKUP', config });
  };

  // ==================== RENDER ====================
  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-6xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <Database className="w-5 h-5 mr-2 text-blue-600" />
              tFileLookup Configuration
              <Badge variant="outline" className="ml-3 bg-purple-100 text-purple-800 border-purple-300">
                {nodeMetadata?.name || 'FileLookup_1'}
              </Badge>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Enrich your data by looking up values in a static file
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-200 rounded-full transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b bg-gray-50 px-4">
          {[
            { id: 'file', label: 'File Source', icon: Upload },
            { id: 'mapping', label: 'Key & Return Fields', icon: ChevronRight },
            { id: 'cache', label: 'Caching', icon: Zap },
            { id: 'advanced', label: 'Advanced', icon: Settings },
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`flex items-center space-x-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-blue-500 text-blue-700'
                  : 'border-transparent text-gray-600 hover:text-gray-900 hover:border-gray-300'
              }`}
            >
              <tab.icon className="w-4 h-4" />
              <span>{tab.label}</span>
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Tab 1: File Source */}
          {activeTab === 'file' && (
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">File Selection</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-3 gap-4">
                    <div className="col-span-2 space-y-2">
                      <Label>File Path</Label>
                      <div className="flex space-x-2">
                        <Input
                          value={filePath}
                          onChange={e => setFilePath(e.target.value)}
                          placeholder="/data/lookup/customers.csv"
                          className="flex-1"
                        />
                        <Button variant="outline" onClick={handlePreview} disabled={isLoadingPreview}>
                          {isLoadingPreview ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Eye className="w-4 h-4" />}
                          <span className="ml-2">Preview</span>
                        </Button>
                      </div>
                    </div>
                    <div className="space-y-2">
                      <Label>Format</Label>
                      <Select value={fileFormat} onValueChange={(val: any) => setFileFormat(val)}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select format" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="CSV">CSV</SelectItem>
                          <SelectItem value="EXCEL">Excel</SelectItem>
                          <SelectItem value="JSON">JSON</SelectItem>
                          <SelectItem value="PARQUET">Parquet</SelectItem>
                          <SelectItem value="AVRO">Avro</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>

                  {fileFormat === 'CSV' && (
                    <div className="grid grid-cols-3 gap-4">
                      <div className="space-y-2">
                        <Label>Delimiter</Label>
                        <Input
                          value={fileOptions.delimiter || ','}
                          onChange={e => setFileOptions({ ...fileOptions, delimiter: e.target.value })}
                          placeholder=","
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Quote Char</Label>
                        <Input
                          value={fileOptions.quoteChar || '"'}
                          onChange={e => setFileOptions({ ...fileOptions, quoteChar: e.target.value })}
                          placeholder='"'
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Escape Char</Label>
                        <Input
                          value={fileOptions.escapeChar || '\\'}
                          onChange={e => setFileOptions({ ...fileOptions, escapeChar: e.target.value })}
                          placeholder='\'
                        />
                      </div>
                      <div className="flex items-center space-x-2">
                        <Checkbox
                          id="header"
                          checked={fileOptions.header !== false}
                          onChange={(e) => setFileOptions({ ...fileOptions, header: e.target.checked })}
                        />
                        <Label htmlFor="header">First row is header</Label>
                      </div>
                    </div>
                  )}
                  {fileFormat === 'EXCEL' && (
                    <div className="space-y-2">
                      <Label>Sheet Name</Label>
                      <Input
                        value={fileOptions.sheet || 'Sheet1'}
                        onChange={e => setFileOptions({ ...fileOptions, sheet: e.target.value })}
                      />
                    </div>
                  )}
                </CardContent>
              </Card>

              {previewError && (
                <div className="bg-red-50 border border-red-200 text-red-800 p-3 rounded flex items-center">
                  <AlertCircle className="w-5 h-5 mr-2" />
                  {previewError}
                </div>
              )}

              {fileColumns.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">File Schema Preview</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="border rounded overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Column Name</TableHead>
                            <TableHead>Type</TableHead>
                            <TableHead>Sample Value</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {fileColumns.map((col, idx) => (
                            <TableRow key={idx}>
                              <TableCell className="font-mono text-sm">{col.name}</TableCell>
                              <TableCell>
                                <Badge variant="outline">{col.type}</Badge>
                              </TableCell>
                              <TableCell className="text-gray-600">{col.sample}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              )}
            </div>
          )}

          {/* Tab 2: Mapping */}
          {activeTab === 'mapping' && (
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Key Mapping</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="text-sm text-gray-600">
                    Map input fields to file columns to define the lookup key.
                  </div>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Input Field</TableHead>
                        <TableHead>Operator</TableHead>
                        <TableHead>File Column</TableHead>
                        <TableHead></TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {keyMappings.map((km, index) => (
                        <TableRow key={index}>
                          <TableCell>
                            <Select
                              value={km.inputField}
                              onValueChange={val => updateKeyMapping(index, 'inputField', val)}
                            >
                              <SelectTrigger>
                                <SelectValue placeholder="Select input field" />
                              </SelectTrigger>
                              <SelectContent>
                                {inputColumns.map(col => (
                                  <SelectItem key={col.name} value={col.name}>
                                    {col.name} ({col.type})
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </TableCell>
                          <TableCell>
                            <Select
                              value={km.operator || '='}
                              onValueChange={val => updateKeyMapping(index, 'operator', val)}
                            >
                              <SelectTrigger className="w-20">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem value="=">=</SelectItem>
                                <SelectItem value="!=">!=</SelectItem>
                                <SelectItem value="<">&lt;</SelectItem>
                                <SelectItem value="<=">&lt;=</SelectItem>
                                <SelectItem value=">">&gt;</SelectItem>
                                <SelectItem value=">=">&gt;=</SelectItem>
                              </SelectContent>
                            </Select>
                          </TableCell>
                          <TableCell>
                            <Select
                              value={km.fileColumn}
                              onValueChange={val => updateKeyMapping(index, 'fileColumn', val)}
                            >
                              <SelectTrigger>
                                <SelectValue placeholder="Select file column" />
                              </SelectTrigger>
                              <SelectContent>
                                {fileColumns.map(col => (
                                  <SelectItem key={col.name} value={col.name}>
                                    {col.name} ({col.type})
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                          </TableCell>
                          <TableCell>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => removeKeyMapping(index)}
                              className="text-red-600 hover:text-red-800"
                            >
                              <Trash2 className="w-4 h-4" />
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                  <Button variant="outline" size="sm" onClick={addKeyMapping}>
                    <Plus className="w-4 h-4 mr-2" /> Add Key Mapping
                  </Button>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Return Fields</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-sm text-gray-600 mb-4">
                    Select which file columns to add to the output and optionally rename them.
                  </div>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-10">Include</TableHead>
                        <TableHead>File Column</TableHead>
                        <TableHead>Output Name</TableHead>
                        <TableHead>Type</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {returnFields.map((rf, idx) => (
                        <TableRow key={idx}>
                          <TableCell>
                            <Checkbox
                              checked={rf.selected}
                              onChange={() => toggleReturnField(rf.fileColumn)}
                            />
                          </TableCell>
                          <TableCell className="font-mono">{rf.fileColumn}</TableCell>
                          <TableCell>
                            <Input
                              value={rf.outputName}
                              onChange={e => updateReturnFieldName(rf.fileColumn, e.target.value)}
                              className="h-8"
                              disabled={!rf.selected}
                            />
                          </TableCell>
                          <TableCell>
                            <Badge variant="outline">{rf.dataType}</Badge>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </CardContent>
              </Card>
            </div>
          )}

          {/* Tab 3: Caching */}
          {activeTab === 'cache' && (
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Cache Settings</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center space-x-2">
                    <Switch
                      id="cache-enable"
                      checked={cacheEnabled}
                      onCheckedChange={setCacheEnabled}
                    />
                    <Label htmlFor="cache-enable">Enable in-memory cache</Label>
                  </div>

                  {cacheEnabled && (
                    <>
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label>Cache Size (entries)</Label>
                          <Input
                            type="number"
                            value={cacheSize}
                            onChange={e => setCacheSize(parseInt(e.target.value) || 1000)}
                            min={1}
                            max={100000}
                          />
                        </div>
                        <div className="space-y-2">
                          <Label>TTL (seconds)</Label>
                          <Input
                            type="number"
                            value={cacheTtl}
                            onChange={e => setCacheTtl(parseInt(e.target.value) || 300)}
                            min={0}
                            placeholder="0 = infinite"
                          />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <Label>Cache Type</Label>
                        <Select value={cacheType} onValueChange={(val: any) => setCacheType(val)}>
                          <SelectTrigger>
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="LRU">LRU (Least Recently Used)</SelectItem>
                            <SelectItem value="FIFO">FIFO (First In, First Out)</SelectItem>
                            <SelectItem value="NONE">No eviction</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                    </>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Fallback Behavior</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2">
                    <Label>When key not found</Label>
                    <Select value={onMissing} onValueChange={(val: any) => setOnMissing(val)}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="NULL">Return NULL</SelectItem>
                        <SelectItem value="DEFAULT">Use default value</SelectItem>
                        <SelectItem value="FAIL">Fail the job</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  {onMissing === 'DEFAULT' && (
                    <div className="space-y-2">
                      <Label>Default Value</Label>
                      <Input
                        value={defaultValue}
                        onChange={e => setDefaultValue(e.target.value)}
                        placeholder="e.g., 'N/A'"
                      />
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          )}

          {/* Tab 4: Advanced */}
          {activeTab === 'advanced' && (
            <div className="space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Error Handling</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    <Label>On lookup error (e.g., malformed row)</Label>
                    <Select value={errorHandling} onValueChange={(val: any) => setErrorHandling(val)}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="FAIL">Fail job</SelectItem>
                        <SelectItem value="SKIP_ROW">Skip row and continue</SelectItem>
                        <SelectItem value="LOG_CONTINUE">Log warning and continue</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Parallelization</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center space-x-2">
                    <Switch
                      id="parallel-enable"
                      checked={parallelEnabled}
                      onCheckedChange={setParallelEnabled}
                    />
                    <Label htmlFor="parallel-enable">Enable parallel processing</Label>
                  </div>

                  {parallelEnabled && (
                    <div className="grid grid-cols-2 gap-4">
                      <div className="space-y-2">
                        <Label>Max Threads</Label>
                        <Input
                          type="number"
                          value={maxThreads}
                          onChange={e => setMaxThreads(parseInt(e.target.value) || 4)}
                          min={1}
                          max={32}
                        />
                      </div>
                      <div className="space-y-2">
                        <Label>Batch Size</Label>
                        <Input
                          type="number"
                          value={batchSize}
                          onChange={e => setBatchSize(parseInt(e.target.value) || 1000)}
                          min={1}
                          max={10000}
                        />
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">SQL Preview</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="bg-gray-100 p-3 rounded font-mono text-xs text-gray-700">
                    {filePath ? (
                      <>
                        -- Lookup join against {filePath}
                        <br />
                        LEFT JOIN lookup_table ON {keyMappings.map(km => `${km.inputField} = ${km.fileColumn}`).join(' AND ')}
                      </>
                    ) : (
                      <span className="text-gray-400">Configure file source to see SQL preview</span>
                    )}
                  </div>
                </CardContent>
              </Card>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-4 border-t bg-gray-50">
          <div className="text-xs text-gray-500">
            <Shield className="w-3 h-3 inline mr-1" />
            Configuration will be saved to node metadata
          </div>
          <div className="flex space-x-3">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button onClick={handleSave} className="bg-blue-600 hover:bg-blue-700 text-white">
              <Check className="w-4 h-4 mr-2" />
              Save Configuration
            </Button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};