import React, { useState, useCallback } from 'react';
import { motion } from 'framer-motion';
import { 
  X, 
  Upload, 
  Download, 
  FileText, 
  FileCode, 
  FileJson, 
  Check,
  AlertCircle,
  Copy,
  Clipboard,
  Database} from 'lucide-react';
// Import FileX instead of FileXml
import { FileX } from 'lucide-react';
import { Tab } from '@headlessui/react';
import { RuleSet } from '../../../types/types';

// Create a more flexible type that includes all properties you're using
interface ExtendedRule {
  id: string;
  inputColumn: string;
  operation?: string;
  matchPattern?: string;
  replacement?: string;
  lookup?: any;
  priority?: number;
  enabled?: boolean;
  description?: string;
  patternType?: string;
  [key: string]: any; // Allow additional properties
}

interface ExtendedRuleSet extends Omit<RuleSet, 'rules'> {
  rules: ExtendedRule[];
  outputSchema?: any;
  metadata?: any;
  [key: string]: any;
}

interface ImportExportDialogProps {
  mode: 'import' | 'export';
  ruleSet: ExtendedRuleSet;
  onClose: () => void;
  onImport: (ruleSet: RuleSet) => void;
}

const ImportExportDialog: React.FC<ImportExportDialogProps> = ({
  mode,
  ruleSet,
  onClose,
  onImport
}) => {
  const [selectedTab, setSelectedTab] = useState(0);
  const [importFormat, setImportFormat] = useState<'json' | 'xml' | 'talend'>('json');
  const [exportFormat, setExportFormat] = useState<'json' | 'xml' | 'talend' | 'sql'>('json');
  const [importData, setImportData] = useState('');
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [exportOptions, setExportOptions] = useState({
    prettyPrint: true,
    includeMetadata: true,
    includeSchema: true,
    includeTestCases: false,
    includeValidation: false,
    compress: false
  });

  // Use FileX icon for XML format
  const formats = [
    { id: 'json', name: 'JSON', icon: FileJson, description: 'JavaScript Object Notation', mime: 'application/json' },
    { id: 'xml', name: 'XML', icon: FileX, description: 'Extensible Markup Language', mime: 'application/xml' },
    { id: 'talend', name: 'Talend Metadata', icon: Database, description: 'Talend Data Quality format', mime: 'application/json' },
    { id: 'sql', name: 'SQL Script', icon: FileCode, description: 'Database schema and rules', mime: 'application/sql' }
  ];

  const handleFileSelect = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setImportFile(file);
      const reader = new FileReader();
      reader.onload = (event) => {
        try {
          setImportData(event.target?.result as string);
          setImportError(null);
        } catch (error) {
          setImportError('Failed to read file');
        }
      };
      reader.onerror = () => setImportError('Failed to read file');
      reader.readAsText(file);
    }
  }, []);

  const validateImportData = (data: string, format: string): RuleSet | null => {
    try {
      if (format === 'json') {
        const parsed = JSON.parse(data);
        
        // Basic validation
        if (!parsed.name || !Array.isArray(parsed.rules)) {
          throw new Error('Invalid rule set structure');
        }

        // Create a validated ruleset
        const validatedRuleset: any = {
          id: parsed.id || `imported_${Date.now()}`,
          name: parsed.name,
          description: parsed.description || '',
          version: parsed.version || '1.0.0',
          author: parsed.author || 'Imported',
          created: parsed.created ? new Date(parsed.created) : new Date(),
          lastModified: new Date(),
          rules: parsed.rules.map((rule: any) => ({
            id: rule.id || `rule_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            inputColumn: rule.inputColumn || '',
            // Handle different possible property names
            operation: rule.operation || rule.op || 'Replace',
            matchPattern: rule.matchPattern || rule.pattern || '',
            replacement: rule.replacement || '',
            lookup: rule.lookup,
            priority: rule.priority || 50,
            enabled: rule.enabled !== false,
            description: rule.description,
            patternType: rule.patternType || 'regex'
          }))
        };

        // Add optional properties
        if (parsed.inputSchema) validatedRuleset.inputSchema = parsed.inputSchema;
        if (parsed.outputSchema) validatedRuleset.outputSchema = parsed.outputSchema;
        if (parsed.metadata) validatedRuleset.metadata = parsed.metadata;

        return validatedRuleset as RuleSet;
      }
      // Add validation for other formats here
      return null;
    } catch (error: any) {
      setImportError(error.message || 'Invalid format');
      return null;
    }
  };

  const handleImport = useCallback(() => {
    if (!importData.trim()) {
      setImportError('Please provide import data or select a file');
      return;
    }

    setIsProcessing(true);
    try {
      const validated = validateImportData(importData, importFormat);
      if (validated) {
        onImport(validated);
        onClose();
      }
    } catch (error: any) {
      setImportError(error.message || 'Import failed');
    } finally {
      setIsProcessing(false);
    }
  }, [importData, importFormat, onImport, onClose]);

  const handleExport = useCallback(() => {
    setIsProcessing(true);
    try {
      let exportData: any = {
        ...ruleSet,
        lastModified: new Date(),
        exportedAt: new Date().toISOString(),
        exportFormat: exportFormat
      };

      // Apply export options
      if (!exportOptions.includeMetadata) {
        delete exportData.metadata;
      }
      if (!exportOptions.includeSchema) {
        delete exportData.inputSchema;
        delete exportData.outputSchema;
      }

      // Format the data
      let content: string;
      let mimeType: string;
      let fileName = `ruleset_${ruleSet.name.replace(/\s+/g, '_').toLowerCase()}`;

      switch (exportFormat) {
        case 'json':
          content = exportOptions.prettyPrint 
            ? JSON.stringify(exportData, null, 2)
            : JSON.stringify(exportData);
          mimeType = 'application/json';
          fileName += '.json';
          break;
        
        case 'xml':
          // Simple XML conversion for demonstration
          content = `<?xml version="1.0" encoding="UTF-8"?>
<ruleSet>
  <name>${ruleSet.name}</name>
  <description>${ruleSet.description || ''}</description>
  <version>${ruleSet.version || '1.0.0'}</version>
  <author>${ruleSet.author || 'Unknown'}</author>
  <rules>
    ${ruleSet.rules.map(rule => `
    <rule>
      <id>${rule.id}</id>
      <inputColumn>${rule.inputColumn}</inputColumn>
      <operation>${rule.operation || ''}</operation>
      <matchPattern>${rule.matchPattern || ''}</matchPattern>
      <replacement>${rule.replacement || ''}</replacement>
      <priority>${rule.priority || 50}</priority>
      <enabled>${rule.enabled !== false}</enabled>
    </rule>`).join('')}
  </rules>
</ruleSet>`;
          mimeType = 'application/xml';
          fileName += '.xml';
          break;
        
        case 'sql':
          content = `-- Standardization Rules SQL Script
-- Generated: ${new Date().toISOString()}
-- Rule Set: ${ruleSet.name}

CREATE TABLE IF NOT EXISTS standardization_rules (
    id VARCHAR(50) PRIMARY KEY,
    rule_set_id VARCHAR(50),
    input_column VARCHAR(100),
    operation VARCHAR(50),
    match_pattern TEXT,
    replacement TEXT,
    priority INTEGER DEFAULT 50,
    enabled BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert rules
${ruleSet.rules.map(rule => 
`INSERT INTO standardization_rules (id, rule_set_id, input_column, operation, match_pattern, replacement, priority, enabled, description)
VALUES ('${rule.id}', '${ruleSet.id}', '${rule.inputColumn}', '${rule.operation || ''}', '${rule.matchPattern || ''}', '${rule.replacement || ''}', ${rule.priority || 50}, ${rule.enabled !== false ? 'TRUE' : 'FALSE'}, '${rule.description || ''}');`
).join('\n')}`;
          mimeType = 'application/sql';
          fileName += '.sql';
          break;
        
        case 'talend':
          // Talend DQ format (simplified)
          content = JSON.stringify({
            talendVersion: "7.3.1",
            component: "tDataStandardization",
            ruleset: {
              name: ruleSet.name,
              description: ruleSet.description || '',
              rules: ruleSet.rules.map(rule => ({
                column: rule.inputColumn,
                operation: rule.operation || '',
                pattern: rule.matchPattern || '',
                replacement: rule.replacement || '',
                active: rule.enabled !== false
              }))
            }
          }, null, 2);
          mimeType = 'application/json';
          fileName += '_talend.json';
          break;
        
        default:
          throw new Error('Unsupported export format');
      }

      // Compress if requested
      if (exportOptions.compress) {
        // In a real implementation, use compression library
        console.log('Compression would be applied here');
      }

      // Create download
      const blob = new Blob([content], { type: mimeType });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);

    } catch (error: any) {
      console.error('Export failed:', error);
      alert(`Export failed: ${error.message}`);
    } finally {
      setIsProcessing(false);
    }
  }, [ruleSet, exportFormat, exportOptions]);

  const handleCopyToClipboard = useCallback(() => {
    const exportData = {
      ...ruleSet,
      lastModified: new Date(),
      exportedAt: new Date().toISOString()
    };
    
    const content = JSON.stringify(exportData, null, 2);
    navigator.clipboard.writeText(content)
      .then(() => alert('Copied to clipboard!'))
      .catch(() => alert('Failed to copy to clipboard'));
  }, [ruleSet]);

  const renderImportPanel = () => (
    <div className="space-y-6">
      {/* Format Selection */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
          Import Format
        </h4>
        <div className="grid grid-cols-3 gap-3">
          {formats.slice(0, 3).map((format) => (
            <button
              key={format.id}
              onClick={() => setImportFormat(format.id as any)}
              className={`p-4 rounded-lg border-2 transition-all ${
                importFormat === format.id
                  ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                  : 'border-gray-300 dark:border-gray-600 hover:border-gray-400 dark:hover:border-gray-500'
              }`}
            >
              <div className="flex items-center space-x-3">
                <format.icon className="w-5 h-5" />
                <div className="text-left">
                  <div className="font-medium">{format.name}</div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">
                    {format.description}
                  </div>
                </div>
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* File Upload */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
          Upload File
        </h4>
        <div className="border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg p-8 text-center hover:border-gray-400 dark:hover:border-gray-500 transition-colors">
          <input
            type="file"
            id="file-upload"
            accept=".json,.xml,.txt"
            onChange={handleFileSelect}
            className="hidden"
          />
          <label htmlFor="file-upload" className="cursor-pointer">
            <Upload className="w-12 h-12 mx-auto text-gray-400 mb-4" />
            <div className="text-gray-600 dark:text-gray-300 font-medium mb-1">
              Click to upload or drag and drop
            </div>
            <div className="text-sm text-gray-500 dark:text-gray-400">
              {importFormat === 'json' && 'JSON files only'}
              {importFormat === 'xml' && 'XML files only'}
              {importFormat === 'talend' && 'Talend metadata files'}
            </div>
          </label>
          {importFile && (
            <div className="mt-4 p-3 bg-gray-100 dark:bg-gray-700 rounded-lg flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <FileText className="w-5 h-5" />
                <div className="text-left">
                  <div className="font-medium text-sm">{importFile.name}</div>
                  <div className="text-xs text-gray-500">
                    {(importFile.size / 1024).toFixed(1)} KB
                  </div>
                </div>
              </div>
              <button
                onClick={() => setImportFile(null)}
                className="text-red-500 hover:text-red-700"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Data Input */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Or Paste Data
          </h4>
          <button
            onClick={() => navigator.clipboard.readText().then(setImportData)}
            className="text-sm text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 flex items-center space-x-1"
          >
            <Clipboard className="w-4 h-4" />
            <span>Paste from clipboard</span>
          </button>
        </div>
        <textarea
          value={importData}
          onChange={(e) => setImportData(e.target.value)}
          placeholder={`Paste your ${importFormat.toUpperCase()} data here...`}
          rows={8}
          className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white font-mono text-sm resize-none"
        />
      </div>

      {/* Error Display */}
      {importError && (
        <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
          <div className="flex items-start space-x-3">
            <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
            <div className="text-sm text-red-700 dark:text-red-300">
              <div className="font-medium">Import Error</div>
              <div className="mt-1">{importError}</div>
            </div>
          </div>
        </div>
      )}

      {/* Preview */}
      {importData && !importError && (
        <div>
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
            Preview
          </h4>
          <div className="bg-gray-900 text-gray-100 p-4 rounded-lg overflow-auto max-h-40">
            <pre className="text-xs">
              {importData.length > 500 
                ? importData.substring(0, 500) + '...' 
                : importData}
            </pre>
          </div>
        </div>
      )}

      {/* Import Button */}
      <button
        onClick={handleImport}
        disabled={isProcessing || !importData.trim()}
        className="w-full py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center justify-center space-x-2"
      >
        {isProcessing ? (
          <>
            <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
            <span>Processing...</span>
          </>
        ) : (
          <>
            <Download className="w-5 h-5" />
            <span>Import Rule Set</span>
          </>
        )}
      </button>
    </div>
  );

  const renderExportPanel = () => (
    <div className="space-y-6">
      {/* Format Selection */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
          Export Format
        </h4>
        <div className="grid grid-cols-2 gap-3">
          {formats.map((format) => (
            <button
              key={format.id}
              onClick={() => setExportFormat(format.id as any)}
              className={`p-4 rounded-lg border-2 transition-all ${
                exportFormat === format.id
                  ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                  : 'border-gray-300 dark:border-gray-600 hover:border-gray-400 dark:hover:border-gray-500'
              }`}
            >
              <div className="flex items-center space-x-3">
                <format.icon className="w-5 h-5" />
                <div className="text-left">
                  <div className="font-medium">{format.name}</div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">
                    {format.description}
                  </div>
                </div>
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* Export Options */}
      <div>
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
          Options
        </h4>
        <div className="space-y-3 bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
          <label className="flex items-center space-x-3 cursor-pointer">
            <input
              type="checkbox"
              checked={exportOptions.prettyPrint}
              onChange={(e) => setExportOptions(prev => ({ ...prev, prettyPrint: e.target.checked }))}
              className="rounded border-gray-300 dark:border-gray-600"
            />
            <span className="text-sm">Pretty print (formatted output)</span>
          </label>

          <label className="flex items-center space-x-3 cursor-pointer">
            <input
              type="checkbox"
              checked={exportOptions.includeMetadata}
              onChange={(e) => setExportOptions(prev => ({ ...prev, includeMetadata: e.target.checked }))}
              className="rounded border-gray-300 dark:border-gray-600"
            />
            <span className="text-sm">Include metadata (author, dates, etc.)</span>
          </label>

          <label className="flex items-center space-x-3 cursor-pointer">
            <input
              type="checkbox"
              checked={exportOptions.includeSchema}
              onChange={(e) => setExportOptions(prev => ({ ...prev, includeSchema: e.target.checked }))}
              className="rounded border-gray-300 dark:border-gray-600"
            />
            <span className="text-sm">Include schema information</span>
          </label>

          <label className="flex items-center space-x-3 cursor-pointer">
            <input
              type="checkbox"
              checked={exportOptions.compress}
              onChange={(e) => setExportOptions(prev => ({ ...prev, compress: e.target.checked }))}
              className="rounded border-gray-300 dark:border-gray-600"
            />
            <span className="text-sm">Compress output (minify)</span>
          </label>
        </div>
      </div>

      {/* Summary */}
      <div className="bg-gray-50 dark:bg-gray-800 p-4 rounded-lg">
        <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
          Export Summary
        </h4>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-600 dark:text-gray-400">Rule Set:</span>
            <span className="font-medium">{ruleSet.name}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600 dark:text-gray-400">Rules Count:</span>
            <span className="font-medium">{ruleSet.rules.length}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600 dark:text-gray-400">Format:</span>
            <span className="font-medium">{exportFormat.toUpperCase()}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-600 dark:text-gray-400">Estimated Size:</span>
            <span className="font-medium">
              {Math.round(JSON.stringify(ruleSet).length / 1024 * 10) / 10} KB
            </span>
          </div>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="flex space-x-3">
        <button
          onClick={handleCopyToClipboard}
          className="flex-1 py-3 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center justify-center space-x-2"
        >
          <Copy className="w-5 h-5" />
          <span>Copy to Clipboard</span>
        </button>
        <button
          onClick={handleExport}
          disabled={isProcessing}
          className="flex-1 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center justify-center space-x-2"
        >
          {isProcessing ? (
            <>
              <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
              <span>Exporting...</span>
            </>
          ) : (
            <>
              <Download className="w-5 h-5" />
              <span>Download File</span>
            </>
          )}
        </button>
      </div>
    </div>
  );

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4"
      onClick={onClose}
    >
      <motion.div
        initial={{ scale: 0.9, y: 20 }}
        animate={{ scale: 1, y: 0 }}
        exit={{ scale: 0.9, y: 20 }}
        className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-2xl w-full max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="border-b border-gray-200 dark:border-gray-700 p-6">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-bold text-gray-900 dark:text-white">
                {mode === 'import' ? 'Import Rule Set' : 'Export Rule Set'}
              </h2>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                {mode === 'import' 
                  ? 'Import standardization rules from external sources'
                  : 'Export your rule set for sharing or backup'}
              </p>
            </div>
            <button
              onClick={onClose}
              className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-auto p-6">
          <Tab.Group selectedIndex={selectedTab} onChange={setSelectedTab}>
            <Tab.List className="flex space-x-1 border-b border-gray-200 dark:border-gray-700 mb-6">
              <Tab className={({ selected }) =>
                `px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                  selected
                    ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                    : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                }`
              }>
                {mode === 'import' ? 'Import Settings' : 'Export Settings'}
              </Tab>
              <Tab className={({ selected }) =>
                `px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                  selected
                    ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                    : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
                }`
              }>
                Preview
              </Tab>
            </Tab.List>

            <Tab.Panels>
              <Tab.Panel>
                {mode === 'import' ? renderImportPanel() : renderExportPanel()}
              </Tab.Panel>
              
              <Tab.Panel>
                <div className="bg-gray-900 text-gray-100 p-4 rounded-lg overflow-auto">
                  <pre className="text-xs">
                    {mode === 'import' 
                      ? importData || 'No data to preview'
                      : JSON.stringify(ruleSet, null, 2)}
                  </pre>
                </div>
              </Tab.Panel>
            </Tab.Panels>
          </Tab.Group>
        </div>

        {/* Footer */}
        <div className="border-t border-gray-200 dark:border-gray-700 p-6">
          <div className="flex items-center justify-between text-sm text-gray-500 dark:text-gray-400">
            <div className="flex items-center space-x-2">
              {mode === 'import' ? (
                <>
                  <FileText className="w-4 h-4" />
                  <span>Supported formats: JSON, XML, Talend</span>
                </>
              ) : (
                <>
                  <Check className="w-4 h-4 text-green-500" />
                  <span>Ready to export {ruleSet.rules.length} rules</span>
                </>
              )}
            </div>
            <button
              onClick={onClose}
              className="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700"
            >
              Cancel
            </button>
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
};

export default ImportExportDialog;