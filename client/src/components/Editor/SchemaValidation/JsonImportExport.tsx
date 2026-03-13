// components/JsonImportExport.tsx
import React, { useState } from 'react';
import { SchemaValidationConfig } from './schema-validation';
import { 
  
  Button, Textarea, Alert, AlertDescription
} from '../../ui';

interface JsonImportExportProps {
  config: SchemaValidationConfig;
  onImport: (config: SchemaValidationConfig) => void;
  onExport: () => SchemaValidationConfig;
}

const JsonImportExport: React.FC<JsonImportExportProps> = ({
  config,
  onImport,
  onExport
}) => {
  const [jsonInput, setJsonInput] = useState<string>('');
  const [error, setError] = useState<string | null>(null);
  const [isImporting, setIsImporting] = useState(false);

  const handleImport = () => {
    try {
      setError(null);
      setIsImporting(true);
      
      if (!jsonInput.trim()) {
        throw new Error('Please enter JSON configuration');
      }

      const parsed = JSON.parse(jsonInput);
      
      // Basic validation
      if (!parsed.expectedSchema || !Array.isArray(parsed.expectedSchema)) {
        throw new Error('Invalid schema format');
      }
      
      if (!parsed.validationRules || !Array.isArray(parsed.validationRules)) {
        throw new Error('Invalid validation rules format');
      }

      onImport(parsed);
      setError(null);
      alert('Configuration imported successfully!');
    } catch (err: any) {
      setError(err.message || 'Invalid JSON format');
    } finally {
      setIsImporting(false);
    }
  };

  const handleExport = () => {
    const exportedConfig = onExport();
    const jsonString = JSON.stringify(exportedConfig, null, 2);
    
    // Copy to clipboard
    navigator.clipboard.writeText(jsonString).then(() => {
      alert('Configuration copied to clipboard!');
    }).catch(() => {
      // Fallback for older browsers
      const textArea = document.createElement('textarea');
      textArea.value = jsonString;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand('copy');
      document.body.removeChild(textArea);
      alert('Configuration copied to clipboard!');
    });
    
    // Also update the textarea for viewing
    setJsonInput(jsonString);
  };

  const handleDownload = () => {
    const exportedConfig = onExport();
    const jsonString = JSON.stringify(exportedConfig, null, 2);
    const blob = new Blob([jsonString], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${config.schemaName || 'schema'}-config.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  const handleLoadExample = () => {
    const exampleConfig = {
      schemaName: 'Example Schema',
      version: '1.0.0',
      strictMode: false,
      treatWarningsAsErrors: false,
      errorThreshold: 0.1,
      expectedSchema: [
        {
          name: 'id',
          type: 'uuid',
          nullable: false,
          format: 'uuid',
          description: 'Unique identifier'
        },
        {
          name: 'email',
          type: 'email',
          nullable: false,
          format: 'email',
          description: 'User email address'
        }
      ],
      validationRules: [
        {
          id: 'req_email',
          field: 'email',
          rule: 'required',
          message: 'Email is required',
          enabled: true,
          severity: 'error'
        }
      ]
    };
    
    setJsonInput(JSON.stringify(exampleConfig, null, 2));
  };

  return (
    <div className="space-y-4">
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}

      <div className="space-y-3">
        <div className="flex justify-between items-center">
          <h4 className="font-semibold">Export Configuration</h4>
          <div className="flex space-x-2">
            <Button variant="outline" size="sm" onClick={handleLoadExample}>
              Load Example
            </Button>
            <Button variant="outline" size="sm" onClick={handleExport}>
              Copy to Clipboard
            </Button>
            <Button variant="outline" size="sm" onClick={handleDownload}>
              Download JSON
            </Button>
          </div>
        </div>
        
        <Textarea
          value={jsonInput}
          onChange={(e) => setJsonInput(e.target.value)}
          placeholder="Paste JSON configuration here or use the buttons above"
          className="font-mono text-sm h-40"
        />
      </div>

      <div className="pt-4 border-t">
        <div className="flex justify-between items-center">
          <div>
            <h4 className="font-semibold">Import Configuration</h4>
            <p className="text-sm text-gray-500">
              Paste JSON above and click Import
            </p>
          </div>
          <Button 
            onClick={handleImport} 
            disabled={isImporting || !jsonInput.trim()}
          >
            {isImporting ? 'Importing...' : 'Import Configuration'}
          </Button>
        </div>
      </div>

      <div className="text-xs text-gray-500 space-y-1">
        <p><strong>Note:</strong> Importing will replace your current configuration.</p>
        <p>Make sure to export your current configuration before importing if you want to keep it.</p>
      </div>
    </div>
  );
};

export default JsonImportExport;