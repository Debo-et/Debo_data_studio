// components/SchemaValidationConfigEditor.tsx
import React, { useState, useCallback } from 'react';
import { 
  SchemaValidationConfig, 
  SchemaField, 
  ValidationRule,
  SchemaTemplate 
} from './schema-validation';
import { 
  Tabs, TabsList, TabsTrigger,
  Card, CardContent, CardHeader, CardTitle,
  Button, Label, Slider,
  Switch, Badge, Alert, AlertDescription
} from '../../ui';
import SchemaFieldEditor from './SchemaFieldEditor';
import RuleBuilder from './RuleBuilder';
import SchemaPreview from './SchemaPreview';
import TemplateSelector from './TemplateSelector';
import JsonImportExport from './JsonImportExport';
import SchemaComplianceMeter from './SchemaComplianceMeter';

interface SchemaValidationConfigEditorProps {
  initialConfig?: Partial<SchemaValidationConfig>;
  onChange: (config: SchemaValidationConfig) => void;
  onValidate?: () => void;
}

const DEFAULT_CONFIG: SchemaValidationConfig = {
  expectedSchema: [],
  validationRules: [],
  strictMode: false,
  treatWarningsAsErrors: false,
  errorThreshold: 0.1,
  schemaName: 'New Schema',
  version: '1.0.0'
};

const SchemaValidationConfigEditor: React.FC<SchemaValidationConfigEditorProps> = ({
  initialConfig,
  onChange,
  onValidate
}) => {
  const [config, setConfig] = useState<SchemaValidationConfig>({
    ...DEFAULT_CONFIG,
    ...initialConfig
  });
  const [mode, setMode] = useState<'schema-first' | 'rule-first'>('schema-first');
  const [selectedField, setSelectedField] = useState<string | null>(null);
  const [importError, setImportError] = useState<string | null>(null);

  const handleConfigUpdate = useCallback((updates: Partial<SchemaValidationConfig>) => {
    const newConfig = { ...config, ...updates };
    setConfig(newConfig);
    onChange(newConfig);
  }, [config, onChange]);

  const handleSchemaChange = (schema: SchemaField[]) => {
    handleConfigUpdate({ expectedSchema: schema });
    
    // In rule-first mode, infer validation rules from schema
    if (mode === 'rule-first' && schema.length > 0) {
      const inferredRules = inferRulesFromSchema(schema);
      handleConfigUpdate({ validationRules: inferredRules });
    }
  };

  const handleRulesChange = (rules: ValidationRule[]) => {
    handleConfigUpdate({ validationRules: rules });
    
    // In schema-first mode, ensure rules reference valid fields
    if (mode === 'schema-first') {
      validateRuleFieldReferences(rules, config.expectedSchema);
    }
  };

  const inferRulesFromSchema = (schema: SchemaField[]): ValidationRule[] => {
    const rules: ValidationRule[] = [];
    
    schema.forEach(field => {
      // Add required rule for non-nullable fields
      if (!field.nullable) {
        rules.push({
          id: `req_${field.name}_${Date.now()}`,
          field: field.name,
          rule: 'required',
          message: `Field '${field.name}' is required`,
          enabled: true,
          severity: 'error'
        });
      }

      // Add type validation
      rules.push({
        id: `type_${field.name}_${Date.now()}`,
        field: field.name,
        rule: 'type',
        parameters: { expectedType: field.type },
        message: `Field '${field.name}' must be of type ${field.type}`,
        enabled: true,
        severity: 'error'
      });

      // Add format validation if format is specified
      if (field.format) {
        rules.push({
          id: `format_${field.name}_${Date.now()}`,
          field: field.name,
          rule: 'format',
          parameters: { format: field.format },
          message: `Field '${field.name}' must match format ${field.format}`,
          enabled: true,
          severity: 'error'
        });
      }
    });

    return rules;
  };

  const validateRuleFieldReferences = (rules: ValidationRule[], schema: SchemaField[]) => {
    const fieldNames = schema.map(f => f.name);
    const invalidRules = rules.filter(rule => !fieldNames.includes(rule.field));
    
    if (invalidRules.length > 0) {
      console.warn('Rules reference non-existent fields:', invalidRules);
    }
  };

  const handleTemplateSelect = (template: SchemaTemplate) => {
    setConfig({
      ...config,
      expectedSchema: template.schema,
      validationRules: template.rules
    });
  };

  const calculateCoverage = () => {
    const fieldsWithRules = new Set(
      config.validationRules.map(rule => rule.field)
    ).size;
    const totalFields = config.expectedSchema.length;
    
    return totalFields > 0 ? fieldsWithRules / totalFields : 0;
  };

  const findConflictingRules = () => {
    const conflicts: Array<{ field: string; rules: string[] }> = [];
    
    config.expectedSchema.forEach(field => {
      const fieldRules = config.validationRules.filter(r => r.field === field.name);
      
      // Check for type conflicts
      const typeRules = fieldRules.filter(r => r.rule === 'type');
      if (typeRules.length > 1) {
        conflicts.push({
          field: field.name,
          rules: typeRules.map(r => r.id)
        });
      }
    });
    
    return conflicts;
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">
              Schema Compliance Check Configuration
            </h1>
            <p className="text-gray-600">
              Define schema structure and validation rules for data compliance
            </p>
          </div>
          <div className="flex space-x-3">
            <Button variant="outline" onClick={() => onValidate?.()}>
              Test Validation
            </Button>
            <Button onClick={() => console.log('Save config', config)}>
              Save Configuration
            </Button>
          </div>
        </div>

        {importError && (
          <Alert variant="destructive">
            <AlertDescription>{importError}</AlertDescription>
          </Alert>
        )}

        {/* Mode Selector */}
        <Card>
          <CardContent className="pt-6">
            <div className="flex justify-between items-center">
              <div>
                <Label className="text-lg font-semibold">Interface Mode</Label>
                <Tabs 
                  value={mode} 
                  onValueChange={(v: any) => setMode(v as any)}
                  className="mt-2"
                >
                  <TabsList>
                    <TabsTrigger value="schema-first">
                      Schema-First
                    </TabsTrigger>
                    <TabsTrigger value="rule-first">
                      Rule-First
                    </TabsTrigger>
                  </TabsList>
                </Tabs>
              </div>
              
              <div className="flex items-center space-x-4">
                <div className="text-right">
                  <div className="text-sm text-gray-500">Rule Coverage</div>
                  <SchemaComplianceMeter 
                    coverage={calculateCoverage()}
                    threshold={config.errorThreshold}
                  />
                </div>
                <TemplateSelector onSelect={handleTemplateSelect} />
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Main Editor */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left Column - Schema & Rules */}
          <div className="space-y-6">
            {/* Schema Editor */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Schema Definition</span>
                  <Badge variant="outline">
                    {config.expectedSchema.length} fields
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <SchemaFieldEditor
                  fields={config.expectedSchema}
                  onChange={handleSchemaChange}
                  onFieldSelect={setSelectedField}
                />
              </CardContent>
            </Card>

            {/* Rule Builder */}
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between">
                  <span>Validation Rules</span>
                  <Badge variant="outline">
                    {config.validationRules.length} rules
                  </Badge>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <RuleBuilder
                  rules={config.validationRules}
                  schema={config.expectedSchema}
                  onChange={handleRulesChange}
                  selectedField={selectedField}
                />
              </CardContent>
            </Card>
          </div>

          {/* Right Column - Settings & Preview */}
          <div className="space-y-6">
            {/* Validation Settings */}
            <Card>
              <CardHeader>
                <CardTitle>Validation Settings</CardTitle>
              </CardHeader>
              <CardContent className="space-y-6">
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <div>
                      <Label className="font-medium">Strict Mode</Label>
                      <p className="text-sm text-gray-500">
                        Reject fields not defined in schema
                      </p>
                    </div>
                    <Switch
                      checked={config.strictMode}
                      onCheckedChange={(checked) =>
                        handleConfigUpdate({ strictMode: checked })
                      }
                    />
                  </div>

                  <div className="flex items-center justify-between">
                    <div>
                      <Label className="font-medium">Treat Warnings as Errors</Label>
                      <p className="text-sm text-gray-500">
                        Upgrade all warnings to error severity
                      </p>
                    </div>
                    <Switch
                      checked={config.treatWarningsAsErrors}
                      onCheckedChange={(checked) =>
                        handleConfigUpdate({ treatWarningsAsErrors: checked })
                      }
                    />
                  </div>

                  <div className="space-y-3">
                    <div className="flex justify-between">
                      <Label className="font-medium">Error Threshold</Label>
                      <span className="text-sm font-medium">
                        {Math.round(config.errorThreshold * 100)}%
                      </span>
                    </div>
                    <Slider
                      value={[config.errorThreshold]}
                      min={0}
                      max={1}
                      step={0.01}
                      onValueChange={([value]) =>
                        handleConfigUpdate({ errorThreshold: value })
                      }
                    />
                    <p className="text-sm text-gray-500">
                      Fail validation if error rate exceeds this threshold
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Import/Export */}
            <Card>
              <CardHeader>
                <CardTitle>Import/Export</CardTitle>
              </CardHeader>
              <CardContent>
                <JsonImportExport
                  config={config}
                  onImport={(importedConfig) => {
                    try {
                      setConfig(importedConfig);
                      setImportError(null);
                    } catch (error) {
                      setImportError('Invalid JSON configuration');
                    }
                  }}
                  onExport={() => config}
                />
              </CardContent>
            </Card>

            {/* Preview Panel */}
            <Card>
              <CardHeader>
                <CardTitle>Validation Preview</CardTitle>
              </CardHeader>
              <CardContent>
                <SchemaPreview
                  schema={config.expectedSchema}
                  rules={config.validationRules}
                  strictMode={config.strictMode}
                  errorThreshold={config.errorThreshold}
                />
              </CardContent>
            </Card>
          </div>
        </div>

        {/* Conflict Detection */}
        {findConflictingRules().length > 0 && (
          <Alert variant="warning">
            <AlertDescription>
              Found {findConflictingRules().length} conflicting rule sets. 
              Review and resolve conflicts for proper validation.
            </AlertDescription>
          </Alert>
        )}
      </div>
    </div>
  );
};

export default SchemaValidationConfigEditor;