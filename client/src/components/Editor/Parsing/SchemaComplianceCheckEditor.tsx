import React, { useState, useCallback } from 'react';
import {
  SchemaComplianceCheckConfiguration,
  ExpectedColumn,
  DataType,
  ComplianceValidationRule,
} from '../../../types/unified-pipeline.types';
import { SimpleColumn } from '../../../pages/canvas.types';
import { Button } from '../../ui/Button';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { Switch } from '../../ui/switch';
import { Textarea } from '../../ui/textarea';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '../../ui/table';
import { Badge } from '../../ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '../../ui/card';
import { Plus, Trash2, Edit2, X } from 'lucide-react';

// ----------------------------------------------------------------------
// New Modal Components
// ----------------------------------------------------------------------

interface ColumnEditorModalProps {
  column: ExpectedColumn | null;
  onSave: (column: ExpectedColumn) => void;
  onClose: () => void;
}

const ColumnEditorModal: React.FC<ColumnEditorModalProps> = ({ column, onSave, onClose }) => {
  const [editedColumn, setEditedColumn] = useState<ExpectedColumn>(
    column || {
      id: `col-${Date.now()}`,
      name: '',
      dataType: 'STRING',
      nullable: true,
      required: false,
      validationRules: [],
      expression: '',
    }
  );

  const handleSave = () => {
    onSave(editedColumn);
    onClose();
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-70 flex items-center justify-center z-[10000]">
      <div className="bg-white rounded-lg w-full max-w-2xl p-6">
        <h3 className="text-lg font-bold mb-4">Edit Column</h3>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <Label>Column Name</Label>
            <Input
              value={editedColumn.name}
              onChange={e => setEditedColumn({ ...editedColumn, name: e.target.value })}
            />
          </div>
          <div>
            <Label>Data Type</Label>
            <Select
              value={editedColumn.dataType}
              onValueChange={(v: DataType) => setEditedColumn({ ...editedColumn, dataType: v })}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {DATA_TYPES.map(t => (
                  <SelectItem key={t} value={t}>
                    {t}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label>Length (for string)</Label>
            <Input
              type="number"
              value={editedColumn.length || ''}
              onChange={e =>
                setEditedColumn({
                  ...editedColumn,
                  length: e.target.value ? Number(e.target.value) : undefined,
                })
              }
            />
          </div>
          <div>
            <Label>Precision</Label>
            <Input
              type="number"
              value={editedColumn.precision || ''}
              onChange={e =>
                setEditedColumn({
                  ...editedColumn,
                  precision: e.target.value ? Number(e.target.value) : undefined,
                })
              }
            />
          </div>
          <div>
            <Label>Scale</Label>
            <Input
              type="number"
              value={editedColumn.scale || ''}
              onChange={e =>
                setEditedColumn({
                  ...editedColumn,
                  scale: e.target.value ? Number(e.target.value) : undefined,
                })
              }
            />
          </div>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              <Switch
                checked={editedColumn.nullable}
                onCheckedChange={v => setEditedColumn({ ...editedColumn, nullable: v })}
              />
              <Label>Nullable</Label>
            </div>
            <div className="flex items-center gap-2">
              <Switch
                checked={editedColumn.required}
                onCheckedChange={v => setEditedColumn({ ...editedColumn, required: v })}
              />
              <Label>Required</Label>
            </div>
          </div>
          <div className="col-span-2">
            <Label>Default Value (if missing)</Label>
            <Input
              value={editedColumn.defaultValue || ''}
              onChange={e => setEditedColumn({ ...editedColumn, defaultValue: e.target.value || undefined })}
            />
          </div>
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button onClick={handleSave}>Save</Button>
        </div>
      </div>
    </div>
  );
};

interface RuleEditorModalProps {
  columnId: string;
  rule?: ComplianceValidationRule;
  onSave: (columnId: string, rule: ComplianceValidationRule) => void;
  onClose: () => void;
}

const RuleEditorModal: React.FC<RuleEditorModalProps> = ({ columnId, rule, onSave, onClose }) => {
  const [ruleType, setRuleType] = useState<ComplianceValidationRule['type']>(rule?.type || 'type');
  const [errorMessage, setErrorMessage] = useState(rule?.errorMessage || '');
  const [params, setParams] = useState(rule?.params || {});

  const handleSave = () => {
    const newRule: ComplianceValidationRule = {
      id: rule?.id || `rule-${Date.now()}`,
      type: ruleType,
      params,
      errorMessage,
    };
    onSave(columnId, newRule);
    onClose();
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-70 flex items-center justify-center z-[10000]">
      <div className="bg-white rounded-lg w-full max-w-md p-6">
        <h3 className="text-lg font-bold mb-4">{rule ? 'Edit' : 'Add'} Validation Rule</h3>
        <div className="space-y-4">
          <div>
            <Label>Rule Type</Label>
            <Select value={ruleType} onValueChange={(v: any) => setRuleType(v)}>
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="type">Data Type Check</SelectItem>
                <SelectItem value="null">Nullability Check</SelectItem>
                <SelectItem value="pattern">Pattern (Regex)</SelectItem>
                <SelectItem value="expression">SQL Expression</SelectItem>
                <SelectItem value="range">Range (min/max)</SelectItem>
                <SelectItem value="custom">Custom</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label>Error Message (optional)</Label>
            <Input
              value={errorMessage}
              onChange={e => setErrorMessage(e.target.value)}
              placeholder="Custom error message"
            />
          </div>
          <div>
            <Label>Parameters (JSON)</Label>
            <Textarea
              value={JSON.stringify(params, null, 2)}
              onChange={e => {
                try {
                  setParams(JSON.parse(e.target.value));
                } catch (err) {}
              }}
              rows={4}
            />
          </div>
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button onClick={handleSave}>Save</Button>
        </div>
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Main Component
// ----------------------------------------------------------------------

interface SchemaComplianceCheckEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: SchemaComplianceCheckConfiguration;
  onClose: () => void;
  onSave: (config: SchemaComplianceCheckConfiguration) => void;
}

const DATA_TYPES: DataType[] = [
  'STRING', 'INTEGER', 'DECIMAL', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'BINARY'
];

const POSTGRES_TYPE_TO_DATA_TYPE: Record<string, DataType> = {
  'INTEGER': 'INTEGER',
  'BIGINT': 'INTEGER',
  'SMALLINT': 'INTEGER',
  'DECIMAL': 'DECIMAL',
  'NUMERIC': 'DECIMAL',
  'REAL': 'DECIMAL',
  'DOUBLE PRECISION': 'DECIMAL',
  'VARCHAR': 'STRING',
  'CHAR': 'STRING',
  'TEXT': 'STRING',
  'BOOLEAN': 'BOOLEAN',
  'DATE': 'DATE',
  'TIMESTAMP': 'TIMESTAMP',
  'TIMESTAMPTZ': 'TIMESTAMP',
  'BYTEA': 'BINARY',
  'JSON': 'STRING',
  'JSONB': 'STRING',
  'UUID': 'STRING',
};

function mapPostgresTypeToDataType(pgType: string): DataType {
  return POSTGRES_TYPE_TO_DATA_TYPE[pgType.toUpperCase()] || 'STRING';
}

export const SchemaComplianceCheckEditor: React.FC<SchemaComplianceCheckEditorProps> = ({
  nodeId,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // Default configuration
  const defaultConfig: SchemaComplianceCheckConfiguration = {
    version: '1.0',
    expectedSchema: [],
    mode: 'lenient',
    errorHandling: 'skipRow',
    options: {
      caseSensitiveColumnNames: false,
      trimWhitespace: false,
      nullIfEmptyString: false,
      continueOnFirstError: true,
      maxErrorsPerRow: 10,
    },
    compilerMetadata: {
      lastModified: new Date().toISOString(),
      createdBy: 'schema-compliance-editor',
      validationStatus: 'VALID',
      warnings: [],
      dependencies: [],
    },
  };

  const [config, setConfig] = useState<SchemaComplianceCheckConfiguration>(
    initialConfig || defaultConfig
  );
  const [activeTab, setActiveTab] = useState<'basic' | 'columns' | 'advanced'>('basic');
  const [editingColumn, setEditingColumn] = useState<ExpectedColumn | null>(null);
  const [showRuleModal, setShowRuleModal] = useState<{ columnId: string; rule?: ComplianceValidationRule } | null>(null);

  // Helper to add/update expected columns
  const updateExpectedColumn = useCallback((column: ExpectedColumn) => {
    setConfig(prev => {
      const index = prev.expectedSchema.findIndex(c => c.id === column.id);
      const newSchema = [...prev.expectedSchema];
      if (index >= 0) {
        newSchema[index] = column;
      } else {
        newSchema.push(column);
      }
      return { ...prev, expectedSchema: newSchema };
    });
  }, []);

  const removeColumn = useCallback((id: string) => {
    setConfig(prev => ({
      ...prev,
      expectedSchema: prev.expectedSchema.filter(c => c.id !== id),
    }));
  }, []);

  const addColumnFromInput = useCallback(() => {
    const colName = window.prompt('Enter column name:');
    if (!colName) return;
    const inputCol = inputColumns.find(c => c.name === colName);
    const newColumn: ExpectedColumn = {
      id: `col-${Date.now()}`,
      name: colName,
      dataType: inputCol?.type ? mapPostgresTypeToDataType(inputCol.type) : 'STRING',
      nullable: true,
      required: false,
      validationRules: [],
      expression: '',
    };
    updateExpectedColumn(newColumn);
  }, [inputColumns, updateExpectedColumn]);

  const handleSaveRule = useCallback((columnId: string, rule: ComplianceValidationRule) => {
    setConfig(prev => {
      const column = prev.expectedSchema.find(c => c.id === columnId);
      if (!column) return prev;
      const newRules = rule
        ? (column.validationRules?.some(r => r.id === rule.id)
            ? column.validationRules!.map(r => (r.id === rule.id ? rule : r))
            : [...(column.validationRules || []), rule])
        : column.validationRules;
      const updatedColumn = { ...column, validationRules: newRules };
      const newSchema = prev.expectedSchema.map(c => (c.id === columnId ? updatedColumn : c));
      return { ...prev, expectedSchema: newSchema };
    });
  }, []);

  // Render basic settings tab
  const renderBasicSettings = () => (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Validation Mode</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4">
            <label className="flex items-center gap-2">
              <input
                type="radio"
                value="strict"
                checked={config.mode === 'strict'}
                onChange={() => setConfig({ ...config, mode: 'strict' })}
              />
              Strict (no extra columns allowed)
            </label>
            <label className="flex items-center gap-2">
              <input
                type="radio"
                value="lenient"
                checked={config.mode === 'lenient'}
                onChange={() => setConfig({ ...config, mode: 'lenient' })}
              />
              Lenient (ignore extra columns)
            </label>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Error Handling</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <Select
            value={config.errorHandling}
            onValueChange={(v: any) => setConfig({ ...config, errorHandling: v })}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="fail">Fail Job on First Error</SelectItem>
              <SelectItem value="skipRow">Skip Invalid Rows (Discard)</SelectItem>
              <SelectItem value="rejectFlow">Route Invalid Rows to Reject Output</SelectItem>
              <SelectItem value="markInvalid">Add Validation Flag Column</SelectItem>
            </SelectContent>
          </Select>

          {config.errorHandling === 'rejectFlow' && (
            <div className="border rounded p-4 mt-2">
              <div className="flex items-center justify-between mb-2">
                <span className="font-medium">Reject Output Settings</span>
                <Switch
                  checked={config.rejectOutput?.enabled || false}
                  onCheckedChange={enabled =>
                    setConfig({
                      ...config,
                      rejectOutput: {
                        ...config.rejectOutput,
                        enabled,
                        schema: config.rejectOutput?.schema || {
                          id: `${nodeId}_reject`,
                          name: 'Reject Output',
                          fields: [],
                          isTemporary: true,
                          isMaterialized: false,
                        },
                        addErrorDetails: config.rejectOutput?.addErrorDetails || true,
                      },
                    })
                  }
                />
              </div>
              {config.rejectOutput?.enabled && (
                <>
                  <div className="flex items-center gap-2 mt-2">
                    <Switch
                      checked={config.rejectOutput.addErrorDetails}
                      onCheckedChange={addErrorDetails =>
                        setConfig({
                          ...config,
                          rejectOutput: { ...config.rejectOutput!, addErrorDetails },
                        })
                      }
                    />
                    <Label>Add error details columns (_error_messages, _failed_rules)</Label>
                  </div>
                  <div className="text-xs text-gray-500 mt-2">
                    Reject schema will include all input columns plus error columns.
                  </div>
                </>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Options</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.options?.caseSensitiveColumnNames || false}
              onCheckedChange={v =>
                setConfig({ ...config, options: { ...config.options, caseSensitiveColumnNames: v } })
              }
            />
            <Label>Case sensitive column names</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch
              checked={config.options?.trimWhitespace || false}
              onCheckedChange={v =>
                setConfig({ ...config, options: { ...config.options, trimWhitespace: v } })
              }
            />
            <Label>Trim whitespace before validation</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch
              checked={config.options?.nullIfEmptyString || false}
              onCheckedChange={v =>
                setConfig({ ...config, options: { ...config.options, nullIfEmptyString: v } })
              }
            />
            <Label>Treat empty string as NULL</Label>
          </div>
          <div className="flex items-center gap-2">
            <Switch
              checked={config.options?.continueOnFirstError || true}
              onCheckedChange={v =>
                setConfig({ ...config, options: { ...config.options, continueOnFirstError: v } })
              }
            />
            <Label>Stop validation after first error per row (performance)</Label>
          </div>
          <div>
            <Label>Max errors per row</Label>
            <Input
              type="number"
              value={config.options?.maxErrorsPerRow || 10}
              onChange={e =>
                setConfig({
                  ...config,
                  options: { ...config.options, maxErrorsPerRow: Number(e.target.value) },
                })
              }
              className="w-32"
            />
          </div>
        </CardContent>
      </Card>
    </div>
  );

  // Render columns tab
  const renderColumnsTab = () => (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-bold">Expected Columns</h3>
        <div className="flex gap-2">
          <Button onClick={addColumnFromInput} size="sm">
            <Plus className="h-4 w-4 mr-1" /> Add from Input
          </Button>
          <Button
            onClick={() =>
              setEditingColumn({
                id: `col-${Date.now()}`,
                name: '',
                dataType: 'STRING',
                nullable: true,
                required: false,
                validationRules: [],
                expression: '',
              })
            }
            size="sm"
            variant="outline"
          >
            <Plus className="h-4 w-4 mr-1" /> New Column
          </Button>
        </div>
      </div>

      <div className="border rounded-lg overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Column Name</TableHead>
              <TableHead>Data Type</TableHead>
              <TableHead>Nullable</TableHead>
              <TableHead>Required</TableHead>
              <TableHead>Length/Prec</TableHead>
              <TableHead>Expression</TableHead>
              <TableHead>Rules</TableHead>
              <TableHead className="w-20">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {config.expectedSchema.map(col => (
              <TableRow key={col.id}>
                <TableCell className="font-medium">{col.name}</TableCell>
                <TableCell>{col.dataType}</TableCell>
                <TableCell>{col.nullable ? 'Yes' : 'No'}</TableCell>
                <TableCell>{col.required ? 'Yes' : 'No'}</TableCell>
                <TableCell>
                  {col.length ? `L:${col.length}` : ''}
                  {col.precision ? ` P:${col.precision}` : ''}
                  {col.scale ? ` S:${col.scale}` : ''}
                </TableCell>
                <TableCell className="max-w-xs truncate">{col.expression || '-'}</TableCell>
                <TableCell>
                  {col.validationRules?.length ? (
                    <Badge variant="outline">{col.validationRules.length} rules</Badge>
                  ) : (
                    '-'
                  )}
                </TableCell>
                <TableCell>
                  <div className="flex gap-1">
                    <Button variant="ghost" size="sm" onClick={() => setEditingColumn({ ...col })}>
                      <Edit2 className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setShowRuleModal({ columnId: col.id })}
                    >
                      <Badge className="bg-blue-100 text-blue-800">+ rule</Badge>
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => removeColumn(col.id)}>
                      <Trash2 className="h-4 w-4 text-red-500" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
            {config.expectedSchema.length === 0 && (
              <TableRow>
                <TableCell colSpan={8} className="text-center text-gray-500 py-8">
                  No expected columns defined. Click "Add from Input" or "New Column" to start.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );

  // Render advanced tab (mostly schema preview)
  const renderAdvancedTab = () => {
    const outputSchema = config.outputSchema || {
      id: `${nodeId}_output`,
      name: 'Output Schema',
      fields: [],
      isTemporary: true,
      isMaterialized: false,
    };
    const rejectSchema = config.rejectOutput?.enabled && config.rejectOutput.schema;

    return (
      <div className="space-y-6">
        <Card>
          <CardHeader>
            <CardTitle>Output Schema (Valid Rows)</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-sm text-gray-600 mb-2">
              {config.mode === 'strict'
                ? 'Only expected columns will be output.'
                : 'Expected columns + extra columns from input will be output.'}
            </div>
            <div className="bg-gray-100 p-4 rounded">
              {outputSchema.fields.length ? (
                <ul className="list-disc pl-5">
                  {outputSchema.fields.map(f => (
                    <li key={f.id}>
                      {f.name} ({f.type}){f.nullable ? ' nullable' : ' not null'}
                    </li>
                  ))}
                </ul>
              ) : (
                <span className="text-gray-500">
                  Output schema will be derived from expected schema and input columns.
                </span>
              )}
            </div>
          </CardContent>
        </Card>

        {rejectSchema && (
          <Card>
            <CardHeader>
              <CardTitle>Reject Output Schema (Invalid Rows)</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="bg-gray-100 p-4 rounded">
                <ul className="list-disc pl-5">
                  {rejectSchema.fields.map(f => (
                    <li key={f.id}>
                      {f.name} ({f.type})
                    </li>
                  ))}
                </ul>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    );
  };

  const handleSave = () => {
    const finalConfig: SchemaComplianceCheckConfiguration = {
      ...config,
      compilerMetadata: {
        ...config.compilerMetadata,
        lastModified: new Date().toISOString(),
        validationStatus: config.expectedSchema.length === 0 ? 'WARNING' : 'VALID',
        warnings: config.expectedSchema.length === 0 ? ['No expected columns defined'] : [],
      },
    };
    onSave(finalConfig);
    onClose();
  };

  return (
    <>
      {/* Modals rendered conditionally */}
      {editingColumn && (
        <ColumnEditorModal
          column={editingColumn}
          onSave={updateExpectedColumn}
          onClose={() => setEditingColumn(null)}
        />
      )}
      {showRuleModal && (
        <RuleEditorModal
          columnId={showRuleModal.columnId}
          rule={showRuleModal.rule}
          onSave={handleSaveRule}
          onClose={() => setShowRuleModal(null)}
        />
      )}

      <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex flex-col">
        <div className="bg-white rounded-lg shadow-xl w-full max-w-6xl h-[90vh] flex flex-col overflow-hidden mx-auto my-8">
          {/* Header */}
          <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
            <div>
              <h2 className="text-xl font-bold flex items-center">
                <span className="mr-2">📋</span>
                Schema Compliance Check
                <span className="ml-2 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">
                  {nodeId}
                </span>
              </h2>
              <p className="text-sm text-gray-600 mt-1">
                Validate incoming data against a defined schema. Invalid rows can be handled
                separately.
              </p>
            </div>
            <button
              onClick={onClose}
              className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Tabs */}
          <div className="flex border-b">
            <button
              onClick={() => setActiveTab('basic')}
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === 'basic'
                  ? 'text-blue-600 border-b-2 border-blue-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              Basic Settings
            </button>
            <button
              onClick={() => setActiveTab('columns')}
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === 'columns'
                  ? 'text-blue-600 border-b-2 border-blue-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              Column Rules
            </button>
            <button
              onClick={() => setActiveTab('advanced')}
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === 'advanced'
                  ? 'text-blue-600 border-b-2 border-blue-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              Advanced / Preview
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-y-auto p-6">
            {activeTab === 'basic' && renderBasicSettings()}
            {activeTab === 'columns' && renderColumnsTab()}
            {activeTab === 'advanced' && renderAdvancedTab()}
          </div>

          {/* Footer */}
          <div className="flex justify-end gap-2 p-6 border-t bg-gray-50">
            <Button variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button onClick={handleSave}>Save Configuration</Button>
          </div>
        </div>
      </div>
    </>
  );
};

export default SchemaComplianceCheckEditor;