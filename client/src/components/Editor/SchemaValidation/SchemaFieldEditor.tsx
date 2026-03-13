// components/SchemaEditor/SchemaFieldEditor.tsx
import React, { useState } from 'react';
import { SchemaField, DataType } from './schema-validation';
import { 
  Table, TableBody, TableCell, TableHead, 
  TableHeader, TableRow, 
  Input, Select, SelectContent, 
  SelectItem, SelectTrigger, SelectValue,
  Checkbox, Button, Badge
} from '../ui';

interface SchemaFieldEditorProps {
  fields: SchemaField[];
  onChange: (fields: SchemaField[]) => void;
  onFieldSelect?: (fieldName: string) => void;
}

const DATA_TYPE_OPTIONS: Array<{ value: DataType; label: string; description: string }> = [
  { value: 'string', label: 'String', description: 'Text data' },
  { value: 'number', label: 'Number', description: 'Numeric data (floating point)' },
  { value: 'integer', label: 'Integer', description: 'Whole numbers' },
  { value: 'boolean', label: 'Boolean', description: 'True/False values' },
  { value: 'date', label: 'Date', description: 'Calendar date (YYYY-MM-DD)' },
  { value: 'datetime', label: 'DateTime', description: 'Date and time with timezone' },
  { value: 'email', label: 'Email', description: 'Email address format' },
  { value: 'phone', label: 'Phone', description: 'Phone number format' },
  { value: 'url', label: 'URL', description: 'Web address format' },
  { value: 'uuid', label: 'UUID', description: 'Universally unique identifier' },
  { value: 'object', label: 'Object', description: 'Nested object structure' },
  { value: 'array', label: 'Array', description: 'List of items' },
  { value: 'any', label: 'Any', description: 'Any data type' },
];

const SchemaFieldEditor: React.FC<SchemaFieldEditorProps> = ({ 
  fields, 
  onChange,
  onFieldSelect 
}) => {
  const [expandedField, setExpandedField] = useState<string | null>(null);

  const handleAddField = () => {
    const newField: SchemaField = {
      name: `field_${fields.length + 1}`,
      type: 'string',
      nullable: false,
      format: '',
      description: '',
      metadata: {}
    };
    onChange([...fields, newField]);
  };

  const handleUpdateField = (index: number, updates: Partial<SchemaField>) => {
    const updated = [...fields];
    updated[index] = { ...updated[index], ...updates };
    onChange(updated);
  };

  const handleRemoveField = (index: number) => {
    const updated = fields.filter((_, i) => i !== index);
    onChange(updated);
  };

  const getTypeColor = (type: DataType) => {
    const colors: Record<DataType, string> = {
      string: 'bg-blue-100 text-blue-800',
      number: 'bg-green-100 text-green-800',
      integer: 'bg-emerald-100 text-emerald-800',
      boolean: 'bg-purple-100 text-purple-800',
      date: 'bg-yellow-100 text-yellow-800',
      datetime: 'bg-orange-100 text-orange-800',
      timestamp: 'bg-amber-100 text-amber-800',
      email: 'bg-cyan-100 text-cyan-800',
      phone: 'bg-indigo-100 text-indigo-800',
      url: 'bg-sky-100 text-sky-800',
      uuid: 'bg-violet-100 text-violet-800',
      object: 'bg-rose-100 text-rose-800',
      array: 'bg-pink-100 text-pink-800',
      any: 'bg-gray-100 text-gray-800',
    };
    return colors[type] || 'bg-gray-100 text-gray-800';
  };

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-semibold">Schema Fields</h3>
        <Button onClick={handleAddField} variant="outline" size="sm">
          + Add Field
        </Button>
      </div>

      <div className="border rounded-lg overflow-hidden">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[200px]">Field Name</TableHead>
              <TableHead className="w-[150px]">Type</TableHead>
              <TableHead className="w-[100px]">Nullable</TableHead>
              <TableHead className="w-[150px]">Format</TableHead>
              <TableHead>Description</TableHead>
              <TableHead className="w-[100px]">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {fields.map((field, index) => (
              <React.Fragment key={index}>
                <TableRow 
                  className="cursor-pointer hover:bg-gray-50"
                  onClick={() => onFieldSelect?.(field.name)}
                >
                  <TableCell>
                    <Input
                      value={field.name}
                      onChange={(e) => 
                        handleUpdateField(index, { name: e.target.value })
                      }
                      className="font-mono"
                    />
                  </TableCell>
                  <TableCell>
                    <Select
                      value={field.type}
                      onValueChange={(value: DataType) =>
                        handleUpdateField(index, { type: value })
                      }
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {DATA_TYPE_OPTIONS.map((option) => (
                          <SelectItem key={option.value} value={option.value}>
                            <div className="flex flex-col">
                              <span>{option.label}</span>
                              <span className="text-xs text-gray-500">
                                {option.description}
                              </span>
                            </div>
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </TableCell>
                  <TableCell>
                    <Checkbox
                      checked={field.nullable}
                      onCheckedChange={(checked) =>
                        handleUpdateField(index, { nullable: !!checked })
                      }
                    />
                  </TableCell>
                  <TableCell>
                    <Input
                      value={field.format || ''}
                      onChange={(e) =>
                        handleUpdateField(index, { format: e.target.value })
                      }
                      placeholder="e.g., email, date-time"
                    />
                  </TableCell>
                  <TableCell>
                    <Input
                      value={field.description || ''}
                      onChange={(e) =>
                        handleUpdateField(index, { description: e.target.value })
                      }
                      placeholder="Field description"
                    />
                  </TableCell>
                  <TableCell>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleRemoveField(index);
                      }}
                    >
                      Remove
                    </Button>
                  </TableCell>
                </TableRow>
                
                {/* Nested schema for object/array types */}
                {(field.type === 'object' || field.type === 'array') && (
                  <TableRow>
                    <TableCell colSpan={6} className="p-0">
                      <div className="ml-8 border-l-2 border-gray-200 pl-4">
                        {field.type === 'object' && field.objectSchema && (
                          <SchemaFieldEditor
                            fields={field.objectSchema}
                            onChange={(nestedFields) =>
                              handleUpdateField(index, { objectSchema: nestedFields })
                            }
                          />
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                )}
              </React.Fragment>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
};

export default SchemaFieldEditor;