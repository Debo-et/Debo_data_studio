// components/SchemaPreview.tsx
import React from 'react';
import { SchemaField, ValidationRule } from './schema-validation';
import { 
  Card, CardContent, 
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
  Badge
} from '../../ui';

interface SchemaPreviewProps {
  schema: SchemaField[];
  rules: ValidationRule[];
  strictMode: boolean;
  errorThreshold: number;
}

const SchemaPreview: React.FC<SchemaPreviewProps> = ({
  schema,
  rules,
  strictMode,
  errorThreshold
}) => {
  const getFieldRules = (fieldName: string) => {
    return rules.filter(rule => rule.field === fieldName);
  };

  const getTypeColor = (type: string) => {
    const colors: Record<string, string> = {
      string: 'bg-blue-100 text-blue-800',
      number: 'bg-green-100 text-green-800',
      integer: 'bg-emerald-100 text-emerald-800',
      boolean: 'bg-purple-100 text-purple-800',
      date: 'bg-yellow-100 text-yellow-800',
      datetime: 'bg-orange-100 text-orange-800',
      email: 'bg-cyan-100 text-cyan-800',
      phone: 'bg-indigo-100 text-indigo-800',
      url: 'bg-sky-100 text-sky-800',
      uuid: 'bg-violet-100 text-violet-800',
    };
    return colors[type] || 'bg-gray-100 text-gray-800';
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-4 mb-4">
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-2xl font-bold">{schema.length}</div>
              <div className="text-sm text-gray-500">Total Fields</div>
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-2xl font-bold">{rules.length}</div>
              <div className="text-sm text-gray-500">Validation Rules</div>
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardContent className="pt-6">
            <div className="text-center">
              <div className="text-2xl font-bold">
                {Math.round((rules.length / Math.max(schema.length, 1)) * 100)}%
              </div>
              <div className="text-sm text-gray-500">Coverage</div>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="space-y-4">
        <div>
          <h4 className="font-semibold mb-2">Validation Settings</h4>
          <div className="flex flex-wrap gap-2">
            <Badge variant={strictMode ? 'destructive' : 'secondary'}>
              Strict Mode: {strictMode ? 'ON' : 'OFF'}
            </Badge>
            <Badge variant="secondary">
              Error Threshold: {Math.round(errorThreshold * 100)}%
            </Badge>
          </div>
        </div>

        <div>
          <h4 className="font-semibold mb-2">Schema Preview</h4>
          <div className="border rounded-md overflow-hidden">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Field</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Nullable</TableHead>
                  <TableHead>Rules</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {schema.map((field) => {
                  const fieldRules = getFieldRules(field.name);
                  return (
                    <TableRow key={field.name}>
                      <TableCell className="font-medium">{field.name}</TableCell>
                      <TableCell>
                        <Badge className={getTypeColor(field.type)}>
                          {field.type}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant={field.nullable ? 'outline' : 'secondary'}>
                          {field.nullable ? 'Yes' : 'No'}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          {fieldRules.map(rule => (
                            <Badge key={rule.id} variant="outline" className="text-xs">
                              {rule.rule}
                            </Badge>
                          ))}
                          {fieldRules.length === 0 && (
                            <span className="text-gray-400 text-sm">No rules</span>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </div>

        {schema.length === 0 && (
          <div className="text-center py-8 text-gray-500">
            No schema defined. Add fields to get started.
          </div>
        )}
      </div>
    </div>
  );
};

export default SchemaPreview;