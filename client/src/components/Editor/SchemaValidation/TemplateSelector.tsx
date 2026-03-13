// components/Templates/TemplateSelector.tsx
import React, { useState } from 'react';
import { SchemaTemplate } from './schema-validation';

// 单独导入每个 UI 组件
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '../../ui/dialog';
import { Button } from '../../ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../../ui/card';
import { Badge } from '../../ui/badge';

const TEMPLATES: SchemaTemplate[] = [
  {
    id: 'user-profile',
    name: 'User Profile',
    description: 'Standard user profile schema with validation',
    category: 'standard',
    schema: [
      { name: 'id', type: 'uuid', nullable: false, format: 'uuid' },
      { name: 'email', type: 'email', nullable: false, format: 'email' },
      { name: 'username', type: 'string', nullable: false },
      { name: 'firstName', type: 'string', nullable: true },
      { name: 'lastName', type: 'string', nullable: true },
      { name: 'age', type: 'integer', nullable: true },
      { name: 'createdAt', type: 'datetime', nullable: false, format: 'date-time' },
      { name: 'isActive', type: 'boolean', nullable: false }
    ],
    rules: [
      {
        id: 'email-format',
        field: 'email',
        rule: 'format',
        parameters: { format: 'email' },
        message: 'Invalid email format',
        enabled: true,
        severity: 'error'
      },
      {
        id: 'username-length',
        field: 'username',
        rule: 'length',
        parameters: { minLength: 3, maxLength: 50 },
        message: 'Username must be 3-50 characters',
        enabled: true,
        severity: 'error'
      },
      {
        id: 'age-range',
        field: 'age',
        rule: 'range',
        parameters: { min: 13, max: 120, inclusive: true },
        message: 'Age must be between 13 and 120',
        enabled: true,
        severity: 'error'
      }
    ]
  },
  {
    id: 'iso-dates',
    name: 'ISO Date Standards',
    description: 'ISO 8601 date and time formats',
    category: 'industry',
    schema: [
      { name: 'date', type: 'date', nullable: false, format: 'date' },
      { name: 'datetime', type: 'datetime', nullable: false, format: 'date-time' },
      { name: 'timestamp', type: 'timestamp', nullable: false }
    ],
    rules: [
      {
        id: 'iso-date',
        field: 'date',
        rule: 'regex',
        parameters: { 
          pattern: '^\\d{4}-\\d{2}-\\d{2}$',
          caseSensitive: true 
        },
        message: 'Date must be in YYYY-MM-DD format',
        enabled: true,
        severity: 'error'
      }
    ]
  },
  {
    id: 'us-phone',
    name: 'US Phone Numbers',
    description: 'Standard US phone number formats',
    category: 'industry',
    schema: [
      { name: 'phone', type: 'phone', nullable: false, format: 'phone' }
    ],
    rules: [
      {
        id: 'us-phone-format',
        field: 'phone',
        rule: 'regex',
        parameters: { 
          pattern: '^\\+1\\s?\\(?\\d{3}\\)?[\\s.-]?\\d{3}[\\s.-]?\\d{4}$' 
        },
        message: 'Invalid US phone number format',
        enabled: true,
        severity: 'error'
      }
    ]
  }
];

interface TemplateSelectorProps {
  onSelect: (template: SchemaTemplate) => void;
}

const TemplateSelector: React.FC<TemplateSelectorProps> = ({ onSelect }) => {
  const [open, setOpen] = useState(false);

  const getCategoryColor = (category: string) => {
    switch (category) {
      case 'standard': return 'bg-blue-100 text-blue-800';
      case 'industry': return 'bg-green-100 text-green-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="outline">Load Template</Button>
      </DialogTrigger>
      <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Schema Templates</DialogTitle>
        </DialogHeader>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {TEMPLATES.map((template) => (
            <Card 
              key={template.id}
              className="cursor-pointer hover:shadow-lg transition-shadow"
              onClick={() => {
                onSelect(template);
                setOpen(false);
              }}
            >
              <CardHeader>
                <CardTitle className="flex justify-between items-start">
                  {template.name}
                  <Badge className={getCategoryColor(template.category)}>
                    {template.category}
                  </Badge>
                </CardTitle>
                <CardDescription>{template.description}</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-2">
                  <div className="text-sm text-gray-600">
                    {template.schema.length} fields, {template.rules.length} rules
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {template.schema.slice(0, 3).map((field) => (
                      <Badge key={field.name} variant="secondary">
                        {field.name}: {field.type}
                      </Badge>
                    ))}
                    {template.schema.length > 3 && (
                      <Badge variant="secondary">+{template.schema.length - 3} more</Badge>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default TemplateSelector;