import React, { useState, useEffect } from 'react';
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from '@/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  DragDropContext,
  Droppable,
  Draggable,
  DropResult,
} from '@hello-pangea/dnd';
import { Button } from '@/components/ui/Button';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';
import { Badge } from '@/components/ui/badge';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import { RadioGroup, RadioGroupItem } from '@/components/ui/radio-group';
import {
  AlertCircle,
  CheckCircle2,
  AlertTriangle,
  GripVertical,
  Plus,
  Trash2,
  Upload,
  Download,
  Copy,
  Settings,
  TestTube,
} from 'lucide-react';

// Types
type Operator = 'eq' | 'neq' | 'gt' | 'lt' | 'gte' | 'lte' | 'contains' | 'matches' | 'null' | 'not-null';
type FailureAction = 'route' | 'halt' | 'log';

interface ValidationCondition {
  id: string;
  field?: string;
  expression?: string;
  operator: Operator;
  value?: any;
  message?: string;
}

interface FieldSchema {
  name: string;
  type: 'string' | 'number' | 'date' | 'boolean';
  required?: boolean;
}

interface ValidationRuleBuilderProps {
  initialConditions?: ValidationCondition[];
  initialOnFailure?: FailureAction;
  initialStopOnFirstError?: boolean;
  initialIncludeDetails?: boolean;
  schema?: FieldSchema[];
  onConfigurationChange?: (config: {
    conditions: ValidationCondition[];
    onFailure: FailureAction;
    stopOnFirstError: boolean;
    includeDetails: boolean;
  }) => void;
}

const ASSERT_RULE_BUILDER: React.FC<ValidationRuleBuilderProps> = ({
  initialConditions = [],
  initialOnFailure = 'route',
  initialStopOnFirstError = false,
  initialIncludeDetails = true,
  schema = [],
  onConfigurationChange,
}) => {
  // State
  const [conditions, setConditions] = useState<ValidationCondition[]>(initialConditions);
  const [onFailure, setOnFailure] = useState<FailureAction>(initialOnFailure);
  const [stopOnFirstError, setStopOnFirstError] = useState(initialStopOnFirstError);
  const [includeDetails, setIncludeDetails] = useState(initialIncludeDetails);
  const [activeMode, setActiveMode] = useState<'simple' | 'advanced'>('simple');
  const [testData, setTestData] = useState<any[]>([]);
  const [testResults, setTestResults] = useState<any[]>([]);
  const [showTestPanel, setShowTestPanel] = useState(false);
  const [activeRuleTemplates, setActiveRuleTemplates] = useState<string[]>([]);

  // Available operators by field type
  const getOperatorsForType = (type: string): Operator[] => {
    const baseOperators: Operator[] = ['null', 'not-null'];
    
    switch (type) {
      case 'number':
        return [...baseOperators, 'eq', 'neq', 'gt', 'lt', 'gte', 'lte'];
      case 'string':
        return [...baseOperators, 'eq', 'neq', 'contains', 'matches'];
      case 'date':
        return [...baseOperators, 'eq', 'neq', 'gt', 'lt', 'gte', 'lte'];
      case 'boolean':
        return [...baseOperators, 'eq', 'neq'];
      default:
        return baseOperators;
    }
  };

  // Add new condition
  const addCondition = () => {
    const newCondition: ValidationCondition = {
      id: `cond-${Date.now()}`,
      field: schema[0]?.name || '',
      operator: 'eq',
      value: '',
    };
    setConditions([...conditions, newCondition]);
  };

  // Update condition
  const updateCondition = (id: string, updates: Partial<ValidationCondition>) => {
    setConditions(conditions.map(cond => 
      cond.id === id ? { ...cond, ...updates } : cond
    ));
  };

  // Remove condition
  const removeCondition = (id: string) => {
    setConditions(conditions.filter(cond => cond.id !== id));
  };

  // Handle drag and drop reordering
  const handleDragEnd = (result: DropResult) => {
    if (!result.destination) return;
    
    const items = Array.from(conditions);
    const [reorderedItem] = items.splice(result.source.index, 1);
    items.splice(result.destination.index, 0, reorderedItem);
    
    setConditions(items);
  };

  // Render value input based on field type and operator
  const renderValueInput = (condition: ValidationCondition) => {
    const fieldType = schema.find(f => f.name === condition.field)?.type || 'string';
    const isNullOperator = condition.operator === 'null' || condition.operator === 'not-null';
    
    if (isNullOperator) {
      return null;
    }

    switch (fieldType) {
      case 'number':
        return (
          <Input
            type="number"
            value={condition.value || ''}
            onChange={(e) => updateCondition(condition.id, { 
              value: e.target.valueAsNumber || e.target.value 
            })}
            className="w-full"
          />
        );
      case 'date':
        return (
          <Input
            type="datetime-local"
            value={condition.value || ''}
            onChange={(e) => updateCondition(condition.id, { value: e.target.value })}
            className="w-full"
          />
        );
      case 'boolean':
        return (
          <Select
            value={condition.value?.toString() || 'true'}
            onValueChange={(value) => updateCondition(condition.id, { value: value === 'true' })}
          >
            <SelectTrigger className="w-full">
              <SelectValue placeholder="Select value" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="true">True</SelectItem>
              <SelectItem value="false">False</SelectItem>
            </SelectContent>
          </Select>
        );
      default:
        return (
          <div className="space-y-2">
            <Input
              type="text"
              value={condition.value || ''}
              onChange={(e) => updateCondition(condition.id, { value: e.target.value })}
              className="w-full"
            />
            {condition.operator === 'matches' && (
              <div className="text-xs text-muted-foreground flex items-center gap-2">
                <AlertCircle className="h-3 w-3" />
                Enter a valid regular expression
              </div>
            )}
          </div>
        );
    }
  };

  // Field selector
  const renderFieldSelector = (condition: ValidationCondition) => {
    if (activeMode === 'advanced') {
      return (
        <Textarea
          placeholder="Enter custom expression (e.g., age > 18 AND status = 'active')"
          value={condition.expression || ''}
          onChange={(e) => updateCondition(condition.id, { expression: e.target.value })}
          className="min-h-[80px]"
        />
      );
    }

    return (
      <Select
        value={condition.field || ''}
        onValueChange={(value) => updateCondition(condition.id, { field: value })}
      >
        <SelectTrigger className="w-full">
          <SelectValue placeholder="Select field" />
        </SelectTrigger>
        <SelectContent>
          {schema.map((field) => (
            <SelectItem key={field.name} value={field.name}>
              <div className="flex items-center gap-2">
                <span>{field.name}</span>
                <Badge variant="outline" className="text-xs">
                  {field.type}
                </Badge>
                {field.required && (
                  <Badge variant="destructive" className="text-xs">
                    Required
                  </Badge>
                )}
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  };

  // Operator selector
  const renderOperatorSelector = (condition: ValidationCondition) => {
    if (activeMode === 'advanced') return null;
    
    const fieldType = schema.find(f => f.name === condition.field)?.type || 'string';
    const operators = getOperatorsForType(fieldType);

    return (
      <Select
        value={condition.operator}
        onValueChange={(value: Operator) => updateCondition(condition.id, { operator: value })}
      >
        <SelectTrigger className="w-full">
          <SelectValue placeholder="Select operator" />
        </SelectTrigger>
        <SelectContent>
          {operators.map((op) => (
            <SelectItem key={op} value={op}>
              <div className="flex items-center gap-2">
                {getOperatorLabel(op)}
                {['null', 'not-null'].includes(op) && (
                  <Badge variant="secondary" className="text-xs">
                    No value required
                  </Badge>
                )}
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    );
  };

  // Test validation rules
  const runTests = () => {
    if (testData.length === 0) return;
    
    const results = testData.map((row, index) => {
      const errors: { conditionId: string; message: string }[] = [];
      let passed = true;
      
      for (const condition of conditions) {
        if (stopOnFirstError && errors.length > 0) break;
        
        const testResult = testCondition(row, condition);
        if (!testResult.passed) {
          errors.push({
            conditionId: condition.id,
            message: condition.message || testResult.error || 'Validation failed',
          });
          passed = false;
        }
      }
      
      return {
        id: `test-${index}`,
        row,
        passed,
        errors,
        details: includeDetails ? {
          validatedFields: conditions.map(c => c.field).filter(Boolean),
          timestamp: new Date().toISOString(),
        } : undefined,
      };
    });
    
    setTestResults(results);
  };

  // Test single condition
  const testCondition = (row: any, condition: ValidationCondition) => {
    if (condition.expression) {
      // Advanced expression evaluation
      try {
        const result = evaluateExpression(row, condition.expression);
        return { passed: result, error: 'Expression failed' };
      } catch (error) {
        return { passed: false, error: `Expression error: ${error}` };
      }
    }
    
    if (!condition.field) {
      return { passed: false, error: 'No field specified' };
    }
    
    const value = row[condition.field];
    
    switch (condition.operator) {
      case 'eq':
        return { passed: value == condition.value, error: `${condition.field} should equal ${condition.value}` };
      case 'neq':
        return { passed: value != condition.value, error: `${condition.field} should not equal ${condition.value}` };
      case 'gt':
        return { passed: value > condition.value, error: `${condition.field} should be greater than ${condition.value}` };
      case 'lt':
        return { passed: value < condition.value, error: `${condition.field} should be less than ${condition.value}` };
      case 'gte':
        return { passed: value >= condition.value, error: `${condition.field} should be greater than or equal to ${condition.value}` };
      case 'lte':
        return { passed: value <= condition.value, error: `${condition.field} should be less than or equal to ${condition.value}` };
      case 'contains':
        return { passed: String(value).includes(String(condition.value)), error: `${condition.field} should contain ${condition.value}` };
      case 'matches':
        try {
          const regex = new RegExp(condition.value);
          return { passed: regex.test(String(value)), error: `${condition.field} should match pattern ${condition.value}` };
        } catch {
          return { passed: false, error: 'Invalid regular expression' };
        }
      case 'null':
        return { passed: value == null, error: `${condition.field} should be null` };
      case 'not-null':
        return { passed: value != null, error: `${condition.field} should not be null` };
      default:
        return { passed: false, error: 'Unknown operator' };
    }
  };

  // Simple expression evaluator (in production, use a proper expression parser)
  const evaluateExpression = (row: any, expression: string): boolean => {
    // This is a simplified evaluator - in production, use something like expr-eval or similar
    try {
      // Replace field names with actual values
      let evalString = expression;
      for (const field of schema) {
        const regex = new RegExp(`\\b${field.name}\\b`, 'g');
        evalString = evalString.replace(regex, JSON.stringify(row[field.name]));
      }
      
      // Safe evaluation (extremely simplified - implement proper security in production)
      return eval(`(${evalString})`);
    } catch {
      return false;
    }
  };

  // Apply rule template
  const applyRuleTemplate = (template: string) => {
    let newConditions: ValidationCondition[] = [];
    
    switch (template) {
      case 'email-validation':
        newConditions = [
          {
            id: `email-${Date.now()}`,
            field: 'email',
            operator: 'matches',
            value: '^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$',
            message: 'Invalid email format',
          },
          {
            id: `email-not-null-${Date.now()}`,
            field: 'email',
            operator: 'not-null',
            message: 'Email is required',
          },
        ];
        break;
      case 'age-range':
        newConditions = [
          {
            id: `age-min-${Date.now()}`,
            field: 'age',
            operator: 'gte',
            value: 18,
            message: 'Age must be at least 18',
          },
          {
            id: `age-max-${Date.now()}`,
            field: 'age',
            operator: 'lte',
            value: 120,
            message: 'Age must be at most 120',
          },
        ];
        break;
      case 'date-future':
        newConditions = [
          {
            id: `date-future-${Date.now()}`,
            field: 'date',
            operator: 'gte',
            value: new Date().toISOString().split('T')[0],
            message: 'Date must be in the future',
          },
        ];
        break;
      default:
        return;
    }
    
    setConditions([...conditions, ...newConditions]);
    setActiveRuleTemplates([...activeRuleTemplates, template]);
  };

  // Export configuration
  const exportConfiguration = () => {
    const config = {
      conditions,
      onFailure,
      stopOnFirstError,
      includeDetails,
    };
    
    const blob = new Blob([JSON.stringify(config, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'assert-validation-rules.json';
    a.click();
    URL.revokeObjectURL(url);
  };

  // Handle file upload for test data
  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        const data = JSON.parse(content);
        setTestData(Array.isArray(data) ? data : [data]);
      } catch (error) {
        console.error('Error parsing test data:', error);
        alert('Invalid JSON file');
      }
    };
    reader.readAsText(file);
  };

  // Notify parent of configuration changes
  useEffect(() => {
    onConfigurationChange?.({
      conditions,
      onFailure,
      stopOnFirstError,
      includeDetails,
    });
  }, [conditions, onFailure, stopOnFirstError, includeDetails]);

  return (
    <div className="space-y-6">
      {/* Main Rule Builder */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-2">
                <AlertTriangle className="h-5 w-5" />
                Validation Rule Builder
              </CardTitle>
              <CardDescription>
                Define conditions to validate data rows. Rows can be routed based on validation results.
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={exportConfiguration}>
                <Download className="h-4 w-4 mr-2" />
                Export Rules
              </Button>
              <Button
                variant={showTestPanel ? "default" : "outline"}
                size="sm"
                onClick={() => setShowTestPanel(!showTestPanel)}
              >
                <TestTube className="h-4 w-4 mr-2" />
                Test Panel
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {/* Mode Toggle - Simplified approach without Tabs component */}
          <div className="mb-6">
            <div className="flex items-center justify-between mb-4">
              <Label>Editor Mode</Label>
              <div className="flex space-x-2">
                <Button
                  variant={activeMode === 'simple' ? "default" : "outline"}
                  size="sm"
                  onClick={() => setActiveMode('simple')}
                >
                  Simple Mode
                </Button>
                <Button
                  variant={activeMode === 'advanced' ? "default" : "outline"}
                  size="sm"
                  onClick={() => setActiveMode('advanced')}
                >
                  Advanced Mode
                </Button>
              </div>
            </div>
            <p className="text-sm text-muted-foreground mb-4">
              {activeMode === 'simple' 
                ? 'Build rules by selecting fields and operators' 
                : 'Write custom expressions for complex validation logic'}
            </p>
          </div>

          {/* Rule Templates */}
          <div className="mb-6">
            <Label className="mb-2 block">Quick Templates</Label>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => applyRuleTemplate('email-validation')}
                disabled={activeRuleTemplates.includes('email-validation')}
              >
                <Copy className="h-3 w-3 mr-1" />
                Email Validation
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => applyRuleTemplate('age-range')}
                disabled={activeRuleTemplates.includes('age-range')}
              >
                <Copy className="h-3 w-3 mr-1" />
                Age Range
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => applyRuleTemplate('date-future')}
              >
                <Copy className="h-3 w-3 mr-1" />
                Future Date
              </Button>
            </div>
          </div>

          {/* Validation Rules List */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <Label>Validation Conditions ({conditions.length})</Label>
              <Button onClick={addCondition} size="sm">
                <Plus className="h-4 w-4 mr-2" />
                Add Condition
              </Button>
            </div>

            <DragDropContext onDragEnd={handleDragEnd}>
              <Droppable droppableId="conditions">
                {(provided) => (
                  <div
                    {...provided.droppableProps}
                    ref={provided.innerRef}
                    className="space-y-3"
                  >
                    {conditions.map((condition, index) => (
                      <Draggable
                        key={condition.id}
                        draggableId={condition.id}
                        index={index}
                      >
                        {(provided) => (
                          <div
                            ref={provided.innerRef}
                            {...provided.draggableProps}
                            className="border rounded-lg p-4 bg-card hover:bg-accent/5 transition-colors"
                          >
                            <div className="flex items-start gap-4">
                              {/* Drag Handle */}
                              <div
                                {...provided.dragHandleProps}
                                className="cursor-move p-2 hover:bg-accent rounded"
                              >
                                <GripVertical className="h-4 w-4 text-muted-foreground" />
                              </div>

                              {/* Condition Content */}
                              <div className="flex-1 grid grid-cols-1 md:grid-cols-12 gap-4">
                                {/* Field/Expression */}
                                <div className="md:col-span-4">
                                  <Label className="mb-2 block">
                                    {activeMode === 'simple' ? 'Field' : 'Expression'}
                                  </Label>
                                  {renderFieldSelector(condition)}
                                </div>

                                {/* Operator (simple mode only) */}
                                {activeMode === 'simple' && (
                                  <div className="md:col-span-3">
                                    <Label className="mb-2 block">Operator</Label>
                                    {renderOperatorSelector(condition)}
                                  </div>
                                )}

                                {/* Value (if applicable) */}
                                {activeMode === 'simple' && 
                                  condition.operator !== 'null' && 
                                  condition.operator !== 'not-null' && (
                                  <div className="md:col-span-3">
                                    <Label className="mb-2 block">Value</Label>
                                    {renderValueInput(condition)}
                                  </div>
                                )}

                                {/* Error Message */}
                                <div className="md:col-span-2">
                                  <Label className="mb-2 block">Error Message</Label>
                                  <Input
                                    type="text"
                                    value={condition.message || ''}
                                    onChange={(e) => updateCondition(condition.id, { 
                                      message: e.target.value 
                                    })}
                                    placeholder="Validation failed"
                                    className="w-full"
                                  />
                                </div>

                                {/* Remove Button */}
                                <div className="md:col-span-1 flex items-end">
                                  <Button
                                    variant="ghost"
                                    size="sm"
                                    onClick={() => removeCondition(condition.id)}
                                    className="text-destructive hover:text-destructive"
                                  >
                                    <Trash2 className="h-4 w-4" />
                                  </Button>
                                </div>
                              </div>
                            </div>
                          </div>
                        )}
                      </Draggable>
                    ))}
                    {provided.placeholder}
                  </div>
                )}
              </Droppable>
            </DragDropContext>

            {conditions.length === 0 && (
              <div className="text-center py-8 border-2 border-dashed rounded-lg">
                <AlertCircle className="h-12 w-12 mx-auto text-muted-foreground mb-2" />
                <p className="text-muted-foreground">No validation conditions defined</p>
                <Button
                  variant="outline"
                  onClick={addCondition}
                  className="mt-4"
                >
                  <Plus className="h-4 w-4 mr-2" />
                  Add Your First Condition
                </Button>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Failure Handling Section */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings className="h-5 w-5" />
            Failure Handling
          </CardTitle>
          <CardDescription>
            Configure what happens when validation fails
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {/* Action Selection */}
            <div className="space-y-4">
              <Label>Action on Failure</Label>
              <RadioGroup
                value={onFailure}
                onValueChange={(value) => setOnFailure(value as FailureAction)}
                className="space-y-3"
              >
                <div className="flex items-center space-x-2 rounded-lg border p-4 hover:bg-accent/5">
                  <RadioGroupItem value="route" id="route" />
                  <Label htmlFor="route" className="flex-1 cursor-pointer">
                    <div className="font-medium">Route</div>
                    <p className="text-sm text-muted-foreground">
                      Send failing rows to error route
                    </p>
                  </Label>
                  <Badge variant="outline" className="ml-2">
                    Recommended
                  </Badge>
                </div>

                <div className="flex items-center space-x-2 rounded-lg border p-4 hover:bg-accent/5">
                  <RadioGroupItem value="halt" id="halt" />
                  <Label htmlFor="halt" className="flex-1 cursor-pointer">
                    <div className="font-medium">Halt Processing</div>
                    <p className="text-sm text-muted-foreground">
                      Stop processing entire batch on first failure
                    </p>
                  </Label>
                </div>

                <div className="flex items-center space-x-2 rounded-lg border p-4 hover:bg-accent/5">
                  <RadioGroupItem value="log" id="log" />
                  <Label htmlFor="log" className="flex-1 cursor-pointer">
                    <div className="font-medium">Log Only</div>
                    <p className="text-sm text-muted-foreground">
                      Log errors but continue processing
                    </p>
                  </Label>
                </div>
              </RadioGroup>
            </div>

            {/* Additional Options */}
            <div className="space-y-6">
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <div>
                    <Label>Stop on First Error</Label>
                    <p className="text-sm text-muted-foreground">
                      Stop validation when first condition fails
                    </p>
                  </div>
                  <Switch
                    checked={stopOnFirstError}
                    onCheckedChange={setStopOnFirstError}
                  />
                </div>

                <div className="flex items-center justify-between">
                  <div>
                    <Label>Include Details</Label>
                    <p className="text-sm text-muted-foreground">
                      Add validation details to output
                    </p>
                  </div>
                  <Switch
                    checked={includeDetails}
                    onCheckedChange={setIncludeDetails}
                  />
                </div>
              </div>

              {/* Visual Flow Preview */}
              <div className="pt-4 border-t">
                <Label className="mb-3 block">Flow Preview</Label>
                <div className="flex items-center justify-center space-x-2">
                  <div className="bg-primary/10 px-3 py-2 rounded">
                    Input Data
                  </div>
                  <div className="text-muted-foreground">→</div>
                  <div className="bg-blue-100 px-3 py-2 rounded">
                    Validation
                  </div>
                  <div className="text-muted-foreground">→</div>
                  <div className="flex flex-col gap-1">
                    <div className="bg-green-100 px-3 py-1 rounded text-sm">
                      Valid Rows
                    </div>
                    <div className="bg-red-100 px-3 py-1 rounded text-sm">
                      Error Route
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Test Panel */}
      {showTestPanel && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <TestTube className="h-5 w-5" />
              Test Panel
            </CardTitle>
            <CardDescription>
              Upload sample data to test your validation rules
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-6">
              {/* Upload Section */}
              <div className="border rounded-lg p-4">
                <div className="flex items-center justify-between mb-4">
                  <Label>Test Data</Label>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setTestData([
                          { id: 1, name: 'John', age: 30, email: 'john@example.com' },
                          { id: 2, name: 'Jane', age: 17, email: 'invalid-email' },
                        ]);
                      }}
                    >
                      Use Sample Data
                    </Button>
                    <div className="relative">
                      <Button variant="outline" size="sm" asChild>
                        <label>
                          <Upload className="h-4 w-4 mr-2" />
                          Upload JSON
                          <input
                            type="file"
                            accept=".json"
                            onChange={handleFileUpload}
                            className="absolute inset-0 opacity-0 cursor-pointer"
                          />
                        </label>
                      </Button>
                    </div>
                  </div>
                </div>
                
                {testData.length > 0 && (
                  <div className="text-sm text-muted-foreground">
                    Loaded {testData.length} rows for testing
                  </div>
                )}
              </div>

              {/* Run Tests */}
              <div className="flex items-center gap-4">
                <Button onClick={runTests} disabled={testData.length === 0}>
                  Run Validation Tests
                </Button>
                {testResults.length > 0 && (
                  <div className="flex items-center gap-4">
                    <div className="flex items-center gap-2">
                      <CheckCircle2 className="h-4 w-4 text-green-600" />
                      <span className="text-sm">
                        Pass: {testResults.filter(r => r.passed).length}
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <AlertCircle className="h-4 w-4 text-red-600" />
                      <span className="text-sm">
                        Fail: {testResults.filter(r => !r.passed).length}
                      </span>
                    </div>
                  </div>
                )}
              </div>

              {/* Results */}
              {testResults.length > 0 && (
                <div className="border rounded-lg">
                  <div className="p-4 border-b">
                    <Label>Test Results</Label>
                  </div>
                  <div className="divide-y">
                    {testResults.map((result, index) => (
                      <div
                        key={result.id}
                        className={`p-4 ${result.passed ? 'bg-green-50' : 'bg-red-50'}`}
                      >
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex items-center gap-2">
                            {result.passed ? (
                              <CheckCircle2 className="h-4 w-4 text-green-600" />
                            ) : (
                              <AlertCircle className="h-4 w-4 text-red-600" />
                            )}
                            <span className="font-medium">
                              Row {index + 1} - {result.passed ? 'PASS' : 'FAIL'}
                            </span>
                          </div>
                          <Badge variant={result.passed ? "outline" : "destructive"}>
                            {result.passed ? 'Valid' : `${result.errors.length} errors`}
                          </Badge>
                        </div>
                        
                        {!result.passed && (
                          <div className="mt-3 space-y-2">
                            <div className="text-sm font-medium">Errors:</div>
                            {result.errors.map((error: any, i: number) => (
                              <div
                                key={i}
                                className="text-sm bg-white/50 p-2 rounded border"
                              >
                                {error.message}
                              </div>
                            ))}
                          </div>
                        )}
                        
                        {includeDetails && result.details && (
                          <div className="mt-3 text-xs text-muted-foreground">
                            <details>
                              <summary>Details</summary>
                              <pre className="mt-2 p-2 bg-white rounded border">
                                {JSON.stringify(result.details, null, 2)}
                              </pre>
                            </details>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};

// Helper function to get operator label
const getOperatorLabel = (operator: Operator): string => {
  const labels: Record<Operator, string> = {
    'eq': 'Equals',
    'neq': 'Not equals',
    'gt': 'Greater than',
    'lt': 'Less than',
    'gte': 'Greater than or equals',
    'lte': 'Less than or equals',
    'contains': 'Contains',
    'matches': 'Matches regex',
    'null': 'Is null',
    'not-null': 'Is not null',
  };
  return labels[operator];
};

export default ASSERT_RULE_BUILDER;