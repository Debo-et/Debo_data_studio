// TestPreviewPanel.tsx
import React, { useState } from 'react';
import { Tab } from '@headlessui/react';
import { 
  Play, 
  RefreshCw, 
  Eye, 
  GitCommit,
  BarChart3,
  ChevronDown,
  ChevronRight,
  AlertCircle,
  CheckCircle,
  XCircle,
  Clock,
  Zap,
  Plus
} from 'lucide-react';
import { RuleSet, TestCase } from '../../../types/types';

// Define ExecutionTrace locally since it's not exported from types
interface ExecutionTrace {
  ruleId: string;
  input: string;
  output: string;
  matched: boolean;
  executionTime: number;
  timestamp: Date;
  error?: string;
}

// Extend TestCase interface if needed, or define a local version
interface TestPreviewPanelProps {
  ruleSet: RuleSet;
  testCases: TestCase[];
  onTestCasesUpdate: (testCases: TestCase[]) => void;
}

// If TestCase doesn't have inputColumn and testValue, define a local type
interface LocalTestCase {
  id: string;
  inputColumn: string;
  testValue: string;
}

const TestPreviewPanel: React.FC<TestPreviewPanelProps> = ({
  ruleSet,
  testCases,
  onTestCasesUpdate
}) => {
  const [selectedTab, setSelectedTab] = useState(0);
  const [isRunningTests, setIsRunningTests] = useState(false);
  const [executionTraces, setExecutionTraces] = useState<ExecutionTrace[]>([]);
  const [expandedTestRows, setExpandedTestRows] = useState<Set<string>>(new Set());

  // Convert TestCase[] to LocalTestCase[] if needed
  const localTestCases: LocalTestCase[] = testCases.map(testCase => ({
    id: testCase.id || `test_${Date.now()}`,
    inputColumn: (testCase as any).inputColumn || ruleSet.rules[0]?.inputColumn || '',
    testValue: (testCase as any).testValue || ''
  }));

  const runTests = async () => {
    setIsRunningTests(true);
    // Simulate test execution
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    // Generate mock execution traces
    const traces: ExecutionTrace[] = localTestCases.map(testCase => {
      const matchedRule = ruleSet.rules.find(rule => 
        rule.enabled && 
        rule.inputColumn === testCase.inputColumn
      );
      
      return {
        ruleId: matchedRule?.id || '',
        input: testCase.testValue,
        output: matchedRule ? 
          testCase.testValue.toUpperCase() : // Simulated transformation
          testCase.testValue,
        matched: !!matchedRule,
        executionTime: Math.random() * 100,
        timestamp: new Date()
      };
    });
    
    setExecutionTraces(traces);
    setIsRunningTests(false);
  };

  const addTestCase = () => {
    const newTestCase: TestCase = {
      id: `test_${Date.now()}`,
      // Add the missing properties to TestCase or use a type assertion
      ...({} as any) // This is a workaround - you should update your types file
    };
    onTestCasesUpdate([...testCases, newTestCase]);
  };

  const updateTestCase = (id: string, updates: Partial<LocalTestCase>) => {
    const updated = localTestCases.map(tc => 
      tc.id === id ? { ...tc, ...updates } : tc
    );
    // You need to convert back to TestCase[] if needed
    onTestCasesUpdate(updated as any);
  };

  const deleteTestCase = (id: string) => {
    const updated = localTestCases.filter(tc => tc.id !== id);
    onTestCasesUpdate(updated as any);
  };

  const toggleTestRow = (id: string) => {
    const newExpanded = new Set(expandedTestRows);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpandedTestRows(newExpanded);
  };

  const renderStandardizedOutput = () => {
    return (
      <div className="overflow-auto">
        <table className="w-full border-collapse">
          <thead className="bg-gray-50 dark:bg-gray-800">
            <tr>
              <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Input Column</th>
              <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Input Value</th>
              <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Output Value</th>
              <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Status</th>
              <th className="p-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
            {localTestCases.map((testCase) => {
              const trace = executionTraces.find(t => t.input === testCase.testValue);
              const output = trace?.output || testCase.testValue;
              const matched = trace?.matched || false;
              
              return (
                <React.Fragment key={testCase.id}>
                  <tr className="hover:bg-gray-50 dark:hover:bg-gray-800/50">
                    <td className="p-3">
                      <select
                        value={testCase.inputColumn}
                        onChange={(e) => updateTestCase(testCase.id, { inputColumn: e.target.value })}
                        className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-sm"
                      >
                        {ruleSet.rules.map(rule => (
                          <option key={rule.id} value={rule.inputColumn}>
                            {rule.inputColumn}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td className="p-3">
                      <input
                        type="text"
                        value={testCase.testValue}
                        onChange={(e) => updateTestCase(testCase.id, { testValue: e.target.value })}
                        placeholder="Enter test value..."
                        className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-700 text-sm"
                      />
                    </td>
                    <td className="p-3">
                      <div className={`px-2 py-1 rounded text-sm ${
                        matched 
                          ? 'bg-green-100 dark:bg-green-900/30 text-green-800 dark:text-green-300'
                          : 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400'
                      }`}>
                        {output}
                      </div>
                    </td>
                    <td className="p-3">
                      <div className="flex items-center space-x-2">
                        {matched ? (
                          <CheckCircle className="w-4 h-4 text-green-500" />
                        ) : (
                          <XCircle className="w-4 h-4 text-gray-400" />
                        )}
                        <span className="text-sm">
                          {matched ? 'Transformed' : 'No match'}
                        </span>
                      </div>
                    </td>
                    <td className="p-3">
                      <div className="flex items-center space-x-2">
                        <button
                          onClick={() => toggleTestRow(testCase.id)}
                          className="p-1 hover:bg-gray-200 dark:hover:bg-gray-700 rounded"
                        >
                          {expandedTestRows.has(testCase.id) ? (
                            <ChevronDown className="w-4 h-4" />
                          ) : (
                            <ChevronRight className="w-4 h-4" />
                          )}
                        </button>
                        <button
                          onClick={() => deleteTestCase(testCase.id)}
                          className="p-1 hover:bg-red-100 dark:hover:bg-red-900/30 text-red-600 rounded"
                        >
                          ✕
                        </button>
                      </div>
                    </td>
                  </tr>
                  
                  {expandedTestRows.has(testCase.id) && trace && (
                    <tr className="bg-blue-50 dark:bg-blue-900/10">
                      <td colSpan={5} className="p-3">
                        <div className="text-sm space-y-2">
                          <div className="flex items-center space-x-2">
                            <Clock className="w-4 h-4" />
                            <span>Execution time: {trace.executionTime.toFixed(2)}ms</span>
                          </div>
                          <div className="flex items-center space-x-2">
                            <Zap className="w-4 h-4" />
                            <span>Matched rule: {trace.ruleId || 'None'}</span>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    );
  };

  const renderTraceView = () => {
    return (
      <div className="space-y-4">
        {executionTraces.map((trace, index) => (
          <div 
            key={index}
            className={`p-4 rounded-lg border ${
              trace.matched
                ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
                : 'bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700'
            }`}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                {trace.matched ? (
                  <CheckCircle className="w-5 h-5 text-green-500" />
                ) : (
                  <AlertCircle className="w-5 h-5 text-gray-400" />
                )}
                <div>
                  <div className="font-medium">
                    {trace.matched ? 'Rule Applied' : 'No Match Found'}
                  </div>
                  <div className="text-sm text-gray-500 dark:text-gray-400">
                    {trace.timestamp.toLocaleTimeString()}
                  </div>
                </div>
              </div>
              <div className="text-sm text-gray-500 dark:text-gray-400">
                {trace.executionTime.toFixed(2)}ms
              </div>
            </div>
            
            <div className="mt-3 grid grid-cols-2 gap-4">
              <div>
                <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Input</div>
                <div className="p-2 bg-white dark:bg-gray-800 rounded border">
                  {trace.input}
                </div>
              </div>
              <div>
                <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Output</div>
                <div className="p-2 bg-white dark:bg-gray-800 rounded border">
                  {trace.output}
                </div>
              </div>
            </div>
            
            {trace.error && (
              <div className="mt-2 p-2 bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 rounded text-sm">
                {trace.error}
              </div>
            )}
          </div>
        ))}
      </div>
    );
  };

  const renderComparisonView = () => {
    return (
      <div className="space-y-4">
        {localTestCases.map((testCase, index) => {
          const trace = executionTraces[index];
          const output = trace?.output || testCase.testValue;
          const input = testCase.testValue;
          
          // Simple diff visualization
          const getDiffSegments = (input: string, output: string) => {
            if (input === output) {
              return [{ text: input, type: 'same' }];
            }
            
            // Simple diff for demonstration
            const segments = [];
            const minLength = Math.min(input.length, output.length);
            
            for (let i = 0; i < minLength; i++) {
              if (input[i] !== output[i]) {
                segments.push({ text: input[i], type: 'removed' });
                segments.push({ text: output[i], type: 'added' });
              } else {
                segments.push({ text: input[i], type: 'same' });
              }
            }
            
            // Add remaining characters
            if (input.length > minLength) {
              segments.push({ 
                text: input.substring(minLength), 
                type: 'removed' 
              });
            }
            if (output.length > minLength) {
              segments.push({ 
                text: output.substring(minLength), 
                type: 'added' 
              });
            }
            
            return segments;
          };
          
          const segments = getDiffSegments(input, output);
          
          return (
            <div key={testCase.id} className="p-4 bg-white dark:bg-gray-800 rounded-lg border">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Before</div>
                  <div className="p-3 bg-gray-50 dark:bg-gray-900 rounded border">
                    {input}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">After</div>
                  <div className="p-3 bg-gray-50 dark:bg-gray-900 rounded border">
                    <div className="flex flex-wrap">
                      {segments.map((segment, i) => (
                        <span
                          key={i}
                          className={`${
                            segment.type === 'added'
                              ? 'bg-green-200 dark:bg-green-800 text-green-900 dark:text-green-300'
                              : segment.type === 'removed'
                              ? 'bg-red-200 dark:bg-red-800 text-red-900 dark:text-red-300 line-through'
                              : ''
                          }`}
                        >
                          {segment.text}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  return (
    <div className="h-full flex flex-col">
      {/* Panel Header */}
      <div className="border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800 p-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <h3 className="font-medium text-gray-900 dark:text-white">Test & Preview</h3>
            <button
              onClick={runTests}
              disabled={isRunningTests}
              className="px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center space-x-2"
            >
              {isRunningTests ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Play className="w-4 h-4" />
              )}
              <span>Run Tests</span>
            </button>
            <button
              onClick={addTestCase}
              className="px-3 py-1.5 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center space-x-2"
            >
              <Plus className="w-4 h-4" />
              <span>Add Test Case</span>
            </button>
          </div>
          <div className="text-sm text-gray-500 dark:text-gray-400">
            {localTestCases.length} test cases
          </div>
        </div>
      </div>

      {/* Tabs */}
      <Tab.Group selectedIndex={selectedTab} onChange={setSelectedTab}>
        <Tab.List className="border-b border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 px-4">
          <div className="flex space-x-1">
            <Tab className={({ selected }) =>
              `px-4 py-2 text-sm font-medium rounded-t-lg border-b-2 transition-colors ${
                selected
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20'
                  : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
              }`
            }>
              <div className="flex items-center space-x-2">
                <Eye className="w-4 h-4" />
                <span>Standardized Output</span>
              </div>
            </Tab>
            <Tab className={({ selected }) =>
              `px-4 py-2 text-sm font-medium rounded-t-lg border-b-2 transition-colors ${
                selected
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20'
                  : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
              }`
            }>
              <div className="flex items-center space-x-2">
                <GitCommit className="w-4 h-4" />
                <span>Trace View</span>
              </div>
            </Tab>
            <Tab className={({ selected }) =>
              `px-4 py-2 text-sm font-medium rounded-t-lg border-b-2 transition-colors ${
                selected
                  ? 'border-blue-500 text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20'
                  : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'
              }`
            }>
              <div className="flex items-center space-x-2">
                <BarChart3 className="w-4 h-4" />
                <span>Comparison</span>
              </div>
            </Tab>
          </div>
        </Tab.List>
        
        <Tab.Panels className="flex-1 overflow-auto">
          <Tab.Panel className="h-full p-4">
            {renderStandardizedOutput()}
          </Tab.Panel>
          <Tab.Panel className="h-full p-4">
            {renderTraceView()}
          </Tab.Panel>
          <Tab.Panel className="h-full p-4">
            {renderComparisonView()}
          </Tab.Panel>
        </Tab.Panels>
      </Tab.Group>
    </div>
  );
};

export default TestPreviewPanel;