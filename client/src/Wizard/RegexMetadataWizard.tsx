// RegexMetadataWizard.tsx
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  FileText,
  AlertCircle,
  Search,
  Plus,
  Trash2,
  Play,
  TestTube
} from 'lucide-react';
import {
  FileRegexMetadataFormData,
  FileRegexMetadataWizardProps,
  RegexPatternDefinition,
  RegexTestResult
} from '../types/types';

const RegexMetadataWizard: React.FC<FileRegexMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<FileRegexMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    encoding: 'UTF-8',
    patterns: [],
    sampleData: [],
    totalPatterns: 0,
    validationMode: 'strict',
    caseSensitive: true,
    multiline: false,
    global: true
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<RegexTestResult[]>([]);
  const [activeTab, setActiveTab] = useState<'patterns' | 'test'>('patterns');

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 4;

  // Supported file extensions
  const supportedExtensions = ['.txt', '.log', '.csv', '.json', '.xml', '.sql'];

  // Common regex patterns for quick selection
  const commonPatterns = [
    { name: 'Email', pattern: '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}' },
    { name: 'Phone Number', pattern: '\\+?\\d{1,4}?[-.\\s]?\\(?\\d{1,3}?\\)?[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,4}[-.\\s]?\\d{1,9}' },
    { name: 'URL', pattern: 'https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)' },
    { name: 'IP Address', pattern: '\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b' },
    { name: 'Date (YYYY-MM-DD)', pattern: '\\d{4}-\\d{2}-\\d{2}' },
    { name: 'Credit Card', pattern: '\\b(?:\\d{4}[- ]?){3}\\d{4}\\b' },
    { name: 'SSN', pattern: '\\d{3}-\\d{2}-\\d{4}' },
    { name: 'Hexadecimal Color', pattern: '#[0-9a-fA-F]{6}' },
  ];

  // Field types for pattern categorization
  const fieldTypes = [
    'Email', 'Phone', 'URL', 'IP Address', 'Date', 'Time', 'Currency', 
    'Number', 'Text', 'Identifier', 'Code', 'Custom'
  ];

  // Parse file and extract sample data
  const parseFile = async (file: File): Promise<string[]> => {
    const content = await file.text();
    const lines = content.split('\n').filter(line => line.trim().length > 0);
    
    // Return first 50 lines or all lines if fewer
    return lines.slice(0, 50);
  };

  // Test a single regex pattern against sample data
  const testRegexPattern = (pattern: RegexPatternDefinition, sampleData: string[]): RegexTestResult => {
    const flags = pattern.flags || getDefaultFlags();
    const regex = new RegExp(pattern.pattern, flags);
    const matches: Array<{ match: string; index: number; groups: string[] }> = [];
    let totalMatches = 0;

    sampleData.forEach((line, lineIndex) => {
      const lineMatches = line.matchAll(regex);
      for (const match of lineMatches) {
        matches.push({
          match: match[0],
          index: lineIndex,
          groups: match.slice(1)
        });
        totalMatches++;
        
        // Limit matches to prevent performance issues
        if (totalMatches >= 100) break;
      }
      if (totalMatches >= 100) return;
    });

    return {
      patternId: pattern.id,
      matches: matches.slice(0, 20), // Show only first 20 matches
      totalMatches
    };
  };

  // Test all patterns against sample data
  const testAllPatterns = () => {
    if (formData.sampleData.length === 0 || formData.patterns.length === 0) {
      setError('No sample data or patterns to test');
      return;
    }

    const results: RegexTestResult[] = formData.patterns.map(pattern => 
      testRegexPattern(pattern, formData.sampleData)
    );

    setTestResults(results);
    setActiveTab('test');
  };

  // Get default regex flags based on form settings
  const getDefaultFlags = (): string => {
    let flags = '';
    if (formData.global) flags += 'g';
    if (formData.caseSensitive) flags += 'i';
    if (formData.multiline) flags += 'm';
    return flags;
  };

  // Handle file selection
  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!supportedExtensions.includes(fileExtension)) {
      setError(`Please select a supported file: ${supportedExtensions.join(', ')}`);
      return;
    }

    setError(null);
    setIsLoading(true);
    
    try {
      const sampleData = await parseFile(file);
      
      updateFormData({
        file,
        filePath: file.name,
        sampleData
      });
      
    } catch (err: any) {
      setError(`Failed to process file: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  // Update form data
  const updateFormData = (updates: Partial<FileRegexMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Add a new pattern
  const addPattern = () => {
    const newPattern: RegexPatternDefinition = {
      id: `pattern-${Date.now()}`,
      name: `Pattern ${formData.patterns.length + 1}`,
      pattern: '',
      description: '',
      flags: getDefaultFlags(),
      sampleMatches: [],
      matchCount: 0,
      fieldType: 'Custom'
    };

    updateFormData({
      patterns: [...formData.patterns, newPattern],
      totalPatterns: formData.patterns.length + 1
    });
  };

  // Update pattern field
  const updatePattern = (patternId: string, field: string, value: any) => {
    const newPatterns = formData.patterns.map(pattern => 
      pattern.id === patternId ? { ...pattern, [field]: value } : pattern
    );
    updateFormData({ patterns: newPatterns });
  };

  // Remove pattern
  const removePattern = (patternId: string) => {
    const newPatterns = formData.patterns.filter(pattern => pattern.id !== patternId);
    updateFormData({ 
      patterns: newPatterns,
      totalPatterns: newPatterns.length
    });
  };

  // Test a single pattern
  const testPattern = (patternId: string) => {
    const pattern = formData.patterns.find(p => p.id === patternId);
    if (!pattern || formData.sampleData.length === 0) return;

    const result = testRegexPattern(pattern, formData.sampleData);
    
    // Update pattern with match count and sample matches
    updatePattern(patternId, 'matchCount', result.totalMatches);
    updatePattern(patternId, 'sampleMatches', result.matches.slice(0, 5).map(m => m.match));
    
    // Update test results
    setTestResults(prev => {
      const filtered = prev.filter(r => r.patternId !== patternId);
      return [...filtered, result];
    });
  };

  // Add common pattern
  const addCommonPattern = (commonPattern: { name: string; pattern: string }) => {
    const newPattern: RegexPatternDefinition = {
      id: `pattern-${Date.now()}`,
      name: commonPattern.name,
      pattern: commonPattern.pattern,
      description: `Common pattern for ${commonPattern.name}`,
      flags: getDefaultFlags(),
      sampleMatches: [],
      matchCount: 0,
      fieldType: commonPattern.name
    };

    updateFormData({
      patterns: [...formData.patterns, newPattern],
      totalPatterns: formData.patterns.length + 1
    });
  };

  const handleNext = () => {
    if (currentStep < totalSteps) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleSave = () => {
    onSave(formData);
    handleClose();
  };

  const handleClose = () => {
    onClose();
    // Reset state
    setCurrentStep(1);
    setFormData({
      name: '',
      purpose: '',
      description: '',
      file: null,
      filePath: '',
      encoding: 'UTF-8',
      patterns: [],
      sampleData: [],
      totalPatterns: 0,
      validationMode: 'strict',
      caseSensitive: true,
      multiline: false,
      global: true
    });
    setTestResults([]);
    setError(null);
  };

  // Render patterns table
  const renderPatternsTable = () => {
    if (formData.patterns.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <Search className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No regex patterns defined</p>
            <p className="text-sm mt-2">Add patterns to start matching against your data</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Regex Patterns ({formData.patterns.length} patterns)
          </h4>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Name
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Pattern
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Field Type
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Matches
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.patterns.map((pattern) => (
                <tr key={pattern.id} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2">
                    <input
                      type="text"
                      value={pattern.name}
                      onChange={(e) => updatePattern(pattern.id, 'name', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <input
                      type="text"
                      value={pattern.pattern}
                      onChange={(e) => updatePattern(pattern.id, 'pattern', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white font-mono text-sm"
                      placeholder="Enter regex pattern"
                    />
                  </td>
                  <td className="px-3 py-2">
                    <select
                      value={pattern.fieldType}
                      onChange={(e) => updatePattern(pattern.id, 'fieldType', e.target.value)}
                      className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      {fieldTypes.map(type => (
                        <option key={type} value={type}>{type}</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex items-center space-x-2">
                      <span className={`text-xs px-2 py-1 rounded ${
                        pattern.matchCount > 0 
                          ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300'
                          : 'bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300'
                      }`}>
                        {pattern.matchCount} matches
                      </span>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => testPattern(pattern.id)}
                        className="h-6 w-6 p-0"
                      >
                        <Play className="h-3 w-3" />
                      </Button>
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removePattern(pattern.id)}
                      className="h-6 w-6 p-0 text-red-600 hover:text-red-800"
                    >
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  // Render test results
  const renderTestResults = () => {
    if (testResults.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <TestTube className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No test results available</p>
            <p className="text-sm mt-2">Run tests to see pattern matches</p>
          </div>
        </div>
      );
    }

    return (
      <div className="space-y-4">
        {testResults.map((result) => {
          const pattern = formData.patterns.find(p => p.id === result.patternId);
          if (!pattern) return null;

          return (
            <div key={result.patternId} className="border border-gray-200 dark:border-gray-600 rounded-md">
              <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
                <div className="flex justify-between items-center">
                  <div>
                    <h5 className="font-medium text-gray-700 dark:text-gray-300">{pattern.name}</h5>
                    <p className="text-xs text-gray-500 dark:text-gray-400 font-mono">{pattern.pattern}</p>
                  </div>
                  <span className={`text-xs px-2 py-1 rounded ${
                    result.totalMatches > 0 
                      ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300'
                      : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'
                  }`}>
                    {result.totalMatches} total matches
                  </span>
                </div>
              </div>
              <div className="p-3 max-h-48 overflow-y-auto">
                {result.matches.length > 0 ? (
                  <div className="space-y-1">
                    {result.matches.slice(0, 10).map((match, index) => (
                      <div key={index} className="text-sm p-2 bg-blue-50 dark:bg-blue-900/20 rounded">
                        <div className="font-mono text-blue-700 dark:text-blue-300">
                          {match.match}
                        </div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                          Line {match.index + 1}
                        </div>
                      </div>
                    ))}
                    {result.matches.length > 10 && (
                      <div className="text-center text-gray-500 dark:text-gray-400 text-xs py-2">
                        ... and {result.matches.length - 10} more matches
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="text-center text-gray-500 dark:text-gray-400 py-4">
                    No matches found
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  // Render sample data preview
  const renderSampleData = () => {
    if (formData.sampleData.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No sample data available</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Sample Data ({formData.sampleData.length} lines)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-48">
          <pre className="p-4 text-sm bg-gray-50 dark:bg-gray-800 text-gray-700 dark:text-gray-300 font-mono">
            {formData.sampleData.slice(0, 20).join('\n')}
            {formData.sampleData.length > 20 && (
              <div className="text-gray-500 dark:text-gray-400 mt-2">
                ... and {formData.sampleData.length - 20} more lines
              </div>
            )}
          </pre>
        </div>
      </div>
    );
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-6xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              File Regex Metadata Wizard
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Step {currentStep} of {totalSteps}
            </p>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={handleClose}
            className="h-8 w-8 p-0"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Progress Bar */}
        <div className="px-6 pt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${(currentStep / totalSteps) * 100}%` }}
            />
          </div>
        </div>

        {/* Content */}
        <div className="p-6 max-h-[60vh] overflow-y-auto">
          {currentStep === 1 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                General Properties
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Configure the basic properties for your regex metadata.
              </p>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => updateFormData({ name: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter metadata name"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Purpose
                  </label>
                  <input
                    type="text"
                    value={formData.purpose}
                    onChange={(e) => updateFormData({ purpose: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter purpose"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Description
                  </label>
                  <textarea
                    value={formData.description}
                    onChange={(e) => updateFormData({ description: e.target.value })}
                    rows={3}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter description"
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Validation Mode
                    </label>
                    <select
                      value={formData.validationMode}
                      onChange={(e) => updateFormData({ validationMode: e.target.value as 'strict' | 'lenient' | 'custom' })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="strict">Strict Validation</option>
                      <option value="lenient">Lenient Validation</option>
                      <option value="custom">Custom Validation</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Encoding
                    </label>
                    <select
                      value={formData.encoding}
                      onChange={(e) => updateFormData({ encoding: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="UTF-8">UTF-8</option>
                      <option value="UTF-16">UTF-16</option>
                      <option value="ISO-8859-1">ISO-8859-1</option>
                      <option value="ASCII">ASCII</option>
                    </select>
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-4">
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      checked={formData.caseSensitive}
                      onChange={(e) => updateFormData({ caseSensitive: e.target.checked })}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                      Case Sensitive
                    </label>
                  </div>
                  
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      checked={formData.multiline}
                      onChange={(e) => updateFormData({ multiline: e.target.checked })}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                      Multiline
                    </label>
                  </div>
                  
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      checked={formData.global}
                      onChange={(e) => updateFormData({ global: e.target.checked })}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                      Global Match
                    </label>
                  </div>
                </div>
              </div>
            </div>
          )}

          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                File Selection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Select a file to test your regex patterns against. This will provide sample data for pattern validation.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept={supportedExtensions.join(',')}
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Sample File *
                  </label>
                  <div className="flex space-x-2">
                    <input
                      type="text"
                      value={formData.filePath}
                      readOnly
                      className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white bg-gray-50 dark:bg-gray-600"
                      placeholder="No file selected"
                    />
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => fileInputRef.current?.click()}
                      disabled={isLoading}
                    >
                      {isLoading ? (
                        <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-current mr-2" />
                      ) : (
                        <Upload className="h-4 w-4 mr-2" />
                      )}
                      {isLoading ? 'Loading...' : 'Browse'}
                    </Button>
                  </div>
                  {formData.file && (
                    <p className="text-sm text-green-600 dark:text-green-400 mt-1">
                      ✓ File selected: {formData.file.name} ({(formData.file.size / 1024).toFixed(2)} KB)
                    </p>
                  )}
                </div>

                {error && (
                  <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                    <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                    <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                  </div>
                )}

                {formData.sampleData.length > 0 && (
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Sample Data Preview</h4>
                    {renderSampleData()}
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-6">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Regex Pattern Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Define and test your regex patterns. You can add custom patterns or use common patterns.
              </p>
              
              <div className="flex justify-between items-center">
                <div className="flex space-x-2">
                  <Button
                    onClick={addPattern}
                    variant="outline"
                    className="flex items-center space-x-2"
                  >
                    <Plus className="h-4 w-4" />
                    <span>Add Custom Pattern</span>
                  </Button>
                  
                  {formData.sampleData.length > 0 && formData.patterns.length > 0 && (
                    <Button
                      onClick={testAllPatterns}
                      variant="outline"
                      className="flex items-center space-x-2"
                    >
                      <TestTube className="h-4 w-4" />
                      <span>Test All Patterns</span>
                    </Button>
                  )}
                </div>

                <div className="text-sm text-gray-500 dark:text-gray-400">
                  {formData.patterns.length} patterns defined
                </div>
              </div>

              {/* Common Patterns */}
              <div>
                <h4 className="font-medium text-gray-900 dark:text-white mb-2">Common Patterns</h4>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mb-4">
                  {commonPatterns.map((commonPattern, index) => (
                    <Button
                      key={index}
                      onClick={() => addCommonPattern(commonPattern)}
                      variant="outline"
                      size="sm"
                      className="text-xs h-auto py-2"
                    >
                      {commonPattern.name}
                    </Button>
                  ))}
                </div>
              </div>

              {/* Tab Navigation */}
              <div className="border-b border-gray-200 dark:border-gray-600">
                <nav className="-mb-px flex space-x-8">
                  <button
                    onClick={() => setActiveTab('patterns')}
                    className={`py-2 px-1 border-b-2 font-medium text-sm ${
                      activeTab === 'patterns'
                        ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                        : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'
                    }`}
                  >
                    Patterns
                  </button>
                  <button
                    onClick={() => setActiveTab('test')}
                    className={`py-2 px-1 border-b-2 font-medium text-sm ${
                      activeTab === 'test'
                        ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                        : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'
                    }`}
                  >
                    Test Results
                  </button>
                </nav>
              </div>

              {/* Tab Content */}
              <div>
                {activeTab === 'patterns' ? renderPatternsTable() : renderTestResults()}
              </div>
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your regex metadata configuration and click Finish to save it to the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your regex metadata configuration is complete and ready to be saved to the repository.
                </p>
              </div>
              
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-4">
                  <h4 className="font-medium text-gray-900 dark:text-white">Configuration Summary</h4>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Name:</span>
                      <span className="text-gray-900 dark:text-white font-medium">{formData.name || 'Not specified'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">File:</span>
                      <span className="text-gray-900 dark:text-white">{formData.filePath || 'No file selected'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Validation Mode:</span>
                      <span className="text-gray-900 dark:text-white">{formData.validationMode}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Patterns:</span>
                      <span className="text-gray-900 dark:text-white">{formData.patterns.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Encoding:</span>
                      <span className="text-gray-900 dark:text-white">{formData.encoding}</span>
                    </div>
                  </div>
                </div>

                {formData.patterns.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Patterns Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.patterns.slice(0, 6).map((pattern) => (
                          <div key={pattern.id} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <div>
                              <span className="text-gray-600 dark:text-gray-400 font-medium">{pattern.name}</span>
                              <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded ml-2">
                                {pattern.fieldType}
                              </span>
                            </div>
                            <span className={`text-xs px-2 py-1 rounded ${
                              pattern.matchCount > 0 
                                ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-300' 
                                : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-300'
                            }`}>
                              {pattern.matchCount} matches
                            </span>
                          </div>
                        ))}
                        {formData.patterns.length > 6 && (
                          <div className="text-center text-gray-500 dark:text-gray-400 text-xs">
                            ... and {formData.patterns.length - 6} more patterns
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t border-gray-200 dark:border-gray-700">
          <Button
            variant="outline"
            onClick={currentStep === 1 ? handleClose : handleBack}
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            {currentStep === 1 ? 'Cancel' : 'Back'}
          </Button>
          
          <div className="flex items-center space-x-3">
            <span className="text-sm text-gray-500 dark:text-gray-400">
              Step {currentStep} of {totalSteps}
            </span>
            {currentStep < totalSteps ? (
              <Button 
                onClick={handleNext}
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && formData.patterns.length === 0)}
              >
                Next
                <ArrowRight className="h-4 w-4 ml-2" />
              </Button>
            ) : (
              <Button onClick={handleSave}>
                <CheckCircle className="h-4 w-4 mr-2" />
                Finish & Save to Repository
              </Button>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default RegexMetadataWizard;