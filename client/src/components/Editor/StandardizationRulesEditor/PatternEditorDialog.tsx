// PatternEditorDialog.tsx
import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { X, Play, Save, RefreshCw, HelpCircle, Regex } from 'lucide-react';
import { PatternType } from '../../../types/types';

interface PatternEditorDialogProps {
  ruleId: string;
  initialPattern: string;
  patternType: PatternType;
  onClose: () => void;
  onSave: (pattern: string, patternType: PatternType) => void;
}

const PatternEditorDialog: React.FC<PatternEditorDialogProps> = ({
  initialPattern,
  patternType,
  onClose,
  onSave
}) => {
  const [pattern, setPattern] = useState(initialPattern);
  const [currentPatternType, setCurrentPatternType] = useState<PatternType>(patternType);
  const [testString, setTestString] = useState('');
  const [matches, setMatches] = useState<RegExpMatchArray[]>([]);
  const [error, setError] = useState<string>();
  const [flags, setFlags] = useState<string[]>(['g', 'i']);
  const [isTesting, setIsTesting] = useState(false);

  const patternTypes: { value: PatternType; label: string; description: string }[] = [
    { value: 'regex', label: 'Regular Expression', description: 'Advanced pattern matching using regex syntax' },
    { value: 'contains', label: 'Contains', description: 'Matches if string contains the pattern' },
    { value: 'startsWith', label: 'Starts With', description: 'Matches if string starts with the pattern' },
    { value: 'endsWith', label: 'Ends With', description: 'Matches if string ends with the pattern' },
    { value: 'exactMatch', label: 'Exact Match', description: 'Matches if string exactly equals the pattern' },
    { value: 'dictionary', label: 'Dictionary Lookup', description: 'Matches against a dictionary of values' },
    { value: 'custom', label: 'Custom Logic', description: 'Custom JavaScript matching logic' }
  ];

  const commonRegexPatterns = [
    { name: 'Email', pattern: '^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$' },
    { name: 'Phone Number', pattern: '^\\+?[1-9]\\d{1,14}$' },
    { name: 'URL', pattern: '^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$' },
    { name: 'IP Address', pattern: '^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$' },
    { name: 'Date (YYYY-MM-DD)', pattern: '^\\d{4}-\\d{2}-\\d{2}$' },
    { name: 'Digits Only', pattern: '^\\d+$' },
    { name: 'Letters Only', pattern: '^[A-Za-z]+$' },
    { name: 'Alphanumeric', pattern: '^[A-Za-z0-9]+$' },
    { name: 'Whitespace', pattern: '\\s+' },
    { name: 'Word Boundary', pattern: '\\b\\w+\\b' }
  ];

  const testPattern = () => {
    setIsTesting(true);
    setError(undefined);
    setMatches([]);

    try {
      if (!testString.trim()) {
        setMatches([]);
        return;
      }

      switch (currentPatternType) {
        case 'regex':
          const regex = new RegExp(pattern, flags.join(''));
          const regexMatches = Array.from(testString.matchAll(regex));
          setMatches(regexMatches);
          break;

        case 'contains':
          if (testString.includes(pattern)) {
            setMatches([[pattern, testString.indexOf(pattern)]] as any);
          }
          break;

        case 'startsWith':
          if (testString.startsWith(pattern)) {
            setMatches([[pattern, 0]] as any);
          }
          break;

        case 'endsWith':
          if (testString.endsWith(pattern)) {
            setMatches([[pattern, testString.length - pattern.length]] as any);
          }
          break;

        case 'exactMatch':
          if (testString === pattern) {
            setMatches([[pattern, 0]] as any);
          }
          break;

        case 'dictionary':
          // For demo purposes, treat pattern as comma-separated dictionary
          const dictionary = pattern.split(',').map(p => p.trim());
          const found = dictionary.find(p => testString.includes(p));
          if (found) {
            setMatches([[found, testString.indexOf(found)]] as any);
          }
          break;

        case 'custom':
          try {
            // WARNING: In production, this should be sandboxed
            const customTest = new Function('input', 'pattern', `
              try {
                ${pattern}
              } catch (e) {
                return { error: e.message };
              }
            `);
            const result = customTest(testString, pattern);
            if (result && !result.error) {
              setMatches([[result, 0]] as any);
            } else if (result?.error) {
              setError(result.error);
            }
          } catch (e: any) {
            setError(`Custom function error: ${e.message}`);
          }
          break;
      }
    } catch (e: any) {
      setError(`Pattern error: ${e.message}`);
    } finally {
      setIsTesting(false);
    }
  };

  const handleSave = () => {
    onSave(pattern, currentPatternType);
    onClose();
  };

  const insertRegexPattern = (regexPattern: string) => {
    setPattern(regexPattern);
  };

  const toggleFlag = (flag: string) => {
    if (flags.includes(flag)) {
      setFlags(flags.filter(f => f !== flag));
    } else {
      setFlags([...flags, flag]);
    }
  };

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
        className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl max-w-4xl w-full max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="border-b border-gray-200 dark:border-gray-700 p-6">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-xl font-bold text-gray-900 dark:text-white">Pattern Editor</h2>
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                Define and test matching patterns for your rule
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
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Left Column: Pattern Configuration */}
            <div className="space-y-6">
              {/* Pattern Type Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Pattern Type
                </label>
                <div className="grid grid-cols-2 gap-2">
                  {patternTypes.map((type) => (
                    <button
                      key={type.value}
                      onClick={() => setCurrentPatternType(type.value)}
                      className={`p-3 rounded-lg border text-left transition-colors ${
                        currentPatternType === type.value
                          ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20 text-blue-700 dark:text-blue-300'
                          : 'border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700'
                      }`}
                    >
                      <div className="font-medium">{type.label}</div>
                      <div className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                        {type.description}
                      </div>
                    </button>
                  ))}
                </div>
              </div>

              {/* Pattern Input */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Pattern
                </label>
                {currentPatternType === 'regex' && (
                  <div className="mb-3">
                    <div className="flex flex-wrap gap-2 mb-2">
                      <button
                        onClick={() => toggleFlag('g')}
                        className={`px-2 py-1 text-xs rounded ${
                          flags.includes('g')
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-200 dark:bg-gray-700'
                        }`}
                      >
                        Global (g)
                      </button>
                      <button
                        onClick={() => toggleFlag('i')}
                        className={`px-2 py-1 text-xs rounded ${
                          flags.includes('i')
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-200 dark:bg-gray-700'
                        }`}
                      >
                        Case-insensitive (i)
                      </button>
                      <button
                        onClick={() => toggleFlag('m')}
                        className={`px-2 py-1 text-xs rounded ${
                          flags.includes('m')
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-200 dark:bg-gray-700'
                        }`}
                      >
                        Multiline (m)
                      </button>
                    </div>
                    
                    <div className="mb-3">
                      <label className="text-sm font-medium text-gray-600 dark:text-gray-400 mb-2 block">
                        Common Patterns
                      </label>
                      <div className="flex flex-wrap gap-2">
                        {commonRegexPatterns.map((p) => (
                          <button
                            key={p.name}
                            onClick={() => insertRegexPattern(p.pattern)}
                            className="px-2 py-1 text-xs bg-gray-100 dark:bg-gray-700 rounded hover:bg-gray-200 dark:hover:bg-gray-600"
                            title={p.pattern}
                          >
                            {p.name}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                )}

                <textarea
                  value={pattern}
                  onChange={(e) => setPattern(e.target.value)}
                  placeholder={
                    currentPatternType === 'regex' 
                      ? 'Enter regular expression pattern...'
                      : currentPatternType === 'dictionary'
                      ? 'Enter comma-separated values...'
                      : 'Enter pattern...'
                  }
                  rows={4}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white font-mono text-sm resize-none"
                />

                {currentPatternType === 'regex' && (
                  <div className="mt-2 text-xs text-gray-500 dark:text-gray-400">
                    <div className="flex items-center space-x-2">
                      <HelpCircle className="w-4 h-4" />
                      <span>
                        Use{' '}
                        <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">^</code> for start,{' '}
                        <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">$</code> for end,{' '}
                        <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">.</code> for any character,{' '}
                        <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded">*</code> for zero or more
                      </span>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Right Column: Testing Area */}
            <div className="space-y-6">
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Test String
                </label>
                <textarea
                  value={testString}
                  onChange={(e) => setTestString(e.target.value)}
                  placeholder="Enter text to test the pattern against..."
                  rows={4}
                  className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white resize-none"
                />
              </div>

              <div className="flex justify-end">
                <button
                  onClick={testPattern}
                  disabled={isTesting || !pattern}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center space-x-2"
                >
                  {isTesting ? (
                    <RefreshCw className="w-4 h-4 animate-spin" />
                  ) : (
                    <Play className="w-4 h-4" />
                  )}
                  <span>Test Pattern</span>
                </button>
              </div>

              {/* Test Results */}
              <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                <div className="bg-gray-50 dark:bg-gray-800 px-4 py-3 border-b border-gray-200 dark:border-gray-700">
                  <h3 className="font-medium text-gray-900 dark:text-white">Test Results</h3>
                </div>
                
                <div className="p-4">
                  {error ? (
                    <div className="p-3 bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 rounded-lg">
                      <div className="font-medium">Error</div>
                      <div className="text-sm mt-1">{error}</div>
                    </div>
                  ) : matches.length === 0 ? (
                    <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                      <Regex className="w-12 h-12 mx-auto mb-3 opacity-50" />
                      <p>No matches found. Enter a test string and click "Test Pattern".</p>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      <div className="text-sm text-gray-600 dark:text-gray-400">
                        Found {matches.length} match{matches.length !== 1 ? 'es' : ''}
                      </div>
                      
                      {matches.map((match, index) => (
                        <div
                          key={index}
                          className="p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg"
                        >
                          <div className="font-medium text-green-800 dark:text-green-300 mb-2">
                            Match #{index + 1}
                          </div>
                          <div className="space-y-2">
                            <div>
                              <span className="text-xs text-gray-500 dark:text-gray-400">Full match:</span>
                              <div className="font-mono text-sm bg-white dark:bg-gray-800 p-2 rounded mt-1">
                                {match[0]}
                              </div>
                            </div>
                            
                            {match.groups && Object.keys(match.groups).length > 0 && (
                              <div>
                                <span className="text-xs text-gray-500 dark:text-gray-400">Groups:</span>
                                <div className="grid grid-cols-2 gap-2 mt-1">
                                  {Object.entries(match.groups).map(([name, value]) => (
                                    <div key={name} className="bg-white dark:bg-gray-800 p-2 rounded">
                                      <div className="text-xs text-gray-500 dark:text-gray-400">{name}</div>
                                      <div className="font-mono text-sm">{value}</div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                            
                            {match.index !== undefined && (
                              <div className="text-sm text-gray-600 dark:text-gray-400">
                                Position: {match.index}
                              </div>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="border-t border-gray-200 dark:border-gray-700 p-6">
          <div className="flex items-center justify-between">
            <div className="text-sm text-gray-500 dark:text-gray-400">
              Pattern type: <span className="font-medium">{currentPatternType}</span>
              {currentPatternType === 'regex' && flags.length > 0 && (
                <span className="ml-3">Flags: {flags.join('')}</span>
              )}
            </div>
            
            <div className="flex items-center space-x-3">
              <button
                onClick={onClose}
                className="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700"
              >
                Cancel
              </button>
              <button
                onClick={handleSave}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center space-x-2"
              >
                <Save className="w-4 h-4" />
                <span>Save Pattern</span>
              </button>
            </div>
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
};

export default PatternEditorDialog;