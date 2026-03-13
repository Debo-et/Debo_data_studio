// components/MatchGroupConfigEditor.tsx
import React, { useState, useEffect, useCallback } from 'react';
import { 
  MatchGroupConfiguration, 
  FieldSchema, 
  MatchingAlgorithm, 
  AlgorithmInfo,
  MatchPreview 
} from './match-group';
import { debounce } from 'lodash';

// Icons for algorithms

interface MatchGroupConfigEditorProps {
  initialConfig?: Partial<MatchGroupConfiguration>;
  inputSchema: FieldSchema[];
  onChange: (config: MatchGroupConfiguration) => void;
  debounceMs?: number;
}

const PRESETS = {
  'Name Matching': {
    keys: [],
    threshold: 0.85,
    algorithm: 'jaro-winkler' as MatchingAlgorithm,
    outputGroupId: true,
    description: 'Optimized for person name matching'
  },
  'Address Matching': {
    keys: [],
    threshold: 0.75,
    algorithm: 'cosine' as MatchingAlgorithm,
    outputGroupId: true,
    blockingFields: ['postalCode', 'city']
  },
  'Product Matching': {
    keys: [],
    threshold: 0.8,
    algorithm: 'jaccard' as MatchingAlgorithm,
    outputGroupId: true,
    maxGroupSize: 50
  }
};

const ALGORITHM_INFO: Record<MatchingAlgorithm, AlgorithmInfo> = {
  levenshtein: {
    name: 'Levenshtein Distance',
    description: 'Measures the minimum number of single-character edits required to change one string into another',
    speed: 3,
    accuracy: 4,
    icon: '🔤',
    useCases: ['Short strings', 'Typos correction', 'Spell checking']
  },
  'jaro-winkler': {
    name: 'Jaro-Winkler',
    description: 'Measures similarity between two strings with emphasis on matching prefixes',
    speed: 4,
    accuracy: 5,
    icon: '🎯',
    useCases: ['Person names', 'Address matching', 'Record linkage']
  },
  cosine: {
    name: 'Cosine Similarity',
    description: 'Measures similarity between two vectors of an inner product space',
    speed: 2,
    accuracy: 5,
    icon: '📐',
    useCases: ['Text documents', 'Large strings', 'TF-IDF vectors']
  },
  jaccard: {
    name: 'Jaccard Index',
    description: 'Measures similarity between finite sample sets as intersection over union',
    speed: 5,
    accuracy: 3,
    icon: '🎲',
    useCases: ['Set comparisons', 'Categorical data', 'Token-based matching']
  },
  exact: {
    name: 'Exact Match',
    description: 'Requires strings to be identical',
    speed: 5,
    accuracy: 5,
    icon: '✅',
    useCases: ['Identical matching', 'Key fields', 'Primary key matching']
  }
};

const MatchGroupConfigEditor: React.FC<MatchGroupConfigEditorProps> = ({
  initialConfig,
  inputSchema,
  onChange,
  debounceMs = 300
}) => {
  const [config, setConfig] = useState<MatchGroupConfiguration>({
    keys: [],
    threshold: 0.8,
    algorithm: 'jaro-winkler',
    outputGroupId: true,
    ...initialConfig
  });

  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(
    new Set(initialConfig?.keys?.map(k => k.name) || [])
  );

  const [previewData, setPreviewData] = useState<MatchPreview[]>([]);
  const [sampleData, setSampleData] = useState<any[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);

  // Debounced config change handler
  const debouncedOnChange = useCallback(
    debounce((newConfig: MatchGroupConfiguration) => {
      onChange(newConfig);
    }, debounceMs),
    [onChange, debounceMs]
  );

  useEffect(() => {
    debouncedOnChange(config);
  }, [config, debouncedOnChange]);

  // Update keys when selectedKeys changes
  useEffect(() => {
    const keys = Array.from(selectedKeys).map(keyName => {
      const existing = config.keys.find(k => k.name === keyName);
      const schema = inputSchema.find(f => f.name === keyName);
      return {
        name: keyName,
        weight: existing?.weight || 1,
        type: schema?.type || 'string'
      };
    });

    setConfig(prev => ({
      ...prev,
      keys
    }));
  }, [selectedKeys]);

  const handleKeyToggle = (fieldName: string) => {
    const newSelected = new Set(selectedKeys);
    if (newSelected.has(fieldName)) {
      newSelected.delete(fieldName);
    } else {
      newSelected.add(fieldName);
    }
    setSelectedKeys(newSelected);
  };

  const handleKeyWeightChange = (fieldName: string, weight: number) => {
    setConfig(prev => ({
      ...prev,
      keys: prev.keys.map(key =>
        key.name === fieldName ? { ...key, weight } : key
      )
    }));
  };

  const handlePresetSelect = (presetName: keyof typeof PRESETS) => {
    const preset = PRESETS[presetName];
    const keys = inputSchema
      .filter(f => f.type === 'string')
      .slice(0, 2)
      .map(f => ({ name: f.name, type: f.type, weight: 1 }));

    setConfig(prev => ({
      ...prev,
      ...preset,
      keys
    }));

    setSelectedKeys(new Set(keys.map(k => k.name)));
  };

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const data = JSON.parse(e.target?.result as string);
        setSampleData(data.slice(0, 50)); // Limit to 50 records for preview
      } catch (error) {
        console.error('Error parsing JSON:', error);
      }
    };
    reader.readAsText(file);
  };

  const runPreview = async () => {
    if (sampleData.length === 0) return;

    setIsProcessing(true);
    // Simulate matching process
    await new Promise(resolve => setTimeout(resolve, 1000));

    // Generate mock preview data
    const mockPreview: MatchPreview[] = [
      {
        groupId: 1,
        similarity: 0.92,
        records: [
          { id: '1', data: sampleData[0] || {} },
          { id: '2', data: sampleData[1] || {} }
        ]
      },
      {
        groupId: 2,
        similarity: 0.87,
        records: [
          { id: '3', data: sampleData[2] || {} },
          { id: '4', data: sampleData[3] || {} }
        ]
      }
    ];

    setPreviewData(mockPreview);
    setIsProcessing(false);
  };

  const getSimilarityColor = (similarity: number) => {
    const hue = similarity * 120; // 0 = red (0°), 1 = green (120°)
    return `hsl(${hue}, 70%, 50%)`;
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">
            MATCH GROUP Configuration
          </h1>
          <p className="text-gray-600 mt-2">
            Configure fuzzy matching algorithms to group similar records
          </p>
        </div>

        {/* Preset Selection */}
        <div className="mb-8">
          <h2 className="text-lg font-semibold text-gray-800 mb-4">Quick Presets</h2>
          <div className="flex flex-wrap gap-3">
            {Object.entries(PRESETS).map(([name]) => (
              <button
                key={name}
                onClick={() => handlePresetSelect(name as keyof typeof PRESETS)}
                className="px-4 py-2 bg-purple-50 text-purple-700 rounded-lg hover:bg-purple-100 border border-purple-200 transition-colors"
              >
                {name}
              </button>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Left Column - Configuration */}
          <div className="lg:col-span-2 space-y-6">
            {/* Key Selection Panel */}
            <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-200">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold text-gray-900">
                  Matching Keys
                </h2>
                <span className="text-sm text-gray-500">
                  {selectedKeys.size} key{selectedKeys.size !== 1 ? 's' : ''} selected
                </span>
              </div>

              <div className="space-y-4">
                {inputSchema.map(field => (
                  <div
                    key={field.name}
                    className={`flex items-center justify-between p-4 rounded-lg border transition-all ${selectedKeys.has(field.name)
                        ? 'border-purple-300 bg-purple-50'
                        : 'border-gray-200 hover:border-gray-300'
                      }`}
                  >
                    <div className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={selectedKeys.has(field.name)}
                        onChange={() => handleKeyToggle(field.name)}
                        className="h-5 w-5 text-purple-600 rounded"
                      />
                      <div>
                        <div className="font-medium text-gray-900">
                          {field.name}
                        </div>
                        <div className="flex items-center space-x-2 text-sm text-gray-500">
                          <span className={`px-2 py-1 rounded ${field.type === 'string'
                              ? 'bg-blue-100 text-blue-800'
                              : field.type === 'number'
                                ? 'bg-green-100 text-green-800'
                                : 'bg-yellow-100 text-yellow-800'
                            }`}>
                            {field.type}
                          </span>
                          {field.nullable && (
                            <span className="text-gray-400">nullable</span>
                          )}
                        </div>
                      </div>
                    </div>

                    {selectedKeys.has(field.name) && (
                      <div className="w-48">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                          Weight: {config.keys.find(k => k.name === field.name)?.weight || 1}
                        </label>
                        <input
                          type="range"
                          min="0.1"
                          max="2"
                          step="0.1"
                          value={config.keys.find(k => k.name === field.name)?.weight || 1}
                          onChange={(e) =>
                            handleKeyWeightChange(field.name, parseFloat(e.target.value))
                          }
                          className="w-full h-2 bg-purple-100 rounded-lg appearance-none cursor-pointer"
                        />
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>

            {/* Algorithm Selection */}
            <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900 mb-6">
                Matching Algorithm
              </h2>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {(Object.entries(ALGORITHM_INFO) as [MatchingAlgorithm, AlgorithmInfo][]).map(
                  ([algoKey, info]) => (
                    <div
                      key={algoKey}
                      onClick={() => setConfig(prev => ({ ...prev, algorithm: algoKey }))}
                      className={`p-4 rounded-xl border-2 cursor-pointer transition-all ${config.algorithm === algoKey
                          ? 'border-purple-500 bg-purple-50'
                          : 'border-gray-200 hover:border-gray-300'
                        }`}
                    >
                      <div className="flex items-start justify-between">
                        <div>
                          <div className="flex items-center space-x-3 mb-2">
                            <span className="text-2xl">{info.icon}</span>
                            <h3 className="font-semibold text-gray-900">
                              {info.name}
                            </h3>
                          </div>
                          <p className="text-sm text-gray-600 mb-3">
                            {info.description}
                          </p>
                        </div>
                        <div className={`h-3 w-3 rounded-full ${config.algorithm === algoKey ? 'bg-purple-500' : 'bg-gray-300'
                          }`} />
                      </div>

                      {/* Performance Indicators */}
                      <div className="grid grid-cols-2 gap-4 mt-4">
                        <div>
                          <div className="text-xs text-gray-500 mb-1">Speed</div>
                          <div className="flex space-x-1">
                            {[...Array(5)].map((_, i) => (
                              <div
                                key={i}
                                className={`h-2 flex-1 rounded ${i < info.speed
                                    ? 'bg-green-500'
                                    : 'bg-gray-200'
                                  }`}
                              />
                            ))}
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-gray-500 mb-1">Accuracy</div>
                          <div className="flex space-x-1">
                            {[...Array(5)].map((_, i) => (
                              <div
                                key={i}
                                className={`h-2 flex-1 rounded ${i < info.accuracy
                                    ? 'bg-blue-500'
                                    : 'bg-gray-200'
                                  }`}
                              />
                            ))}
                          </div>
                        </div>
                      </div>

                      <div className="mt-3">
                        <div className="text-xs text-gray-500 mb-1">Best for:</div>
                        <div className="flex flex-wrap gap-1">
                          {info.useCases.map(useCase => (
                            <span
                              key={useCase}
                              className="px-2 py-1 bg-gray-100 text-gray-700 text-xs rounded"
                            >
                              {useCase}
                            </span>
                          ))}
                        </div>
                      </div>
                    </div>
                  )
                )}
              </div>
            </div>

            {/* Advanced Settings */}
            <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900 mb-6">
                Advanced Settings
              </h2>

              <div className="space-y-6">
                {/* Blocking Fields */}
                <div>
                  <h3 className="font-medium text-gray-900 mb-3">
                    Blocking Fields (Optional)
                  </h3>
                  <p className="text-sm text-gray-600 mb-4">
                    Reduce computational load by only comparing records that match on these fields
                  </p>
                  <select
                    multiple
                    value={config.blockingFields || []}
                    onChange={(e) => {
                      const values = Array.from(e.target.selectedOptions, option => option.value);
                      setConfig(prev => ({ ...prev, blockingFields: values }));
                    }}
                    className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                  >
                    {inputSchema.map(field => (
                      <option key={field.name} value={field.name}>
                        {field.name} ({field.type})
                      </option>
                    ))}
                  </select>
                </div>

                {/* Max Group Size */}
                <div>
                  <label className="block font-medium text-gray-900 mb-2">
                    Maximum Group Size
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="1000"
                    value={config.maxGroupSize || ''}
                    onChange={(e) =>
                      setConfig(prev => ({
                        ...prev,
                        maxGroupSize: e.target.value ? parseInt(e.target.value) : undefined
                      }))
                    }
                    className="w-full p-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
                    placeholder="Unlimited"
                  />
                </div>

                {/* Output Options */}
                <div className="space-y-3">
                  <label className="flex items-center space-x-3">
                    <input
                      type="checkbox"
                      checked={config.outputGroupId}
                      onChange={(e) =>
                        setConfig(prev => ({ ...prev, outputGroupId: e.target.checked }))
                      }
                      className="h-5 w-5 text-purple-600 rounded"
                    />
                    <span className="font-medium text-gray-900">
                      Output Group Identifier
                    </span>
                  </label>
                  <p className="text-sm text-gray-600 ml-8">
                    Adds a unique group ID to each matched record for downstream processing
                  </p>
                </div>
              </div>
            </div>
          </div>

          {/* Right Column - Threshold & Preview */}
          <div className="space-y-6">
            {/* Threshold Configuration */}
            <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900 mb-6">
                Similarity Threshold
              </h2>

              <div className="mb-8">
                <div className="flex justify-between mb-2">
                  <span className="text-sm font-medium text-gray-700">
                    Current: {config.threshold.toFixed(2)}
                  </span>
                  <span className="text-sm text-gray-500">
                    {config.threshold < 0.5 ? 'Loose' :
                      config.threshold < 0.8 ? 'Moderate' :
                        'Strict'} matching
                  </span>
                </div>

                {/* Gradient Slider Track */}
                <div className="relative mb-2">
                  <div
                    className="h-3 rounded-lg"
                    style={{
                      background: 'linear-gradient(90deg, #ef4444 0%, #f59e0b 50%, #10b981 100%)'
                    }}
                  />
                  <input
                    type="range"
                    min="0"
                    max="1"
                    step="0.01"
                    value={config.threshold}
                    onChange={(e) =>
                      setConfig(prev => ({ ...prev, threshold: parseFloat(e.target.value) }))
                    }
                    className="absolute top-0 left-0 w-full h-3 opacity-0 cursor-pointer"
                  />
                  <div
                    className="absolute top-1/2 w-6 h-6 bg-white border-2 border-purple-600 rounded-full -translate-y-1/2 -translate-x-1/2 shadow-lg"
                    style={{ left: `${config.threshold * 100}%` }}
                  />
                </div>

                <div className="flex justify-between text-sm text-gray-500">
                  <span>0.0 (All)</span>
                  <span>0.5</span>
                  <span>1.0 (Exact)</span>
                </div>
              </div>

              {/* Threshold Legend */}
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div className="w-4 h-4 rounded bg-red-500 mr-2" />
                    <span className="text-sm text-gray-700">Low (0.0 - 0.4)</span>
                  </div>
                  <span className="text-sm text-gray-500">Many matches</span>
                </div>
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div className="w-4 h-4 rounded bg-yellow-500 mr-2" />
                    <span className="text-sm text-gray-700">Medium (0.4 - 0.7)</span>
                  </div>
                  <span className="text-sm text-gray-500">Balanced</span>
                </div>
                <div className="flex items-center justify-between">
                  <div className="flex items-center">
                    <div className="w-4 h-4 rounded bg-green-500 mr-2" />
                    <span className="text-sm text-gray-700">High (0.7 - 1.0)</span>
                  </div>
                  <span className="text-sm text-gray-500">Few matches</span>
                </div>
              </div>
            </div>

            {/* Preview/Test Section */}
            <div className="bg-white rounded-xl shadow-sm p-6 border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-900 mb-6">
                Test Configuration
              </h2>

              <div className="space-y-4">
                {/* File Upload */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Upload Sample Data
                  </label>
                  <div className="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center">
                    <input
                      type="file"
                      accept=".json,.csv"
                      onChange={handleFileUpload}
                      className="hidden"
                      id="file-upload"
                    />
                    <label
                      htmlFor="file-upload"
                      className="cursor-pointer inline-flex items-center px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors"
                    >
                      <svg className="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                      </svg>
                      Upload JSON/CSV
                    </label>
                    {sampleData.length > 0 && (
                      <p className="text-sm text-gray-600 mt-2">
                        Loaded {sampleData.length} records
                      </p>
                    )}
                  </div>
                </div>

                {/* Run Preview Button */}
                <button
                  onClick={runPreview}
                  disabled={isProcessing || sampleData.length === 0 || config.keys.length === 0}
                  className={`w-full py-3 rounded-lg font-medium transition-colors ${isProcessing || sampleData.length === 0 || config.keys.length === 0
                      ? 'bg-gray-200 text-gray-500 cursor-not-allowed'
                      : 'bg-purple-600 text-white hover:bg-purple-700'
                    }`}
                >
                  {isProcessing ? (
                    <span className="flex items-center justify-center">
                      <svg className="animate-spin h-5 w-5 mr-3" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                      </svg>
                      Processing...
                    </span>
                  ) : (
                    'Run Preview'
                  )}
                </button>

                {/* Preview Results */}
                {previewData.length > 0 && (
                  <div className="mt-6">
                    <h3 className="font-medium text-gray-900 mb-4">
                      Preview Groups ({previewData.length} found)
                    </h3>
                    <div className="space-y-4 max-h-96 overflow-y-auto">
                      {previewData.map(group => (
                        <div
                          key={group.groupId}
                          className="border border-gray-200 rounded-lg p-4"
                        >
                          <div className="flex items-center justify-between mb-3">
                            <div className="flex items-center">
                              <div
                                className="w-3 h-3 rounded-full mr-2"
                                style={{ backgroundColor: getSimilarityColor(group.similarity) }}
                              />
                              <span className="font-medium text-gray-900">
                                Group #{group.groupId}
                              </span>
                            </div>
                            <span className="text-sm font-medium" style={{ color: getSimilarityColor(group.similarity) }}>
                              {(group.similarity * 100).toFixed(1)}%
                            </span>
                          </div>

                          <div className="space-y-2">
                            {group.records.map((record, idx) => (
                              <div
                                key={record.id}
                                className="bg-gray-50 rounded p-3 text-sm"
                              >
                                <div className="font-medium text-gray-700 mb-1">
                                  Record {idx + 1}
                                </div>
                                {config.keys.map(key => (
                                  <div key={key.name} className="text-gray-600">
                                    <span className="font-medium">{key.name}:</span>{' '}
                                    {record.data[key.name] || '(empty)'}
                                  </div>
                                ))}
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Configuration Summary */}
            <div className="bg-gradient-to-r from-purple-50 to-pink-50 rounded-xl shadow-sm p-6 border border-purple-200">
              <h2 className="text-xl font-semibold text-gray-900 mb-4">
                Configuration Summary
              </h2>
              <div className="space-y-3">
                <div className="flex justify-between">
                  <span className="text-gray-600">Algorithm</span>
                  <span className="font-medium text-gray-900">
                    {ALGORITHM_INFO[config.algorithm].name}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Threshold</span>
                  <span className="font-medium text-gray-900">
                    {config.threshold.toFixed(2)}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Matching Keys</span>
                  <span className="font-medium text-gray-900">
                    {config.keys.length}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-600">Blocking Fields</span>
                  <span className="font-medium text-gray-900">
                    {config.blockingFields?.length || 0}
                  </span>
                </div>
                {config.maxGroupSize && (
                  <div className="flex justify-between">
                    <span className="text-gray-600">Max Group Size</span>
                    <span className="font-medium text-gray-900">
                      {config.maxGroupSize}
                    </span>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default MatchGroupConfigEditor;