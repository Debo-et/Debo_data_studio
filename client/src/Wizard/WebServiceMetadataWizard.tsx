// WebServiceMetadataWizard.tsx
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  AlertCircle,
  Server,
  Key,
  Code,
  Plus,
  Trash2
} from 'lucide-react';
import {
  WebServiceMetadataFormData,
  WebServiceMetadataWizardProps,
  WebServiceEndpoint,
  WebServiceParameter
} from '../types/types';

const WebServiceMetadataWizard: React.FC<WebServiceMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<WebServiceMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    serviceType: 'REST',
    baseUrl: '',
    authenticationType: 'none',
    endpoints: [],
    headers: [],
    rateLimit: {
      requests: 100,
      period: 'hour'
    },
    version: '1.0',
    totalEndpoints: 0,
    validationMode: 'strict'
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeEndpointIndex, setActiveEndpointIndex] = useState<number | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 5;

  // Parse OpenAPI/Swagger file
  const parseOpenAPIFile = (content: string): Partial<WebServiceMetadataFormData> => {
    try {
      const spec = JSON.parse(content);
      const endpoints: WebServiceEndpoint[] = [];
      
      // Extract endpoints from OpenAPI spec
      if (spec.paths) {
        Object.entries(spec.paths).forEach(([path, pathObj]: [string, any]) => {
          Object.entries(pathObj).forEach(([method, methodObj]: [string, any]) => {
            if (['get', 'post', 'put', 'delete', 'patch'].includes(method.toLowerCase())) {
              const endpoint: WebServiceEndpoint = {
                id: `${method.toUpperCase()}-${path}`,
                path,
                method: method.toUpperCase(),
                description: methodObj.description || methodObj.summary || '',
                parameters: [],
                responseType: 'application/json',
                requiresAuth: !!methodObj.security,
                sampleRequest: '',
                sampleResponse: ''
              };

              // Extract parameters
              if (methodObj.parameters) {
                endpoint.parameters = methodObj.parameters.map((param: any) => ({
                  name: param.name,
                  type: param.in || 'query',
                  dataType: param.schema?.type || 'string',
                  required: param.required || false,
                  location: param.in || 'query',
                  description: param.description || '',
                  exampleValue: param.example || param.schema?.example || ''
                }));
              }

              endpoints.push(endpoint);
            }
          });
        });
      }

      return {
        baseUrl: spec.servers?.[0]?.url || '',
        endpoints,
        totalEndpoints: endpoints.length,
        version: spec.info?.version || '1.0'
      };
    } catch (err) {
      throw new Error('Failed to parse OpenAPI specification');
    }
  };

  // Handle file selection and parsing
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const validExtensions = ['.json', '.yaml', '.yml'];
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please select a valid OpenAPI file (.json, .yaml, .yml)');
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        const parsedData = parseOpenAPIFile(content);
        
        // Update form data with parsed information
        updateFormData({
          ...parsedData,
          name: file.name.replace(/\.(json|yaml|yml)$/, ''),
          description: `Web Service from ${file.name}`
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to process OpenAPI file: ${err.message}`);
        setIsLoading(false);
      }
    };

    reader.onerror = () => {
      setError('Failed to read file. The file may be corrupted or in an unsupported format.');
      setIsLoading(false);
    };

    reader.readAsText(file);
  };

  // Update form data
  const updateFormData = (updates: Partial<WebServiceMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Endpoint management
  const addEndpoint = () => {
    const newEndpoint: WebServiceEndpoint = {
      id: `endpoint-${Date.now()}`,
      path: '/api/endpoint',
      method: 'GET',
      description: 'New API endpoint',
      parameters: [],
      responseType: 'application/json',
      requiresAuth: false
    };
    
    updateFormData({
      endpoints: [...formData.endpoints, newEndpoint],
      totalEndpoints: formData.endpoints.length + 1
    });
    setActiveEndpointIndex(formData.endpoints.length);
  };

  const updateEndpoint = (index: number, updates: Partial<WebServiceEndpoint>) => {
    const newEndpoints = [...formData.endpoints];
    newEndpoints[index] = { ...newEndpoints[index], ...updates };
    updateFormData({ endpoints: newEndpoints });
  };

  const removeEndpoint = (index: number) => {
    const newEndpoints = formData.endpoints.filter((_, i) => i !== index);
    updateFormData({
      endpoints: newEndpoints,
      totalEndpoints: newEndpoints.length
    });
    if (activeEndpointIndex === index) {
      setActiveEndpointIndex(null);
    }
  };

  // Parameter management
  const addParameter = (endpointIndex: number) => {
    const newParameter: WebServiceParameter = {
      name: 'param',
      type: 'query',
      dataType: 'string',
      required: false,
      location: 'query',
      description: 'New parameter'
    };
    
    const endpoint = formData.endpoints[endpointIndex];
    const updatedEndpoint = {
      ...endpoint,
      parameters: [...endpoint.parameters, newParameter]
    };
    
    updateEndpoint(endpointIndex, updatedEndpoint);
  };

  const updateParameter = (endpointIndex: number, paramIndex: number, updates: Partial<WebServiceParameter>) => {
    const endpoint = formData.endpoints[endpointIndex];
    const newParameters = [...endpoint.parameters];
    newParameters[paramIndex] = { ...newParameters[paramIndex], ...updates };
    updateEndpoint(endpointIndex, { parameters: newParameters });
  };

  const removeParameter = (endpointIndex: number, paramIndex: number) => {
    const endpoint = formData.endpoints[endpointIndex];
    const newParameters = endpoint.parameters.filter((_, i) => i !== paramIndex);
    updateEndpoint(endpointIndex, { parameters: newParameters });
  };

  // Header management
  const addHeader = () => {
    const newHeader = { name: 'Header-Name', value: 'header-value', required: false };
    updateFormData({ headers: [...formData.headers, newHeader] });
  };

  const updateHeader = (index: number, field: string, value: string | boolean) => {
    const newHeaders = [...formData.headers];
    newHeaders[index] = { ...newHeaders[index], [field]: value };
    updateFormData({ headers: newHeaders });
  };

  const removeHeader = (index: number) => {
    const newHeaders = formData.headers.filter((_, i) => i !== index);
    updateFormData({ headers: newHeaders });
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
      serviceType: 'REST',
      baseUrl: '',
      authenticationType: 'none',
      endpoints: [],
      headers: [],
      rateLimit: {
        requests: 100,
        period: 'hour'
      },
      version: '1.0',
      totalEndpoints: 0,
      validationMode: 'strict'
    });
    setActiveEndpointIndex(null);
    setError(null);
  };

  // Render endpoints list
  const renderEndpointsList = () => {
    if (formData.endpoints.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-6 text-center">
          <Server className="h-12 w-12 mx-auto mb-4 text-gray-400" />
          <p className="text-gray-500 dark:text-gray-400">No endpoints defined</p>
          <Button
            variant="outline"
            onClick={addEndpoint}
            className="mt-4"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add First Endpoint
          </Button>
        </div>
      );
    }

    return (
      <div className="space-y-3">
        {formData.endpoints.map((endpoint, index) => (
          <div
            key={endpoint.id}
            className={`border rounded-md p-4 cursor-pointer transition-colors ${
              activeEndpointIndex === index
                ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
                : 'border-gray-200 dark:border-gray-600 hover:border-gray-300 dark:hover:border-gray-500'
            }`}
            onClick={() => setActiveEndpointIndex(index)}
          >
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <span className={`px-2 py-1 text-xs font-medium rounded ${
                  endpoint.method === 'GET' ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' :
                  endpoint.method === 'POST' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200' :
                  endpoint.method === 'PUT' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200' :
                  endpoint.method === 'DELETE' ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200' :
                  'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
                }`}>
                  {endpoint.method}
                </span>
                <span className="font-mono text-sm text-gray-700 dark:text-gray-300">
                  {endpoint.path}
                </span>
              </div>
              <div className="flex items-center space-x-2">
                <span className="text-xs text-gray-500 dark:text-gray-400">
                  {endpoint.parameters.length} params
                </span>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={(e) => {
                    e.stopPropagation();
                    removeEndpoint(index);
                  }}
                  className="h-6 w-6 p-0 text-red-600 hover:text-red-800"
                >
                  <Trash2 className="h-3 w-3" />
                </Button>
              </div>
            </div>
            {endpoint.description && (
              <p className="text-sm text-gray-600 dark:text-gray-400 mt-2">
                {endpoint.description}
              </p>
            )}
          </div>
        ))}
        
        <Button
          variant="outline"
          onClick={addEndpoint}
          className="w-full"
        >
          <Plus className="h-4 w-4 mr-2" />
          Add Endpoint
        </Button>
      </div>
    );
  };

  // Render endpoint details
  const renderEndpointDetails = () => {
    if (activeEndpointIndex === null || !formData.endpoints[activeEndpointIndex]) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-6 text-center">
          <Code className="h-12 w-12 mx-auto mb-4 text-gray-400" />
          <p className="text-gray-500 dark:text-gray-400">Select an endpoint to edit</p>
        </div>
      );
    }

    const endpoint = formData.endpoints[activeEndpointIndex];

    return (
      <div className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              HTTP Method
            </label>
            <select
              value={endpoint.method}
              onChange={(e) => updateEndpoint(activeEndpointIndex, { method: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            >
              <option value="GET">GET</option>
              <option value="POST">POST</option>
              <option value="PUT">PUT</option>
              <option value="DELETE">DELETE</option>
              <option value="PATCH">PATCH</option>
            </select>
          </div>
          
          <div>
            <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
              Response Type
            </label>
            <select
              value={endpoint.responseType}
              onChange={(e) => updateEndpoint(activeEndpointIndex, { responseType: e.target.value })}
              className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            >
              <option value="application/json">JSON</option>
              <option value="application/xml">XML</option>
              <option value="text/plain">Text</option>
              <option value="application/octet-stream">Binary</option>
            </select>
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Endpoint Path
          </label>
          <input
            type="text"
            value={endpoint.path}
            onChange={(e) => updateEndpoint(activeEndpointIndex, { path: e.target.value })}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            placeholder="/api/endpoint"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Description
          </label>
          <textarea
            value={endpoint.description}
            onChange={(e) => updateEndpoint(activeEndpointIndex, { description: e.target.value })}
            rows={3}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
            placeholder="Endpoint description"
          />
        </div>

        <div className="flex items-center">
          <input
            type="checkbox"
            id={`auth-${activeEndpointIndex}`}
            checked={endpoint.requiresAuth}
            onChange={(e) => updateEndpoint(activeEndpointIndex, { requiresAuth: e.target.checked })}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor={`auth-${activeEndpointIndex}`} className="ml-2 block text-sm text-gray-700 dark:text-gray-300">
            Requires Authentication
          </label>
        </div>

        {/* Parameters Section */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <h4 className="font-medium text-gray-900 dark:text-white">Parameters</h4>
            <Button
              variant="outline"
              size="sm"
              onClick={() => addParameter(activeEndpointIndex)}
            >
              <Plus className="h-4 w-4 mr-1" />
              Add Parameter
            </Button>
          </div>

          {endpoint.parameters.length === 0 ? (
            <div className="border border-dashed border-gray-300 dark:border-gray-600 rounded-md p-4 text-center">
              <Key className="h-8 w-8 mx-auto mb-2 text-gray-400" />
              <p className="text-sm text-gray-500 dark:text-gray-400">No parameters defined</p>
            </div>
          ) : (
            <div className="space-y-2">
              {endpoint.parameters.map((param, paramIndex) => (
                <div key={paramIndex} className="border border-gray-200 dark:border-gray-600 rounded-md p-3">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center space-x-2">
                      <span className="font-medium text-gray-900 dark:text-white">{param.name}</span>
                      <span className={`text-xs px-2 py-1 rounded ${
                        param.required 
                          ? 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200'
                          : 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
                      }`}>
                        {param.required ? 'Required' : 'Optional'}
                      </span>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeParameter(activeEndpointIndex, paramIndex)}
                      className="h-6 w-6 p-0 text-red-600 hover:text-red-800"
                    >
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  </div>
                  
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <span className="text-gray-500 dark:text-gray-400">Type:</span>
                      <select
                        value={param.location}
                        onChange={(e) => updateParameter(activeEndpointIndex, paramIndex, { location: e.target.value })}
                        className="ml-2 px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      >
                        <option value="query">Query</option>
                        <option value="path">Path</option>
                        <option value="body">Body</option>
                        <option value="header">Header</option>
                      </select>
                    </div>
                    <div>
                      <span className="text-gray-500 dark:text-gray-400">Data Type:</span>
                      <select
                        value={param.dataType}
                        onChange={(e) => updateParameter(activeEndpointIndex, paramIndex, { dataType: e.target.value })}
                        className="ml-2 px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      >
                        <option value="string">String</option>
                        <option value="number">Number</option>
                        <option value="integer">Integer</option>
                        <option value="boolean">Boolean</option>
                        <option value="array">Array</option>
                        <option value="object">Object</option>
                      </select>
                    </div>
                  </div>
                  
                  <div className="mt-2">
                    <input
                      type="text"
                      value={param.description}
                      onChange={(e) => updateParameter(activeEndpointIndex, paramIndex, { description: e.target.value })}
                      placeholder="Parameter description"
                      className="w-full px-2 py-1 text-sm border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    />
                  </div>
                </div>
              ))}
            </div>
          )}
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
              Web Service Metadata Wizard
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
                Define the basic properties of your web service including name, type, and base URL.
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
                    placeholder="Enter web service name"
                  />
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Service Type
                    </label>
                    <select
                      value={formData.serviceType}
                      onChange={(e) => updateFormData({ serviceType: e.target.value as 'REST' | 'SOAP' | 'GraphQL' })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="REST">REST API</option>
                      <option value="SOAP">SOAP Web Service</option>
                      <option value="GraphQL">GraphQL API</option>
                    </select>
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Authentication
                    </label>
                    <select
                      value={formData.authenticationType}
                      onChange={(e) => updateFormData({ authenticationType: e.target.value as any })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="none">No Authentication</option>
                      <option value="basic">Basic Auth</option>
                      <option value="bearer">Bearer Token</option>
                      <option value="apiKey">API Key</option>
                      <option value="oauth2">OAuth 2.0</option>
                    </select>
                  </div>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Base URL
                  </label>
                  <input
                    type="text"
                    value={formData.baseUrl}
                    onChange={(e) => updateFormData({ baseUrl: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="https://api.example.com"
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
                    placeholder="Enter web service description"
                  />
                </div>
              </div>
            </div>
          )}

          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Import Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Import an OpenAPI/Swagger specification file or manually configure endpoints.
              </p>
              
              <div className="space-y-4">
                {/* File Import */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Import OpenAPI Specification
                  </label>
                  <div className="flex space-x-2">
                    <input
                      type="text"
                      value={formData.endpoints.length > 0 ? `${formData.endpoints.length} endpoints loaded` : 'No file selected'}
                      readOnly
                      className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white bg-gray-50 dark:bg-gray-600"
                    />
                    <input
                      type="file"
                      ref={fileInputRef}
                      onChange={handleFileSelect}
                      accept=".json,.yaml,.yml"
                      className="hidden"
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
                      {isLoading ? 'Parsing...' : 'Browse'}
                    </Button>
                  </div>
                </div>

                {error && (
                  <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                    <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                    <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                  </div>
                )}

                {formData.endpoints.length > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <Server className="h-4 w-4" />
                      <span className="font-medium">OpenAPI specification loaded successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Endpoints: <strong>{formData.endpoints.length}</strong> | 
                      Base URL: <strong>{formData.baseUrl || 'Not specified'}</strong>
                    </p>
                  </div>
                )}

                {/* Manual Configuration */}
                <div className="border-t border-gray-200 dark:border-gray-600 pt-4">
                  <h4 className="font-medium text-gray-900 dark:text-white mb-3">Manual Configuration</h4>
                  <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                    You can manually add and configure endpoints if you prefer not to import a specification file.
                  </p>
                  {renderEndpointsList()}
                </div>
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Endpoint Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Configure individual endpoints, their methods, paths, and parameters.
              </p>
              
              {formData.endpoints.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Server className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No endpoints defined. Please go back to Step 2 and add endpoints.</p>
                </div>
              ) : (
                <div className="grid grid-cols-3 gap-6">
                  <div className="col-span-1">
                    <h4 className="font-medium text-gray-900 dark:text-white mb-3">Endpoints</h4>
                    {renderEndpointsList()}
                  </div>
                  <div className="col-span-2">
                    <h4 className="font-medium text-gray-900 dark:text-white mb-3">Endpoint Details</h4>
                    {renderEndpointDetails()}
                  </div>
                </div>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Headers & Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Configure global headers, rate limiting, and other service-wide settings.
              </p>
              
              <div className="space-y-6">
                {/* Headers Configuration */}
                <div>
                  <div className="flex items-center justify-between mb-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Global Headers</h4>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={addHeader}
                    >
                      <Plus className="h-4 w-4 mr-1" />
                      Add Header
                    </Button>
                  </div>

                  {formData.headers.length === 0 ? (
                    <div className="border border-dashed border-gray-300 dark:border-gray-600 rounded-md p-4 text-center">
                      <Code className="h-8 w-8 mx-auto mb-2 text-gray-400" />
                      <p className="text-sm text-gray-500 dark:text-gray-400">No global headers defined</p>
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {formData.headers.map((header, index) => (
                        <div key={index} className="border border-gray-200 dark:border-gray-600 rounded-md p-3">
                          <div className="flex items-center justify-between mb-2">
                            <div className="flex items-center space-x-3">
                              <input
                                type="text"
                                value={header.name}
                                onChange={(e) => updateHeader(index, 'name', e.target.value)}
                                className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                                placeholder="Header name"
                              />
                              <input
                                type="text"
                                value={header.value}
                                onChange={(e) => updateHeader(index, 'value', e.target.value)}
                                className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                                placeholder="Header value"
                              />
                            </div>
                            <div className="flex items-center space-x-2">
                              <div className="flex items-center">
                                <input
                                  type="checkbox"
                                  id={`header-required-${index}`}
                                  checked={header.required}
                                  onChange={(e) => updateHeader(index, 'required', e.target.checked)}
                                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                                />
                                <label htmlFor={`header-required-${index}`} className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                                  Required
                                </label>
                              </div>
                              <Button
                                variant="ghost"
                                size="sm"
                                onClick={() => removeHeader(index)}
                                className="h-6 w-6 p-0 text-red-600 hover:text-red-800"
                              >
                                <Trash2 className="h-3 w-3" />
                              </Button>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                {/* Rate Limiting */}
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Rate Limit (requests)
                    </label>
                    <input
                      type="number"
                      value={formData.rateLimit.requests}
                      onChange={(e) => updateFormData({ 
                        rateLimit: { ...formData.rateLimit, requests: parseInt(e.target.value) || 0 }
                      })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Rate Limit Period
                    </label>
                    <select
                      value={formData.rateLimit.period}
                      onChange={(e) => updateFormData({ 
                        rateLimit: { ...formData.rateLimit, period: e.target.value }
                      })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="second">Per Second</option>
                      <option value="minute">Per Minute</option>
                      <option value="hour">Per Hour</option>
                      <option value="day">Per Day</option>
                    </select>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Version
                    </label>
                    <input
                      type="text"
                      value={formData.version}
                      onChange={(e) => updateFormData({ version: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      placeholder="1.0.0"
                    />
                  </div>
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
                      <option value="custom">Custom Rules</option>
                    </select>
                  </div>
                </div>
              </div>
            </div>
          )}

          {currentStep === 5 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your web service configuration and click Finish to save it to the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your web service configuration is complete and ready to be saved to the repository.
                </p>
              </div>
              
              <div className="grid grid-cols-2 gap-6">
                <div className="space-y-4">
                  <h4 className="font-medium text-gray-900 dark:text-white">Service Summary</h4>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Name:</span>
                      <span className="text-gray-900 dark:text-white font-medium">{formData.name || 'Not specified'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Service Type:</span>
                      <span className="text-gray-900 dark:text-white">{formData.serviceType}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Base URL:</span>
                      <span className="text-gray-900 dark:text-white">{formData.baseUrl || 'Not specified'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Authentication:</span>
                      <span className="text-gray-900 dark:text-white">{formData.authenticationType}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Version:</span>
                      <span className="text-gray-900 dark:text-white">{formData.version}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Endpoints:</span>
                      <span className="text-gray-900 dark:text-white">{formData.endpoints.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Rate Limit:</span>
                      <span className="text-gray-900 dark:text-white">{formData.rateLimit.requests} req/{formData.rateLimit.period}</span>
                    </div>
                  </div>
                </div>

                {formData.endpoints.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Endpoints Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.endpoints.slice(0, 6).map((endpoint, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <div className="flex items-center space-x-2">
                              <span className={`text-xs px-1 py-0.5 rounded ${
                                endpoint.method === 'GET' ? 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200' :
                                endpoint.method === 'POST' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200' :
                                'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200'
                              }`}>
                                {endpoint.method}
                              </span>
                              <span className="text-gray-600 dark:text-gray-400 font-mono text-xs">
                                {endpoint.path.length > 20 ? endpoint.path.substring(0, 20) + '...' : endpoint.path}
                              </span>
                            </div>
                            <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded">
                              {endpoint.parameters.length} params
                            </span>
                          </div>
                        ))}
                        {formData.endpoints.length > 6 && (
                          <div className="text-center text-gray-500 dark:text-gray-400 text-xs">
                            ... and {formData.endpoints.length - 6} more endpoints
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
                disabled={currentStep === 1 && (!formData.name || !formData.baseUrl)}
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

export default WebServiceMetadataWizard;