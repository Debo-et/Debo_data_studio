// XMLMetadataWizard.tsx
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  Code,
  AlertCircle,
  FolderTree
} from 'lucide-react';
import {
  XMLMetadataFormData,
  XMLMetadataWizardProps,
  XMLElement
} from '../types/types';

const XMLMetadataWizard: React.FC<XMLMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<XMLMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    rootElement: '',
    namespace: '',
    schemaType: 'inferred',
    elements: [],
    attributes: [],
    structure: []
  });

  const [, setXmlContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [, setParsedStructure] = useState<any>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 5;

  // Parse XML file and extract structure
  const parseXML = (xmlString: string): any => {
    try {
      const parser = new DOMParser();
      const xmlDoc = parser.parseFromString(xmlString, 'text/xml');
      
      // Check for parsing errors
      const parseError = xmlDoc.getElementsByTagName('parsererror')[0];
      if (parseError) {
        throw new Error('XML parsing error: ' + parseError.textContent);
      }

      return xmlDoc.documentElement;
    } catch (err: any) {
      throw new Error(`Failed to parse XML: ${err.message}`);
    }
  };

  // Extract elements, attributes, and structure from XML
  const extractXMLMetadata = (rootElement: Element) => {
    const elements: XMLElement[] = [];
    const attributes: Array<{ name: string; value: string; element: string }> = [];
    const structure: any[] = [];

    const extractFromNode = (node: Element, level: number = 0, path: string = ''): any => {
      const currentPath = path ? `${path}.${node.nodeName}` : node.nodeName;
      
      // Extract element information
      const element: XMLElement = {
        name: node.nodeName,
        type: node.childElementCount > 0 ? 'complex' : 'simple',
        path: currentPath,
        level,
        hasChildren: node.childElementCount > 0,
        hasAttributes: node.attributes.length > 0,
        sampleData: node.textContent?.trim() || ''
      };

      elements.push(element);

      // Extract attributes
      Array.from(node.attributes).forEach(attr => {
        attributes.push({
          name: attr.name,
          value: attr.value,
          element: node.nodeName
        });
      });

      // Build structure object
      const nodeStructure = {
        name: node.nodeName,
        type: node.childElementCount > 0 ? 'complex' : 'simple',
        path: currentPath,
        level,
        attributes: Array.from(node.attributes).map(attr => ({
          name: attr.name,
          value: attr.value
        })),
        children: [] as any[]
      };

      // Recursively process child elements
      Array.from(node.children).forEach(child => {
        if (child.nodeType === Node.ELEMENT_NODE) {
          const childStructure = extractFromNode(child as Element, level + 1, currentPath);
          nodeStructure.children.push(childStructure);
        }
      });

      structure.push(nodeStructure);
      return nodeStructure;
    };

    extractFromNode(rootElement);

    return { elements, attributes, structure, rootElement: rootElement.nodeName };
  };

  // Handle file selection and parsing
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const validExtensions = ['.xml'];
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please select a valid XML file (.xml)');
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        setXmlContent(content);
        
        // Parse XML and extract metadata
        const rootElement = parseXML(content);
        const metadata = extractXMLMetadata(rootElement);
        
        setParsedStructure(metadata);
        
        // Update form data
        updateFormData({
          file,
          filePath: file.name,
          rootElement: metadata.rootElement,
          elements: metadata.elements,
          attributes: metadata.attributes,
          structure: metadata.structure
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to process XML file: ${err.message}`);
        setIsLoading(false);
      }
    };

    reader.onerror = () => {
      setError('Failed to read file. The file may be corrupted or in an unsupported format.');
      setIsLoading(false);
    };

    reader.readAsText(file);
  };

  // Infer data type from sample data
  const inferDataType = (value: string): string => {
    if (!value) return 'String';
    
    // Check for boolean
    if (['true', 'false', 'yes', 'no', '1', '0'].includes(value.toLowerCase())) {
      return 'Boolean';
    }
    
    // Check for number
    if (!isNaN(Number(value)) && value.trim() !== '') {
      return Number.isInteger(Number(value)) ? 'Integer' : 'Decimal';
    }
    
    // Check for date
    const date = new Date(value);
    if (!isNaN(date.getTime())) {
      return 'Date';
    }
    
    return 'String';
  };

  // Update form data
  const updateFormData = (updates: Partial<XMLMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Update element field
  const updateElement = (index: number, field: string, value: string) => {
    const newElements = [...formData.elements];
    newElements[index] = { ...newElements[index], [field]: value };
    updateFormData({ elements: newElements });
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
      rootElement: '',
      namespace: '',
      schemaType: 'inferred',
      elements: [],
      attributes: [],
      structure: []
    });
    setXmlContent('');
    setParsedStructure(null);
    setError(null);
  };

  // Data types for dropdown
  const dataTypes = ['String', 'Integer', 'Decimal', 'Date', 'Boolean', 'Complex'];

  // Render XML structure tree
  const renderStructureTree = () => {
    if (!formData.structure || formData.structure.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <FolderTree className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No XML structure available</p>
          </div>
        </div>
      );
    }

    const renderNode = (node: any, level: number = 0) => {
      const paddingLeft = level * 20;
      
      return (
        <div key={node.path} className="mb-1">
          <div 
            className="flex items-center space-x-2 py-1 hover:bg-gray-50 dark:hover:bg-gray-700 rounded"
            style={{ paddingLeft: `${paddingLeft}px` }}
          >
            <FolderTree className="h-3 w-3 text-blue-500" />
            <span className="font-medium text-gray-700 dark:text-gray-300">{node.name}</span>
            <span className="text-xs text-gray-500 bg-gray-100 dark:bg-gray-600 px-2 py-1 rounded">
              {node.type}
            </span>
            {node.attributes.length > 0 && (
              <span className="text-xs text-orange-600 dark:text-orange-400">
                {node.attributes.length} attr
              </span>
            )}
          </div>
          {node.children.map((child: any) => renderNode(child, level + 1))}
        </div>
      );
    };

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4 max-h-64 overflow-y-auto">
        <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
          XML Structure Tree
        </div>
        {formData.structure.map(node => renderNode(node))}
      </div>
    );
  };

  // Render attributes table
  const renderAttributesTable = () => {
    if (!formData.attributes || formData.attributes.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <p>No attributes found</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Attributes ({formData.attributes.length} found)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Element
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Attribute Name
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Value
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Inferred Type
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.attributes.map((attr, index) => (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 text-gray-600 dark:text-gray-300">
                    {attr.element}
                  </td>
                  <td className="px-3 py-2 text-gray-600 dark:text-gray-300">
                    @{attr.name}
                  </td>
                  <td className="px-3 py-2 text-gray-600 dark:text-gray-300">
                    {attr.value}
                  </td>
                  <td className="px-3 py-2">
                    <span className="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                      {inferDataType(attr.value)}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
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
              XML Metadata Wizard
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
                Enter a Name for the metadata entry. Optionally, add a Purpose and Description to clarify its use.
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
              </div>
            </div>
          )}

          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                File Selection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Click Browse to locate and select your XML file. The application will automatically parse the file and extract its structure.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept=".xml"
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    XML File *
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
                      {isLoading ? 'Parsing...' : 'Browse'}
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

                {formData.rootElement && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <Code className="h-4 w-4" />
                      <span className="font-medium">XML parsed successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Root element: <strong>{formData.rootElement}</strong> | 
                      Elements: <strong>{formData.elements.length}</strong> | 
                      Attributes: <strong>{formData.attributes.length}</strong>
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                XML Structure Analysis
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review the extracted XML structure, including elements, attributes, and hierarchical relationships.
              </p>
              
              {!formData.rootElement ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Code className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No XML file loaded. Please go back to Step 2 and select a file.</p>
                </div>
              ) : (
                <div className="space-y-6">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Structure Overview</h4>
                    {renderStructureTree()}
                  </div>
                  
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Attributes</h4>
                    {renderAttributesTable()}
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Schema Type
                    </label>
                    <select
                      value={formData.schemaType}
                      onChange={(e) => updateFormData({ schemaType: e.target.value as 'inferred' | 'custom' })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="inferred">Inferred from XML</option>
                      <option value="custom">Custom Schema</option>
                    </select>
                  </div>
                </div>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Define Element Schema
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review and configure the data types for each XML element. The types have been inferred from the sample data.
              </p>
              
              {formData.elements.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Code className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No elements available. Please complete previous steps.</p>
                </div>
              ) : (
                <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 dark:bg-gray-700">
                      <tr>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Element Name</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Path</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Type</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Level</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Sample Data</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                      {formData.elements.map((element, index) => (
                        <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                          <td className="px-4 py-3">
                            <div className="flex items-center space-x-2">
                              <FolderTree className="h-3 w-3 text-blue-500" />
                              <span className="font-medium text-gray-700 dark:text-gray-300">
                                {element.name}
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-gray-600 dark:text-gray-300">
                            <code className="text-xs bg-gray-100 dark:bg-gray-600 px-2 py-1 rounded">
                              {element.path}
                            </code>
                          </td>
                          <td className="px-4 py-3">
                            <select
                              value={element.type}
                              onChange={(e) => updateElement(index, 'type', e.target.value)}
                              className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                            >
                              {dataTypes.map(type => (
                                <option key={type} value={type}>{type}</option>
                              ))}
                            </select>
                          </td>
                          <td className="px-4 py-3">
                            <span className="text-xs bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300 px-2 py-1 rounded">
                              Level {element.level}
                            </span>
                          </td>
                          <td className="px-4 py-3">
                            <div className="text-xs text-gray-500 dark:text-gray-400 max-w-xs truncate">
                              {element.sampleData || 'No data'}
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {currentStep === 5 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your XML metadata configuration and click Finish to save it to the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your XML metadata configuration is complete and ready to be saved to the repository.
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
                      <span className="text-gray-500 dark:text-gray-400">Root Element:</span>
                      <span className="text-gray-900 dark:text-white">{formData.rootElement || 'Not detected'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Schema Type:</span>
                      <span className="text-gray-900 dark:text-white">{formData.schemaType}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Elements:</span>
                      <span className="text-gray-900 dark:text-white">{formData.elements.length}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Attributes:</span>
                      <span className="text-gray-900 dark:text-white">{formData.attributes.length}</span>
                    </div>
                  </div>
                </div>

                {formData.elements.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Structure Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.elements.slice(0, 10).map((element, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <span className="text-gray-600 dark:text-gray-400 font-medium">{element.name}</span>
                            <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded">
                              {element.type}
                            </span>
                          </div>
                        ))}
                        {formData.elements.length > 10 && (
                          <div className="text-center text-gray-500 dark:text-gray-400 text-xs">
                            ... and {formData.elements.length - 10} more elements
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
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && !formData.rootElement)}
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

export default XMLMetadataWizard;