// LDIFMetadataWizard.tsx
import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  FolderTree,
  AlertCircle,
  Users,
  Key} from 'lucide-react';
import {
  LDIFMetadataFormData,
  LDIFMetadataWizardProps,
  LDIFEntry,
  LDIFAttribute,
  LDIFSchemaDefinition
} from '../types/types';

const LDIFMetadataWizard: React.FC<LDIFMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<LDIFMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    encoding: 'UTF-8',
    baseDN: '',
    entries: [],
    schema: [],
    totalEntries: 0,
    totalAttributes: 0,
    validationMode: 'strict'
  });

  const [, setLdifContent] = useState<string>('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 5;

  // In the parseLDIF function, update the currentAttribute initialization and usage:

const parseLDIF = (content: string): { entries: LDIFEntry[], baseDN?: string } => {
  const entries: LDIFEntry[] = [];
  const lines = content.split('\n');
  let currentEntry: Partial<LDIFEntry> = {};
  // Initialize currentAttribute with values array
  let currentAttribute: Partial<LDIFAttribute> & { values: string[] } = { values: [] };
  let entryIndex = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    // Skip empty lines and comments
    if (!line || line.startsWith('#')) continue;

    // Check for DN line (start of new entry)
    if (line.toLowerCase().startsWith('dn:')) {
      // Save previous entry if exists
      if (currentEntry.dn && currentEntry.attributes) {
        // Save the last attribute if it exists
        if (currentAttribute.name && currentAttribute.values.length > 0) {
          currentEntry.attributes.push({
            name: currentAttribute.name,
            values: currentAttribute.values,
            type: inferAttributeType(currentAttribute.name, currentAttribute.values[0]),
            isMultiValued: currentAttribute.values.length > 1,
            isBinary: isBinaryData(currentAttribute.values[0])
          });
          // Reset current attribute
          currentAttribute = { values: [] };
        }

        entries.push({
          dn: currentEntry.dn,
          objectClasses: currentEntry.objectClasses || [],
          attributes: currentEntry.attributes,
          entryIndex: entryIndex++
        });
      }

      // Start new entry
      const dn = line.substring(3).trim();
      currentEntry = {
        dn,
        objectClasses: [],
        attributes: []
      };
      // Reset current attribute for new entry
      currentAttribute = { values: [] };
      continue;
    }

    // Handle attribute lines
    if (line.includes(':')) {
      const colonIndex = line.indexOf(':');
      const attributeName = line.substring(0, colonIndex).trim();
      let attributeValue = line.substring(colonIndex + 1).trim();

      // Handle folded lines (lines starting with space)
      if (attributeValue === '' && i + 1 < lines.length) {
        let nextLine = lines[i + 1].trim();
        while (nextLine.startsWith(' ')) {
          attributeValue += nextLine.trim();
          i++;
          if (i + 1 >= lines.length) break;
          nextLine = lines[i + 1].trim();
        }
      }

      // Handle objectClass attribute specially
      if (attributeName.toLowerCase() === 'objectclass') {
        if (!currentEntry.objectClasses) currentEntry.objectClasses = [];
        currentEntry.objectClasses.push(attributeValue);
        continue;
      }

      // Check if this is a continuation of the same attribute
      if (currentAttribute.name === attributeName) {
        // Add to existing multi-valued attribute
        currentAttribute.values.push(attributeValue);
        currentAttribute.isMultiValued = true;
      } else {
        // Save previous attribute if exists
        if (currentAttribute.name && currentAttribute.values.length > 0) {
          currentEntry.attributes?.push({
            name: currentAttribute.name,
            values: currentAttribute.values,
            type: inferAttributeType(currentAttribute.name, currentAttribute.values[0]),
            isMultiValued: currentAttribute.values.length > 1,
            isBinary: isBinaryData(currentAttribute.values[0])
          });
        }

        // Start new attribute
        currentAttribute = {
          name: attributeName,
          values: [attributeValue],
          isMultiValued: false,
          isBinary: isBinaryData(attributeValue)
        };
      }
    }
  }

  // Save the last entry
  if (currentEntry.dn && currentEntry.attributes) {
    // Save the last attribute if it exists
    if (currentAttribute.name && currentAttribute.values.length > 0) {
      currentEntry.attributes.push({
        name: currentAttribute.name,
        values: currentAttribute.values,
        type: inferAttributeType(currentAttribute.name, currentAttribute.values[0]),
        isMultiValued: currentAttribute.values.length > 1,
        isBinary: currentAttribute.isBinary || false
      });
    }

    entries.push({
      dn: currentEntry.dn,
      objectClasses: currentEntry.objectClasses || [],
      attributes: currentEntry.attributes,
      entryIndex: entryIndex
    });
  }

  // Extract base DN from first entry if available
  const baseDN = entries.length > 0 ? extractBaseDN(entries[0].dn) : undefined;

  return { entries, baseDN };
};

  // Extract base DN from full DN
  const extractBaseDN = (dn: string): string => {
    const parts = dn.split(',');
    // Return the last 2-3 components as base DN
    return parts.slice(-2).join(',');
  };

  // Infer attribute type based on name and sample value
  const inferAttributeType = (name: string, value: string): string => {
    const nameLower = name.toLowerCase();
    
    // Common LDAP attribute type mappings
    if (nameLower.includes('uid') || nameLower.includes('cn')) return 'Distinguished Name';
    if (nameLower.includes('mail')) return 'Email';
    if (nameLower.includes('telephone') || nameLower.includes('phone')) return 'Telephone';
    if (nameLower.includes('password') || nameLower.includes('userpassword')) return 'Password';
    if (nameLower.includes('objectclass')) return 'Object Class';
    if (nameLower.includes('timestamp') || nameLower.includes('modifytimestamp')) return 'Timestamp';
    if (nameLower.includes('jpegphoto') || nameLower.includes('photo')) return 'Binary';
    if (nameLower.includes('count') || nameLower.includes('gidnumber')) return 'Integer';
    
    // Infer from value
    if (!value) return 'String';
    
    if (value.match(/^\d+$/)) return 'Integer';
    if (value.match(/^\d+\.\d+$/)) return 'Decimal';
    if (value.match(/^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/)) return 'Email';
    if (value.match(/^[a-fA-F0-9]+$/) && value.length > 16) return 'Binary Hash';
    
    return 'String';
  };

  // Check if data appears to be binary
  const isBinaryData = (value: string): boolean => {
    // Simple heuristic: if it contains many non-printable characters or is base64-like
    const nonPrintableRatio = (value.match(/[\x00-\x1F\x7F-\x9F]/g) || []).length / value.length;
    return nonPrintableRatio > 0.3 || value.length > 1000;
  };

  // Generate schema from entries
  const generateSchema = (entries: LDIFEntry[]): LDIFSchemaDefinition[] => {
    const schemaMap = new Map<string, LDIFSchemaDefinition>();
    
    entries.forEach(entry => {
      entry.attributes.forEach(attr => {
        if (!schemaMap.has(attr.name)) {
          schemaMap.set(attr.name, {
            name: attr.name,
            type: attr.type,
            required: false, // Would need more analysis to determine
            multiValued: attr.isMultiValued,
            description: `LDAP attribute: ${attr.name}`
          });
        }
      });
    });
    
    return Array.from(schemaMap.values());
  };

  // Handle file selection and parsing
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const validExtensions = ['.ldif', '.ldf'];
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please select a valid LDIF file (.ldif or .ldf)');
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        setLdifContent(content);
        
        // Parse LDIF and extract metadata
        const { entries, baseDN } = parseLDIF(content);
        const schema = generateSchema(entries);
        
        const totalAttributes = entries.reduce((count, entry) => count + entry.attributes.length, 0);
        
        // Update form data
        updateFormData({
          file,
          filePath: file.name,
          baseDN: baseDN || '',
          entries,
          schema,
          totalEntries: entries.length,
          totalAttributes,
          encoding: 'UTF-8' // LDIF is typically UTF-8
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to process LDIF file: ${err.message}`);
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
  const updateFormData = (updates: Partial<LDIFMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
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
      baseDN: '',
      entries: [],
      schema: [],
      totalEntries: 0,
      totalAttributes: 0,
      validationMode: 'strict'
    });
    setLdifContent('');
    setError(null);
  };

  // Render entries table
  const renderEntriesTable = () => {
    if (!formData.entries || formData.entries.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <Users className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No LDIF entries found</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            LDIF Entries ({formData.entries.length} found)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  DN
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Object Classes
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Attributes
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.entries.slice(0, 10).map((entry, index) => (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2">
                    <div className="max-w-xs truncate text-gray-600 dark:text-gray-300" title={entry.dn}>
                      {entry.dn}
                    </div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-wrap gap-1">
                      {entry.objectClasses.slice(0, 3).map((oc, idx) => (
                        <span key={idx} className="text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 px-2 py-1 rounded">
                          {oc}
                        </span>
                      ))}
                      {entry.objectClasses.length > 3 && (
                        <span className="text-xs text-gray-500">+{entry.objectClasses.length - 3} more</span>
                      )}
                    </div>
                  </td>
                  <td className="px-3 py-2 text-gray-600 dark:text-gray-300">
                    {entry.attributes.length} attributes
                  </td>
                </tr>
              ))}
              {formData.entries.length > 10 && (
                <tr>
                  <td colSpan={3} className="px-3 py-2 text-center text-gray-500 dark:text-gray-400 text-sm">
                    ... and {formData.entries.length - 10} more entries
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  // Render schema table
  const renderSchemaTable = () => {
    if (!formData.schema || formData.schema.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <Key className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No schema generated</p>
          </div>
        </div>
      );
    }

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Inferred Schema ({formData.schema.length} attributes)
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Attribute Name
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Type
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Multi-valued
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300">
                  Required
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {formData.schema.map((attr, index) => (
                <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  <td className="px-3 py-2 font-medium text-gray-700 dark:text-gray-300">
                    {attr.name}
                  </td>
                  <td className="px-3 py-2">
                    <span className="text-xs bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200 px-2 py-1 rounded">
                      {attr.type}
                    </span>
                  </td>
                  <td className="px-3 py-2">
                    {attr.multiValued ? (
                      <span className="text-xs bg-yellow-100 dark:bg-yellow-900 text-yellow-800 dark:text-yellow-200 px-2 py-1 rounded">
                        Yes
                      </span>
                    ) : (
                      <span className="text-xs bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300 px-2 py-1 rounded">
                        No
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-2">
                    {attr.required ? (
                      <span className="text-xs bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-200 px-2 py-1 rounded">
                        Required
                      </span>
                    ) : (
                      <span className="text-xs bg-gray-100 dark:bg-gray-600 text-gray-700 dark:text-gray-300 px-2 py-1 rounded">
                        Optional
                      </span>
                    )}
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
              LDIF Metadata Wizard
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
                Enter a Name for the LDIF metadata entry. Optionally, add a Purpose and Description to clarify its use.
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
                    placeholder="Enter LDIF metadata name"
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
                Click Browse to locate and select your LDIF file. The application will automatically parse the file and extract its structure.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept=".ldif,.ldf"
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    LDIF File *
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

                {formData.totalEntries > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <Users className="h-4 w-4" />
                      <span className="font-medium">LDIF parsed successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Entries: <strong>{formData.totalEntries}</strong> | 
                      Attributes: <strong>{formData.totalAttributes}</strong> | 
                      Base DN: <strong>{formData.baseDN || 'Not detected'}</strong>
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                LDIF Structure Analysis
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review the extracted LDIF structure, including directory entries and their attributes.
              </p>
              
              {!formData.totalEntries ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <FolderTree className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No LDIF file loaded. Please go back to Step 2 and select a file.</p>
                </div>
              ) : (
                <div className="space-y-6">
                  <div>
                    <h4 className="font-medium text-gray-900 dark:text-white mb-2">Directory Entries</h4>
                    {renderEntriesTable()}
                  </div>
                  
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Base DN
                    </label>
                    <input
                      type="text"
                      value={formData.baseDN}
                      onChange={(e) => updateFormData({ baseDN: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      placeholder="Enter base DN"
                    />
                  </div>
                </div>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Schema Definition
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review the inferred schema from the LDIF file. You can modify attribute types and properties as needed.
              </p>
              
              {formData.schema.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Key className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No schema available. Please complete previous steps.</p>
                </div>
              ) : (
                <div className="space-y-4">
                  {renderSchemaTable()}
                  
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
              )}
            </div>
          )}

          {currentStep === 5 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your LDIF metadata configuration and click Finish to save it to the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your LDIF metadata configuration is complete and ready to be saved to the repository.
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
                      <span className="text-gray-500 dark:text-gray-400">Base DN:</span>
                      <span className="text-gray-900 dark:text-white">{formData.baseDN || 'Not specified'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Encoding:</span>
                      <span className="text-gray-900 dark:text-white">{formData.encoding}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Entries:</span>
                      <span className="text-gray-900 dark:text-white">{formData.totalEntries}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Attributes:</span>
                      <span className="text-gray-900 dark:text-white">{formData.totalAttributes}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Validation Mode:</span>
                      <span className="text-gray-900 dark:text-white">{formData.validationMode}</span>
                    </div>
                  </div>
                </div>

                {formData.schema.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Schema Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.schema.slice(0, 8).map((attr, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <span className="text-gray-600 dark:text-gray-400 font-medium">{attr.name}</span>
                            <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded">
                              {attr.type}
                            </span>
                          </div>
                        ))}
                        {formData.schema.length > 8 && (
                          <div className="text-center text-gray-500 dark:text-gray-400 text-xs">
                            ... and {formData.schema.length - 8} more attributes
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
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && !formData.totalEntries)}
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

export default LDIFMetadataWizard;