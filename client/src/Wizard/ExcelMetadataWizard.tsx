import React, { useState, useRef } from 'react';
import { motion } from 'framer-motion';
import * as XLSX from 'xlsx';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  Upload,
  CheckCircle,
  Table,
  AlertCircle
} from 'lucide-react';
import {
  ExcelMetadataFormData,
  ExcelMetadataWizardProps,
  SheetData
} from '../types/types';

const ExcelMetadataWizard: React.FC<ExcelMetadataWizardProps> = ({ 
  isOpen, 
  onClose, 
  onSave 
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<ExcelMetadataFormData>({
    name: '',
    purpose: '',
    description: '',
    file: null,
    filePath: '',
    readExcel2007: true,
    selectedSheet: '',
    hasHeaders: true,
    schema: []
  });

  const [workbook, setWorkbook] = useState<XLSX.WorkBook | null>(null);
  const [sheets, setSheets] = useState<SheetData[]>([]);
  const [previewData, setPreviewData] = useState<any[][]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const totalSteps = 5;

  // Handle file selection and parsing with SheetJS
  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    // Validate file type
    const validExtensions = ['.xlsx', '.xls', '.xlsm', '.xlsb'];
    const fileExtension = file.name.toLowerCase().slice(file.name.lastIndexOf('.'));
    
    if (!validExtensions.includes(fileExtension)) {
      setError('Please select a valid Excel file (.xlsx, .xls, .xlsm, .xlsb)');
      return;
    }

    setError(null);
    setIsLoading(true);
    
    const reader = new FileReader();
    
    reader.onload = (e) => {
      try {
        const data = new Uint8Array(e.target?.result as ArrayBuffer);
        
        // Parse Excel file with SheetJS
        const wb = XLSX.read(data, {
          type: 'array',
          cellDates: true,
          cellText: true,
          cellNF: false,
          dense: false
        });

        setWorkbook(wb);
        
        // Extract sheet information with proper typing
        const sheetData: SheetData[] = wb.SheetNames.map(sheetName => {
          const worksheet = wb.Sheets[sheetName];
          const jsonData = XLSX.utils.sheet_to_json(worksheet, {
            header: 1,
            defval: '',
            raw: false
          }) as any[][]; // Cast to any[][] to fix TypeScript error

          // Get headers from first row if data exists
          const headers = jsonData.length > 0 ? 
            jsonData[0].map((cell: any, index: number) => 
              String(cell || `Column${index + 1}`)
            ) : 
            [];

          const dataRows = jsonData;
          const rowCount = dataRows.length > 0 ? dataRows.length - (formData.hasHeaders ? 1 : 0) : 0;

          return {
            name: sheetName,
            headers,
            data: dataRows,
            rowCount,
            columnCount: headers.length
          };
        });

        setSheets(sheetData);
        
        // Auto-select first sheet and generate preview
        if (sheetData.length > 0) {
          const firstSheet = sheetData[0];
          handleSheetChange(firstSheet.name, sheetData, true);
        }
        
        // Update form data
        updateFormData({
          file,
          filePath: file.name,
          readExcel2007: fileExtension === '.xlsx'
        });
        
        setIsLoading(false);
      } catch (err: any) {
        setError(`Failed to parse Excel file: ${err.message}`);
        setIsLoading(false);
      }
    };

    reader.onerror = () => {
      setError('Failed to read file. The file may be corrupted or in an unsupported format.');
      setIsLoading(false);
    };

    reader.readAsArrayBuffer(file);
  };

  // Generate schema from detected headers and data
  const generateSchemaFromData = (headers: string[], data: any[][]): Array<{ 
    name: string; 
    type: string; 
    length?: number 
  }> => {
    return headers.map((header, index) => {
      // Analyze column data to infer type (skip header row if hasHeaders is true)
      const startRow = formData.hasHeaders && data.length > 1 ? 1 : 0;
      const columnData = data.slice(startRow).map(row => row[index]).filter(val => val !== '' && val != null);
      const inferredType = inferDataTypeFromSample(columnData);
      
      return {
        name: header,
        type: inferredType,
        length: inferDefaultLength(inferredType, columnData)
      };
    });
  };

  // Infer data type from sample data
  const inferDataTypeFromSample = (samples: any[]): string => {
    if (samples.length === 0) return 'String';
    
    // Check for dates
    const dateCount = samples.filter(val => {
      if (val instanceof Date) return true;
      if (typeof val === 'string') {
        const date = new Date(val);
        return !isNaN(date.getTime());
      }
      return false;
    }).length;
    
    if (dateCount / samples.length > 0.8) return 'Date';
    
    // Check for numbers
    const numberCount = samples.filter(val => {
      if (typeof val === 'number') return true;
      if (typeof val === 'string') {
        const num = parseFloat(val);
        return !isNaN(num) && isFinite(num);
      }
      return false;
    }).length;
    
    if (numberCount / samples.length > 0.8) {
      // Check if all numbers are integers
      const integerCount = samples.filter(val => {
        if (typeof val === 'number') return Number.isInteger(val);
        if (typeof val === 'string') {
          const num = parseFloat(val);
          return !isNaN(num) && isFinite(num) && Number.isInteger(num);
        }
        return false;
      }).length;
      
      return integerCount / numberCount > 0.9 ? 'Integer' : 'Decimal';
    }
    
    // Check for booleans
    const booleanCount = samples.filter(val => 
      [true, false, 'true', 'false', 'yes', 'no', '1', '0', 'Y', 'N'].includes(val)
    ).length;
    
    if (booleanCount / samples.length > 0.9) return 'Boolean';
    
    return 'String';
  };

  // Infer default length based on type and data
  const inferDefaultLength = (dataType: string, samples: any[]): number | undefined => {
    switch (dataType) {
      case 'String':
        const maxLength = samples.length > 0 ? 
          Math.max(...samples.map(val => String(val).length)) : 50;
        return Math.min(Math.max(maxLength, 10), 4000);
      case 'Integer':
        return 10;
      case 'Decimal':
        return 15;
      default:
        return undefined;
    }
  };

  // Handle sheet selection
  const handleSheetChange = (sheetName: string, sheetsData: SheetData[] = sheets, initialLoad: boolean = false) => {
    const selectedSheet = sheetsData.find(sheet => sheet.name === sheetName);
    if (selectedSheet && workbook) {
      const worksheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(worksheet, {
        header: 1,
        defval: '',
        raw: false
      }) as any[][]; // Cast to any[][]

      const headers = selectedSheet.headers;
      const data = jsonData;
      
      // Generate preview data (first 10 rows, excluding header if needed)
      const previewStartRow = formData.hasHeaders && !initialLoad ? 1 : 0;
      const previewRows = data.slice(previewStartRow, previewStartRow + 10);
      
      updateFormData({ 
        selectedSheet: sheetName,
        schema: generateSchemaFromData(headers, data)
      });
      setPreviewData(previewRows);
    }
  };

  // Update schema when headers option changes
  const handleHasHeadersChange = (hasHeaders: boolean) => {
    if (!formData.selectedSheet || !workbook) return;
    
    const selectedSheet = sheets.find(sheet => sheet.name === formData.selectedSheet);
    if (selectedSheet) {
      const worksheet = workbook.Sheets[formData.selectedSheet];
      const jsonData = XLSX.utils.sheet_to_json(worksheet, {
        header: 1,
        defval: '',
        raw: false
      }) as any[][]; // Cast to any[][]

      const headers = hasHeaders && jsonData.length > 0 ? 
        jsonData[0].map((cell: any, index: number) => String(cell || `Column${index + 1}`)) :
        jsonData[0]?.map((_, index) => `Column${index + 1}`) || [];

      const data = jsonData;

      const newSchema = generateSchemaFromData(headers, data);
      
      // Update the specific sheet data
      const updatedSheets = sheets.map(sheet => 
        sheet.name === formData.selectedSheet 
          ? { ...sheet, headers, data }
          : sheet
      );
      
      setSheets(updatedSheets);
      updateFormData({ 
        hasHeaders,
        schema: newSchema
      });
      
      // Update preview data
      const previewStartRow = hasHeaders ? 1 : 0;
      setPreviewData(data.slice(previewStartRow, previewStartRow + 10));
    }
  };

  // Update form data
  const updateFormData = (updates: Partial<ExcelMetadataFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Update schema field
  const updateSchema = (index: number, field: string, value: string | number) => {
    const newSchema = [...formData.schema];
    newSchema[index] = { ...newSchema[index], [field]: value };
    updateFormData({ schema: newSchema });
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
      readExcel2007: true,
      selectedSheet: '',
      hasHeaders: true,
      schema: []
    });
    setWorkbook(null);
    setSheets([]);
    setPreviewData([]);
    setError(null);
  };

  // Data types for dropdown
  const dataTypes = ['String', 'Integer', 'Decimal', 'Date', 'Boolean'];

  // Render data preview table
  const renderDataPreview = () => {
    if (!formData.selectedSheet || previewData.length === 0) {
      return (
        <div className="border border-gray-200 dark:border-gray-600 rounded-md p-4">
          <div className="text-center text-gray-500 dark:text-gray-400">
            <Table className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p>No data available for preview</p>
          </div>
        </div>
      );
    }

    const selectedSheet = sheets.find(sheet => sheet.name === formData.selectedSheet);
    const headers = selectedSheet?.headers || [];

    return (
      <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
        <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 border-b border-gray-200 dark:border-gray-600">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300">
            Data Preview ({previewData.length} rows)
            {formData.hasHeaders && ` - Showing data rows (excluding headers)`}
          </h4>
        </div>
        <div className="overflow-x-auto max-h-64">
          <table className="w-full text-sm">
            <thead className="bg-gray-100 dark:bg-gray-800">
              <tr>
                {headers.map((header, index) => (
                  <th key={index} className="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-300 border-r border-gray-200 dark:border-gray-600 last:border-r-0">
                    <div>{header}</div>
                    <div className="text-xs text-gray-500 font-normal mt-1">
                      {formData.schema[index]?.type}
                      {formData.schema[index]?.length && ` (${formData.schema[index]?.length})`}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
              {previewData.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-gray-50 dark:hover:bg-gray-700">
                  {row.map((cell, cellIndex) => (
                    <td key={cellIndex} className="px-3 py-2 text-gray-600 dark:text-gray-300 border-r border-gray-200 dark:border-gray-600 last:border-r-0">
                      {cell !== '' && cell != null ? cell.toString() : <span className="text-gray-400 italic">empty</span>}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {selectedSheet && selectedSheet.rowCount > 10 && (
          <div className="bg-gray-50 dark:bg-gray-700 px-4 py-2 text-xs text-gray-500 dark:text-gray-400 border-t border-gray-200 dark:border-gray-600">
            Showing first 10 rows of {selectedSheet.rowCount} total rows
          </div>
        )}
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
              Excel Metadata Wizard
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
                Click Browse to locate and select your Excel file. The application will automatically read the file structure and extract available sheets.
              </p>
              
              <div className="space-y-4">
                {/* Hidden file input */}
                <input
                  type="file"
                  ref={fileInputRef}
                  onChange={handleFileSelect}
                  accept=".xlsx,.xls,.xlsm,.xlsb"
                  className="hidden"
                />
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Excel File *
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
                      {isLoading ? 'Reading...' : 'Browse'}
                    </Button>
                  </div>
                  {formData.file && (
                    <p className="text-sm text-green-600 dark:text-green-400 mt-1">
                      ✓ File selected: {formData.file.name} ({(formData.file.size / 1024 / 1024).toFixed(2)} MB)
                    </p>
                  )}
                </div>
                
                <div className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    id="excel2007"
                    checked={formData.readExcel2007}
                    onChange={(e) => updateFormData({ readExcel2007: e.target.checked })}
                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                  <label htmlFor="excel2007" className="text-sm text-gray-700 dark:text-gray-300">
                    Read Excel2007 file format (.xlsx)
                  </label>
                </div>

                {error && (
                  <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                    <AlertCircle className="h-4 w-4 text-red-600 dark:text-red-400" />
                    <span className="text-sm text-red-600 dark:text-red-400">{error}</span>
                  </div>
                )}

                {sheets.length > 0 && (
                  <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-3">
                    <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                      <Table className="h-4 w-4" />
                      <span className="font-medium">File loaded successfully!</span>
                    </div>
                    <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                      Found {sheets.length} sheet(s) with a total of {sheets.reduce((acc, sheet) => acc + sheet.rowCount, 0)} data rows.
                    </p>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Sheet and Header Configuration
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Choose the specific sheet you want to work with. Specify whether the first row contains column headers. The preview shows the actual data from your Excel file.
              </p>
              
              {sheets.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Table className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No Excel file loaded. Please go back to Step 2 and select a file.</p>
                </div>
              ) : (
                <div className="space-y-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Select Sheet *
                    </label>
                    <select
                      value={formData.selectedSheet}
                      onChange={(e) => handleSheetChange(e.target.value)}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    >
                      <option value="">Select a sheet</option>
                      {sheets.map(sheet => (
                        <option key={sheet.name} value={sheet.name}>
                          {sheet.name} ({sheet.rowCount} rows × {sheet.columnCount} columns)
                        </option>
                      ))}
                    </select>
                  </div>
                  
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="hasHeaders"
                      checked={formData.hasHeaders}
                      onChange={(e) => handleHasHeadersChange(e.target.checked)}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <label htmlFor="hasHeaders" className="text-sm text-gray-700 dark:text-gray-300">
                      First row contains column headers
                    </label>
                  </div>
                  
                  {formData.selectedSheet && renderDataPreview()}
                </div>
              )}
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Define Schema
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                The application has inferred the schema from your Excel data. You can manually adjust column names, data types, and lengths. This schema will be stored in the Repository.
              </p>
              
              {formData.schema.length === 0 ? (
                <div className="text-center py-8 text-gray-500 dark:text-gray-400">
                  <Table className="h-12 w-12 mx-auto mb-4 opacity-50" />
                  <p>No schema data available. Please complete previous steps.</p>
                </div>
              ) : (
                <div className="border border-gray-200 dark:border-gray-600 rounded-md overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 dark:bg-gray-700">
                      <tr>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Column Name</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Data Type</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Length</th>
                        <th className="px-4 py-3 text-left font-medium text-gray-700 dark:text-gray-300">Sample Data</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200 dark:divide-gray-600">
                      {formData.schema.map((column, index) => {
                        const sampleData = previewData.slice(0, 3).map(row => row[index]).filter(val => val !== '' && val != null);
                        return (
                          <tr key={index} className="hover:bg-gray-50 dark:hover:bg-gray-700/50">
                            <td className="px-4 py-3">
                              <input
                                type="text"
                                value={column.name}
                                onChange={(e) => updateSchema(index, 'name', e.target.value)}
                                className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                              />
                            </td>
                            <td className="px-4 py-3">
                              <select
                                value={column.type}
                                onChange={(e) => updateSchema(index, 'type', e.target.value)}
                                className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                              >
                                {dataTypes.map(type => (
                                  <option key={type} value={type}>{type}</option>
                                ))}
                              </select>
                            </td>
                            <td className="px-4 py-3">
                              <input
                                type="number"
                                value={column.length || ''}
                                onChange={(e) => updateSchema(index, 'length', e.target.value ? parseInt(e.target.value) : '')}
                                className="w-full px-2 py-1 border border-gray-300 dark:border-gray-600 rounded focus:outline-none focus:ring-1 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                                placeholder="Auto"
                                min="1"
                                max="4000"
                              />
                            </td>
                            <td className="px-4 py-3">
                              <div className="text-xs text-gray-500 dark:text-gray-400 max-w-xs truncate">
                                {sampleData.length > 0 
                                  ? sampleData.map(val => String(val)).join(', ')
                                  : 'No sample data'
                                }
                              </div>
                            </td>
                          </tr>
                        );
                      })}
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
                Review your configuration and click Finish to save the metadata. The Excel file structure and schema will be centralized in the Repository.
              </p>
              
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your Excel metadata configuration is complete and ready to be saved to the repository.
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
                      <span className="text-gray-500 dark:text-gray-400">Sheet:</span>
                      <span className="text-gray-900 dark:text-white">{formData.selectedSheet || 'No sheet selected'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Has Headers:</span>
                      <span className="text-gray-900 dark:text-white">{formData.hasHeaders ? 'Yes' : 'No'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">File Format:</span>
                      <span className="text-gray-900 dark:text-white">{formData.readExcel2007 ? 'Excel 2007+' : 'Excel 97-2003'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Total Columns:</span>
                      <span className="text-gray-900 dark:text-white">{formData.schema.length}</span>
                    </div>
                  </div>
                </div>

                {formData.schema.length > 0 && (
                  <div className="space-y-3">
                    <h4 className="font-medium text-gray-900 dark:text-white">Schema Summary</h4>
                    <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3 max-h-48 overflow-y-auto">
                      <div className="space-y-2 text-sm">
                        {formData.schema.map((column, index) => (
                          <div key={index} className="flex justify-between items-center py-1 border-b border-gray-100 dark:border-gray-700 last:border-b-0">
                            <span className="text-gray-600 dark:text-gray-400 font-medium">{column.name}</span>
                            <span className="text-gray-900 dark:text-white text-xs bg-blue-100 dark:bg-blue-900 px-2 py-1 rounded">
                              {column.type}{column.length ? `(${column.length})` : ''}
                            </span>
                          </div>
                        ))}
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
                disabled={(currentStep === 2 && !formData.file) || (currentStep === 3 && !formData.selectedSheet)}
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

export default ExcelMetadataWizard;