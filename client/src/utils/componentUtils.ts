// utils/componentTypeMapping.ts
import { ComponentType } from '../components/Editor/BasicSettingsPanel';

export const TARGETED_COMPONENTS = [
  // Data Transformation
  'tJoin', 'tDenormalize', 'tNormalize', 'tAggregateRow', 
  'tSortRow', 'tFilterRow', 'tFilterColumns',
  
  // Field Manipulation
  'tReplace', 'tReplaceList', 'tConvertType', 
  'tExtractDelimitedFields', 'tExtractRegexFields', 
  'tExtractJSONFields', 'tExtractXMLField',
  
  // Row & Record Processing
  'tParseRecordSet', 'tSplitRow', 'tPivotToColumnsDelimited', 
  'tUnpivotRow', 'tDenormalizeSortedRow', 'tUniqRow', 'tSampleRow',
  
  // Validation & Quality
  'tSchemaComplianceCheck', 'tAddCRCRow', 'tAddCRC', 
  'tStandardizeRow', 'tDataMasking', 'tAssert',
  
  // Flow & Orchestration
  'tFlowToIterate', 'tIterateToFlow', 'tReplicate', 'tUnite', 
  'tFlowMerge', 'tFlowMeter', 'tFlowMeterCatcher',
  
  // System & Generation
  'tRowGenerator', 'tNormalizeNumber', 'tFileLookup', 
  'tCacheIn', 'tCacheOut', 'tRecordMatching'
] as const;

export type TargetedComponentName = typeof TARGETED_COMPONENTS[number];

// Map targeted component names to BasicSettingsPanel ComponentType
const COMPONENT_TYPE_MAPPING: Record<TargetedComponentName, ComponentType> = {
  // Data Transformation
  'tJoin': 'tJoin',
  'tDenormalize': 'tJoin', // Map to closest match
  'tNormalize': 'tSchemaEditor',
  'tAggregateRow': 'tAggregateRow',
  'tSortRow': 'tFilterRow', // Map to closest match
  'tFilterRow': 'tFilterRow',
  'tFilterColumns': 'tFilterRow',
  
  // Field Manipulation
  'tReplace': 'tReplace',
  'tReplaceList': 'tReplaceList',
  'tConvertType': 'tConvertType',
  'tExtractDelimitedFields': 'tExtractDelimited',
  'tExtractRegexFields': 'tExtractRegex',
  'tExtractJSONFields': 'tExtractJSON',
  'tExtractXMLField': 'tExtractXML',
  
  // Row & Record Processing
  'tParseRecordSet': 'tSchemaEditor',
  'tSplitRow': 'tFilterRow',
  'tPivotToColumnsDelimited': 'tExtractDelimited',
  'tUnpivotRow': 'tFilterRow',
  'tDenormalizeSortedRow': 'tJoin',
  'tUniqRow': 'tFilterRow',
  'tSampleRow': 'tFilterRow',
  
  // Validation & Quality
  'tSchemaComplianceCheck': 'tSchemaEditor',
  'tAddCRCRow': 'tFilterRow',
  'tAddCRC': 'tFilterRow',
  'tStandardizeRow': 'tSchemaEditor',
  'tDataMasking': 'tFilterRow',
  'tAssert': 'tFilterRow',
  
  // Flow & Orchestration
  'tFlowToIterate': 'tJoin',
  'tIterateToFlow': 'tJoin',
  'tReplicate': 'tJoin',
  'tUnite': 'tJoin',
  'tFlowMerge': 'tJoin',
  'tFlowMeter': 'tFilterRow',
  'tFlowMeterCatcher': 'tFilterRow',
  
  // System & Generation
  'tRowGenerator': 'tRowGenerator',
  'tNormalizeNumber': 'tConvertType',
  'tFileLookup': 'tSchemaEditor',
  'tCacheIn': 'tSchemaEditor',
  'tCacheOut': 'tSchemaEditor',
  'tRecordMatching': 'tJoin'
};

export const mapToComponentType = (componentName: string): ComponentType => {
  // Type guard to check if componentName is a TargetedComponentName
  if (TARGETED_COMPONENTS.includes(componentName as TargetedComponentName)) {
    return COMPONENT_TYPE_MAPPING[componentName as TargetedComponentName];
  }
  
  // Default fallback for unknown components
  console.warn(`Component "${componentName}" is not in the targeted components list, defaulting to tSchemaEditor`);
  return 'tSchemaEditor';
};

export const isTargetedComponent = (componentName: string): boolean => {
  return TARGETED_COMPONENTS.includes(componentName as TargetedComponentName);
};

export const getComponentDescription = (componentName: string): string => {
  const descriptions: Record<TargetedComponentName, string> = {
    // Data Transformation
    'tJoin': 'Joins data from two input flows based on key columns',
    'tDenormalize': 'Denormalizes data structure for reporting',
    'tNormalize': 'Normalizes data structure for database storage',
    'tAggregateRow': 'Aggregates data using group by and aggregate functions',
    'tSortRow': 'Sorts rows based on specified columns',
    'tFilterRow': 'Filters rows based on specified conditions',
    'tFilterColumns': 'Filters and selects specific columns',
    
    // Field Manipulation
    'tReplace': 'Replaces text patterns in specified columns',
    'tReplaceList': 'Performs multiple replacements using lookup tables',
    'tConvertType': 'Converts data types between different formats',
    'tExtractDelimitedFields': 'Extracts fields from delimited text',
    'tExtractRegexFields': 'Extracts data using regular expressions',
    'tExtractJSONFields': 'Extracts data from JSON structures',
    'tExtractXMLField': 'Extracts data from XML documents',
    
    // Row & Record Processing
    'tParseRecordSet': 'Parses record sets from structured data',
    'tSplitRow': 'Splits rows based on specified criteria',
    'tPivotToColumnsDelimited': 'Pivots data to delimited columns',
    'tUnpivotRow': 'Unpivots rows for data transformation',
    'tDenormalizeSortedRow': 'Denormalizes sorted rows for analysis',
    'tUniqRow': 'Removes duplicate rows from data',
    'tSampleRow': 'Samples rows for testing or analysis',
    
    // Validation & Quality
    'tSchemaComplianceCheck': 'Validates data against schema definitions',
    'tAddCRCRow': 'Adds CRC checksum to rows for data integrity',
    'tAddCRC': 'Adds CRC checksum for data validation',
    'tStandardizeRow': 'Standardizes row formats and values',
    'tDataMasking': 'Masks sensitive data for privacy',
    'tAssert': 'Validates data assertions and conditions',
    
    // Flow & Orchestration
    'tFlowToIterate': 'Converts flow to iteration for processing',
    'tIterateToFlow': 'Converts iteration back to flow',
    'tReplicate': 'Replicates data flow to multiple outputs',
    'tUnite': 'Unites multiple data flows into one',
    'tFlowMerge': 'Merges multiple data flows',
    'tFlowMeter': 'Monitors and meters data flow',
    'tFlowMeterCatcher': 'Catches and handles flow meter events',
    
    // System & Generation
    'tRowGenerator': 'Generates synthetic data rows',
    'tNormalizeNumber': 'Normalizes numerical data',
    'tFileLookup': 'Performs file-based lookups',
    'tCacheIn': 'Caches input data for performance',
    'tCacheOut': 'Caches output data for performance',
    'tRecordMatching': 'Matches records across datasets'
  };
  
  return descriptions[componentName as TargetedComponentName] || 'Configure settings for this component';
};