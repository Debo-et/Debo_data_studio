// src/components/canvas/ComponentRegistry.tsx
import React from 'react';
import { 
  Database, 
  FileText, 
  FileSpreadsheet, 
  FileCode, 
  FileJson, 
  Regex, 
  Webhook,
  Settings,
  Filter,
  Map,
  GitMerge,
  Cpu,
  Search,
  Shield,
  Layers,
  ArrowDown,
  File,
  Folder,
  Hash,
  Type,
  Mail,
  RotateCw,
  RefreshCw,
  Lock,
  Unlock,
  Columns,
  MessageCircle,
  HardDrive,
  Code} from 'lucide-react';
import { ConnectionPort } from '@/types/unified-pipeline.types';

export type ComponentCategory = 'input' | 'transform' | 'output';

export interface ComponentDefinition {
  id: string;
  displayName: string;
  icon: React.ReactNode;
  category: ComponentCategory;
  defaultDimensions: { width: number; height: number };
  defaultRole: 'INPUT' | 'TRANSFORM' | 'OUTPUT';
  description: string;
  defaultPorts?: ConnectionPort[];
  source: 'sidebar' | 'rightPanel';
}

export const COMPONENT_REGISTRY: Record<string, ComponentDefinition> = {
  // ==================== INPUT COMPONENTS ====================
  'delimited-file': {
    id: 'delimited-file',
    displayName: 'DelimitedFile',
    icon: <FileText className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read delimited text files (CSV, TSV)',
    source: 'sidebar'
  },
  'positional-file': {
    id: 'positional-file',
    displayName: 'PositionalFile',
    icon: <FileCode className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read fixed-width text files',
    source: 'sidebar'
  },
  'excel-file': {
    id: 'excel-file',
    displayName: 'ExcelFile',
    icon: <FileSpreadsheet className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read Excel spreadsheets',
    source: 'sidebar'
  },
  'xml-file': {
    id: 'xml-file',
    displayName: 'XMLFile',
    icon: <FileCode className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read XML documents',
    source: 'sidebar'
  },
  'json-avro-parquet': {
    id: 'json-avro-parquet',
    displayName: 'JsonAvroParquet',
    icon: <FileJson className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read JSON, Avro, or Parquet files',
    source: 'sidebar'
  },
  'database-input': {
    id: 'database-input',
    displayName: 'DatabaseInput',
    icon: <Database className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read from databases',
    source: 'sidebar'
  },
  'web-service-input': {
    id: 'web-service-input',
    displayName: 'WebServiceInput',
    icon: <Webhook className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Consume web services as input',
    source: 'sidebar'
  },
  'ldap-input': {
    id: 'ldap-input',
    displayName: 'LDAPInput',
    icon: <Folder className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read from LDAP directories',
    source: 'sidebar'
  },
  'regex-input': {
    id: 'regex-input',
    displayName: 'RegexInput',
    icon: <Regex className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Parse data using regex patterns',
    source: 'sidebar'
  },
  'file-schema-input': {
    id: 'file-schema-input',
    displayName: 'FileSchemaInput',
    icon: <File className="w-5 h-5" />,
    category: 'input',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'INPUT',
    description: 'Read files with complex schemas',
    source: 'sidebar'
  },
  
  // ==================== TRANSFORM COMPONENTS ====================
  'tMap': {
    id: 'tMap',
    displayName: 'tMap',
    icon: <Map className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 35, height: 25 },
    defaultRole: 'TRANSFORM',
    description: 'Map and transform data fields',
    source: 'rightPanel'
  },
  'tFilterRow': {
    id: 'tFilterRow',
    displayName: 'tFilterRow',
    icon: <Filter className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Filter rows based on conditions',
    source: 'rightPanel'
  },
  'tAggregateRow': {
    id: 'tAggregateRow',
    displayName: 'tAggregateRow',
    icon: <Layers className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Aggregate and group data',
    source: 'rightPanel'
  },
  'tJoin': {
    id: 'tJoin',
    displayName: 'tJoin',
    icon: <GitMerge className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Join multiple data streams',
    source: 'rightPanel'
  },
  'tSortRow': {
    id: 'tSortRow',
    displayName: 'tSortRow',
    icon: <ArrowDown className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Sort rows by specified columns',
    source: 'rightPanel'
  },
  'tNormalize': {
    id: 'tNormalize',
    displayName: 'tNormalize',
    icon: <Settings className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Normalize data structure',
    source: 'rightPanel'
  },
  'tRegexExtract': {
    id: 'tRegexExtract',
    displayName: 'tRegexExtract',
    icon: <Regex className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Extract data using regex patterns',
    source: 'rightPanel'
  },
  'tWebService': {
    id: 'tWebService',
    displayName: 'tWebService',
    icon: <Webhook className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Call web services',
    source: 'rightPanel'
  },
  'tMatchGroup': {
    id: 'tMatchGroup',
    displayName: 'tMatchGroup',
    icon: <Search className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Group matching records',
    source: 'rightPanel'
  },
  'tDataQuality': {
    id: 'tDataQuality',
    displayName: 'tDataQuality',
    icon: <Shield className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Data quality validation',
    source: 'rightPanel'
  },
  'tSplitRow': {
    id: 'tSplitRow',
    displayName: 'tSplitRow',
    icon: <GitMerge className="w-5 h-5 rotate-180" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Split rows into multiple outputs',
    source: 'rightPanel'
  },
  'tDenormalize': {
    id: 'tDenormalize',
    displayName: 'tDenormalize',
    icon: <Settings className="w-5 h-5 rotate-180" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Denormalize data structure',
    source: 'rightPanel'
  },
  'tPivot': {
    id: 'tPivot',
    displayName: 'tPivot',
    icon: <RefreshCw className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Pivot table transformation',
    source: 'rightPanel'
  },
  'tUnpivot': {
    id: 'tUnpivot',
    displayName: 'tUnpivot',
    icon: <RotateCw className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Unpivot table transformation',
    source: 'rightPanel'
  },
  'tRowGenerator': {
    id: 'tRowGenerator',
    displayName: 'tRowGenerator',
    icon: <Cpu className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Generate synthetic rows',
    source: 'rightPanel'
  },
  'tSchemaMapper': {
    id: 'tSchemaMapper',
    displayName: 'tSchemaMapper',
    icon: <Columns className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Map between different schemas',
    source: 'rightPanel'
  },
  'tTypeConverter': {
    id: 'tTypeConverter',
    displayName: 'tTypeConverter',
    icon: <Type className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Convert data types',
    source: 'rightPanel'
  },
  'tExpression': {
    id: 'tExpression',
    displayName: 'tExpression',
    icon: <Code className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Apply expressions to data',
    source: 'rightPanel'
  },
  'tHash': {
    id: 'tHash',
    displayName: 'tHash',
    icon: <Hash className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Generate hash values',
    source: 'rightPanel'
  },
  'tEncrypt': {
    id: 'tEncrypt',
    displayName: 'tEncrypt',
    icon: <Lock className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Encrypt data fields',
    source: 'rightPanel'
  },
  'tDecrypt': {
    id: 'tDecrypt',
    displayName: 'tDecrypt',
    icon: <Unlock className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Decrypt data fields',
    source: 'rightPanel'
  },
  'tLookup': {
    id: 'tLookup',
    displayName: 'tLookup',
    icon: <Search className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Lookup values from reference tables',
    source: 'rightPanel'
  },
  'tCache': {
    id: 'tCache',
    displayName: 'tCache',
    icon: <HardDrive className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Cache data for performance',
    source: 'rightPanel'
  },
  // ----- Additional transform components from RightPanel -----
  'tConvertType': {
    id: 'tConvertType',
    displayName: 'tConvertType',
    icon: <Type className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Convert data types with advanced options',
    source: 'rightPanel'
  },
  'tReplace': {
    id: 'tReplace',
    displayName: 'tReplace',
    icon: <Settings className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Replace values in fields',
    source: 'rightPanel'
  },
  'tReplaceList': {
    id: 'tReplaceList',
    displayName: 'tReplaceList',
    icon: <Settings className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Replace multiple values using a list',
    source: 'rightPanel'
  },
  'tParseRecordSet': {
    id: 'tParseRecordSet',
    displayName: 'tParseRecordSet',
    icon: <Layers className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Parse record sets from complex fields',
    source: 'rightPanel'
  },
  'tPivotToColumnsDelimited': {
    id: 'tPivotToColumnsDelimited',
    displayName: 'tPivotToColumnsDelimited',
    icon: <RefreshCw className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Pivot data to delimited columns',
    source: 'rightPanel'
  },
  'tUnpivotRow': {
    id: 'tUnpivotRow',
    displayName: 'tUnpivotRow',
    icon: <RotateCw className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Unpivot rows to columns',
    source: 'rightPanel'
  },
  'tDenormalizeSortedRow': {
    id: 'tDenormalizeSortedRow',
    displayName: 'tDenormalizeSortedRow',
    icon: <Settings className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Denormalize sorted rows',
    source: 'rightPanel'
  },
  'tNormalizeNumber': {
    id: 'tNormalizeNumber',
    displayName: 'tNormalizeNumber',
    icon: <Hash className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Normalize number formats',
    source: 'rightPanel'
  },
  'tExtractDelimitedFields': {
    id: 'tExtractDelimitedFields',
    displayName: 'tExtractDelimitedFields',
    icon: <Columns className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Extract fields from delimited strings',
    source: 'rightPanel'
  },
  'tExtractRegexFields': {
    id: 'tExtractRegexFields',
    displayName: 'tExtractRegexFields',
    icon: <Regex className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Extract fields using regular expressions',
    source: 'rightPanel'
  },
  'tExtractJSONFields': {
    id: 'tExtractJSONFields',
    displayName: 'tExtractJSONFields',
    icon: <FileJson className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Extract fields from JSON data',
    source: 'rightPanel'
  },
  'tExtractXMLField': {
    id: 'tExtractXMLField',
    displayName: 'tExtractXMLField',
    icon: <FileCode className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Extract fields from XML data',
    source: 'rightPanel'
  },
  'tFilterColumns': {
    id: 'tFilterColumns',
    displayName: 'tFilterColumns',
    icon: <Filter className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Filter columns to keep or remove',
    source: 'rightPanel'
  },
  'tUniqRow': {
    id: 'tUniqRow',
    displayName: 'tUniqRow',
    icon: <Layers className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Remove duplicate rows',
    source: 'rightPanel'
  },
  'tSampleRow': {
    id: 'tSampleRow',
    displayName: 'tSampleRow',
    icon: <Layers className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Sample rows from dataset',
    source: 'rightPanel'
  },
  'tSchemaComplianceCheck': {
    id: 'tSchemaComplianceCheck',
    displayName: 'tSchemaComplianceCheck',
    icon: <Shield className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Check rows against schema',
    source: 'rightPanel'
  },
  'tAddCRCRow': {
    id: 'tAddCRCRow',
    displayName: 'tAddCRCRow',
    icon: <Hash className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Add CRC checksum to rows',
    source: 'rightPanel'
  },
  'tAddCRC': {
    id: 'tAddCRC',
    displayName: 'tAddCRC',
    icon: <Hash className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Add CRC checksum',
    source: 'rightPanel'
  },
  'tStandardizeRow': {
    id: 'tStandardizeRow',
    displayName: 'tStandardizeRow',
    icon: <Settings className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Standardize row data',
    source: 'rightPanel'
  },
  'tSurvivorshipRule': {
    id: 'tSurvivorshipRule',
    displayName: 'tSurvivorshipRule',
    icon: <Shield className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Apply survivorship rules',
    source: 'rightPanel'
  },
  'tDataMasking': {
    id: 'tDataMasking',
    displayName: 'tDataMasking',
    icon: <Lock className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Mask sensitive data',
    source: 'rightPanel'
  },
  'tRuleSurvivorship': {
    id: 'tRuleSurvivorship',
    displayName: 'tRuleSurvivorship',
    icon: <Shield className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Rule-based survivorship',
    source: 'rightPanel'
  },
  'tAssert': {
    id: 'tAssert',
    displayName: 'tAssert',
    icon: <Shield className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Assert conditions on data',
    source: 'rightPanel'
  },
  'tReplicate': {
    id: 'tReplicate',
    displayName: 'tReplicate',
    icon: <GitMerge className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Replicate data to multiple outputs',
    source: 'rightPanel'
  },
  'tUnite': {
    id: 'tUnite',
    displayName: 'tUnite',
    icon: <GitMerge className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Unite multiple data streams',
    source: 'rightPanel'
  },
  'tFlowMerge': {
    id: 'tFlowMerge',
    displayName: 'tFlowMerge',
    icon: <GitMerge className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Merge flows',
    source: 'rightPanel'
  },
  'tFlowMeter': {
    id: 'tFlowMeter',
    displayName: 'tFlowMeter',
    icon: <Cpu className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Measure flow metrics',
    source: 'rightPanel'
  },
  'tFlowMeterCatcher': {
    id: 'tFlowMeterCatcher',
    displayName: 'tFlowMeterCatcher',
    icon: <Cpu className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Catch flow meter data',
    source: 'rightPanel'
  },
  'tRecordMatching': {
    id: 'tRecordMatching',
    displayName: 'tRecordMatching',
    icon: <Search className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Match records across datasets',
    source: 'rightPanel'
  },
  'tFileLookup': {
    id: 'tFileLookup',
    displayName: 'tFileLookup',
    icon: <Search className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Lookup data from files',
    source: 'rightPanel'
  },
  'tFlowToIterate': {
    id: 'tFlowToIterate',
    displayName: 'tFlowToIterate',
    icon: <RotateCw className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Convert flow to iterate',
    source: 'rightPanel'
  },
  'tIterateToFlow': {
    id: 'tIterateToFlow',
    displayName: 'tIterateToFlow',
    icon: <RefreshCw className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Convert iterate to flow',
    source: 'rightPanel'
  },
  'tCacheIn': {
    id: 'tCacheIn',
    displayName: 'tCacheIn',
    icon: <HardDrive className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Cache input data',
    source: 'rightPanel'
  },
  'tCacheOut': {
    id: 'tCacheOut',
    displayName: 'tCacheOut',
    icon: <HardDrive className="w-5 h-5" />,
    category: 'transform',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'TRANSFORM',
    description: 'Cache output data',
    source: 'rightPanel'
  },
  
  // ==================== OUTPUT COMPONENTS ====================
  'delimited-output': {
    id: 'delimited-output',
    displayName: 'DelimitedOutput',
    icon: <FileText className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write to delimited files',
    source: 'sidebar'
  },
  'database-output': {
    id: 'database-output',
    displayName: 'DatabaseOutput',
    icon: <Database className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write to databases',
    source: 'sidebar'
  },
  'excel-output': {
    id: 'excel-output',
    displayName: 'ExcelOutput',
    icon: <FileSpreadsheet className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write to Excel files',
    source: 'sidebar'
  },
  'xml-output': {
    id: 'xml-output',
    displayName: 'XMLOutput',
    icon: <FileCode className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write XML documents',
    source: 'sidebar'
  },
  'json-output': {
    id: 'json-output',
    displayName: 'JsonOutput',
    icon: <FileJson className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write JSON files',
    source: 'sidebar'
  },
  'avro-output': {
    id: 'avro-output',
    displayName: 'AvroOutput',
    icon: <FileJson className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write Avro files',
    source: 'sidebar'
  },
  'parquet-output': {
    id: 'parquet-output',
    displayName: 'ParquetOutput',
    icon: <FileJson className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write Parquet files',
    source: 'sidebar'
  },
  'web-service-output': {
    id: 'web-service-output',
    displayName: 'WebServiceOutput',
    icon: <Webhook className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Call web services as output',
    source: 'sidebar'
  },
  'ldap-output': {
    id: 'ldap-output',
    displayName: 'LDAPOutput',
    icon: <Folder className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write to LDAP directories',
    source: 'sidebar'
  },
  'file-output': {
    id: 'file-output',
    displayName: 'FileOutput',
    icon: <File className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Write to generic files',
    source: 'sidebar'
  },
  'report-output': {
    id: 'report-output',
    displayName: 'ReportOutput',
    icon: <FileText className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Generate formatted reports',
    source: 'sidebar'
  },
  'email-output': {
    id: 'email-output',
    displayName: 'EmailOutput',
    icon: <Mail className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Send output via email',
    source: 'sidebar'
  },
  'message-queue-output': {
    id: 'message-queue-output',
    displayName: 'MessageQueueOutput',
    icon: <MessageCircle className="w-5 h-5" />,
    category: 'output',
    defaultDimensions: { width: 30, height: 20 },
    defaultRole: 'OUTPUT',
    description: 'Send to message queues',
    source: 'sidebar'
  }
};

// Helper functions
export function getComponentDefinition(componentId: string): ComponentDefinition | null {
  return COMPONENT_REGISTRY[componentId] || null;
}

export function getComponentsByCategory(category: ComponentCategory): ComponentDefinition[] {
  return Object.values(COMPONENT_REGISTRY).filter((comp): comp is ComponentDefinition => 
    comp.category === category
  );
}

export function getComponentIcon(componentId: string): React.ReactNode {
  return COMPONENT_REGISTRY[componentId]?.icon || <Cpu className="w-5 h-5" />;
}

export function getCategoryColor(category: ComponentCategory): string {
  switch (category) {
    case 'input': return '#4f46e5'; // Indigo
    case 'transform': return '#7c3aed'; // Purple
    case 'output': return '#059669'; // Emerald
    default: return '#6b7280'; // Gray
  }
}

export function getAllComponents(): ComponentDefinition[] {
  return Object.values(COMPONENT_REGISTRY);
}

export function getComponentsBySource(source: 'sidebar' | 'rightPanel'): ComponentDefinition[] {
  return Object.values(COMPONENT_REGISTRY).filter((comp): comp is ComponentDefinition => 
    comp.source === source
  );
}