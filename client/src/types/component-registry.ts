// src/types/component-registry.ts
export type ComponentType = 
  | 'file-delimited'
  | 'file-positional'
  | 'file-xml'
  | 'file-excel'
  | 'file-schema'
  | 'file-regex'
  | 'file-ldif'
  | 'file-json-avro-parquet'
  | 'directory'
  | 'ldap'
  | 'mysql'
  | 'oracle'
  | 'sqlserver'
  | 'db2'
  | 'sap_hana'
  | 'sybase'
  | 'netezza'
  | 'informix'
  | 'firebird';

export interface ComponentMetadata {
  id: ComponentType;
  name: string;
  type: ComponentType;
  category: 'input' | 'output' | 'processing';
  icon: string;
  defaultRole: 'IN' | 'OUT';
  width: number;
  height: number;
  color: string;
}

export interface TalendNodeData {
  id: string;
  componentType: ComponentType;
  name: string;
  role: 'IN' | 'OUT';
  metadata: Record<string, any>;
  counter: number;
}

export interface ReactFlowDragData {
  type: 'reactflow';
  componentType: ComponentType;
  metadata: Record<string, any>;
}