// backend/src/database/types/inspection.types.ts
export interface TableInfo {
  schemaname: string;
  tablename: string;
  tabletype: string;
  columns: ColumnMetadata[];
  comment?: string;
  rowCount?: number;
  size?: string;
  originalData?: any;
}

export interface ColumnMetadata {
  name: string;
  type: string;
  dataType: string;
  length?: number;
  precision?: number;
  scale?: number;
  nullable: boolean;
  default?: string;
  isPrimaryKey?: boolean;
  isForeignKey?: boolean;
  foreignTable?: string;
  foreignColumn?: string;
  ordinalPosition: number;
  comment?: string;
}

export interface ConstraintInfo {
  name: string;
  type: string;
  tableName: string;
  columnName: string;
  foreignTable?: string;
  foreignColumn?: string;
}

export interface SchemaInspectionResult {
  tables: TableInfo[];
  totalTables: number;
  databaseType: string;
  timestamp: Date;
}

export interface DatabaseConfig {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  schema?: string;
  connectionLimit?: number;
  idleTimeout?: number;
  options?: Record<string, any>;
}