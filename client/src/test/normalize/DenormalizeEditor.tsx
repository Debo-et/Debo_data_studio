// src/components/Editor/Aggregates/DenormalizeEditor.tsx
import { SimpleColumn } from '@/pages/canvas.types';

export interface DenormalizeEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: SimpleColumn[];
  initialConfig?: DenormalizeComponentConfiguration;
  onClose: () => void;
  onSave: (config: DenormalizeComponentConfiguration) => void;
}

export interface DenormalizeComponentConfiguration {
  version: string;
  groupByColumns: string[];
  denormalizeColumn: string;
  delimiter: string;
  sortBy?: string;
  sortOrder?: 'ASC' | 'DESC';
  outputColumnName: string;
  nullHandling: 'skip' | 'placeholder' | 'include';
  placeholder?: string;
  addCount?: boolean;
  countColumnName?: string;
  // ... other fields
}

