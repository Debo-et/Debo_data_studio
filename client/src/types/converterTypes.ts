// src/types/converterTypes.ts
export interface ConvertWorkerMessage {
  type: 'convert';
  payload: {
    fileType: 'json' | 'avro' | 'parquet';
    buffer: ArrayBuffer;
    selectedColumns?: string[];
  };
}

export interface ConvertResult {
  csvString: string;
  rowCount: number;
}

export interface WorkerResponse {
  success: boolean;
  csvString?: string;
  rowCount?: number;
  error?: string;
}