// src/workers/formatConverter.worker.ts

import Papa from 'papaparse';
import init, { readParquet } from 'parquet-wasm';
import type { ConvertWorkerMessage, WorkerResponse, ConvertResult } from '../types/converterTypes';

// ------------------------------------------------------------------
// Parquet WASM initialisation
// ------------------------------------------------------------------
let wasmInitPromise: Promise<any> | null = null;
async function ensureWasm() {
  if (!wasmInitPromise) {
    wasmInitPromise = init();
  }
  await wasmInitPromise;
}

// ------------------------------------------------------------------
// Format converters
// ------------------------------------------------------------------

/**
 * JSON – robustly handles:
 *  - Top‑level array of objects
 *  - Single object
 *  - JSON Lines (newline‑delimited JSON objects)
 */
function processJSON(buffer: ArrayBuffer): ConvertResult {
  const text = new TextDecoder().decode(buffer);
  let parsed: any;

  // 1. Try standard JSON.parse
  try {
    parsed = JSON.parse(text);
  } catch {
    // 2. Fallback to JSON Lines
    const lines = text.split('\n').filter(line => line.trim());
    if (lines.length === 0) {
      throw new Error('Empty file or invalid JSON (no valid lines found)');
    }
    parsed = lines.map(line => JSON.parse(line));
  }

  // 3. Normalise to an array of objects (match wizard behaviour)
  let array: any[];
  if (Array.isArray(parsed)) {
    array = parsed;
  } else if (typeof parsed === 'object' && parsed !== null) {
    // Single object → wrap in array
    array = [parsed];
  } else {
    throw new Error('JSON content is not an object or array');
  }

  // 4. Remove non‑object entries to avoid Papa.unparse issues
  array = array.filter(item => item && typeof item === 'object');
  if (array.length === 0) {
    throw new Error('No object records found in the JSON file');
  }

  const csv = Papa.unparse(array);
  return { csvString: csv, rowCount: array.length };
}

/**
 * Parquet – uses parquet-wasm to read the entire file.
 * Wraps the call in a try/catch for clearer error messages.
 */
async function processParquet(buffer: ArrayBuffer): Promise<ConvertResult> {
  await ensureWasm();
  const uint8 = new Uint8Array(buffer);

  let table: any;
  try {
    table = readParquet(uint8);
  } catch (e: any) {
    throw new Error(`Failed to read Parquet file: ${e.message}`);
  }

  const numRows: number = table.numRows();
  const columnNames: string[] = [];
  for (let i = 0; i < table.numColumns(); i++) {
    columnNames.push(table.columnName(i));
  }

  const rows: Record<string, unknown>[] = [];
  for (let rowIdx = 0; rowIdx < numRows; rowIdx++) {
    const row: Record<string, unknown> = {};
    for (const col of columnNames) {
      row[col] = table.getColumn(col).get(rowIdx);
    }
    rows.push(row);
  }

  const csv = Papa.unparse(rows);
  return { csvString: csv, rowCount: numRows };
}

// ------------------------------------------------------------------
// Message handler
// ------------------------------------------------------------------
self.onmessage = async (e: MessageEvent<ConvertWorkerMessage>) => {
  const { type, payload } = e.data;
  if (type !== 'convert') return;

  const { fileType, buffer } = payload;

  try {
    let result: ConvertResult;

    switch (fileType) {
      case 'json':
        result = processJSON(buffer);
        break;
      case 'parquet':
        result = await processParquet(buffer);
        break;
      // Avro is now handled by the backend – never reaches the worker
      default:
        throw new Error(`Unsupported file type: ${fileType}`);
    }

    const response: WorkerResponse = {
      success: true,
      csvString: result.csvString,
      rowCount: result.rowCount,
    };
    self.postMessage(response);
  } catch (error: any) {
    // 🔍 Enhanced logging: print the actual error so it appears in the browser console
    console.error('[Worker] Conversion error:', error);

    const response: WorkerResponse = {
      success: false,
      error: error?.message || 'Unknown conversion error',
    };
    self.postMessage(response);
  }
};

// Required for TypeScript module isolation
export {};