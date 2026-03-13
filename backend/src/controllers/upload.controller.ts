import { Request, Response } from 'express';
import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';

const UPLOAD_DIR = process.env.CSV_UPLOAD_DIR || path.join(__dirname, '../../uploads');

// Ensure upload directory exists
if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true, mode: 0o755 });
}

/**
 * Helper to run a Python script and return the output file path.
 * @param scriptName Name of the Python script (e.g., 'positional_to_csv.py')
 * @param args Array of arguments to pass to the script
 * @param res Express response object (used for error handling)
 * @param onSuccess Callback with the output file path
 */
function runPythonScript(
  scriptName: string,
  args: string[],
  res: Response,
  onSuccess: (outputPath: string) => void
) {
  const scriptPath = path.join(__dirname, '../../scripts', scriptName);
  const outputFileName = `converted_${Date.now()}.csv`;
  const outputPath = path.join(UPLOAD_DIR, outputFileName);

  const pythonProcess = spawn('python3', [scriptPath, ...args, outputPath]);

  let errorOutput = '';
  pythonProcess.stderr.on('data', (data) => {
    errorOutput += data.toString();
  });

  pythonProcess.on('close', (code) => {
    if (code !== 0) {
      console.error(`Python script ${scriptName} failed:`, errorOutput);
      res.status(500).json({ error: 'Conversion failed', details: errorOutput });
      return; // Explicitly return to avoid TypeScript error
    }
    onSuccess(outputPath);
  });
}

// ----------------------------------------------------------------------------
// Existing CSV upload (already present)
// ----------------------------------------------------------------------------
export const uploadCSV = (req: Request, res: Response): void => {
  try {
    if (!req.file) {
      res.status(400).json({ error: 'No file uploaded' });
      return;
    }
    const absolutePath = path.resolve(req.file.path);
    res.json({ filePath: absolutePath });
  } catch (error) {
    console.error('CSV upload error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
};

// ----------------------------------------------------------------------------
// Positional file conversion
// ----------------------------------------------------------------------------
export const convertPositional = (req: Request, res: Response): void => {
  if (!req.file) {
    res.status(400).json({ error: 'No file uploaded' });
    return;
  }
  const columns = req.body.columns; // JSON string
  if (!columns) {
    res.status(400).json({ error: 'Missing columns definition' });
    return;
  }

  runPythonScript(
    'positional_to_csv.py',
    [req.file.path, columns],
    res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

// ----------------------------------------------------------------------------
// XML conversion
// ----------------------------------------------------------------------------
export const convertXml = (req: Request, res: Response): void => {
  if (!req.file) {
    res.status(400).json({ error: 'No file uploaded' });
    return;
  }
  const rowXPath = req.body.rowXPath;
  const columns = req.body.columns; // JSON string
  if (!rowXPath || !columns) {
    res.status(400).json({ error: 'Missing rowXPath or columns' });
    return;
  }

  runPythonScript(
    'xml_to_csv.py',
    [req.file.path, rowXPath, columns],
    res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

// ----------------------------------------------------------------------------
// Structured (JSON/Avro/Parquet) conversion
// ----------------------------------------------------------------------------
export const convertStructured = (req: Request, res: Response): void => {
  if (!req.file) {
    res.status(400).json({ error: 'No file uploaded' });
    return;
  }
  const format = req.body.format; // 'json', 'avro', 'parquet'
  if (!format || !['json', 'avro', 'parquet'].includes(format)) {
    res.status(400).json({ error: 'Missing or invalid format' });
    return;
  }

  runPythonScript(
    'structured_to_csv.py',
    [req.file.path, format],
    res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

// ----------------------------------------------------------------------------
// Regex conversion
// ----------------------------------------------------------------------------
export const convertRegex = (req: Request, res: Response): void => {
  if (!req.file) {
    res.status(400).json({ error: 'No file uploaded' });
    return;
  }
  const pattern = req.body.pattern;
  const flags = req.body.flags || '';
  const columns = req.body.columns || ''; // optional JSON string

  if (!pattern) {
    res.status(400).json({ error: 'Missing regex pattern' });
    return;
  }

  const args = [req.file.path, pattern, flags];
  if (columns) args.push(columns);

  runPythonScript(
    'regex_to_csv.py',
    args,
    res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

// ----------------------------------------------------------------------------
// LDIF conversion
// ----------------------------------------------------------------------------
export const convertLdif = (req: Request, res: Response): void => {
  if (!req.file) {
    res.status(400).json({ error: 'No file uploaded' });
    return;
  }

  runPythonScript(
    'ldif_to_csv.py',
    [req.file.path],
    res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

// ----------------------------------------------------------------------------
// Schema‑driven conversion (schema file + data file)
// ----------------------------------------------------------------------------
export const convertSchema = (req: Request, res: Response): void => {
  const files = req.files as { [fieldname: string]: Express.Multer.File[] };
  if (!files || !files.schemaFile || !files.dataFile) {
    res.status(400).json({ error: 'Both schemaFile and dataFile are required' });
    return;
  }

  const schemaFile = files.schemaFile[0];
  const dataFile = files.dataFile[0];
  const dataFormat = req.body.dataFormat; // e.g., 'delimited', 'positional', 'xml', ...
  const delimiter = req.body.delimiter || ',';

  if (!dataFormat) {
    res.status(400).json({ error: 'Missing dataFormat' });
    return;
  }

  runPythonScript(
    'schema_to_csv.py',
    [schemaFile.path, dataFile.path, dataFormat, delimiter],
    res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};