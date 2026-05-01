// backend/src/controllers/upload.controller.ts
import { Request, Response } from 'express';
import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';

// ─── Configuration constants ────────────────────────────────────────────────
const UPLOAD_DIR = process.env.CSV_UPLOAD_DIR || path.join(__dirname, '../../uploads');

// ─── Resolve Python executable with fallbacks and logging ───────────────────
const PYTHON_EXECUTABLE = (() => {
  if (process.env.PYTHON_EXECUTABLE) {
    const resolved = path.resolve(process.env.PYTHON_EXECUTABLE);
    if (fs.existsSync(resolved)) {
      console.log(`🐍 Using Python from PYTHON_EXECUTABLE: ${resolved}`);
      return resolved;
    }
    console.warn(`⚠️  PYTHON_EXECUTABLE set but file not found: ${resolved}`);
  }

  const projectRoot = path.resolve(__dirname, '../../');
  const venvPython = path.join(projectRoot, 'venv', 'bin', 'python3');
  if (fs.existsSync(venvPython)) {
    console.log(`🐍 Using venv Python: ${venvPython}`);
    return venvPython;
  }

  console.log('🐍 Falling back to system python3');
  return 'python3';
})();

// ─── Robust script directory resolution ────────────────────────────────────
function resolveScriptsDir(): string {
  if (process.env.PYTHON_SCRIPTS_DIR) {
    const dir = path.resolve(process.env.PYTHON_SCRIPTS_DIR);
    if (fs.existsSync(dir)) {
      console.log(`📂 Using scripts dir from env: ${dir}`);
      return dir;
    }
    console.warn(`⚠️  PYTHON_SCRIPTS_DIR not found: ${dir}`);
  }

  const projectRoot = path.resolve(__dirname, '../../../');
  const scriptsDir = path.join(projectRoot, 'scripts');
  if (fs.existsSync(scriptsDir)) {
    console.log(`📂 Using scripts dir: ${scriptsDir}`);
    return scriptsDir;
  }

  console.warn('⚠️  Scripts dir not found, using CWD/scripts');
  return path.join(process.cwd(), 'scripts');
}

const SCRIPTS_DIR = resolveScriptsDir();

// ─── Ensure upload directory exists ─────────────────────────────────────────
if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR, { recursive: true, mode: 0o755 });
  console.log(`📁 Created upload directory: ${UPLOAD_DIR}`);
}

// ─── Helper: get script path (throws if missing) ────────────────────────────
function getScriptPath(scriptName: string): string {
  const fullPath = path.join(SCRIPTS_DIR, scriptName);
  if (!fs.existsSync(fullPath)) {
    throw new Error(`Script not found: ${fullPath}`);
  }
  return fullPath;
}

// ─── Enhanced helper to run a Python script ─────────────────────────────────
function runPythonScript(
  scriptName: string,
  args: string[],
  res: Response,
  onSuccess: (outputPath: string) => void
): void {
  let scriptPath: string;
  try {
    scriptPath = getScriptPath(scriptName);
  } catch (err: any) {
    console.error(`❌ ${err.message}`);
    res.status(500).json({ error: err.message });
    return; // early exit
  }

  const outputFileName = `converted_${Date.now()}.csv`;
  const outputPath = path.join(UPLOAD_DIR, outputFileName);

  // ─── Diagnostic dump ────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════════════');
  console.log('📄 Python Conversion Request');
  console.log('══════════════════════════════════════════════════════');
  console.log(`🕒 Timestamp        : ${new Date().toISOString()}`);
  console.log(`📜 Script Name      : ${scriptName}`);
  console.log(`📂 Script Path      : ${scriptPath}`);
  console.log(`🐍 Python Executable: ${PYTHON_EXECUTABLE}`);
  console.log(`🧾 Arguments        : ${JSON.stringify(args)}`);
  console.log(`📁 Upload Dir       : ${UPLOAD_DIR}`);
  console.log(`📤 Output File      : ${outputPath}`);
  console.log(`💻 Node Env         : ${process.env.NODE_ENV || 'not set'}`);
  console.log('══════════════════════════════════════════════════════\n');

  const pythonProcess = spawn(PYTHON_EXECUTABLE, [scriptPath, ...args, outputPath], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env },
  });

  let stdout = '';
  let stderr = '';

  pythonProcess.stdout.on('data', (data: Buffer) => {
    const chunk = data.toString();
    stdout += chunk;
    process.stdout.write(`[py:out] ${chunk}`);
  });

  pythonProcess.stderr.on('data', (data: Buffer) => {
    const chunk = data.toString();
    stderr += chunk;
    process.stderr.write(`[py:err] ${chunk}`);
  });

  pythonProcess.on('error', (err: NodeJS.ErrnoException) => {
    console.error(`❌ Failed to spawn Python process: ${err.message}`);
    res.status(500).json({
      error: `Failed to start Python process: ${err.message}`,
      details: {
        script: scriptPath,
        code: err.code,
        syscall: err.syscall,
        path: err.path,
      },
    });
  });

  pythonProcess.on('close', (code: number | null): void => {
    console.log(`🏁 Python process exited with code ${code}`);

    if (code !== 0) {
      const errorReport = {
        error: 'Python conversion script failed',
        exitCode: code,
        script: scriptPath,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
        state: {
          scriptExists: fs.existsSync(scriptPath),
          pythonExecutable: PYTHON_EXECUTABLE,
          args,
          outputFile: outputPath,
        },
      };
      console.error('❌ Conversion failed. Full details:', JSON.stringify(errorReport, null, 2));
      res.status(500).json(errorReport);
      return;
    }

    console.log(`✅ Conversion successful → ${outputPath}`);
    if (!fs.existsSync(outputPath)) {
      console.error('⚠️  Output file was NOT created even though script exited with 0.');
      res.status(500).json({
        error: 'Conversion script finished successfully but output file is missing.',
        expectedOutput: outputPath,
      });
      return;
    }

    onSuccess(outputPath);
  });
}

// ─── Upload and conversion handlers ─────────────────────────────────────────

export const uploadCSV = (req: Request, res: Response): void => {
  try {
    if (!req.file) {
      res.status(400).json({ error: 'No file uploaded' });
      return;
    }
    const absolutePath = path.resolve(req.file.path);
    console.log(`📥 CSV upload received: ${absolutePath}`);
    res.json({ filePath: absolutePath });
  } catch (error: any) {
    console.error('CSV upload error:', error);
    res.status(500).json({ error: 'Internal server error', details: error.message });
  }
};

export const convertPositional = (req: Request, res: Response): void => {
  if (!req.file) { res.status(400).json({ error: 'No file uploaded' }); return; }
  const columns = req.body.columns;
  if (!columns) { res.status(400).json({ error: 'Missing columns definition' }); return; }

  runPythonScript('positional_to_csv.py', [req.file.path, columns], res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

export const convertXml = (req: Request, res: Response): void => {
  if (!req.file) { res.status(400).json({ error: 'No file uploaded' }); return; }
  const rowXPath = req.body.rowXPath;
  const columns = req.body.columns;
  if (!rowXPath || !columns) { res.status(400).json({ error: 'Missing rowXPath or columns' }); return; }

  runPythonScript('xml_to_csv.py', [req.file.path, rowXPath, columns], res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

export const convertStructured = (req: Request, res: Response): void => {
  if (!req.file) { res.status(400).json({ error: 'No file uploaded' }); return; }
  const format = req.body.format;
  if (!format || !['json', 'avro', 'parquet'].includes(format)) {
    res.status(400).json({ error: 'Missing or invalid format' }); return;
  }

  runPythonScript('structured_to_csv.py', [req.file.path, format], res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

export const convertRegex = (req: Request, res: Response): void => {
  if (!req.file) { res.status(400).json({ error: 'No file uploaded' }); return; }
  const pattern = req.body.pattern;
  if (!pattern) { res.status(400).json({ error: 'Missing regex pattern' }); return; }
  const flags = req.body.flags || '';
  const columns = req.body.columns || '';

  const args = [req.file.path, pattern, flags];
  if (columns) args.push(columns);

  runPythonScript('regex_to_csv.py', args, res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

export const convertLdif = (req: Request, res: Response): void => {
  if (!req.file) { res.status(400).json({ error: 'No file uploaded' }); return; }

  runPythonScript('ldif_to_csv.py', [req.file.path], res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};

export const convertSchema = (req: Request, res: Response): void => {
  const files = req.files as { [fieldname: string]: Express.Multer.File[] };
  if (!files || !files.schemaFile || !files.dataFile) {
    res.status(400).json({ error: 'Both schemaFile and dataFile are required' }); return;
  }
  const schemaFile = files.schemaFile[0];
  const dataFile = files.dataFile[0];
  const dataFormat = req.body.dataFormat;
  const delimiter = req.body.delimiter || ',';

  if (!dataFormat) { res.status(400).json({ error: 'Missing dataFormat' }); return; }

  runPythonScript('schema_to_csv.py', [schemaFile.path, dataFile.path, dataFormat, delimiter], res,
    (outputPath) => res.json({ filePath: outputPath })
  );
};