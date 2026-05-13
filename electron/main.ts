import { app, BrowserWindow, shell, dialog } from 'electron';
import path from 'path';
import { spawn, ChildProcess } from 'child_process';
import fs from 'fs';
import http from 'http';

const isDev = process.env.NODE_ENV === 'development' || !app.isPackaged;

let mainWindow: BrowserWindow | null = null;
let backendProcess: ChildProcess | null = null;

// --- Production backend path (compiled JS) ---
function getBackendScriptPath(): string {
  if (isDev) {
    return ''; // not used in dev mode
  }
  // In packaged app, backend is copied to resources/backend
  // The entry point is dist/server.js (or dist/app.js – check your backend)
  return path.join(process.resourcesPath, 'backend/dist/server.js');
}

function getBackendCwd(): string | undefined {
  if (isDev) {
    return path.join(__dirname, '..');
  }
  // Working directory is the backend root (contains node_modules, dist, etc.)
  return path.join(process.resourcesPath, 'backend');
}

const BACKEND_SCRIPT = getBackendScriptPath();
const FRONTEND_URL = isDev
  ? 'http://localhost:3001'
  : `file://${path.join(__dirname, '../client/dist/index.html')}`;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1024,
    minHeight: 768,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      nodeIntegration: false,
      contextIsolation: true,
    },
    show: false,
    backgroundColor: '#ffffff',
  });

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  mainWindow.loadURL(FRONTEND_URL);

  mainWindow.once('ready-to-show', () => {
    mainWindow?.show();
  });

  if (isDev) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

async function startBackend(): Promise<void> {
  return new Promise((resolve, reject) => {
    if (!isDev && !fs.existsSync(BACKEND_SCRIPT)) {
      dialog.showErrorBox(
        'Backend Missing',
        `Cannot find backend server at:\n${BACKEND_SCRIPT}\n\nPlease ensure the backend is built.`
      );
      app.quit();
      return reject(new Error('Backend script not found'));
    }

    const backendCommand = isDev ? 'npm' : 'node';
    const backendArgs = isDev
      ? ['run', 'dev:full']
      : [BACKEND_SCRIPT];

    const cwd = getBackendCwd();

    backendProcess = spawn(backendCommand, backendArgs, {
      cwd,
      shell: false,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, ELECTRON_RUNNING: 'true' },
    });

    backendProcess.stdout?.on('data', (data) => {
      console.log(`[backend] ${data}`);
    });

    backendProcess.stderr?.on('data', (data) => {
      console.error(`[backend] ${data}`);
    });

    backendProcess.on('error', (err) => {
      console.error('Failed to start backend:', err);
      reject(err);
    });

    const checkHealth = () => {
      http.get('http://localhost:3000/health', (res) => {
        let data = '';
        res.on('data', (chunk) => (data += chunk));
        res.on('end', () => {
          try {
            const status = JSON.parse(data).status;
            if (status === 'OK' || status === 'DEGRADED') {
              console.log('✅ Backend is healthy');
              resolve();
            } else {
              setTimeout(checkHealth, 500);
            }
          } catch {
            setTimeout(checkHealth, 500);
          }
        });
      }).on('error', () => {
        setTimeout(checkHealth, 500);
      });
    };

    setTimeout(checkHealth, 1000);
  });
}

app.whenReady().then(async () => {
  try {
    await startBackend();
    createWindow();
  } catch (error) {
    console.error('Startup error:', error);
    app.quit();
  }
});

app.on('window-all-closed', () => {
  if (backendProcess) {
    backendProcess.kill();
  }
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (mainWindow === null) {
    createWindow();
  }
});

app.on('before-quit', () => {
  if (backendProcess) {
    backendProcess.kill();
  }
});