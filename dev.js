#!/usr/bin/env node

const { spawn } = require('child_process');
const readline = require('readline');
const http = require('http');

const BACKEND_HEALTH_URL = 'http://localhost:3000/health';
const BACKEND_START_TIMEOUT = 30000; // 30 seconds
const HEALTH_CHECK_INTERVAL = 500;    // 0.5 seconds

let backendProcess = null;
let frontendProcess = null;

// Helper to prefix output lines
function prefixStream(stream, prefix) {
  const rl = readline.createInterface({ input: stream });
  rl.on('line', (line) => {
    console.log(`${prefix} ${line}`);
  });
}

// Start backend process
console.log('[system] Starting backend...');
backendProcess = spawn('npm', ['run', 'dev:full'], {
  cwd: './backend',
  shell: true,
  stdio: ['ignore', 'pipe', 'pipe']
});

prefixStream(backendProcess.stdout, '[backend]');
prefixStream(backendProcess.stderr, '[backend]');

// Wait for backend to become healthy
let healthy = false;
const startTime = Date.now();

function checkHealth() {
  if (Date.now() - startTime > BACKEND_START_TIMEOUT) {
    console.error('[system] Backend did not become healthy within timeout');
    cleanup(1);
    return;
  }

  http.get(BACKEND_HEALTH_URL, (res) => {
    let data = '';
    res.on('data', chunk => data += chunk);
    res.on('end', () => {
      try {
        const status = JSON.parse(data).status;
        if (status === 'OK' || status === 'DEGRADED') {
          healthy = true;
          console.log('[system] Backend is healthy, starting frontend...');
          startFrontend();
        } else {
          setTimeout(checkHealth, HEALTH_CHECK_INTERVAL);
        }
      } catch {
        setTimeout(checkHealth, HEALTH_CHECK_INTERVAL);
      }
    });
  }).on('error', () => {
    setTimeout(checkHealth, HEALTH_CHECK_INTERVAL);
  });
}

checkHealth();

function startFrontend() {
  frontendProcess = spawn('npm', ['run', 'dev'], {
    cwd: './client',
    shell: true,
    stdio: ['ignore', 'pipe', 'pipe']
  });

  prefixStream(frontendProcess.stdout, '[frontend]');
  prefixStream(frontendProcess.stderr, '[frontend]');

  // When frontend exits, kill backend and exit with its code
  frontendProcess.on('exit', (code) => {
    if (code !== 0 && code !== null) {
      console.error(`[system] Frontend exited with code ${code}`);
    }
    cleanup(code);
  });
}

// Handle backend exit
backendProcess.on('exit', (code) => {
  if (!healthy) {
    console.error('[system] Backend crashed before becoming healthy');
    cleanup(code || 1);
  } else if (code !== 0 && code !== null) {
    console.error(`[system] Backend exited with code ${code}`);
    cleanup(code);
  }
});

// Cleanup: kill both processes and exit
function cleanup(exitCode = 0) {
  if (backendProcess && !backendProcess.killed) {
    backendProcess.kill();
  }
  if (frontendProcess && !frontendProcess.killed) {
    frontendProcess.kill();
  }
  process.exit(exitCode);
}

// Handle manual interruption (Ctrl+C)
process.on('SIGINT', () => {
  console.log('\n[system] Shutting down...');
  cleanup(0);
});