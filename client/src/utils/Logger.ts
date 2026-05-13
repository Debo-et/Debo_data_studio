// src/utils/Logger.ts
import * as fs from 'fs';
import * as path from 'path';

export enum LogLevel {
  NONE = 0,
  ERROR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4,
}

export class Logger {
  private level: LogLevel;
  private stream: fs.WriteStream | null = null;

  constructor(level: LogLevel = LogLevel.INFO, logFilePath?: string) {
    this.level = level;
    if (logFilePath) {
      const dir = path.dirname(logFilePath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      this.stream = fs.createWriteStream(logFilePath, { flags: 'a' });
    }
  }

  private write(levelStr: string, message: string, data?: any) {
    const timestamp = new Date().toISOString();
    const logLine = `[${timestamp}] [${levelStr}] ${message}${data ? ' ' + JSON.stringify(data, null, 2) : ''}`;
    console.log(logLine);
    if (this.stream) {
      this.stream.write(logLine + '\n');
    }
  }

  error(message: string, data?: any) {
    if (this.level >= LogLevel.ERROR) this.write('ERROR', message, data);
  }
  warn(message: string, data?: any) {
    if (this.level >= LogLevel.WARN) this.write('WARN', message, data);
  }
  info(message: string, data?: any) {
    if (this.level >= LogLevel.INFO) this.write('INFO', message, data);
  }
  debug(message: string, data?: any) {
    if (this.level >= LogLevel.DEBUG) this.write('DEBUG', message, data);
  }

  close() {
    if (this.stream) this.stream.end();
  }
}

// Global singleton – can be set via environment variable
export let globalLogger = new Logger(
  process.env.LOG_LEVEL ? LogLevel[process.env.LOG_LEVEL as keyof typeof LogLevel] : LogLevel.INFO,
  process.env.LOG_FILE
);

export function setGlobalLogger(logger: Logger) {
  globalLogger = logger;
}