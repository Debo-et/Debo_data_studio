// backend/src/app.ts

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import morgan from 'morgan';
import databaseRoutes from './src/routes/database.routes';
import uploadRoutes from './src/routes/upload.routes'; // File upload & conversion routes
import { localPostgres } from './src/database/local-postgres';
const app = express();

// ---------------------------------------------------------------------------
// CORS Configuration – MUST be applied before any route definitions
// ---------------------------------------------------------------------------
const corsOptions: cors.CorsOptions = {
  origin: process.env.CORS_ORIGIN || 'http://localhost:3001', // your frontend dev server
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: false, // set to true if you send cookies / auth headers; then origin must be an exact string, not '*'
};

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------
app.use(helmet()); // Security headers
app.use(cors(corsOptions)); // Must be before compression and routes
app.use(compression()); // Compress responses
app.use(morgan('combined')); // HTTP request logging
app.use(express.json()); // Parse JSON bodies
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded bodies

// ---------------------------------------------------------------------------
// API Routes
// ---------------------------------------------------------------------------
app.use('/api/database', databaseRoutes);
app.use('/api', uploadRoutes); // Mounts /api/upload-csv, /api/convert/positional, etc.

// ---------------------------------------------------------------------------
// Health check endpoint
// ---------------------------------------------------------------------------
app.get('/health', async (_req, res) => {
  try {
    const postgresStatus = localPostgres.getStatus();
    const isHealthy = postgresStatus.connected && await localPostgres.testConnection();

    // Optional: check if the canvases table exists (schema readiness)
    let schemaReady = false;
    if (isHealthy) {
      try {
        const result = await localPostgres.query(`
          SELECT 1 FROM information_schema.tables 
          WHERE table_name = 'canvases' 
          AND table_schema = 'public'
        `);
        schemaReady = result.rowCount > 0;
      } catch (err) {
        console.warn('Schema readiness check failed:', err);
      }
    }

    return res.json({
      status: isHealthy && schemaReady ? 'OK' : 'DEGRADED',
      timestamp: new Date().toISOString(),
      service: 'Database Metadata Inspector API',
      postgres: {
        ...postgresStatus,
        healthy: isHealthy,
        schemaReady,
      },
      success: true,
      message: schemaReady ? 'Backend fully ready' : 'Backend starting up...',
    });
  } catch (error) {
    return res.status(503).json({
      status: 'ERROR',
      timestamp: new Date().toISOString(),
      service: 'Database Metadata Inspector API',
      error: 'PostgreSQL connection failed',
      postgres: localPostgres.getStatus(),
      success: false,
    });
  }
});

// ---------------------------------------------------------------------------
// PostgreSQL status endpoint (internal)
// ---------------------------------------------------------------------------
app.get('/api/postgres/status', (_req, res) => {
  try {
    const status = localPostgres.getStatus();
    return res.json({
      success: true,
      ...status,
      message: status.connected
        ? 'PostgreSQL connection is active'
        : 'PostgreSQL connection is not available',
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: 'Failed to get PostgreSQL status',
    });
  }
});

// ---------------------------------------------------------------------------
// PostgreSQL query endpoint (internal)
// ---------------------------------------------------------------------------
app.post('/api/postgres/query', async (req, res) => {
  const { sql, params } = req.body;

  if (!sql) {
    return res.status(400).json({
      success: false,
      error: 'SQL query is required',
    });
  }

  try {
    const result = await localPostgres.query(sql, params);
    return res.json({
      success: true,
      rows: result.rows,
      rowCount: result.rowCount,
      fields: result.fields,
    });
  } catch (error) {
    return res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// ---------------------------------------------------------------------------
// 404 handler
// ---------------------------------------------------------------------------
app.use((_req, res) => {
  return res.status(404).json({
    success: false,
    error: 'Route not found',
  });
});

// ---------------------------------------------------------------------------
// Global error handler
// ---------------------------------------------------------------------------
app.use((error: any, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error('Error:', error);

  return res.status(error.status || 500).json({
    success: false,
    error: process.env.NODE_ENV === 'production'
      ? 'Internal server error'
      : error.message,
  });
});

export default app;