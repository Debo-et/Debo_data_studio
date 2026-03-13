// backend/src/app.ts

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import morgan from 'morgan';
import databaseRoutes from './src/routes/database.routes';
import { localPostgres } from './src/database/local-postgres';
import corsOptions from './src/config/cors'; // Import the CORS configuration

const app = express();

// Middleware
app.use(helmet()); // Security headers
app.use(cors(corsOptions)); // Apply CORS configuration
app.use(compression()); // Compress responses
app.use(morgan('combined')); // HTTP request logging
app.use(express.json()); // Parse JSON bodies
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded bodies

// Routes
app.use('/api/database', databaseRoutes);

// Health check with PostgreSQL status
app.get('/health', async (_req, res) => {
  try {
    const postgresStatus = localPostgres.getStatus();
    const isHealthy = postgresStatus.connected && await localPostgres.testConnection();
    
    // Add CORS headers explicitly
    res.setHeader('Access-Control-Allow-Origin', 'http://localhost:3001');
    res.setHeader('Access-Control-Allow-Credentials', 'true');
    
    res.json({ 
      status: isHealthy ? 'OK' : 'DEGRADED',
      timestamp: new Date().toISOString(),
      service: 'Database Metadata Inspector API',
      postgres: {
        ...postgresStatus,
        healthy: isHealthy
      }
    });
  } catch (error) {
    res.status(503).json({ 
      status: 'ERROR',
      timestamp: new Date().toISOString(),
      service: 'Database Metadata Inspector API',
      error: 'PostgreSQL connection failed',
      postgres: localPostgres.getStatus()
    });
  }
});

// PostgreSQL connection status endpoint
app.get('/api/postgres/status', (_req, res) => {
  try {
    const status = localPostgres.getStatus();
    res.json({
      success: true,
      ...status,
      message: status.connected ? 'PostgreSQL connection is active' : 'PostgreSQL connection is not available'
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to get PostgreSQL status'
    });
  }
});

// PostgreSQL query endpoint (for internal use)
app.post('/api/postgres/query', async (req, res) => {
  const { sql, params } = req.body;
  
  if (!sql) {
    return res.status(400).json({ 
      success: false, 
      error: 'SQL query is required' 
    });
  }
  
  try {
    const result = await localPostgres.query(sql, params);
    res.json({
      success: true,
      rows: result.rows,
      rowCount: result.rowCount,
      fields: result.fields
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

// 404 handler
app.use((_req, res) => {
  res.status(404).json({ 
    success: false, 
    error: 'Route not found' 
  });
});

// Error handler
app.use((error: any, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error('Error:', error);
  
  res.status(error.status || 500).json({
    success: false,
    error: process.env.NODE_ENV === 'production' 
      ? 'Internal server error' 
      : error.message
  });
});

export default app;