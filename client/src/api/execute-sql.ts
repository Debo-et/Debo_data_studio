// backend/api/database/execute-sql.ts
import express from 'express';
import { Pool } from 'pg';

const router = express.Router();

// PostgreSQL connection pool
const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT || '5432'),
  database: process.env.PG_DATABASE || 'postgres',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || '',
  max: 10,
  idleTimeoutMillis: 30000,
});

router.post('/execute-sql', async (req, res) => {
  const { sql } = req.body;
  
  if (!sql) {
    return res.status(400).json({
      success: false,
      error: 'No SQL provided'
    });
  }

  const client = await pool.connect();
  
  try {
    const startTime = Date.now();
    const result = await client.query(sql);
    const executionTime = Date.now() - startTime;
    
    res.json({
      success: true,
      data: {
        rowCount: result.rowCount,
        rows: result.rows,
        fields: result.fields,
        command: result.command,
        oid: result.oid,
        executionTime
      }
    });
  } catch (error: any) {
    console.error('SQL execution error:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      detail: error.detail,
      code: error.code
    });
  } finally {
    client.release();
  }
});

export default router;