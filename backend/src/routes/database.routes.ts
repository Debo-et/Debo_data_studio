// backend/src/routes/database.routes.ts

import { Router, Request, Response } from 'express';
import { databaseService } from '../database/services/database.service';
import cors from 'cors';
import corsOptions from '../config/cors';
import { createForeignTableInPostgres, dropForeignTable } from '../database/services/foreign-table.service';
import { getAllMetadataEntries, saveDatabaseMetadata, deleteDatabaseMetadata } from '../database/services/metadata.service';

const router = Router();

// ===========================================================================
// CORS Preflight Handling (IMPORTANT)
// ===========================================================================

// Handle OPTIONS requests (preflight) for all routes
router.options('*', cors(corsOptions));

// ===========================================================================
// Database Connection Management
// ===========================================================================

// Test database connection
router.post('/test-connection', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { dbType, config } = req.body;
    
    if (!dbType || !config) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing required parameters: dbType and config' 
      });
      return;
    }

    const result = await databaseService.testConnection(dbType, config);
    res.json(result);
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

router.get('/metadata', cors(corsOptions), async (_req: Request, res: Response): Promise<void> => {
  try {
    const result = await getAllMetadataEntries();
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

router.post('/:connectionId/query', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { connectionId } = req.params;
    const { sql, params = [], options } = req.body;   // ✅ default to []

    if (!connectionId || !sql) {
      res.status(400).json({ success: false, error: 'Missing connectionId or sql' });
      return;
    }

    const queryOptions = { ...options, params };
    const result = await databaseService.executeQuery(connectionId, sql, queryOptions);
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : String(error) });
  }
});


// Connect to database
router.post('/connect', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { dbType, config } = req.body;
    
    if (!dbType || !config) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing required parameters: dbType and config' 
      });
      return;
    }

    const result = await databaseService.connect(dbType, config);
    res.json(result);
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

// Disconnect from database
router.delete('/:connectionId', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { connectionId } = req.params;
    
    if (!connectionId) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing connectionId' 
      });
      return;
    }

    const result = await databaseService.disconnect(connectionId);
    res.json(result);
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

// Get active connections
router.get('/connections/active', cors(corsOptions), async (_req: Request, res: Response): Promise<void> => {
  try {
    const connections = databaseService.getActiveConnections();
    res.json({ 
      success: true, 
      connections 
    });
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

// ===========================================================================
// Database Inspection Operations
// ===========================================================================

// Get tables
router.post('/tables', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { connectionId, options } = req.body;
    
    if (!connectionId) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing connectionId' 
      });
      return;
    }

    const result = await databaseService.getTables(connectionId, options);
    res.json(result);
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

// Get database information
router.get('/:connectionId/info', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { connectionId } = req.params;
    
    if (!connectionId) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing connectionId' 
      });
      return;
    }

    const result = await databaseService.getDatabaseInfo(connectionId);
    res.json(result);
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

// ===========================================================================
// Query Execution
// ===========================================================================

// Execute SQL query
// backend/src/routes/database.routes.ts

router.post('/:connectionId/query', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { connectionId } = req.params;
    const { sql, params = [], options } = req.body;   // ← default to empty array

    if (!connectionId || !sql) {
      res.status(400).json({ success: false, error: 'Missing connectionId or sql' });
      return;
    }

    const queryOptions = { ...options, params };
    const result = await databaseService.executeQuery(connectionId, sql, queryOptions);
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : String(error) });
  }
});

router.post('/metadata', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const result = await saveDatabaseMetadata(req.body);
    res.json(result);
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

// ===========================================================================
// Foreign Table Operations
// ===========================================================================

// Create foreign table
// backend/src/routes/database.routes.ts

router.post('/create-foreign-table', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { 
      connectionId, 
      tableName, 
      columns, 
      fileType, 
      filePath, 
      options,
      serverName    // <-- ADD THIS
    } = req.body;
    
    if (!connectionId || !tableName || !columns || !fileType) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing required parameters: connectionId, tableName, columns, fileType' 
      });
      return;
    }

    console.log(`📝 Creating foreign table: ${tableName} for ${fileType} source`);
    
    // Pass serverName to the service
    const result = await createForeignTableInPostgres(
      connectionId,
      tableName,
      columns,
      fileType,
      filePath || '',
      options,
      serverName    // <-- ADD THIS
    );

    res.json(result);
  } catch (error) {
    console.error('Error creating foreign table:', error);
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

// Drop foreign table
router.delete('/foreign-tables/:tableName', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { tableName } = req.params;
    
    if (!tableName) {
      res.status(400).json({ 
        success: false, 
        error: 'Missing tableName parameter' 
      });
      return;
    }

    const result = await dropForeignTable(tableName);
    res.json(result);
  } catch (error) {
    res.status(500).json({ 
      success: false, 
      error: error instanceof Error ? error.message : String(error) 
    });
  }
});

router.delete('/metadata/:id', cors(corsOptions), async (req: Request, res: Response): Promise<void> => {
  try {
    const { id } = req.params;
    if (!id) {
      res.status(400).json({ success: false, error: 'Missing metadata ID' });
      return;
    }
    const result = await deleteDatabaseMetadata(id);
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error instanceof Error ? error.message : String(error) });
  }
});

export default router;