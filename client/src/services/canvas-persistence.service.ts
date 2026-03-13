// src/services/canvas-persistence.service.ts

import { databaseApi, DatabaseApiService } from './database-api.service';
import { Node, Edge, Viewport } from 'reactflow';
import { ClientConnectResult } from './database-api.types';

// Type for canvas data to store
export interface CanvasPersistedData {
  reactFlow: {
    nodes: Node[];
    edges: Edge[];
    viewport: Viewport;
  };
  metadata: {
    description?: string;
    tags?: string[];
    compilerMetadata?: any;
    savedBy?: string;
    lastSaved?: string;
    version?: number;
    otherUiState?: Record<string, any>;
  };
}

// Canvas record from database
export interface CanvasRecord {
  id: string;
  name: string;
  data: CanvasPersistedData;
  version: number;
  created_at: string;
  updated_at: string;
  owner_id?: string;
}

export class CanvasPersistenceService {
  private api: DatabaseApiService;
  private isInitialized = false;

  constructor() {
    this.api = databaseApi;
    this.initialize().catch(console.error);
  }

  // Initialize the service (no table creation)
  private async initialize(): Promise<void> {
    if (this.isInitialized) return;

    try {
      console.log('🔧 Initializing canvas persistence service...');
      // Table is assumed to exist – creation is handled on first query
      this.isInitialized = true;
      console.log('✅ Canvas persistence service initialized');
    } catch (error) {
      console.error('❌ Failed to initialize canvas persistence:', error);
    }
  }

  // ==================== SAVE CANVAS ====================
  // src/services/canvas-persistence.service.ts
// Inside saveCanvas method, replace the existing check with direct insert

async saveCanvas(
  name: string,
  reactFlow: { nodes: Node[]; edges: Edge[]; viewport: Viewport },
  metadata: Omit<CanvasPersistedData['metadata'], 'lastSaved' | 'savedBy' | 'version'> = {},
  ownerId: string | null = null
): Promise<CanvasRecord | null> {
  try {
    if (!this.isInitialized) {
      await this.initialize();
    }

    // 🚫 Do NOT check for existing canvas with same name
    // Always insert a new record

    const canvasData: CanvasPersistedData = {
      reactFlow,
      metadata: {
        ...metadata,
        lastSaved: new Date().toISOString(),
        savedBy: this.getCurrentUser(),
        version: 1
      }
    };

    const insertQuery = `
      INSERT INTO canvases (name, data, version, owner_id, created_at, updated_at)
      VALUES ($1, $2::jsonb, 1, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      RETURNING *
    `;

    const result = await this.executeAppDbQuery(insertQuery, [
      name,
      JSON.stringify(canvasData),
      ownerId
    ]);

    if (result.rows.length === 0) {
      console.error('❌ No rows returned from INSERT – query may have failed silently');
      return null;
    }

    return this.mapCanvasRecord(result.rows[0]);
  } catch (error) {
    console.error('Error saving canvas:', error);
    throw error;
  }
}

  // ==================== GET CANVAS BY NAME ====================
  async getCanvasByName(name: string, ownerId: string | null = null): Promise<CanvasRecord | null> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const query = `
        SELECT * FROM canvases 
        WHERE name = $1 AND (owner_id = $2 OR ($2 IS NULL AND owner_id IS NULL))
        ORDER BY version DESC 
        LIMIT 1
      `;

      const result = await this.executeAppDbQuery(query, [name, ownerId]);

      if (result.rows.length > 0) {
        return this.mapCanvasRecord(result.rows[0]);
      }

      return null;
    } catch (error) {
      console.error('Error getting canvas by name:', error);
      return null;
    }
  }

  // ==================== GET CANVAS BY ID (data only) ====================
  async getCanvas(id: string): Promise<CanvasPersistedData | null> {
    try {
      if (!this.isInitialized) await this.initialize();
      console.log(`📂 [CanvasPersistence] Fetching canvas with id: ${id}`);
      const query = 'SELECT data FROM canvases WHERE id = $1';
      const result = await this.executeAppDbQuery(query, [id]);

      if (result.rows.length > 0) {
        try {
          const data = result.rows[0].data;
          console.log(`✅ [CanvasPersistence] Canvas data retrieved. Nodes: ${data.reactFlow?.nodes?.length || 0}, Edges: ${data.reactFlow?.edges?.length || 0}`);
          return data;
        } catch (parseError) {
          console.error('❌ [CanvasPersistence] Malformed JSON in canvas data:', parseError);
          return null;
        }
      }
      console.warn(`⚠️ [CanvasPersistence] No canvas found with id: ${id}`);
      return null;
    } catch (error) {
      console.error('❌ [CanvasPersistence] Error getting canvas by ID:', error);
      throw error;
    }
  }

  // ==================== UPDATE CANVAS BY ID ====================
  async updateCanvas(
    id: string,
    reactFlow: { nodes: Node[]; edges: Edge[]; viewport: Viewport }
  ): Promise<CanvasRecord | null> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      // First get the current canvas to preserve metadata
      const existing = await this.getCanvasById(id);
      if (!existing) {
        throw new Error(`Canvas with id ${id} not found`);
      }

      const updatedData: CanvasPersistedData = {
        reactFlow,
        metadata: {
          ...existing.data.metadata,
          lastSaved: new Date().toISOString(),
          savedBy: this.getCurrentUser(),
          version: existing.data.metadata.version ? existing.data.metadata.version + 1 : 2
        }
      };

      const updateQuery = `
        UPDATE canvases 
        SET data = $1::jsonb,
            version = version + 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $2
        RETURNING *
      `;

      const result = await this.executeAppDbQuery(updateQuery, [
        JSON.stringify(updatedData),
        id
      ]);

      if (result.rows.length === 0) {
        throw new Error(`Update failed: canvas with id ${id} not found`);
      }

      return this.mapCanvasRecord(result.rows[0]);
    } catch (error) {
      console.error('Error updating canvas:', error);
      throw error;
    }
  }

  // ==================== LOAD CANVAS (by name) ====================
  async loadCanvas(name: string): Promise<CanvasPersistedData | null> {
    try {
      const record = await this.getCanvasByName(name);
      return record ? record.data : null;
    } catch (error) {
      console.error('Error loading canvas:', error);
      throw error;
    }
  }

  // ==================== LIST ALL CANVASES ====================
  async listCanvases(): Promise<{id: string; name: string; updated_at: string}[]> {
    try {
      if (!this.isInitialized) await this.initialize();
      console.log('📋 [CanvasPersistence] Executing listCanvases query...');
      const query = `SELECT id, name, updated_at FROM canvases ORDER BY updated_at DESC`;
      const result = await this.executeAppDbQuery(query);
      console.log(`📋 [CanvasPersistence] Query returned ${result.rows.length} rows.`);
      return result.rows.map((row: any) => ({
        id: row.id,
        name: row.name,
        updated_at: row.updated_at
      }));
    } catch (error) {
      console.error('❌ [CanvasPersistence] Error listing canvases:', error);
      throw error;
    }
  }

  // ==================== DELETE CANVAS ====================
  async deleteCanvas(name: string): Promise<boolean> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const query = `
        DELETE FROM canvases 
        WHERE name = $1 
        RETURNING id
      `;

      const result = await this.executeAppDbQuery(query, [name]);

      return result.rows.length > 0;
    } catch (error) {
      console.error('Error deleting canvas:', error);
      throw error;
    }
  }

  // ==================== UPDATE CANVAS METADATA ====================
  async updateCanvasMetadata(
    name: string,
    metadata: Partial<CanvasPersistedData['metadata']>
  ): Promise<boolean> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const existing = await this.getCanvasByName(name);

      if (!existing) {
        return false;
      }

      const updatedData: CanvasPersistedData = {
        ...existing.data,
        metadata: {
          ...existing.data.metadata,
          ...metadata,
          lastSaved: new Date().toISOString()
        }
      };

      const query = `
        UPDATE canvases 
        SET data = $1::jsonb,
            updated_at = CURRENT_TIMESTAMP
        WHERE name = $2
        RETURNING id
      `;

      const result = await this.executeAppDbQuery(query, [
        JSON.stringify(updatedData),
        name
      ]);

      return result.rows.length > 0;
    } catch (error) {
      console.error('Error updating canvas metadata:', error);
      throw error;
    }
  }

  // ==================== GET CANVAS BY ID (full record) ====================
  async getCanvasById(id: string): Promise<CanvasRecord | null> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const query = 'SELECT * FROM canvases WHERE id = $1';
      const result = await this.executeAppDbQuery(query, [id]);

      if (result.rows.length > 0) {
        return this.mapCanvasRecord(result.rows[0]);
      }

      return null;
    } catch (error) {
      console.error('Error getting canvas by ID:', error);
      throw error;
    }
  }

  // ==================== GET CANVAS VERSIONS ====================
  async getCanvasVersions(name: string): Promise<{id: string; version: number; created_at: string}[]> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const query = `
        SELECT id, version, created_at 
        FROM canvases 
        WHERE name = $1 
        ORDER BY version DESC
      `;

      const result = await this.executeAppDbQuery(query, [name]);

      return result.rows.map((row: any) => ({
        id: row.id,
        version: row.version,
        created_at: row.created_at
      }));
    } catch (error) {
      console.error('Error getting canvas versions:', error);
      throw error;
    }
  }

  // ==================== RESTORE CANVAS VERSION ====================
  async restoreCanvasVersion(name: string, version: number): Promise<CanvasRecord | null> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const query = `
        SELECT * FROM canvases 
        WHERE name = $1 AND version = $2
        LIMIT 1
      `;

      const result = await this.executeAppDbQuery(query, [name, version]);

      if (result.rows.length > 0) {
        const canvasData = result.rows[0].data;

        // Save as new version
        return await this.saveCanvas(name, canvasData.reactFlow, {
          description: `Restored from version ${version}`,
          tags: canvasData.metadata.tags,
          compilerMetadata: canvasData.metadata.compilerMetadata,
          otherUiState: canvasData.metadata.otherUiState
        });
      }

      return null;
    } catch (error) {
      console.error('Error restoring canvas version:', error);
      throw error;
    }
  }

  // ==================== GET CANVAS BY JOB ID ====================
  async getCanvasByJobId(jobId: string): Promise<CanvasRecord | null> {
    try {
      if (!this.isInitialized) {
        await this.initialize();
      }

      const query = `
        SELECT * FROM canvases 
        WHERE name LIKE $1 
        ORDER BY version DESC 
        LIMIT 1
      `;

      const result = await this.executeAppDbQuery(query, [`auto_${jobId}_%`]);

      if (result.rows.length > 0) {
        return this.mapCanvasRecord(result.rows[0]);
      }

      return null;
    } catch (error) {
      console.error('Error getting canvas by job ID:', error);
      return null;
    }
  }

  // ==================== EXECUTE APP DB QUERY (with table creation fallback) ====================
  private async executeAppDbQuery(sql: string, params: any[] = []): Promise<{ rows: any[]; rowCount?: number }> {
  try {
    const connectionId = await this.getAppDbConnectionId();

    if (!connectionId) {
      throw new Error('Application database connection not available');
    }

    console.log('📝 Executing SQL:', sql, 'with params:', params);

    const queryResult: any = await this.api.executeQuery(
      connectionId,
      sql,
      { params, autoDisconnect: false }
    );

    // 🔍 Log the raw response to see the actual structure
    console.log('🔍 Raw queryResult:', JSON.stringify(queryResult, null, 2).substring(0, 500));

    if (!queryResult.success) {
      const errorMsg = queryResult.error || '';
      // Check for missing table
      if (errorMsg.includes('relation "canvases" does not exist')) {
        console.log('⚠️ Canvases table does not exist – attempting to create it...');
        await this.ensureCanvasTableExists(connectionId);
        // Retry the original query
        return this.executeAppDbQuery(sql, params);
      }
      throw new Error(queryResult.error || 'Query execution failed');
    }

    // --- Extract rows from various possible locations ---
    let rows: any[] = [];
    let rowCount: number | undefined;

    // 1. Direct rows property (standard)
    if (Array.isArray(queryResult.rows)) {
      rows = queryResult.rows;
      rowCount = queryResult.rowCount;
    }
    // 2. Nested under data (backend wrapper)
    else if (queryResult.data && Array.isArray(queryResult.data.rows)) {
      rows = queryResult.data.rows;
      rowCount = queryResult.data.rowCount;
    }
    // 3. Nested under result (some backends)
    else if (queryResult.result && Array.isArray(queryResult.result.rows)) {
      rows = queryResult.result.rows;
      rowCount = queryResult.result.rowCount;
    }
    // 4. Fallback: find any array property
    else {
      for (const key in queryResult) {
        if (Array.isArray(queryResult[key])) {
          rows = queryResult[key];
          rowCount = rows.length;
          console.log(`📦 Found rows under unexpected key: "${key}"`);
          break;
        }
      }
    }

    console.log(`📊 Extracted rows: ${rows.length}, rowCount: ${rowCount}`);
    return { rows, rowCount };
  } catch (error: any) {
    console.error('Database query error:', error);
    throw error;
  }
}

  // ==================== ENSURE CANVAS TABLE EXISTS ====================
  private async ensureCanvasTableExists(connectionId: string): Promise<void> {
    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS canvases (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        data JSONB NOT NULL,
        version INTEGER DEFAULT 1,
        owner_id VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
    `;
    try {
      const result = await this.api.executeQuery(connectionId, createTableSQL, { autoDisconnect: false });
      if (result.success) {
        console.log('✅ Canvases table created successfully');
      } else {
        console.error('❌ Failed to create canvases table:', result.error);
      }
    } catch (error) {
      console.error('❌ Error creating canvases table:', error);
      throw error;
    }
  }

  // ==================== GET APP DB CONNECTION ID (always fresh) ====================
  private async getAppDbConnectionId(): Promise<string | null> {
    try {
      // Always get fresh list of active connections
      const connections = await this.api.getActiveConnections();
      const appConnection = connections.find(conn =>
        conn.dbType === 'postgresql' &&
        conn.config.dbname === 'postgres'
      );

      if (appConnection) {
        return appConnection.connectionId;
      }

      // Create new connection using system username (fallback to 'postgres')
      const usersToTry = [this.getCurrentUser(), 'postgres'];
      for (const user of usersToTry) {
        try {
          console.log(`🔌 Attempting PostgreSQL connection as user: ${user}`);
          const connectResult = await this.api.connect(
            'postgresql',
            {
              host: 'localhost',
              port: '5432',
              dbname: 'postgres',
              user: user,
              password: '',
              schema: 'public'
            }
          ) as ClientConnectResult;

          if (connectResult.success) {
            console.log(`✅ Connected to PostgreSQL as ${user}`);
            return connectResult.connectionId;
          }
        } catch (error) {
          console.warn(`Connection attempt as ${user} failed:`, error);
        }
      }

      console.error('❌ Could not connect to PostgreSQL with any user');
      return null;
    } catch (error) {
      console.error('Error getting app DB connection:', error);
      return null;
    }
  }

  // ==================== GET CURRENT USER ====================
  private getCurrentUser(): string {
    if (typeof window !== 'undefined') {
      try {
        const storedUser = localStorage.getItem('last_db_user');
        if (storedUser) return storedUser;
      } catch (e) {}
    }

    if (typeof process !== 'undefined' && process.env) {
      const systemUser = process.platform === 'win32' ?
        process.env.USERNAME :
        process.env.USER;

      return process.env.REACT_APP_DB_USER ||
             process.env.APP_DB_USER ||
             systemUser ||
             'postgres';
    }

    return 'postgres';
  }

  // ==================== MAP DATABASE ROW TO CANVAS RECORD ====================
  private mapCanvasRecord(dbRow: any): CanvasRecord {
    return {
      id: dbRow.id,
      name: dbRow.name,
      data: dbRow.data,
      version: dbRow.version,
      created_at: dbRow.created_at,
      updated_at: dbRow.updated_at,
      owner_id: dbRow.owner_id
    };
  }
}

// Singleton instance
export const canvasPersistence = new CanvasPersistenceService();