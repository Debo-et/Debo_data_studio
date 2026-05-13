// backend/src/database/services/metadata.service.ts

import { localPostgres } from '../local-postgres';

export interface SaveMetadataRequest {
  name: string;
  purpose?: string;
  description?: string;
  dbType: string;
  // New connection details (if creating a new connection)
  connection?: {
    host?: string;
    port?: number;
    dbname: string;
    user?: string;
    password?: string;
    schema?: string;
  };
  // Existing connection ID (if reusing)
  connectionId?: string;
  // Selected tables with columns
  tables: Array<{
    schemaname: string;
    tablename: string;
    tabletype: string;
    columns: Array<{
      name: string;
      type: string;
      nullable?: boolean;
      default?: string;
      comment?: string;
      length?: number;
      precision?: number;
      scale?: number;
      isIdentity?: boolean;
    }>;
  }>;
  // Which tables are selected (and optionally which columns)
  selectedTables: string[]; // e.g., ["public.employees"]
  // For each selected table, which columns are included (if empty, all columns)
  tableSelections?: Record<string, { include: boolean; selectedColumns: string[] }>;
}

export async function saveDatabaseMetadata(
  data: SaveMetadataRequest
): Promise<{ success: boolean; metadataEntryId?: string; error?: string }> {
  const pool = localPostgres.getPool();
  if (!pool) throw new Error('Database pool not available');

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    let connectionId: string;

    // 1. Determine connection to use
    if (data.connectionId) {
      // Reuse existing connection – verify it exists
      const result = await client.query(
        'SELECT id FROM connections WHERE id = $1',
        [data.connectionId]
      );
      if (result.rows.length === 0) {
        throw new Error(`Connection with id ${data.connectionId} not found`);
      }
      connectionId = data.connectionId;
    } else if (data.connection) {
      // Create new connection
      const insertResult = await client.query(
        `INSERT INTO connections
          (name, db_type, host, port, database_name, username, password, schema, created_at, updated_at)
         VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
         RETURNING id`,
        [
          data.name, // use metadata name as connection name for simplicity
          data.dbType,
          data.connection.host || null,
          data.connection.port || null,
          data.connection.dbname,
          data.connection.user || null,
          data.connection.password || null,
          data.connection.schema || null,
        ]
      );
      connectionId = insertResult.rows[0].id;
    } else {
      throw new Error('Either connectionId or connection details must be provided');
    }

    // 2. Insert metadata entry
    const entryResult = await client.query(
      `INSERT INTO metadata_entries
        (name, purpose, description, connection_id, db_type, created_at, updated_at)
       VALUES
        ($1, $2, $3, $4, $5, NOW(), NOW())
       RETURNING id`,
      [
        data.name,
        data.purpose || null,
        data.description || null,
        connectionId,
        data.dbType,
      ]
    );
    const metadataEntryId = entryResult.rows[0].id;

    // 3. Process selected tables
    // Build a map of table identifier to full table info
    const tableMap = new Map<string, typeof data.tables[0]>();
    for (const table of data.tables) {
      const key = `${table.schemaname}.${table.tablename}`;
      tableMap.set(key, table);
    }

    // For each selected table identifier
    for (let position = 0; position < data.selectedTables.length; position++) {
      const tableId = data.selectedTables[position];
      const tableInfo = tableMap.get(tableId);
      if (!tableInfo) continue; // should not happen

      // Insert selected table
      const tableResult = await client.query(
        `INSERT INTO selected_tables
          (metadata_entry_id, schema_name, table_name, table_type, position, created_at, updated_at)
         VALUES
          ($1, $2, $3, $4, $5, NOW(), NOW())
         RETURNING id`,
        [
          metadataEntryId,
          tableInfo.schemaname,
          tableInfo.tablename,
          tableInfo.tabletype,
          position,
        ]
      );
      const selectedTableId = tableResult.rows[0].id;

      // Determine which columns to save
      const selection = data.tableSelections?.[tableId];
      const columnsToSave = selection && selection.selectedColumns.length > 0
        ? tableInfo.columns.filter(col => selection.selectedColumns.includes(col.name))
        : tableInfo.columns; // if no selection, save all columns

      for (let colPos = 0; colPos < columnsToSave.length; colPos++) {
        const col = columnsToSave[colPos];
        await client.query(
          `INSERT INTO selected_columns
            (selected_table_id, column_name, data_type, length, precision, scale,
             nullable, default_value, position, created_at)
           VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())`,
          [
            selectedTableId,
            col.name,
            col.type,
            col.length || null,
            col.precision || null,
            col.scale || null,
            col.nullable ?? true,
            col.default || null,
            colPos,
          ]
        );
      }
    }

    await client.query('COMMIT');
    return { success: true, metadataEntryId };
  } catch (error: any) {
    await client.query('ROLLBACK');
    console.error('Failed to save database metadata:', error);
    return { success: false, error: error.message };
  } finally {
    client.release();
  }
}

export async function getAllMetadataEntries(): Promise<{
  success: boolean;
  entries?: any[];
  error?: string;
}> {
  const pool = localPostgres.getPool();
  if (!pool) return { success: false, error: 'Database pool not available' };

  try {
    const result = await pool.query(`
      SELECT 
        me.id as entry_id,
        me.name as entry_name,
        me.purpose,
        me.description,
        me.db_type,
        me.created_at,
        me.updated_at,
        c.id as connection_id,
        c.name as connection_name,
        c.host,
        c.port,
        c.database_name,
        c.username,
        c.schema as connection_schema,
        st.id as table_id,
        st.schema_name,
        st.table_name,
        st.table_type,
        st.position as table_position,
        sc.id as column_id,
        sc.column_name,
        sc.data_type,
        sc.length,
        sc.precision,
        sc.scale,
        sc.nullable,
        sc.default_value,
        sc.position as column_position
      FROM metadata_entries me
      LEFT JOIN connections c ON me.connection_id = c.id
      LEFT JOIN selected_tables st ON st.metadata_entry_id = me.id
      LEFT JOIN selected_columns sc ON sc.selected_table_id = st.id
      ORDER BY me.created_at, st.position, sc.position
    `);

    const entriesMap = new Map<string, any>();

    for (const row of result.rows) {
      const entryId = row.entry_id;
      if (!entriesMap.has(entryId)) {
        entriesMap.set(entryId, {
          id: entryId,
          name: row.entry_name,
          purpose: row.purpose,
          description: row.description,
          dbType: row.db_type,
          createdAt: row.created_at,
          updatedAt: row.updated_at,
          connection: {
            id: row.connection_id,
            name: row.connection_name,
            host: row.host,
            port: row.port,
            databaseName: row.database_name,
            username: row.username,
            schema: row.connection_schema,
          },
          tables: [],
        });
      }

      const entry = entriesMap.get(entryId);
      if (row.table_id) {
        let table = entry.tables.find((t: any) => t.id === row.table_id);
        if (!table) {
          table = {
            id: row.table_id,
            schemaName: row.schema_name,
            tableName: row.table_name,
            tableType: row.table_type,
            position: row.table_position,
            columns: [],
          };
          entry.tables.push(table);
        }
        if (row.column_id) {
          table.columns.push({
            id: row.column_id,
            name: row.column_name,
            dataType: row.data_type,
            length: row.length,
            precision: row.precision,
            scale: row.scale,
            nullable: row.nullable,
            defaultValue: row.default_value,
            position: row.column_position,
          });
        }
      }
    }

    // Sort tables by position, columns by position
    for (const entry of entriesMap.values()) {
      entry.tables.sort((a: any, b: any) => a.position - b.position);
      for (const table of entry.tables) {
        table.columns.sort((a: any, b: any) => a.position - b.position);
      }
    }

    return {
      success: true,
      entries: Array.from(entriesMap.values()),
    };
  } catch (error: any) {
    console.error('Failed to get metadata entries:', error);
    return {
      success: false,
      error: error.message,
    };
  }
}

/**
 * Delete a database metadata entry by its ID.
 * This cascades to delete all selected_tables and selected_columns records.
 * The associated connection is NOT deleted automatically – it may be reused elsewhere.
 */
export async function deleteDatabaseMetadata(
  metadataId: string
): Promise<{ success: boolean; error?: string }> {
  const pool = localPostgres.getPool();
  if (!pool) return { success: false, error: 'Database pool not available' };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // First, delete all columns belonging to selected tables of this entry
    await client.query(
      `DELETE FROM selected_columns
       WHERE selected_table_id IN (
         SELECT id FROM selected_tables WHERE metadata_entry_id = $1
       )`,
      [metadataId]
    );

    // Then delete the selected tables
    await client.query(
      `DELETE FROM selected_tables WHERE metadata_entry_id = $1`,
      [metadataId]
    );

    // Finally delete the metadata entry itself
    const result = await client.query(
      `DELETE FROM metadata_entries WHERE id = $1 RETURNING id`,
      [metadataId]
    );

    if (result.rowCount === 0) {
      await client.query('ROLLBACK');
      return { success: false, error: 'Metadata entry not found' };
    }

    await client.query('COMMIT');
    console.log(`✅ Deleted database metadata entry ${metadataId} and all related tables/columns`);
    return { success: true };
  } catch (error: any) {
    await client.query('ROLLBACK');
    console.error('❌ Failed to delete database metadata:', error);
    return { success: false, error: error.message };
  } finally {
    client.release();
  }
}