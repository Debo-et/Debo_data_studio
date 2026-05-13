import { Pool } from 'pg';

/**
 * Create the metadata tables if they don't exist.
 * Uses IF NOT EXISTS to ensure idempotence.
 */
export async function initMetadataSchema(pool: Pool): Promise<void> {
  console.log('🔍 Checking/creating metadata schema tables...');

  // Order matters due to foreign keys
  const statements = [
    // 1. Connections
    `CREATE TABLE IF NOT EXISTS connections (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(255) NOT NULL,
      db_type VARCHAR(50) NOT NULL,
      host VARCHAR(255),
      port INTEGER,
      database_name VARCHAR(255) NOT NULL,
      username VARCHAR(255),
      password VARCHAR(255),
      schema VARCHAR(255),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )`,

    // 2. Metadata entries
    `CREATE TABLE IF NOT EXISTS metadata_entries (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name VARCHAR(255) NOT NULL,
      purpose TEXT,
      description TEXT,
      connection_id UUID NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
      db_type VARCHAR(50),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )`,

    // 3. Selected tables
    `CREATE TABLE IF NOT EXISTS selected_tables (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      metadata_entry_id UUID NOT NULL REFERENCES metadata_entries(id) ON DELETE CASCADE,
      schema_name VARCHAR(255),
      table_name VARCHAR(255) NOT NULL,
      table_type VARCHAR(50),
      position INTEGER,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      UNIQUE (metadata_entry_id, schema_name, table_name)
    )`,

    // 4. Selected columns
    `CREATE TABLE IF NOT EXISTS selected_columns (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      selected_table_id UUID NOT NULL REFERENCES selected_tables(id) ON DELETE CASCADE,
      column_name VARCHAR(255) NOT NULL,
      data_type VARCHAR(100) NOT NULL,
      length INTEGER,
      precision INTEGER,
      scale INTEGER,
      nullable BOOLEAN DEFAULT TRUE,
      default_value TEXT,
      position INTEGER NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      UNIQUE (selected_table_id, column_name)
    )`,
  ];

  // Optional indexes for performance
  const indexes = [
    `CREATE INDEX IF NOT EXISTS idx_metadata_entries_name ON metadata_entries(name)`,
    `CREATE INDEX IF NOT EXISTS idx_metadata_entries_connection ON metadata_entries(connection_id)`,
    `CREATE INDEX IF NOT EXISTS idx_selected_tables_entry ON selected_tables(metadata_entry_id)`,
    `CREATE INDEX IF NOT EXISTS idx_selected_columns_table ON selected_columns(selected_table_id)`,
  ];

  for (const stmt of statements) {
    try {
      await pool.query(stmt);
    } catch (err) {
      console.error(`❌ Failed to create table: ${stmt.substring(0, 60)}...`, err);
      throw err;
    }
  }

  for (const idx of indexes) {
    try {
      await pool.query(idx);
    } catch (err) {
      console.warn(`⚠️ Index creation failed (non‑critical): ${idx}`, err);
    }
  }

  console.log('✅ Metadata schema ready');
}