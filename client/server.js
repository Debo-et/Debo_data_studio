// server.js - Multi-Database Schema Inspector
const express = require('express');
const cors = require('cors');
const app = express();

// Database drivers - conditional requires to avoid installation issues
let dbDrivers = {};

try {
  dbDrivers.pg = require('pg');
} catch (e) {
  console.warn('PostgreSQL driver not installed');
}

try {
  dbDrivers.oracledb = require('oracledb');
} catch (e) {
  console.warn('Oracle driver not installed');
}

try {
  dbDrivers.mysql = require('mysql2');
} catch (e) {
  console.warn('MySQL driver not installed');
}

try {
  dbDrivers.sqlite3 = require('sqlite3').verbose();
} catch (e) {
  console.warn('SQLite driver not installed');
}

try {
  dbDrivers.ibm_db = require('ibm_db');
} catch (e) {
  console.warn('DB2 driver not installed');
}

try {
  dbDrivers.informix = require('informix');
} catch (e) {
  console.warn('Informix driver not installed');
}

try {
  dbDrivers.firebird = require('node-firebird');
} catch (e) {
  console.warn('Firebird driver not installed');
}

try {
  dbDrivers.hdb = require('@sap/hana-client');
} catch (e) {
  console.warn('SAP HANA driver not installed');
}

try {
  dbDrivers.mssql = require('mssql');
} catch (e) {
  console.warn('SQL Server/Sybase driver not installed');
}

try {
  dbDrivers.vertica = require('vertica');
} catch (e) {
  console.warn('Vertica driver not installed');
}

try {
  dbDrivers.teradata = require('teradata');
} catch (e) {
  console.warn('Teradata driver not installed');
}

try {
  dbDrivers.exasol = require('exasol');
} catch (e) {
  console.warn('Exasol driver not installed');
}

app.use(cors());
app.use(express.json());

// Enhanced logging with database context
const createLogger = (dbType) => ({
  info: (message, ...args) => console.log(`INFO [${dbType}]: ${message}`, ...args),
  error: (message, ...args) => console.error(`ERROR [${dbType}]: ${message}`, ...args),
  warn: (message, ...args) => console.warn(`WARN [${dbType}]: ${message}`, ...args)
});

// Connection pool management
const connectionPools = new Map();

// Database configuration validators
const validators = {
  postgresql: (config) => {
    const { dbname, pguser, pghost, pgport, password } = config;
    if (!dbname || !pguser) throw new Error('Database name and username are required');
    if (pghost && !/^[a-zA-Z0-9.-]+$/.test(pghost)) throw new Error('Invalid host format');
    if (pgport && (!/^\d+$/.test(pgport) || pgport < 1 || pgport > 65535)) throw new Error('Invalid port number');
    return true;
  },

  oracle: (config) => {
    const { user, service, host, port, password } = config;
    if (!user || !service) throw new Error('Username and service name are required');
    if (host && !/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  mysql: (config) => {
    const { database, user, host, port, password } = config;
    if (!database || !user) throw new Error('Database name and username are required');
    if (host && !/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  sqlite: (config) => {
    const { filename } = config;
    if (!filename) throw new Error('SQLite filename is required');
    return true;
  },

  db2: (config) => {
    const { database, hostname, port, username, password } = config;
    if (!database || !username) throw new Error('Database name and username are required');
    if (hostname && !/^[a-zA-Z0-9.-]+$/.test(hostname)) throw new Error('Invalid hostname format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  informix: (config) => {
    const { database, host, port, username, password } = config;
    if (!database || !username) throw new Error('Database name and username are required');
    if (host && !/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  firebird: (config) => {
    const { database, host, port, username, password } = config;
    if (!database || !username) throw new Error('Database path and username are required');
    if (host && !/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  sap_hana: (config) => {
    const { host, port, username, password } = config;
    if (!host || !username) throw new Error('Host and username are required');
    if (!/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (!port || !/^\d+$/.test(port) || port < 1 || port > 65535) throw new Error('Valid port number is required');
    return true;
  },

  sybase: (config) => {
    const { database, server, port, username, password } = config;
    if (!database || !server || !username) throw new Error('Database, server and username are required');
    if (!/^[a-zA-Z0-9.-]+$/.test(server)) throw new Error('Invalid server format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  netezza: (config) => {
    const { database, host, port, username, password } = config;
    if (!database || !host || !username) throw new Error('Database, host and username are required');
    if (!/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  vertica: (config) => {
    const { database, host, port, username, password } = config;
    if (!database || !host || !username) throw new Error('Database, host and username are required');
    if (!/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (port && (!/^\d+$/.test(port) || port < 1 || port > 65535)) throw new Error('Invalid port number');
    return true;
  },

  teradata: (config) => {
    const { host, username, password } = config;
    if (!host || !username) throw new Error('Host and username are required');
    if (!/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    return true;
  },

  exasol: (config) => {
    const { host, port, username, password } = config;
    if (!host || !username) throw new Error('Host and username are required');
    if (!/^[a-zA-Z0-9.-]+$/.test(host)) throw new Error('Invalid host format');
    if (!port || !/^\d+$/.test(port) || port < 1 || port > 65535) throw new Error('Valid port number is required');
    return true;
  }
};

// Database connection managers
const connectionManagers = {
  // PostgreSQL
  postgresql: {
    connect: async (config) => {
      const log = createLogger('POSTGRES');
      const poolKey = `postgresql:${config.pghost}:${config.pgport}:${config.dbname}:${config.pguser}`;
      
      let pool;
      if (!connectionPools.has(poolKey)) {
        pool = new dbDrivers.pg.Pool({
          host: config.pghost || 'localhost',
          port: config.pgport || 5432,
          user: config.pguser,
          password: config.password,
          database: config.dbname,
          connectionTimeoutMillis: 10000,
          idleTimeoutMillis: 30000,
          max: 5
        });
        
        pool.on('error', (err) => log.error('Pool error:', err.message));
        connectionPools.set(poolKey, pool);
        log.info(`Created new connection pool for ${poolKey}`);
      } else {
        pool = connectionPools.get(poolKey);
      }

      const client = await pool.connect();
      await client.query('SELECT 1');
      log.info(`Connected to database "${config.dbname}"`);
      return { client, release: () => client.release() };
    },

    getTables: async (connection) => {
      const query = `
        SELECT n.nspname as schemaname, c.relname as tablename, c.oid,
               CASE c.relkind 
                 WHEN 'r' THEN 'table' 
                 WHEN 'v' THEN 'view' 
                 WHEN 'm' THEN 'materialized view'
                 WHEN 'f' THEN 'foreign table'
                 WHEN 'p' THEN 'partitioned table'
               END as tabletype
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind IN ('r','v','m','f','p')
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        AND n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast_temp_%'
        ORDER BY n.nspname, c.relname
      `;
      const result = await connection.client.query(query);
      return result.rows;
    },

    getColumns: async (connection, table) => {
      const query = `
        SELECT a.attname, 
               pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
               a.attnotnull as not_null,
               pg_catalog.col_description(a.attrelid, a.attnum) as description
        FROM pg_attribute a
        WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
      `;
      const result = await connection.client.query(query, [table.oid]);
      return result.rows;
    }
  },

  // Oracle
  oracle: {
    connect: async (config) => {
      const log = createLogger('ORACLE');
      const poolKey = `oracle:${config.host}:${config.port}:${config.service}:${config.user}`;
      
      let pool;
      if (!connectionPools.has(poolKey)) {
        pool = await dbDrivers.oracledb.createPool({
          user: config.user,
          password: config.password,
          connectString: `${config.host}:${config.port}/${config.service}`,
          poolMin: 1,
          poolMax: 5,
          poolTimeout: 30000
        });
        connectionPools.set(poolKey, pool);
        log.info(`Created new connection pool for ${poolKey}`);
      } else {
        pool = connectionPools.get(poolKey);
      }

      const connection = await pool.getConnection();
      await connection.execute('SELECT 1 FROM DUAL');
      log.info(`Connected to service "${config.service}"`);
      return { connection, release: () => connection.close() };
    },

    getTables: async (connection) => {
      const query = `
        SELECT owner as schemaname, table_name as tablename, 'table' as tabletype
        FROM all_tables 
        WHERE owner NOT IN ('SYS', 'SYSTEM', 'APEX_040000', 'FLOWS_FILES', 'OUTLN', 'DBSNMP')
        UNION ALL
        SELECT owner as schemaname, view_name as tablename, 'view' as tabletype
        FROM all_views 
        WHERE owner NOT IN ('SYS', 'SYSTEM', 'APEX_040000', 'FLOWS_FILES', 'OUTLN', 'DBSNMP')
        ORDER BY schemaname, tablename
      `;
      const result = await connection.connection.execute(query, [], { outFormat: dbDrivers.oracledb.OUT_FORMAT_OBJECT });
      return result.rows;
    },

    getColumns: async (connection, table) => {
      const query = `
        SELECT column_name, data_type, data_length, data_precision, 
               data_scale, nullable, column_id
        FROM all_tab_columns
        WHERE owner = :owner AND table_name = :tableName
        ORDER BY column_id
      `;
      const result = await connection.connection.execute(
        query, 
        { owner: table.SCHEMANAME, tableName: table.TABLENAME }, 
        { outFormat: dbDrivers.oracledb.OUT_FORMAT_OBJECT }
      );
      return result.rows;
    }
  },

  // MySQL
  mysql: {
    connect: async (config) => {
      const log = createLogger('MYSQL');
      const poolKey = `mysql:${config.host}:${config.port}:${config.database}:${config.user}`;
      
      let pool;
      if (!connectionPools.has(poolKey)) {
        pool = dbDrivers.mysql.createPool({
          host: config.host || 'localhost',
          port: config.port || 3306,
          user: config.user,
          password: config.password,
          database: config.database,
          connectionLimit: 5,
          acquireTimeout: 10000
        });
        connectionPools.set(poolKey, pool);
        log.info(`Created new connection pool for ${poolKey}`);
      } else {
        pool = connectionPools.get(poolKey);
      }

      const connection = await pool.promise().getConnection();
      await connection.execute('SELECT 1');
      log.info(`Connected to database "${config.database}"`);
      return { connection, release: () => connection.release() };
    },

    getTables: async (connection) => {
      const [rows] = await connection.connection.execute(`
        SELECT TABLE_SCHEMA as schemaname, TABLE_NAME as tablename, 
               TABLE_TYPE as tabletype
        FROM information_schema.tables 
        WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
      `);
      return rows;
    },

    getColumns: async (connection, table) => {
      const [rows] = await connection.connection.execute(`
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, 
               CHARACTER_MAXIMUM_LENGTH as length,
               NUMERIC_PRECISION as precision,
               NUMERIC_SCALE as scale,
               COLUMN_DEFAULT as default_value,
               COLUMN_COMMENT as comment
        FROM information_schema.columns 
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
      `, [table.SCHEMANAME, table.TABLENAME]);
      return rows;
    }
  },

  // SQLite
  sqlite: {
    connect: async (config) => {
      const log = createLogger('SQLITE');
      const db = new dbDrivers.sqlite3.Database(config.filename, (err) => {
        if (err) {
          log.error('Connection failed:', err.message);
          throw err;
        }
      });
      log.info(`Connected to database "${config.filename}"`);
      return { db, release: () => db.close() };
    },

    getTables: async (connection) => {
      return new Promise((resolve, reject) => {
        connection.db.all(`
          SELECT name as tablename, type as tabletype
          FROM sqlite_master 
          WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%'
          ORDER BY name
        `, (err, rows) => {
          if (err) reject(err);
          else resolve(rows.map(row => ({ ...row, schemaname: 'main' })));
        });
      });
    },

    getColumns: async (connection, table) => {
      return new Promise((resolve, reject) => {
        connection.db.all(`PRAGMA table_info("${table.TABLENAME}")`, (err, rows) => {
          if (err) reject(err);
          else resolve(rows.map(col => ({
            column_name: col.name,
            data_type: col.type,
            not_null: col.notnull === 1,
            default_value: col.dflt_value
          })));
        });
      });
    }
  },

  // IBM DB2
  db2: {
    connect: async (config) => {
      const log = createLogger('DB2');
      const connString = `DATABASE=${config.database};HOSTNAME=${config.hostname};PORT=${config.port || 50000};PROTOCOL=TCPIP;UID=${config.username};PWD=${config.password};`;
      
      const connection = await new Promise((resolve, reject) => {
        dbDrivers.ibm_db.open(connString, (err, conn) => {
          if (err) reject(err);
          else resolve(conn);
        });
      });
      
      log.info(`Connected to database "${config.database}"`);
      return { connection, release: () => connection.close() };
    },

    getTables: async (connection) => {
      return new Promise((resolve, reject) => {
        connection.query(`
          SELECT TABSCHEMA as schemaname, TABNAME as tablename, TYPE as tabletype
          FROM SYSCAT.TABLES 
          WHERE TABSCHEMA NOT LIKE 'SYS%' AND TYPE IN ('T', 'V')
          ORDER BY TABSCHEMA, TABNAME
        `, (err, tables) => {
          if (err) reject(err);
          else resolve(tables);
        });
      });
    },

    getColumns: async (connection, table) => {
      return new Promise((resolve, reject) => {
        connection.query(`
          SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS
          FROM SYSCAT.COLUMNS 
          WHERE TABSCHEMA = ? AND TABNAME = ?
          ORDER BY COLNO
        `, [table.SCHEMANAME, table.TABLENAME], (err, columns) => {
          if (err) reject(err);
          else resolve(columns);
        });
      });
    }
  },

  // Informix
  informix: {
    connect: async (config) => {
      const log = createLogger('INFORMIX');
      const connString = `SERVER=${config.host};DATABASE=${config.database};UID=${config.username};PWD=${config.password};`;
      
      const connection = await new Promise((resolve, reject) => {
        dbDrivers.informix.connect(connString, (err, conn) => {
          if (err) reject(err);
          else resolve(conn);
        });
      });
      
      log.info(`Connected to database "${config.database}"`);
      return { connection, release: () => connection.close() };
    },

    getTables: async (connection) => {
      return new Promise((resolve, reject) => {
        connection.query(`
          SELECT owner as schemaname, tabname as tablename, tabtype as tabletype
          FROM systables 
          WHERE owner NOT IN ('informix') AND tabid > 99
          ORDER BY owner, tabname
        `, (err, tables) => {
          if (err) reject(err);
          else resolve(tables);
        });
      });
    },

    getColumns: async (connection, table) => {
      return new Promise((resolve, reject) => {
        connection.query(`
          SELECT colname, coltype, collength
          FROM syscolumns 
          WHERE tabname = ? 
          ORDER BY colno
        `, [table.TABLENAME], (err, columns) => {
          if (err) reject(err);
          else resolve(columns);
        });
      });
    }
  },

  // Firebird
  firebird: {
    connect: async (config) => {
      const log = createLogger('FIREBIRD');
      return new Promise((resolve, reject) => {
        dbDrivers.firebird.attach({
          host: config.host || 'localhost',
          port: config.port || 3050,
          database: config.database,
          user: config.username,
          password: config.password
        }, (err, db) => {
          if (err) reject(err);
          else {
            log.info(`Connected to database "${config.database}"`);
            resolve({ db, release: () => db.detach() });
          }
        });
      });
    },

    getTables: async (connection) => {
      return new Promise((resolve, reject) => {
        connection.db.query(`
          SELECT RDB$RELATION_NAME as tablename
          FROM RDB$RELATIONS 
          WHERE RDB$SYSTEM_FLAG = 0 AND RDB$VIEW_BLR IS NULL
          ORDER BY RDB$RELATION_NAME
        `, (err, tables) => {
          if (err) reject(err);
          else resolve(tables.map(t => ({ schemaname: '', tablename: t.TABLENAME.trim(), tabletype: 'table' })));
        });
      });
    },

    getColumns: async (connection, table) => {
      return new Promise((resolve, reject) => {
        connection.db.query(`
          SELECT RDB$FIELD_NAME as column_name, RDB$FIELD_TYPE as data_type
          FROM RDB$RELATION_FIELDS 
          WHERE RDB$RELATION_NAME = ?
          ORDER BY RDB$FIELD_POSITION
        `, [table.TABLENAME], (err, columns) => {
          if (err) reject(err);
          else resolve(columns.map(c => ({ 
            column_name: c.COLUMN_NAME.trim(), 
            data_type: c.DATA_TYPE 
          })));
        });
      });
    }
  },

  // SAP HANA
  sap_hana: {
    connect: async (config) => {
      const log = createLogger('SAP_HANA');
      const client = dbDrivers.hdb.createClient({
        host: config.host,
        port: config.port,
        user: config.username,
        password: config.password
      });

      await new Promise((resolve, reject) => {
        client.connect(err => {
          if (err) reject(err);
          else {
            log.info(`Connected to SAP HANA at ${config.host}:${config.port}`);
            resolve();
          }
        });
      });

      return { client, release: () => client.disconnect() };
    },

    getTables: async (connection) => {
      return new Promise((resolve, reject) => {
        connection.client.exec(`
          SELECT SCHEMA_NAME as schemaname, TABLE_NAME as tablename, TABLE_TYPE as tabletype
          FROM TABLES 
          WHERE SCHEMA_NAME NOT IN ('SYS', '_SYS_BI', '_SYS_EPM')
          ORDER BY SCHEMA_NAME, TABLE_NAME
        `, (err, tables) => {
          if (err) reject(err);
          else resolve(tables);
        });
      });
    },

    getColumns: async (connection, table) => {
      return new Promise((resolve, reject) => {
        connection.client.exec(`
          SELECT COLUMN_NAME, DATA_TYPE_NAME, LENGTH, SCALE
          FROM TABLE_COLUMNS 
          WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
          ORDER BY POSITION
        `, [table.SCHEMANAME, table.TABLENAME], (err, columns) => {
          if (err) reject(err);
          else resolve(columns);
        });
      });
    }
  },

  // Sybase ASE
  sybase: {
    connect: async (config) => {
      const log = createLogger('SYBASE');
      const pool = await dbDrivers.mssql.connect({
        server: config.server,
        port: config.port || 5000,
        database: config.database,
        user: config.username,
        password: config.password,
        options: {
          encrypt: false,
          trustServerCertificate: true
        },
        pool: {
          max: 5,
          min: 0,
          idleTimeoutMillis: 30000
        }
      });
      
      log.info(`Connected to database "${config.database}"`);
      return { pool, release: () => pool.close() };
    },

    getTables: async (connection) => {
      const result = await connection.pool.request().query(`
        SELECT TABLE_SCHEMA as schemaname, TABLE_NAME as tablename, TABLE_TYPE as tabletype
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA NOT IN ('sys')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
      `);
      return result.recordset;
    },

    getColumns: async (connection, table) => {
      const result = await connection.pool.request()
        .input('schema', dbDrivers.mssql.VarChar, table.SCHEMANAME)
        .input('table', dbDrivers.mssql.VarChar, table.TABLENAME)
        .query(`
          SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, 
                 CHARACTER_MAXIMUM_LENGTH as length,
                 NUMERIC_PRECISION as precision,
                 NUMERIC_SCALE as scale
          FROM INFORMATION_SCHEMA.COLUMNS 
          WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
          ORDER BY ORDINAL_POSITION
        `);
      return result.recordset;
    }
  },

  // Netezza (using PostgreSQL protocol)
  netezza: {
    connect: async (config) => {
      const log = createLogger('NETEZZA');
      const poolKey = `netezza:${config.host}:${config.port}:${config.database}:${config.username}`;
      
      let pool;
      if (!connectionPools.has(poolKey)) {
        pool = new dbDrivers.pg.Pool({
          host: config.host,
          port: config.port || 5480,
          user: config.username,
          password: config.password,
          database: config.database,
          connectionTimeoutMillis: 10000
        });
        connectionPools.set(poolKey, pool);
        log.info(`Created new connection pool for ${poolKey}`);
      } else {
        pool = connectionPools.get(poolKey);
      }

      const client = await pool.connect();
      await client.query('SELECT 1');
      log.info(`Connected to database "${config.database}"`);
      return { client, release: () => client.release() };
    },

    getTables: async (connection) => {
      const result = await connection.client.query(`
        SELECT schemaname, tablename, 'table' as tabletype
        FROM _v_table 
        WHERE schemaname NOT IN ('ADMIN', 'INFORMATION_SCHEMA', 'DEFINITION_SCHEMA')
        UNION
        SELECT schemaname, viewname as tablename, 'view' as tabletype
        FROM _v_view 
        WHERE schemaname NOT IN ('ADMIN', 'INFORMATION_SCHEMA', 'DEFINITION_SCHEMA')
        ORDER BY schemaname, tablename
      `);
      return result.rows;
    },

    getColumns: async (connection, table) => {
      const result = await connection.client.query(`
        SELECT attname, format_type(atttypid, atttypmod) as data_type
        FROM _v_relation_column 
        WHERE schemaname = $1 AND tablename = $2
        ORDER BY attnum
      `, [table.SCHEMANAME, table.TABLENAME]);
      return result.rows;
    }
  },

  // Vertica
  vertica: {
    connect: async (config) => {
      const log = createLogger('VERTICA');
      const client = dbDrivers.vertica.connect({
        host: config.host,
        port: config.port || 5433,
        user: config.username,
        password: config.password,
        database: config.database
      });

      log.info(`Connected to database "${config.database}"`);
      return { client, release: () => client.disconnect() };
    },

    getTables: async (connection) => {
      return new Promise((resolve, reject) => {
        connection.client.query(`
          SELECT table_schema as schemaname, table_name as tablename, table_type as tabletype
          FROM v_catalog.tables 
          WHERE table_schema NOT IN ('v_catalog', 'v_monitor')
          ORDER BY table_schema, table_name
        `, (err, result) => {
          if (err) reject(err);
          else resolve(result.rows);
        });
      });
    },

    getColumns: async (connection, table) => {
      return new Promise((resolve, reject) => {
        connection.client.query(`
          SELECT column_name, data_type, is_nullable
          FROM v_catalog.columns 
          WHERE table_schema = ? AND table_name = ?
          ORDER BY ordinal_position
        `, [table.SCHEMANAME, table.TABLENAME], (err, result) => {
          if (err) reject(err);
          else resolve(result.rows);
        });
      });
    }
  },

  // Teradata
  teradata: {
    connect: async (config) => {
      const log = createLogger('TERADATA');
      const connection = dbDrivers.teradata.connect({
        host: config.host,
        user: config.username,
        password: config.password
      });

      log.info(`Connected to Teradata at ${config.host}`);
      return { connection, release: () => connection.close() };
    },

    getTables: async (connection) => {
      const result = await connection.connection.execute(`
        SELECT DatabaseName as schemaname, TableName as tablename, TableKind as tabletype
        FROM DBC.TablesV 
        WHERE DatabaseName NOT IN ('DBC', 'Sys_Calendar')
        ORDER BY DatabaseName, TableName
      `);
      return result;
    },

    getColumns: async (connection, table) => {
      const result = await connection.connection.execute(`
        SELECT ColumnName, ColumnType, Nullable
        FROM DBC.ColumnsV 
        WHERE DatabaseName = ? AND TableName = ?
        ORDER BY ColumnId
      `, [table.SCHEMANAME, table.TABLENAME]);
      return result;
    }
  },

  // Exasol
  exasol: {
    connect: async (config) => {
      const log = createLogger('EXASOL');
      const connection = await dbDrivers.exasol.connect({
        host: config.host,
        port: config.port || 8563,
        user: config.username,
        password: config.password
      });

      log.info(`Connected to Exasol at ${config.host}:${config.port}`);
      return { connection, release: () => connection.disconnect() };
    },

    getTables: async (connection) => {
      const result = await connection.connection.execute(`
        SELECT TABLE_SCHEMA as schemaname, TABLE_NAME as tablename, TABLE_TYPE as tabletype
        FROM EXA_ALL_TABLES 
        WHERE TABLE_SCHEMA NOT IN ('SYS')
        ORDER BY TABLE_SCHEMA, TABLE_NAME
      `);
      return result.rows;
    },

    getColumns: async (connection, table) => {
      const result = await connection.connection.execute(`
        SELECT COLUMN_NAME, TYPE, IS_NULLABLE
        FROM EXA_ALL_COLUMNS 
        WHERE COLUMN_SCHEMA = ? AND COLUMN_TABLE = ?
        ORDER BY ORDINAL_POSITION
      `, [table.SCHEMANAME, table.TABLENAME]);
      return result.rows;
    }
  }
};

// Unified schema inspection endpoint
app.post('/api/schema/:dbType', async (req, res) => {
  const { dbType } = req.params;
  const config = req.body;
  const log = createLogger(dbType.toUpperCase());

  try {
    // Validate database type
    if (!connectionManagers[dbType]) {
      return res.status(400).json({
        success: false,
        error: `Unsupported database type: ${dbType}. Supported types: ${Object.keys(connectionManagers).join(', ')}`
      });
    }

    // Validate configuration
    if (!validators[dbType]) {
      return res.status(400).json({
        success: false,
        error: `No validator available for database type: ${dbType}`
      });
    }

    validators[dbType](config);

    // Check if driver is installed
    const requiredDriver = getRequiredDriver(dbType);
    if (requiredDriver && !dbDrivers[requiredDriver]) {
      return res.status(400).json({
        success: false,
        error: `Database driver not installed for ${dbType}. Please install: ${requiredDriver}`
      });
    }

    // Connect to database
    const connection = await connectionManagers[dbType].connect(config);
    
    try {
      // Get tables
      const tables = await connectionManagers[dbType].getTables(connection);
      
      // Get columns for all tables
      const tablesWithColumns = await Promise.all(
        tables.map(async (table) => {
          try {
            const columns = await connectionManagers[dbType].getColumns(connection, table);
            return {
              schemaname: table.schemaname,
              tablename: table.tablename,
              tabletype: table.tabletype,
              columns: columns.map(col => ({
                name: col.column_name || col.COLUMN_NAME || col.attname,
                type: col.data_type || col.DATA_TYPE || col.TYPE,
                length: col.length || col.LENGTH || col.data_length,
                precision: col.precision || col.PRECISION || col.data_precision,
                scale: col.scale || col.SCALE || col.data_scale,
                nullable: col.nullable || col.NULLABLE || col.is_nullable,
                default: col.default_value || col.DEFAULT_VALUE
              })),
              num_columns: columns.length
            };
          } catch (columnError) {
            log.error(`Failed to get columns for table ${table.tablename}:`, columnError.message);
            return {
              ...table,
              columns: [],
              num_columns: 0,
              error: columnError.message
            };
          }
        })
      );

      res.json({
        success: true,
        database_type: dbType,
        data: tablesWithColumns,
        summary: {
          total_tables: tablesWithColumns.length,
          total_columns: tablesWithColumns.reduce((sum, table) => sum + table.num_columns, 0)
        },
        message: `Retrieved ${tablesWithColumns.length} tables from ${dbType}`
      });

    } finally {
      await connection.release();
    }

  } catch (error) {
    log.error(`Schema inspection failed: ${error.message}`);
    res.status(500).json({
      success: false,
      database_type: dbType,
      error: error.message
    });
  }
});

// Helper function to get required driver for database type
function getRequiredDriver(dbType) {
  const driverMap = {
    postgresql: 'pg',
    oracle: 'oracledb',
    mysql: 'mysql',
    sqlite: 'sqlite3',
    db2: 'ibm_db',
    informix: 'informix',
    firebird: 'firebird',
    sap_hana: 'hdb',
    sybase: 'mssql',
    netezza: 'pg',
    vertica: 'vertica',
    teradata: 'teradata',
    exasol: 'exasol'
  };
  return driverMap[dbType];
}

// Health check endpoint
app.get('/api/health', (req, res) => {
  const healthInfo = {
    status: 'OK',
    timestamp: new Date().toISOString(),
    service: 'Enterprise Multi-Database Schema Inspector',
    supported_databases: Object.keys(connectionManagers).map(dbType => ({
      type: dbType,
      driver_installed: !!getRequiredDriver(dbType) && !!dbDrivers[getRequiredDriver(dbType)],
      endpoint: `/api/schema/${dbType}`
    })),
    active_connection_pools: connectionPools.size
  };

  res.json(healthInfo);
});

// Database-specific health checks
app.get('/api/health/:dbType', async (req, res) => {
  const { dbType } = req.params;
  
  if (!connectionManagers[dbType]) {
    return res.status(400).json({
      status: 'ERROR',
      error: `Unsupported database type: ${dbType}`
    });
  }

  res.json({
    status: 'OK',
    database_type: dbType,
    driver_available: !!getRequiredDriver(dbType) && !!dbDrivers[getRequiredDriver(dbType)],
    timestamp: new Date().toISOString()
  });
});

// Get supported databases
app.get('/api/databases', (req, res) => {
  const databases = Object.keys(connectionManagers).map(dbType => ({
    type: dbType,
    name: getDatabaseDisplayName(dbType),
    driver_required: getRequiredDriver(dbType),
    driver_installed: !!getRequiredDriver(dbType) && !!dbDrivers[getRequiredDriver(dbType)],
    configuration_example: getConfigurationExample(dbType)
  }));

  res.json({
    success: true,
    databases,
    total: databases.length
  });
});

function getDatabaseDisplayName(dbType) {
  const names = {
    postgresql: 'PostgreSQL',
    oracle: 'Oracle Database',
    mysql: 'MySQL',
    sqlite: 'SQLite',
    db2: 'IBM DB2',
    informix: 'IBM Informix',
    firebird: 'Firebird',
    sap_hana: 'SAP HANA',
    sybase: 'Sybase ASE',
    netezza: 'IBM Netezza',
    vertica: 'Vertica',
    teradata: 'Teradata',
    exasol: 'Exasol'
  };
  return names[dbType] || dbType;
}

function getConfigurationExample(dbType) {
  const examples = {
    postgresql: {
      pghost: 'localhost',
      pgport: 5432,
      dbname: 'mydatabase',
      pguser: 'username',
      password: 'password'
    },
    oracle: {
      host: 'localhost',
      port: 1521,
      service: 'ORCL',
      user: 'username',
      password: 'password'
    },
    mysql: {
      host: 'localhost',
      port: 3306,
      database: 'mydatabase',
      user: 'username',
      password: 'password'
    },
    sqlite: {
      filename: '/path/to/database.sqlite'
    }
    // Add other database examples as needed
  };
  return examples[dbType] || { message: 'Refer to database documentation for configuration' };
}

// Cleanup function for graceful shutdown
const cleanup = async () => {
  const log = createLogger('SYSTEM');
  log.info('Shutting down gracefully...');
  
  // Close all connection pools
  for (const [key, pool] of connectionPools.entries()) {
    try {
      if (typeof pool.end === 'function') {
        await pool.end();
      } else if (typeof pool.close === 'function') {
        await pool.close();
      }
      log.info(`Closed connection pool: ${key}`);
    } catch (error) {
      log.error(`Error closing pool ${key}:`, error.message);
    }
  }
  
  process.exit(0);
};

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

// Initialize database clients
const initializeClients = async () => {
  const log = createLogger('SYSTEM');
  
  // Initialize Oracle client if available
  if (dbDrivers.oracledb) {
    try {
      dbDrivers.oracledb.autoCommit = false;
      dbDrivers.oracledb.maxRows = 1000;
      log.info('Oracle client initialized successfully');
    } catch (error) {
      log.error('Failed to initialize Oracle client:', error.message);
    }
  }

  log.info('Multi-Database Schema Inspector initialized');
  log.info(`Supported databases: ${Object.keys(connectionManagers).join(', ')}`);
};

const PORT = process.env.PORT || 3001;

initializeClients().then(() => {
  app.listen(PORT, () => {
    console.log(`\n🚀 Enterprise Multi-Database Schema Inspector running on port ${PORT}`);
    console.log('📊 Supported Databases:');
    Object.keys(connectionManagers).forEach(dbType => {
      const driver = getRequiredDriver(dbType);
      const installed = driver && dbDrivers[driver];
      console.log(`   • ${getDatabaseDisplayName(dbType)}: ${installed ? '✅' : '❌ (driver not installed)'}`);
    });
    console.log(`\n🔗 Health Check: http://localhost:${PORT}/api/health`);
    console.log(`📚 API Documentation: http://localhost:${PORT}/api/databases`);
    console.log(`\n💡 Tip: Install missing database drivers using npm install <driver-package>`);
  });
}).catch(error => {
  console.error('Failed to start server:', error);
  process.exit(1);
});

app.post('/api/preview/:dbType', async (req, res) => {
  const { dbType } = req.params;
  const { table, schema, limit = 10, ...config } = req.body;
  const log = createLogger(dbType.toUpperCase());

  try {
    if (!connectionManagers[dbType]) {
      return res.status(400).json({
        success: false,
        error: `Unsupported database type: ${dbType}`
      });
    }

    // Validate configuration
    validators[dbType](config);

    // Connect to database
    const connection = await connectionManagers[dbType].connect(config);
    
    try {
      let query;
      let params = [];
      
      if (dbType === 'postgresql' || dbType === 'mysql') {
        const schemaPrefix = schema && schema !== 'public' ? `${schema}.` : '';
        query = `SELECT * FROM ${schemaPrefix}${table} LIMIT $1`;
        params = [limit];
      } else if (dbType === 'oracle') {
        query = `SELECT * FROM ${table} WHERE ROWNUM <= :1`;
        params = [limit];
      } else {
        query = `SELECT TOP ${limit} * FROM ${table}`;
      }

      let result;
      if (dbType === 'postgresql') {
        result = await connection.client.query(query, params);
        result = result.rows;
      } else if (dbType === 'mysql') {
        const [rows] = await connection.connection.execute(query, params);
        result = rows;
      } else if (dbType === 'oracle') {
        result = await connection.connection.execute(query, params, { outFormat: dbDrivers.oracledb.OUT_FORMAT_OBJECT });
        result = result.rows;
      }

      res.json({
        success: true,
        data: result,
        message: `Retrieved ${result.length} preview records`
      });

    } finally {
      await connection.release();
    }

  } catch (error) {
    log.error(`Data preview failed: ${error.message}`);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }


});

module.exports = app;