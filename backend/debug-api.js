#!/usr/bin/env node

const axios = require('axios');
const readline = require('readline');
const fs = require('fs').promises;
const path = require('path');

// Configuration
const API_URL = process.env.API_URL || 'http://localhost:3000';
const CONFIG_FILE = process.env.CONFIG_FILE || path.join(process.cwd(), '.debug-api-config.json');

// Static configuration for local PostgreSQL (from standalone-pg-builder.sh)
const LOCAL_POSTGRES_CONFIG = {
  dbType: 'postgresql',
  config: {
    host: 'localhost',
    port: '5432',
    dbname: 'postgres',
    user: process.env.USER || process.env.USERNAME || 'postgres',
    password: process.env.DB_PASSWORD || '',
    schema: 'public'
  }
};

class DatabaseDebugger {
  constructor() {
    this.axios = axios.create({
      baseURL: API_URL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'Database-Debugger/1.0'
      }
    });
    
    // Store active connection IDs
    this.activeConnections = new Map();
    this.savedConfigs = {};
  }

  /**
   * Test any API endpoint
   */
  async testEndpoint(endpoint, method = 'GET', data = null, customHeaders = {}) {
    console.log(`\n🧪 Testing ${method} ${endpoint}...`);
    
    try {
      const config = {
        method,
        url: endpoint,
        headers: { ...this.axios.defaults.headers, ...customHeaders }
      };
      
      if (data) {
        config.data = data;
      }
      
      const response = await this.axios(config);
      
      console.log(`✅ Success: Status ${response.status}`);
      
      // Truncate large responses for readability
      const responseStr = JSON.stringify(response.data, null, 2);
      const preview = responseStr.length > 1000 
        ? responseStr.substring(0, 1000) + '... [truncated]' 
        : responseStr;
      
      console.log('Response:', preview);
      return { success: true, data: response.data };
    } catch (error) {
      console.log(`❌ Failed: ${error.message}`);
      
      if (error.response) {
        console.log('Status:', error.response.status);
        console.log('Error Data:', JSON.stringify(error.response.data));
      } else if (error.request) {
        console.log('No response received. Check if server is running.');
      }
      
      return { 
        success: false, 
        error: error.message,
        response: error.response 
      };
    }
  }

  /**
   * Load saved configurations
   */
  async loadConfigs() {
    try {
      const data = await fs.readFile(CONFIG_FILE, 'utf8');
      this.savedConfigs = JSON.parse(data);
      console.log(`📁 Loaded ${Object.keys(this.savedConfigs).length} saved configurations`);
    } catch (error) {
      // Config file doesn't exist yet, that's OK
      this.savedConfigs = {};
    }
  }

  /**
   * Save configurations
   */
  async saveConfigs() {
    try {
      await fs.writeFile(CONFIG_FILE, JSON.stringify(this.savedConfigs, null, 2));
      console.log(`💾 Saved configurations to ${CONFIG_FILE}`);
    } catch (error) {
      console.error('Failed to save configurations:', error.message);
    }
  }

  /**
   * Save a connection configuration
   */
  async saveConfig(name, dbType, config) {
    this.savedConfigs[name] = { dbType, config, timestamp: new Date().toISOString() };
    await this.saveConfigs();
    console.log(`💾 Saved configuration "${name}"`);
  }

  /**
   * Load a saved configuration
   */
  loadConfig(name) {
    if (!this.savedConfigs[name]) {
      throw new Error(`Configuration "${name}" not found`);
    }
    return this.savedConfigs[name];
  }

  /**
   * List all saved configurations
   */
  listConfigs() {
    console.log('\n📋 Saved Configurations:');
    console.log('=====================');
    
    if (Object.keys(this.savedConfigs).length === 0) {
      console.log('No configurations saved.');
      return;
    }
    
    Object.entries(this.savedConfigs).forEach(([name, config]) => {
      console.log(`\n${name}:`);
      console.log(`  Type: ${config.dbType}`);
      console.log(`  Host: ${config.config.host || 'localhost'}`);
      console.log(`  Database: ${config.config.dbname || config.config.database || 'N/A'}`);
      console.log(`  User: ${config.config.user || config.config.username || 'N/A'}`);
      console.log(`  Saved: ${new Date(config.timestamp).toLocaleString()}`);
    });
  }

  /**
   * Delete a saved configuration
   */
  async deleteConfig(name) {
    if (this.savedConfigs[name]) {
      delete this.savedConfigs[name];
      await this.saveConfigs();
      console.log(`🗑️ Deleted configuration "${name}"`);
    } else {
      console.log(`Configuration "${name}" not found`);
    }
  }

  /**
   * Test API health
   */
  async testHealth() {
    return await this.testEndpoint('/health');
  }

  /**
   * Test local PostgreSQL status
   */
  async testPostgresStatus() {
    return await this.testEndpoint('/api/postgres/status');
  }

  /**
   * Test database connection
   */
  async testConnection(dbType, config, configName = null) {
    const payload = {
      dbType,
      config
    };
    
    const result = await this.testEndpoint('/api/database/test-connection', 'POST', payload);
    
    if (result.success && configName && configName !== 'temp') {
      await this.saveConfig(configName, dbType, config);
    }
    
    return result;
  }

  /**
   * Connect to database
   */
  async connectToDatabase(dbType, config, connectionName = 'default') {
    const payload = {
      dbType,
      config
    };
    
    const result = await this.testEndpoint('/api/database/connect', 'POST', payload);
    
    if (result.success && result.data && result.data.connectionId) {
      this.activeConnections.set(connectionName, {
        id: result.data.connectionId,
        dbType,
        config,
        timestamp: new Date().toISOString()
      });
      console.log(`🔗 Connection "${connectionName}" established with ID: ${result.data.connectionId}`);
    }
    
    return result;
  }

  /**
   * Get tables for a connection
   */
  async getTables(connectionName, options = {}) {
    const connection = this.activeConnections.get(connectionName);
    
    if (!connection) {
      console.log(`❌ No active connection found for "${connectionName}"`);
      console.log('Active connections:', Array.from(this.activeConnections.keys()));
      return { success: false, error: 'Connection not found' };
    }
    
    const payload = {
      connectionId: connection.id,
      options
    };
    
    return await this.testEndpoint('/api/database/tables', 'POST', payload);
  }

  /**
   * Execute SQL query
   */
  async executeQuery(connectionName, sql, options = {}) {
    const connection = this.activeConnections.get(connectionName);
    
    if (!connection) {
      console.log(`❌ No active connection found for "${connectionName}"`);
      return { success: false, error: 'Connection not found' };
    }
    
    const payload = {
      sql,
      options
    };
    
    return await this.testEndpoint(`/api/database/${connection.id}/query`, 'POST', payload);
  }

  /**
   * Get database info
   */
  async getDatabaseInfo(connectionName) {
    const connection = this.activeConnections.get(connectionName);
    
    if (!connection) {
      console.log(`❌ No active connection found for "${connectionName}"`);
      return { success: false, error: 'Connection not found' };
    }
    
    return await this.testEndpoint(`/api/database/${connection.id}/info`, 'GET');
  }

  /**
   * Get active connections from server
   */
  async getActiveConnections() {
    return await this.testEndpoint('/api/database/connections/active', 'GET');
  }

  /**
   * Disconnect from database
   */
  async disconnect(connectionName) {
    const connection = this.activeConnections.get(connectionName);
    
    if (!connection) {
      console.log(`❌ No active connection found for "${connectionName}"`);
      return { success: false, error: 'Connection not found' };
    }
    
    const result = await this.testEndpoint(`/api/database/${connection.id}`, 'DELETE');
    
    if (result.success) {
      this.activeConnections.delete(connectionName);
      console.log(`🔌 Connection "${connectionName}" closed`);
    }
    
    return result;
  }

  /**
   * Test PostgreSQL query endpoint
   */
  async testPostgresQuery(sql, params = []) {
    const payload = { sql, params };
    return await this.testEndpoint('/api/postgres/query', 'POST', payload);
  }

  /**
   * Run comprehensive test suite
   */
  async runTestSuite(dbType = 'postgresql', config = null, connectionName = 'test') {
    console.log('\n🚀 Running Comprehensive Test Suite');
    console.log('================================\n');
    
    // 1. Test health
    await this.testHealth();
    
    // 2. Test PostgreSQL status (for local PostgreSQL)
    if (dbType === 'postgresql') {
      await this.testPostgresStatus();
    }
    
    // 3. Test connection
    const connectionResult = await this.testConnection(dbType, config);
    
    if (!connectionResult.success) {
      console.log('❌ Cannot proceed with tests - connection failed');
      return;
    }
    
    // 4. Connect to database
    const connectResult = await this.connectToDatabase(dbType, config, connectionName);
    
    if (!connectResult.success) {
      console.log('❌ Cannot proceed with tests - connection failed');
      return;
    }
    
    // 5. Get database info
    await this.getDatabaseInfo(connectionName);
    
    // 6. Get tables
    await this.getTables(connectionName);
    
    // 7. Test a simple query
    let testQuery = '';
    switch(dbType) {
      case 'postgresql':
        testQuery = 'SELECT 1 as test_value, current_timestamp as now';
        break;
      case 'mysql':
        testQuery = 'SELECT 1 as test_value, NOW() as now';
        break;
      case 'sqlserver':
      case 'mssql':
        testQuery = 'SELECT 1 as test_value, GETDATE() as now';
        break;
      case 'oracle':
        testQuery = 'SELECT 1 as test_value, SYSDATE as now FROM dual';
        break;
      default:
        testQuery = 'SELECT 1 as test_value';
    }
    
    await this.executeQuery(connectionName, testQuery, { maxRows: 5 });
    
    // 8. Get active connections
    await this.getActiveConnections();
    
    // 9. Cleanup - disconnect
    await this.disconnect(connectionName);
    
    console.log('\n✅ Test suite completed!');
  }

  /**
   * Create database configuration from arguments
   */
  createConfigFromArgs(args) {
    const config = {};
    
    if (args.host) config.host = args.host;
    if (args.port) config.port = args.port;
    if (args.dbname) config.dbname = args.dbname;
    if (args.user) config.user = args.user;
    if (args.password !== undefined) config.password = args.password;
    if (args.schema) config.schema = args.schema;
    if (args.database) config.database = args.database;
    if (args.username) config.username = args.username;
    
    // Set defaults for common database types
    if (!config.port) {
      switch(args.dbType) {
        case 'postgresql':
          config.port = '5432';
          break;
        case 'mysql':
          config.port = '3306';
          break;
        case 'sqlserver':
        case 'mssql':
          config.port = '1433';
          break;
        case 'oracle':
          config.port = '1521';
          break;
      }
    }
    
    return config;
  }

  /**
   * Parse command line arguments
   */
  parseArgs() {
    const args = process.argv.slice(2);
    const parsed = { _: [] };
    
    for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      
      if (arg.startsWith('--')) {
        const key = arg.slice(2);
        const nextArg = args[i + 1];
        
        if (nextArg && !nextArg.startsWith('--')) {
          parsed[key] = nextArg;
          i++;
        } else {
          parsed[key] = true;
        }
      } else {
        parsed._.push(arg);
      }
    }
    
    return parsed;
  }

  /**
   * Interactive prompt
   */
  async prompt(question) {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });
    
    return new Promise((resolve) => {
      rl.question(question, (answer) => {
        rl.close();
        resolve(answer);
      });
    });
  }

  /**
   * Show help
   */
  showHelp() {
    console.log(`
🎯 Database Debugger - Comprehensive Backend Testing Utility
=========================================================

📚 Commands:

General Commands:
  help                           Show this help message
  health                         Test API health endpoint
  postgres-status                Test local PostgreSQL status
  active-connections             List active connections on server

Connection Management:
  connect                        Connect to a database
    --db-type <type>             Database type (postgresql, mysql, sqlserver, etc.)
    --name <name>                Connection name (default: "default")
    [--host <host>]              Database host (default for postgresql: localhost)
    [--port <port>]              Database port (defaults based on db-type)
    [--dbname <name>]            Database name
    [--user <user>]              Database user
    [--password <pass>]          Database password
    [--schema <schema>]          Schema name
  
  disconnect --name <name>       Disconnect from a database

Database Operations:
  test-connection                Test database connection
    (same arguments as connect)

  get-tables --name <name>       Get tables for a connection
    [--schema <schema>]          Filter by schema
  
  query --name <name> --sql <sql>  Execute SQL query
    [--max-rows <n>]             Limit rows returned
  
  info --name <name>             Get database information

Configuration Management:
  save-config --name <config>    Save current connection as named configuration
  load-config --name <config>    Load saved configuration
  list-configs                   List all saved configurations
  delete-config --name <config>  Delete saved configuration

Test Suites:
  test-local-postgres            Run full test on local PostgreSQL
  test-suite                     Run comprehensive test suite
    --db-type <type>             Database type
    (other connection arguments)

PostgreSQL Specific:
  postgres-query --sql <sql>     Execute query on local PostgreSQL

📋 Examples:

  # Test local PostgreSQL (using static configuration)
  node debug-api.js test-local-postgres

  # Test remote PostgreSQL connection
  node debug-api.js test-connection --db-type postgresql --host db.example.com --dbname mydb --user admin --password secret

  # Connect to MySQL and run queries
  node debug-api.js connect --db-type mysql --host localhost --dbname test --user root --password ""
  node debug-api.js query --name default --sql "SHOW TABLES"

  # Save and reuse configurations
  node debug-api.js connect --db-type postgresql --host prod-db --dbname production --user admin --password secret --name prod
  node debug-api.js save-config --name prod
  node debug-api.js load-config --name prod
  node debug-api.js get-tables --name prod

  # Run comprehensive test suite
  node debug-api.js test-suite --db-type mysql --host localhost --dbname test --user root

📝 Supported Database Types:
  postgresql, postgres, mysql, sqlserver, mssql, oracle, db2,
  sap-hana, hana, sybase, netezza, informix, firebird

🔧 Environment Variables:
  API_URL=http://localhost:3000      Backend API URL
  DB_USER=current_user               Default PostgreSQL user
  DB_PASSWORD=                       Database password
  CONFIG_FILE=./debug-api-config.json Configuration file path

💡 Tip: For local PostgreSQL, no connection parameters needed - uses static configuration.
       For remote databases, always provide connection parameters.
    `);
  }
}

/**
 * Main function
 */
async function main() {
  const dbDebugger = new DatabaseDebugger(); // Changed variable name from 'debugger'
  const args = dbDebugger.parseArgs();
  const command = args._[0] || 'help';
  
  console.log('🚀 Database Debugger - Backend Testing Utility');
  console.log('==============================================\n');
  
  // Load saved configurations
  await dbDebugger.loadConfigs();
  
  try {
    switch(command) {
      case 'help':
        dbDebugger.showHelp();
        break;
        
      case 'health':
        await dbDebugger.testHealth();
        break;
        
      case 'postgres-status':
        await dbDebugger.testPostgresStatus();
        break;
        
      case 'active-connections':
        await dbDebugger.getActiveConnections();
        break;
        
      case 'test-connection':
        {
          const dbType = args['db-type'];
          const configName = args.name || args.config;
          
          if (!dbType) {
            console.log('❌ Missing --db-type parameter');
            dbDebugger.showHelp();
            break;
          }
          
          let config;
          
          if (configName && dbDebugger.savedConfigs[configName]) {
            // Use saved configuration
            const saved = dbDebugger.loadConfig(configName);
            config = saved.config;
            console.log(`📁 Using saved configuration: "${configName}"`);
          } else if (dbType === 'postgresql' && !args.host && !configName) {
            // Use local PostgreSQL static configuration
            config = LOCAL_POSTGRES_CONFIG.config;
            console.log('🔧 Using local PostgreSQL static configuration');
          } else {
            // Create config from arguments
            config = dbDebugger.createConfigFromArgs(args);
            
            if (!config.host || !config.dbname || !config.user) {
              console.log('❌ Missing required connection parameters for remote database');
              console.log('Required: --host, --dbname, --user');
              console.log('For local PostgreSQL, no parameters needed');
              break;
            }
          }
          
          await dbDebugger.testConnection(dbType, config, configName);
        }
        break;
        
      case 'connect':
        {
          const dbType = args['db-type'];
          const connectionName = args.name || 'default';
          
          if (!dbType) {
            console.log('❌ Missing --db-type parameter');
            dbDebugger.showHelp();
            break;
          }
          
          let config;
          
          if (args.config && dbDebugger.savedConfigs[args.config]) {
            // Use saved configuration
            const saved = dbDebugger.loadConfig(args.config);
            config = saved.config;
            console.log(`📁 Using saved configuration: "${args.config}"`);
          } else if (dbType === 'postgresql' && !args.host) {
            // Use local PostgreSQL static configuration
            config = LOCAL_POSTGRES_CONFIG.config;
            console.log('🔧 Using local PostgreSQL static configuration');
          } else {
            // Create config from arguments
            config = dbDebugger.createConfigFromArgs(args);
            
            if (!config.host || !config.dbname || !config.user) {
              console.log('❌ Missing required connection parameters for remote database');
              console.log('Required: --host, --dbname, --user');
              console.log('For local PostgreSQL, no parameters needed');
              break;
            }
          }
          
          await dbDebugger.connectToDatabase(dbType, config, connectionName);
        }
        break;
        
      case 'disconnect':
        {
          const connectionName = args.name;
          if (!connectionName) {
            console.log('❌ Missing --name parameter');
            break;
          }
          await dbDebugger.disconnect(connectionName);
        }
        break;
        
      case 'get-tables':
        {
          const connectionName = args.name || 'default';
          const options = {};
          if (args.schema) options.schema = args.schema;
          
          await dbDebugger.getTables(connectionName, options);
        }
        break;
        
      case 'query':
        {
          const connectionName = args.name || 'default';
          const sql = args.sql;
          
          if (!sql) {
            console.log('❌ Missing --sql parameter');
            break;
          }
          
          const options = {};
          if (args['max-rows']) options.maxRows = parseInt(args['max-rows']);
          
          await dbDebugger.executeQuery(connectionName, sql, options);
        }
        break;
        
      case 'info':
        {
          const connectionName = args.name || 'default';
          await dbDebugger.getDatabaseInfo(connectionName);
        }
        break;
        
      case 'postgres-query':
        {
          const sql = args.sql;
          if (!sql) {
            console.log('❌ Missing --sql parameter');
            break;
          }
          
          const params = args.params ? JSON.parse(args.params) : [];
          await dbDebugger.testPostgresQuery(sql, params);
        }
        break;
        
      case 'save-config':
        {
          const configName = args.name;
          if (!configName) {
            console.log('❌ Missing --name parameter');
            break;
          }
          
          // Get active connection or prompt for details
          const activeConnections = Array.from(dbDebugger.activeConnections.entries());
          
          if (activeConnections.length > 0) {
            const connectionName = args.connection || activeConnections[0][0];
            const connection = dbDebugger.activeConnections.get(connectionName);
            
            if (connection) {
              await dbDebugger.saveConfig(configName, connection.dbType, connection.config);
            } else {
              console.log(`❌ Connection "${connectionName}" not found`);
            }
          } else {
            console.log('⚠️ No active connections. Please provide configuration details.');
            const dbType = await dbDebugger.prompt('Database type: ');
            const host = await dbDebugger.prompt('Host: ');
            const dbname = await dbDebugger.prompt('Database name: ');
            const user = await dbDebugger.prompt('Username: ');
            const password = await dbDebugger.prompt('Password: ');
            
            const config = {
              host,
              dbname,
              user,
              password,
              port: args.port || '5432',
              schema: args.schema || 'public'
            };
            
            await dbDebugger.saveConfig(configName, dbType, config);
          }
        }
        break;
        
      case 'load-config':
        {
          const configName = args.name;
          if (!configName) {
            console.log('❌ Missing --name parameter');
            break;
          }
          
          const config = dbDebugger.loadConfig(configName);
          console.log(`📁 Loaded configuration "${configName}":`, config);
        }
        break;
        
      case 'list-configs':
        dbDebugger.listConfigs();
        break;
        
      case 'delete-config':
        {
          const configName = args.name;
          if (!configName) {
            console.log('❌ Missing --name parameter');
            break;
          }
          await dbDebugger.deleteConfig(configName);
        }
        break;
        
      case 'test-local-postgres':
        // Use static local PostgreSQL configuration
        await dbDebugger.runTestSuite('postgresql', LOCAL_POSTGRES_CONFIG.config, 'local-pg');
        break;
        
      case 'test-suite':
        {
          const dbType = args['db-type'] || 'postgresql';
          const connectionName = args.name || 'test-suite';
          
          let config;
          
          if (dbType === 'postgresql' && !args.host) {
            // Use local PostgreSQL static configuration
            config = LOCAL_POSTGRES_CONFIG.config;
            console.log('🔧 Using local PostgreSQL static configuration');
          } else {
            // Create config from arguments
            config = dbDebugger.createConfigFromArgs(args);
            
            if (!config.host || !config.dbname || !config.user) {
              console.log('❌ Missing required connection parameters for remote database');
              console.log('Required for test suite: --host, --dbname, --user');
              console.log('For local PostgreSQL test suite, run: node debug-api.js test-local-postgres');
              break;
            }
          }
          
          await dbDebugger.runTestSuite(dbType, config, connectionName);
        }
        break;
        
      default:
        console.log(`❌ Unknown command: ${command}`);
        dbDebugger.showHelp();
        break;
    }
  } catch (error) {
    console.error('\n💥 Fatal Error:', error.message);
    console.error('Stack:', error.stack);
    process.exit(1);
  }
  
  console.log('\n✅ Debug session completed.');
}

// Run the main function
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { DatabaseDebugger };