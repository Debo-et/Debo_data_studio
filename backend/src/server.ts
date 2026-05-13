import app from './app';
import { initializeLocalPostgresConnection, localPostgres } from './database/local-postgres';
import { initMetadataSchema } from './database/init-metadata-schema'; // <-- import
import { Logger } from './database/inspection/postgreSql-inspector';

// Set log level for startup messages
Logger.setLogLevel('INFO');

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || 'localhost';

/**
 * Initialize the application with PostgreSQL connection
 */
async function initializeApplication(): Promise<void> {
  console.log('🚀 Starting Database Metadata Wizard Backend...');

  try {
    // Step 1: Initialize PostgreSQL connection
    console.log('🔌 Initializing PostgreSQL connection...');
    await initializeLocalPostgresConnection();

    // Step 2: Verify connection is ready
    console.log('🔍 Verifying PostgreSQL connection...');
    const isConnected = await localPostgres.testConnection();
    if (!isConnected) {
      console.error('❌ PostgreSQL connection verification failed');
      console.error('💡 Please check if PostgreSQL is running and accessible');
      process.exit(1);
    }

    console.log('✅ PostgreSQL connection established and ready');

    // Step 3: Initialize metadata schema (create tables if missing)
    await initMetadataSchema(localPostgres.getPool()); // <-- need to expose getPool() from local-postgres

    // Step 4: Start HTTP server
    const server = app.listen(PORT, () => {
      console.log(`
🚀 Server running at:
📍 Local: http://${HOST}:${PORT}
📍 Network: http://0.0.0.0:${PORT}

📊 Health Check: http://${HOST}:${PORT}/health
🔍 Database API: http://${HOST}:${PORT}/api/database

🐘 PostgreSQL Connection: ✅ ESTABLISHED
📚 Metadata Tables: ✅ CREATED/VERIFIED

Press Ctrl+C to stop
      `);
    });

    // Graceful shutdown
    const gracefulShutdown = async (signal: string) => {
      console.log(`${signal} signal received: starting graceful shutdown`);

      server.close(async () => {
        console.log('HTTP server closed');
        await localPostgres.shutdown();
        console.log('Graceful shutdown completed');
        process.exit(0);
      });

      setTimeout(() => {
        console.error('Could not close connections in time, forcefully shutting down');
        process.exit(1);
      }, 10000);
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));

  } catch (error) {
    console.error('❌ Failed to initialize application:');
    console.error(error);

    if (error instanceof Error) {
      const errorMsg = error.message.toLowerCase();

      if (errorMsg.includes('connection refused') || errorMsg.includes('connect econnrefused')) {
        console.error('\n🔴 CRITICAL: Cannot connect to PostgreSQL database');
        console.error('   PostgreSQL is either not running or not accessible');
        console.error('\n💡 SOLUTION:');
        console.error('   1. Make sure PostgreSQL is installed');
        console.error('   2. Start PostgreSQL service:');
        console.error('      - Ubuntu/Debian: sudo systemctl start postgresql');
        console.error('      - macOS: brew services start postgresql');
        console.error('      - Windows: Start PostgreSQL service in Services');
        console.error('   3. Verify it\'s running on port 5432');
        console.error('\n📝 Default connection parameters:');
        console.error('   Host: localhost');
        console.error('   Port: 5432');
        console.error('   User: postgres');
        console.error('   Database: postgres');
      } else if (errorMsg.includes('authentication failed') || errorMsg.includes('password authentication')) {
        console.error('\n🔴 AUTHENTICATION FAILED');
        console.error('   Invalid username or password for PostgreSQL');
        console.error('\n💡 Set correct credentials via environment variables:');
        console.error('   DB_USER=your_username');
        console.error('   DB_PASSWORD=your_password');
      } else if (errorMsg.includes('relation') && errorMsg.includes('does not exist')) {
        // This might be related to table creation – but we already handled it.
        console.error('\n⚠️ Schema initialization error. Check database permissions.');
      }
    }

    process.exit(1);
  }
}

// Start the application
initializeApplication().catch(error => {
  console.error('Failed to initialize application:', error);
  process.exit(1);
});

export default app;