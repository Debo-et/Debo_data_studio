// DatabaseMetadataWizard.tsx
import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  Database,
  CheckCircle, 
  X, 
  ChevronRight,
  ChevronLeft,
  TestTube,
  Table,
  Columns,
  AlertCircle,
  RefreshCw,
  Server,
  Loader2
} from 'lucide-react';
import { Button } from '../components/ui/Button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../components/ui/select';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { Badge } from '../components/ui/badge';
import { ScrollArea } from '../components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/Tabs';
import { toast } from 'react-hot-toast';
import {
  DatabaseApiService,
  DatabaseType,
  DatabaseConfig as ClientDatabaseConfig,
  ConnectionInfo as ClientConnectionInfo,
  TestConnectionResult as ClientTestConnectionResult,
  ConnectResult as ClientConnectResult,
  TableListResult as ClientTableListResult,
  DisconnectResult as ClientDisconnectResult,
  TableInfo as ClientTableInfo,
  ColumnMetadata as ClientColumnMetadata
} from '../services/database-api.service';

interface DatabaseMetadataWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (metadata: any) => void;
  existingConnections?: ClientConnectionInfo[];
}

interface EnhancedTableMetadata extends ClientTableInfo {
  num_columns: number;
}

interface TableSelection {
  include: boolean;
  selectedColumns: string[];
}

interface EnhancedFormData {
  name: string;
  purpose: string;
  description: string;
  dbType: DatabaseType | '';
  connection: {
    dbname: string;
    host?: string;
    port?: string;
    user?: string;
    password?: string;
    schema?: string;
  };
  selectedTables: string[];
  tableSelections: Record<string, TableSelection>;
  schemaInference: Record<string, any>;
  connectionTested: boolean;
  previewData: Record<string, any>;
  connectionId?: string;
  lastTested?: string;
  useExistingConnection?: boolean;
  selectedConnectionId?: string;
}

// Database type definitions
const SUPPORTED_DATABASES: Array<{ value: DatabaseType; label: string; defaultPort: string }> = [
  { value: 'postgresql', label: 'PostgreSQL', defaultPort: '5432' },
  { value: 'postgres', label: 'PostgreSQL', defaultPort: '5432' },
  { value: 'mysql', label: 'MySQL', defaultPort: '3306' },
  { value: 'oracle', label: 'Oracle Database', defaultPort: '1521' },
  { value: 'sqlserver', label: 'SQL Server', defaultPort: '1433' },
  { value: 'mssql', label: 'SQL Server', defaultPort: '1433' },
  { value: 'db2', label: 'IBM DB2', defaultPort: '50000' },
  { value: 'sap-hana', label: 'SAP HANA', defaultPort: '30015' },
  { value: 'hana', label: 'SAP HANA', defaultPort: '30015' },
  { value: 'sybase', label: 'Sybase', defaultPort: '5000' },
  { value: 'netezza', label: 'Netezza', defaultPort: '5480' },
  { value: 'informix', label: 'Informix', defaultPort: '9088' },
  { value: 'firebird', label: 'Firebird', defaultPort: '3050' },
];

const DATABASE_CONNECTION_REQUIREMENTS: Record<DatabaseType, { 
  needsHost: boolean; 
  needsPort: boolean; 
  needsAuth: boolean; 
  needsSchema: boolean;
  connectionInfo: string;
}> = {
  'mysql': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: false,
    connectionInfo: 'MySQL connection parameters'
  },
  'postgresql': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'PostgreSQL connection parameters'
  },
  'postgres': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'PostgreSQL connection parameters'
  },
  'oracle': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'Oracle Database connection parameters'
  },
  'sqlserver': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'SQL Server connection parameters'
  },
  'mssql': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'SQL Server connection parameters'
  },
  'db2': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'IBM DB2 connection parameters'
  },
  'sap-hana': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'SAP HANA connection parameters'
  },
  'hana': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'SAP HANA connection parameters'
  },
  'sybase': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'Sybase connection parameters'
  },
  'netezza': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'Netezza connection parameters'
  },
  'informix': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: true,
    connectionInfo: 'Informix connection parameters'
  },
  'firebird': { 
    needsHost: true, 
    needsPort: true, 
    needsAuth: true, 
    needsSchema: false,
    connectionInfo: 'Firebird connection parameters'
  },
};

// Get default connection requirements for when no database is selected
const getDefaultConnectionRequirements = () => ({
  needsHost: true,
  needsPort: true,
  needsAuth: true,
  needsSchema: false,
  connectionInfo: 'Database connection parameters'
});

const DatabaseMetadataWizard: React.FC<DatabaseMetadataWizardProps> = ({
  isOpen,
  onClose,
  onSave,
  existingConnections = []
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [connectionStatus, setConnectionStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [connectionError, setConnectionError] = useState<string>('');
  const [availableTables, setAvailableTables] = useState<EnhancedTableMetadata[]>([]);
  const [isFetchingMetadata, setIsFetchingMetadata] = useState(false);
  const [isConnectionHealthy, setIsConnectionHealthy] = useState<boolean>(false);
  const [databaseStats, setDatabaseStats] = useState<any>(null);
  const [, setPresetSelected] = useState<string>('');
  const [isDisconnecting, setIsDisconnecting] = useState(false);
  const [, setDatabaseConfigured] = useState<boolean>(false);

  // Initialize form data with no default database
  const [formData, setFormData] = useState<EnhancedFormData>({
    name: '',
    purpose: '',
    description: '',
    dbType: '', // No default database type
    connection: {
      dbname: '',
      host: '',
      port: '',
      user: '',
      password: '',
      schema: ''
    },
    selectedTables: [],
    tableSelections: {},
    schemaInference: {},
    connectionTested: false,
    previewData: {},
    lastTested: '',
    useExistingConnection: false,
    selectedConnectionId: ''
  });

  // Get active connections for the current database type
  const activeConnectionsForType = useMemo(() => {
    if (!formData.dbType) return [];
    return existingConnections.filter(conn => conn.dbType === formData.dbType);
  }, [existingConnections, formData.dbType]);

  const connectionRequirements = useMemo(() => {
    if (!formData.dbType) {
      return getDefaultConnectionRequirements();
    }
    return DATABASE_CONNECTION_REQUIREMENTS[formData.dbType] || 
           DATABASE_CONNECTION_REQUIREMENTS.postgresql;
  }, [formData.dbType]);

  // Initialize form based on selected connection
  const initializeFromConnection = useCallback((connection: ClientConnectionInfo) => {
    setFormData(prev => ({
      ...prev,
      dbType: connection.dbType as DatabaseType,
      connection: {
        dbname: connection.config.dbname || '',
        host: connection.config.host,
        port: connection.config.port,
        user: connection.config.user,
        password: connection.config.password,
        schema: connection.config.schema
      },
      connectionId: connection.connectionId,
      selectedConnectionId: connection.connectionId,
      useExistingConnection: true,
      connectionTested: true
    }));
    setIsConnectionHealthy(true);
    setConnectionStatus('success');
    setDatabaseConfigured(true);
  }, []);

  // Effect to update form when database type changes
  useEffect(() => {
    if (formData.dbType) {
      const defaultPort = SUPPORTED_DATABASES.find(db => db.value === formData.dbType)?.defaultPort || '';
      setFormData(prev => ({
        ...prev,
        connection: {
          ...prev.connection,
          port: defaultPort,
          host: connectionRequirements.needsHost ? prev.connection.host || 'localhost' : undefined,
          schema: connectionRequirements.needsSchema ? prev.connection.schema || '' : undefined
        }
      }));
      setDatabaseConfigured(true);
    } else {
      setDatabaseConfigured(false);
    }
  }, [formData.dbType, connectionRequirements]);

  // Get database display name
  const getDatabaseDisplayName = useCallback((dbType: DatabaseType): string => {
    return DatabaseApiService.getDatabaseDisplayName(dbType);
  }, []);

  // Get connection config for API calls
  const getConnectionConfig = useCallback((): ClientDatabaseConfig => {
    const { connection } = formData;
    const config: ClientDatabaseConfig = {
      dbname: connection.dbname
    };

    if (connection.host && connectionRequirements.needsHost) {
      config.host = connection.host;
    }
    if (connection.port && connectionRequirements.needsPort) {
      config.port = connection.port;
    }
    if (connection.user && connectionRequirements.needsAuth) {
      config.user = connection.user;
    }
    if (connection.password && connectionRequirements.needsAuth) {
      config.password = connection.password;
    }
    if (connection.schema && connectionRequirements.needsSchema) {
      config.schema = connection.schema;
    }

    return config;
  }, [formData.connection, connectionRequirements]);

  // Validate connection parameters
  const validateConnectionParameters = useCallback((): boolean => {
    const { connection } = formData;
    
    if (formData.useExistingConnection && formData.selectedConnectionId) {
      return true;
    }

    if (!formData.dbType) {
      setConnectionError('Please select a database type first');
      return false;
    }

    const errors = DatabaseApiService.validateConfig({
      dbname: connection.dbname,
      host: connection.host,
      port: connection.port,
      user: connection.user,
      password: connection.password
    });

    if (errors.length > 0) {
      setConnectionError(errors[0]);
      return false;
    }

    setConnectionError('');
    return true;
  }, [formData]);

  // Test connection and retrieve metadata
  const testConnection = useCallback(async () => {
    setIsFetchingMetadata(true);
    setConnectionStatus('testing');
    setConnectionError('Validating connection parameters...');
    setAvailableTables([]);
    setDatabaseStats(null);

    try {
      if (!validateConnectionParameters()) {
        throw new Error(connectionError || 'Invalid connection parameters');
      }

      if (!formData.dbType) {
        throw new Error('Database type is not selected');
      }

      const apiService = new DatabaseApiService();
      const config = getConnectionConfig();
      
      setConnectionError('Testing connection...');
      const testResult: ClientTestConnectionResult = await apiService.testConnection(
        formData.dbType,
        config
      );

      if (!testResult.success) {
        throw new Error(testResult.error || 'Connection test failed');
      }

      setConnectionError('Connecting to database...');
      const connectResult: ClientConnectResult = await apiService.connect(
        formData.dbType,
        config
      );

      if (!connectResult.success || !connectResult.connectionId) {
        throw new Error(connectResult.error || 'Failed to establish connection');
      }

      const connectionId = connectResult.connectionId;

      setConnectionError('Retrieving table metadata...');
      const tablesResult: ClientTableListResult = await apiService.getTables(connectionId, {
        includeViews: true,
        includeSystemTables: false
      });

      if (!tablesResult.success) {
        throw new Error(tablesResult.error || 'Failed to retrieve tables');
      }

      // Transform tables to enhanced format
      const tablesWithColumns: EnhancedTableMetadata[] = tablesResult.tables.map(table => ({
        ...table,
        num_columns: table.columns.length
      }));

      setAvailableTables(tablesWithColumns);
      setConnectionStatus('success');
      setConnectionError('');
      setIsConnectionHealthy(true);
      
      const initialTableSelections: Record<string, TableSelection> = {};
      tablesWithColumns.forEach(table => {
        // Use full table identifier including schema
        const tableIdentifier = `${table.schemaname}.${table.tablename}`;
        initialTableSelections[tableIdentifier] = {
          include: false,
          selectedColumns: table.columns.map(col => col.name)
        };
      });

      setFormData(prev => ({ 
        ...prev, 
        connectionTested: true,
        connectionId: connectionId,
        tableSelections: initialTableSelections,
        selectedTables: [],
        lastTested: new Date().toISOString()
      }));

      // Get database info for statistics
      try {
        const dbInfo = await apiService.getDatabaseInfo(connectionId);
        if (dbInfo.success && dbInfo.info) {
          setDatabaseStats({
            version: dbInfo.info.version,
            name: dbInfo.info.name,
            databaseSize: 'Unknown',
            tableCount: tablesWithColumns.length,
            activeConnections: 1
          });
        }
      } catch (statsError) {
        console.warn('Failed to get database stats:', statsError);
      }

      toast.success('Connection successful!');
      
    } catch (error: any) {
      setConnectionStatus('error');
      const userFriendlyError = getUserFriendlyErrorMessage(error, formData.dbType || 'database');
      setConnectionError(userFriendlyError);
      setFormData(prev => ({ 
        ...prev, 
        connectionTested: false,
        connectionId: undefined,
        useExistingConnection: false
      }));
      setIsConnectionHealthy(false);
      setDatabaseStats(null);
      
      toast.error(`Connection failed: ${userFriendlyError}`);
    } finally {
      setIsFetchingMetadata(false);
    }
  }, [formData, validateConnectionParameters, getConnectionConfig]);

  // Get user-friendly error message
  const getUserFriendlyErrorMessage = useCallback((error: Error, dbType: string): string => {
    const errorMsg = error.message.toLowerCase();
    
    const errorMapping: Record<string, string> = {
      'connection refused': `Cannot connect to ${dbType} server. Check if the server is running and the host/port are correct.`,
      'econnrefused': `Cannot connect to ${dbType} server. Check if the server is running and the host/port are correct.`,
      'authentication failed': 'Authentication failed. Please check your username and password.',
      'invalid password': 'Authentication failed. Please check your username and password.',
      'database.*not exist': 'Database does not exist. Please check the database name.',
      'timeout': 'Connection timeout. Check your network connection and server availability.',
      'permission denied': 'Permission denied. Check your user privileges for the database.',
      'access denied': 'Permission denied. Check your user privileges for the database.',
      'driver': 'Database driver issue. Make sure the required database client is installed.'
    };

    for (const [key, message] of Object.entries(errorMapping)) {
      if (errorMsg.includes(key)) {
        return message;
      }
    }
    
    return error.message;
  }, []);

  // Disconnect database
  const disconnectDatabase = useCallback(async (): Promise<void> => {
    if (!formData.connectionId || formData.useExistingConnection) return;

    setIsDisconnecting(true);
    try {
      const apiService = new DatabaseApiService();
      const result: ClientDisconnectResult = await apiService.disconnect(formData.connectionId);
      if (result.success) {
        setFormData(prev => ({ 
          ...prev, 
          connectionId: undefined,
          connectionTested: false,
          useExistingConnection: false
        }));
        setIsConnectionHealthy(false);
        setDatabaseStats(null);
        setAvailableTables([]);
        setConnectionStatus('idle');
        toast.success('Disconnected successfully');
      } else {
        throw new Error(result.error || 'Disconnect failed');
      }
    } catch (error) {
      console.error('Error during disconnect:', error);
      toast.error('Failed to disconnect database');
    } finally {
      setIsDisconnecting(false);
    }
  }, [formData.connectionId, formData.useExistingConnection]);

  // Step 1: Database Selection
  const Step1 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium">Select Database Type</h3>
        <p className="text-sm text-gray-500">
          Choose the database system you want to configure. This selection determines the connection parameters and features available.
        </p>
      </div>

      <div className="space-y-4">
        <div>
          <Label htmlFor="dbType">Database Type *</Label>
          <Select
            value={formData.dbType}
            onValueChange={(value: DatabaseType) => {
              setFormData(prev => ({ 
                ...prev, 
                dbType: value,
                connectionId: undefined,
                connectionTested: false,
                useExistingConnection: false,
                // Reset connection parameters when database type changes
                connection: {
                  dbname: '',
                  host: '',
                  port: '',
                  user: '',
                  password: '',
                  schema: ''
                }
              }));
              setPresetSelected('');
              setConnectionStatus('idle');
              setIsConnectionHealthy(false);
              setDatabaseConfigured(true);
            }}
          >
            <SelectTrigger className="mt-1">
              <SelectValue placeholder="Select a database type" />
            </SelectTrigger>
            <SelectContent>
              {SUPPORTED_DATABASES.map(db => (
                <SelectItem key={db.value} value={db.value}>
                  <div className="flex items-center space-x-2">
                    <span>{db.label}</span>
                    {db.defaultPort && (
                      <Badge variant="outline" className="text-xs">
                        Port: {db.defaultPort}
                      </Badge>
                    )}
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <p className="text-xs text-gray-500 mt-1">
            Select your preferred database system. This cannot be changed later without reconfiguring the connection.
          </p>
        </div>

        {formData.dbType && (
          <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
            <div className="flex items-center space-x-2 text-blue-700 dark:text-blue-300">
              <CheckCircle className="h-4 w-4" />
              <span className="text-sm font-medium">
                {getDatabaseDisplayName(formData.dbType)} selected
              </span>
            </div>
            <div className="text-xs text-blue-600 dark:text-blue-400 mt-1">
              You can now proceed to configure the connection parameters for your {getDatabaseDisplayName(formData.dbType)} database.
            </div>
          </div>
        )}
      </div>
    </div>
  );

  // Step 2: General Properties
  const Step2 = () => (
    <div className="space-y-6">
      <div>
        <h3 className="text-lg font-medium">General Properties</h3>
        <p className="text-sm text-gray-500">
          Define the basic information for your {formData.dbType ? getDatabaseDisplayName(formData.dbType) : 'database'} connection metadata.
        </p>
      </div>

      <div className="space-y-4">
        <div>
          <Label htmlFor="name">Name *</Label>
          <Input
            id="name"
            value={formData.name}
            onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
            placeholder="e.g., Production_Customer_DB"
            className="mt-1"
          />
          <p className="text-xs text-gray-500 mt-1">
            A unique, descriptive name for this database connection metadata
          </p>
        </div>

        <div>
          <Label htmlFor="purpose">Purpose</Label>
          <Input
            id="purpose"
            value={formData.purpose}
            onChange={(e) => setFormData(prev => ({ ...prev, purpose: e.target.value }))}
            placeholder="e.g., Used for production billing data reporting"
            className="mt-1"
          />
          <p className="text-xs text-gray-500 mt-1">
            Brief summary of the primary objective or use case
          </p>
        </div>

        <div>
          <Label htmlFor="description">Description</Label>
          <textarea
            id="description"
            value={formData.description}
            onChange={(e) => setFormData(prev => ({ ...prev, description: e.target.value }))}
            placeholder="Detailed description of the connection scope, data usage, and operational context..."
            rows={4}
            className="w-full mt-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent dark:bg-gray-800 dark:border-gray-600 dark:text-white"
          />
          <p className="text-xs text-gray-500 mt-1">
            Clarify which jobs utilize this connection, data types, and relevant context
          </p>
        </div>
      </div>
    </div>
  );

  // Step 3: Database Connection
  const Step3 = () => {
    if (!formData.dbType) {
      return (
        <div className="space-y-6">
          <div>
            <h3 className="text-lg font-medium">Database Connection</h3>
            <p className="text-sm text-gray-500">
              Please select a database type first to configure the connection.
            </p>
          </div>
          <div className="text-center py-8 text-gray-500">
            <AlertCircle className="h-8 w-8 mx-auto text-yellow-500" />
            <p className="mt-2">Database type not selected</p>
            <Button
              onClick={() => setCurrentStep(1)}
              variant="outline"
              size="sm"
              className="mt-4"
            >
              Go Back to Database Selection
            </Button>
          </div>
        </div>
      );
    }

    return (
      <div className="space-y-6">
        <div>
          <h3 className="text-lg font-medium">Database Connection</h3>
          <p className="text-sm text-gray-500">
            Configure the {connectionRequirements.connectionInfo} and test the connection.
          </p>
        </div>

        {/* Existing Connections Selector */}
        {activeConnectionsForType.length > 0 && (
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <Label>Use Existing Connection</Label>
              <div className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={formData.useExistingConnection || false}
                  onChange={(e) => {
                    const useExisting = e.target.checked;
                    setFormData(prev => ({ 
                      ...prev, 
                      useExistingConnection: useExisting,
                      connectionTested: useExisting && !!prev.selectedConnectionId,
                      connectionId: useExisting ? prev.selectedConnectionId : undefined
                    }));
                    if (useExisting && formData.selectedConnectionId) {
                      setIsConnectionHealthy(true);
                      setConnectionStatus('success');
                    } else {
                      setIsConnectionHealthy(false);
                      setConnectionStatus('idle');
                    }
                  }}
                  className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
                <span className="text-sm">Reuse active connection</span>
              </div>
            </div>

            {formData.useExistingConnection && (
              <Select
                value={formData.selectedConnectionId}
                onValueChange={(value) => {
                  const connection = activeConnectionsForType.find(c => c.connectionId === value);
                  if (connection) {
                    initializeFromConnection(connection);
                  }
                }}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select existing connection" />
                </SelectTrigger>
                <SelectContent>
                  {activeConnectionsForType.map(conn => (
                    <SelectItem key={conn.connectionId} value={conn.connectionId}>
                      <div className="flex flex-col">
                        <span>{conn.config.dbname} @ {conn.config.host}:{conn.config.port}</span>
                        <span className="text-xs text-gray-500">
                          {getDatabaseDisplayName(conn.dbType as DatabaseType)}
                        </span>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          </div>
        )}

        {!formData.useExistingConnection && (
          <div className="grid grid-cols-2 gap-4">
            {connectionRequirements.needsHost && (
              <div>
                <Label htmlFor="host">Host *</Label>
                <Input
                  id="host"
                  value={formData.connection.host || ''}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    connection: { ...prev.connection, host: e.target.value },
                    connectionId: undefined,
                    connectionTested: false
                  }))}
                  placeholder="localhost"
                  className="mt-1"
                />
              </div>
            )}

            {connectionRequirements.needsPort && (
              <div>
                <Label htmlFor="port">Port</Label>
                <Input
                  id="port"
                  value={formData.connection.port || ''}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    connection: { ...prev.connection, port: e.target.value },
                    connectionId: undefined,
                    connectionTested: false
                  }))}
                  placeholder={SUPPORTED_DATABASES.find(db => db.value === formData.dbType)?.defaultPort || ''}
                  className="mt-1"
                />
              </div>
            )}

            <div className={connectionRequirements.needsHost && connectionRequirements.needsPort ? 'col-span-2' : ''}>
              <Label htmlFor="database">Database Name *</Label>
              <Input
                id="database"
                value={formData.connection.dbname}
                onChange={(e) => setFormData(prev => ({
                  ...prev,
                  connection: { ...prev.connection, dbname: e.target.value },
                  connectionId: undefined,
                  connectionTested: false
                }))}
                placeholder="my_database"
                className="mt-1"
              />
            </div>

            {connectionRequirements.needsAuth && (
              <>
                <div>
                  <Label htmlFor="username">Username *</Label>
                  <Input
                    id="username"
                    value={formData.connection.user || ''}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      connection: { ...prev.connection, user: e.target.value },
                      connectionId: undefined,
                      connectionTested: false
                    }))}
                    placeholder="username"
                    className="mt-1"
                  />
                </div>

                <div>
                  <Label htmlFor="password">Password *</Label>
                  <Input
                    id="password"
                    type="password"
                    value={formData.connection.password || ''}
                    onChange={(e) => setFormData(prev => ({
                      ...prev,
                      connection: { ...prev.connection, password: e.target.value },
                      connectionId: undefined,
                      connectionTested: false
                    }))}
                    placeholder="••••••••"
                    className="mt-1"
                  />
                </div>
              </>
            )}

            {connectionRequirements.needsSchema && (
              <div className="col-span-2">
                <Label htmlFor="schema">Schema</Label>
                <Input
                  id="schema"
                  value={formData.connection.schema || ''}
                  onChange={(e) => setFormData(prev => ({
                    ...prev,
                    connection: { ...prev.connection, schema: e.target.value },
                    connectionId: undefined,
                    connectionTested: false
                  }))}
                  placeholder="public"
                  className="mt-1"
                />
              </div>
            )}
          </div>
        )}

        <div className="flex items-center justify-between p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
          <div className="flex items-center space-x-3">
            <div className={`w-3 h-3 rounded-full ${
              connectionStatus === 'idle' ? 'bg-gray-400' :
              connectionStatus === 'testing' ? 'bg-yellow-400 animate-pulse' :
              connectionStatus === 'success' ? 'bg-green-400' : 'bg-red-400'
            }`} />
            <div className="flex flex-col">
              <span className="text-sm font-medium">
                {connectionStatus === 'idle' && 'Not tested'}
                {connectionStatus === 'testing' && 'Testing connection...'}
                {connectionStatus === 'success' && 'Connection successful'}
                {connectionStatus === 'error' && 'Connection failed'}
              </span>
              {connectionError && connectionStatus === 'error' && (
                <span className="text-sm text-red-500 mt-1">{connectionError}</span>
              )}
              {connectionStatus === 'success' && formData.lastTested && (
                <span className="text-xs text-gray-500 mt-1">
                  Last tested: {new Date(formData.lastTested).toLocaleTimeString()}
                </span>
              )}
            </div>
          </div>
          
          <div className="flex items-center space-x-2">
            {formData.connectionId && !formData.useExistingConnection && (
              <Button
                onClick={disconnectDatabase}
                variant="outline"
                size="sm"
                className="flex items-center space-x-1"
                disabled={isDisconnecting}
              >
                {isDisconnecting ? (
                  <Loader2 className="h-3 w-3 animate-spin" />
                ) : (
                  <X className="h-3 w-3" />
                )}
                <span>Disconnect</span>
              </Button>
            )}
            <Button
              onClick={testConnection}
              disabled={isFetchingMetadata || 
                (!formData.useExistingConnection && (
                  !formData.connection.dbname || 
                  (connectionRequirements.needsAuth && !formData.connection.user) ||
                  (connectionRequirements.needsHost && !formData.connection.host)
                ))}
              variant="outline"
              className="flex items-center space-x-2"
            >
              {isFetchingMetadata ? (
                <>
                  <RefreshCw className="h-4 w-4 animate-spin" />
                  <span>Testing...</span>
                </>
              ) : formData.connectionTested ? (
                <>
                  <TestTube className="h-4 w-4" />
                  <span>Re-test Connection</span>
                </>
              ) : (
                <>
                  <TestTube className="h-4 w-4" />
                  <span>Test Connection</span>
                </>
              )}
            </Button>
          </div>
        </div>

        {databaseStats && (
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium flex items-center">
                <Server className="h-4 w-4 mr-2" />
                Database Statistics
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <div className="text-gray-500">Database Name</div>
                  <div className="font-medium">{databaseStats.name}</div>
                </div>
                <div>
                  <div className="text-gray-500">Table Count</div>
                  <div className="font-medium">{databaseStats.tableCount}</div>
                </div>
                <div>
                  <div className="text-gray-500">Version</div>
                  <div className="font-medium truncate">{databaseStats.version}</div>
                </div>
                <div>
                  <div className="text-gray-500">Status</div>
                  <div className="font-medium">
                    <Badge variant="outline" className="text-xs bg-green-100 text-green-800">
                      Connected
                    </Badge>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {availableTables.length > 0 && (
          <div className="p-3 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2 text-green-700 dark:text-green-300">
                <CheckCircle className="h-4 w-4" />
                <span className="text-sm font-medium">
                  Successfully connected and found {availableTables.length} tables
                </span>
              </div>
              <Badge variant="outline" className="text-xs">
                {formData.connectionId ? `ID: ${formData.connectionId.substring(0, 8)}...` : 'Connected'}
              </Badge>
            </div>
            <div className="text-xs text-green-600 dark:text-green-400 mt-1">
              Connection is healthy and ready for metadata configuration
            </div>
          </div>
        )}
      </div>
    );
  };

  // Step 4: Table Selection
  const Step4 = () => {
    if (!formData.dbType || !formData.connectionTested) {
      return (
        <div className="space-y-6">
          <div>
            <h3 className="text-lg font-medium">Table Selection</h3>
            <p className="text-sm text-gray-500">
              Please configure and test the database connection first to load available tables.
            </p>
          </div>
          <div className="text-center py-8 text-gray-500">
            <AlertCircle className="h-8 w-8 mx-auto text-yellow-500" />
            <p className="mt-2">Database connection not established</p>
            <Button
              onClick={() => setCurrentStep(3)}
              variant="outline"
              size="sm"
              className="mt-4"
            >
              Go Back to Connection Configuration
            </Button>
          </div>
        </div>
      );
    }

    return (
      <div className="space-y-6">
        <div>
          <h3 className="text-lg font-medium">Table Selection</h3>
          <p className="text-sm text-gray-500">
            Select the tables you want to include in your metadata configuration.
          </p>
        </div>
        
        {availableTables.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <div className="space-y-2">
              <AlertCircle className="h-8 w-8 mx-auto text-yellow-500" />
              <p>No tables available in the selected database/schema.</p>
              <Button
                onClick={() => setCurrentStep(3)}
                variant="outline"
                size="sm"
              >
                Go Back to Connection
              </Button>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">
                {availableTables.length} tables found in database
              </span>
              <div className="space-x-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    const newSelections: Record<string, TableSelection> = {};
                    availableTables.forEach(table => {
                      const tableIdentifier = `${table.schemaname}.${table.tablename}`;
                      newSelections[tableIdentifier] = {
                        include: true,
                        selectedColumns: table.columns.map((col: ClientColumnMetadata) => col.name)
                      };
                    });
                    setFormData(prev => ({
                      ...prev,
                      tableSelections: newSelections,
                      selectedTables: availableTables.map(t => `${t.schemaname}.${t.tablename}`)
                    }));
                  }}
                >
                  Select All
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    const newSelections: Record<string, TableSelection> = {};
                    availableTables.forEach(table => {
                      const tableIdentifier = `${table.schemaname}.${table.tablename}`;
                      newSelections[tableIdentifier] = {
                        include: false,
                        selectedColumns: table.columns.map((col: ClientColumnMetadata) => col.name)
                      };
                    });
                    setFormData(prev => ({
                      ...prev,
                      tableSelections: newSelections,
                      selectedTables: []
                    }));
                  }}
                >
                  Clear All
                </Button>
              </div>
            </div>
            
            <ScrollArea className="h-64 border rounded-md">
              <div className="p-4 space-y-2">
                {availableTables.map((table) => {
                  const tableIdentifier = `${table.schemaname}.${table.tablename}`;
                  return (
                    <div
                      key={tableIdentifier}
                      className="flex items-center space-x-3 p-2 hover:bg-gray-50 dark:hover:bg-gray-800 rounded"
                    >
                      <input
                        type="checkbox"
                        checked={formData.tableSelections[tableIdentifier]?.include || false}
                        onChange={(e) => {
                          const newSelections = {
                            ...formData.tableSelections,
                            [tableIdentifier]: {
                              include: e.target.checked,
                              selectedColumns: e.target.checked ? table.columns.map((col: ClientColumnMetadata) => col.name) : []
                            }
                          };
                          setFormData(prev => ({
                            ...prev,
                            tableSelections: newSelections,
                            selectedTables: Object.keys(newSelections).filter(
                              key => newSelections[key].include
                            )
                          }));
                        }}
                        className="h-4 w-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                      <Table className="h-4 w-4 text-gray-400" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center space-x-2">
                          <span className="text-sm font-medium truncate">
                            {table.tablename}
                          </span>
                          <Badge variant="outline" className="text-xs">
                            {table.tabletype}
                          </Badge>
                          {table.schemaname !== 'public' && table.schemaname && (
                            <Badge variant="secondary" className="text-xs">
                              {table.schemaname}
                            </Badge>
                          )}
                        </div>
                        <div className="text-xs text-gray-500">
                          {table.columns.length} columns • {table.tabletype}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </ScrollArea>

            {formData.selectedTables.length > 0 && (
              <div className="p-3 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                <div className="flex items-center justify-between">
                  <div className="text-sm text-blue-700 dark:text-blue-300">
                    <strong>{formData.selectedTables.length} tables selected</strong>
                    {formData.selectedTables.length > 0 && (
                      <div className="text-xs mt-1">
                        {formData.selectedTables.slice(0, 3).map(table => {
                          const parts = table.split('.');
                          return parts.length > 1 ? parts[1] : table;
                        }).join(', ')}
                        {formData.selectedTables.length > 3 && '...'}
                      </div>
                    )}
                  </div>
                  <Badge variant="outline" className="text-xs">
                    {formData.selectedTables.length} of {availableTables.length}
                  </Badge>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  // Step 5: Schema Configuration
  const Step5 = () => {
    const selectedTables = availableTables.filter(
      table => formData.selectedTables.includes(`${table.schemaname}.${table.tablename}`)
    );

    if (!formData.dbType || !formData.connectionTested) {
      return (
        <div className="space-y-6">
          <div>
            <h3 className="text-lg font-medium">Schema Configuration</h3>
            <p className="text-sm text-gray-500">
              Please configure the database connection and select tables first.
            </p>
          </div>
          <div className="text-center py-8 text-gray-500">
            <AlertCircle className="h-8 w-8 mx-auto text-yellow-500" />
            <p className="mt-2">Database connection not established or no tables selected</p>
            <Button
              onClick={() => setCurrentStep(3)}
              variant="outline"
              size="sm"
              className="mt-4"
            >
              Go Back to Connection Configuration
            </Button>
          </div>
        </div>
      );
    }

    return (
      <div className="space-y-6">
        <div>
          <h3 className="text-lg font-medium">Schema Configuration</h3>
          <p className="text-sm text-gray-500">
            Review and configure the schema inference for selected tables.
          </p>
        </div>

        {selectedTables.length === 0 ? (
          <div className="text-center py-8 text-gray-500">
            <div className="space-y-2">
              <AlertCircle className="h-8 w-8 mx-auto text-yellow-500" />
              <p>No tables selected. Please select tables in the previous step.</p>
              <Button
                onClick={() => setCurrentStep(4)}
                variant="outline"
                size="sm"
              >
                Go Back to Table Selection
              </Button>
            </div>
          </div>
        ) : (
          <Tabs defaultValue={selectedTables[0] ? `${selectedTables[0].schemaname}.${selectedTables[0].tablename}` : ''} className="space-y-4">
            <TabsList className="grid w-full grid-cols-2 lg:grid-cols-4">
              {selectedTables.map((table) => (
                <TabsTrigger
                  key={`${table.schemaname}.${table.tablename}`}
                  value={`${table.schemaname}.${table.tablename}`}
                  className="text-xs"
                >
                  {table.tablename}
                </TabsTrigger>
              ))}
            </TabsList>

            {selectedTables.map((table) => (
              <TabsContent key={`${table.schemaname}.${table.tablename}`} value={`${table.schemaname}.${table.tablename}`} className="space-y-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">
                      {table.schemaname}.{table.tablename}
                    </CardTitle>
                    <CardDescription>
                      {table.tabletype} • {table.columns.length} columns
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {table.columns.map((column) => (
                        <div
                          key={column.name}
                          className="flex items-center justify-between p-2 border rounded"
                        >
                          <div className="flex items-center space-x-3">
                            <Columns className="h-4 w-4 text-gray-400" />
                            <div>
                              <div className="text-sm font-medium">{column.name}</div>
                              <div className="text-xs text-gray-500">
                                {column.type}
                                {column.length && `(${column.length})`}
                                {column.precision && column.scale && `(${column.precision},${column.scale})`}
                                {column.nullable === false && ' • NOT NULL'}
                                {column.default && ` • DEFAULT: ${column.default}`}
                              </div>
                            </div>
                          </div>
                          <Badge variant="outline" className="text-xs">
                            {column.type}
                          </Badge>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
            ))}
          </Tabs>
        )}
      </div>
    );
  };

  // Step 6: Configuration Summary
  const Step6 = () => {
    const selectedTables = availableTables.filter(
      table => formData.selectedTables.includes(`${table.schemaname}.${table.tablename}`)
    );

    if (!formData.dbType) {
      return (
        <div className="space-y-6">
          <div>
            <h3 className="text-lg font-medium">Configuration Summary</h3>
            <p className="text-sm text-gray-500">
              Please complete all previous steps to review your configuration.
            </p>
          </div>
          <div className="text-center py-8 text-gray-500">
            <AlertCircle className="h-8 w-8 mx-auto text-yellow-500" />
            <p className="mt-2">Database type not selected</p>
            <Button
              onClick={() => setCurrentStep(1)}
              variant="outline"
              size="sm"
              className="mt-4"
            >
              Go Back to Database Selection
            </Button>
          </div>
        </div>
      );
    }

    return (
      <div className="space-y-6">
        <div>
          <h3 className="text-lg font-medium">Configuration Summary</h3>
          <p className="text-sm text-gray-500">
            Review your database metadata configuration before saving.
          </p>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Connection Details</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="font-medium">Name:</span> {formData.name}
              </div>
              <div>
                <span className="font-medium">Database Type:</span> {getDatabaseDisplayName(formData.dbType)}
              </div>
              <div>
                <span className="font-medium">Host:</span> {formData.connection.host || 'N/A'}
              </div>
              <div>
                <span className="font-medium">Database:</span> {formData.connection.dbname}
              </div>
              <div>
                <span className="font-medium">Username:</span> {formData.connection.user || 'N/A'}
              </div>
              <div>
                <span className="font-medium">Schema:</span> {formData.connection.schema || 'N/A'}
              </div>
              {formData.useExistingConnection && (
                <div className="col-span-2">
                  <span className="font-medium">Using Existing Connection:</span> Yes
                </div>
              )}
            </div>
            {formData.connectionId && (
              <div className="p-2 bg-gray-50 dark:bg-gray-800 rounded">
                <div className="text-xs text-gray-500">Connection ID:</div>
                <div className="text-xs font-mono truncate">{formData.connectionId}</div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Selected Tables ({selectedTables.length})</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {selectedTables.map((table) => (
                <div
                  key={`${table.schemaname}.${table.tablename}`}
                  className="flex items-center justify-between p-2 border rounded"
                >
                  <div>
                    <div className="text-sm font-medium">
                      {table.schemaname}.{table.tablename}
                    </div>
                    <div className="text-xs text-gray-500">
                      {table.columns.length} columns • {table.tabletype}
                    </div>
                  </div>
                  <Badge variant="outline">
                    {table.columns.length} cols
                  </Badge>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
          <div className="flex items-center space-x-2 text-blue-700 dark:text-blue-300">
            <CheckCircle className="h-4 w-4" />
            <span className="text-sm font-medium">
              Ready to save database metadata configuration
            </span>
          </div>
          <div className="text-xs text-blue-600 dark:text-blue-400 mt-1">
            This configuration will be saved to the metadata repository and can be used for data lineage and governance.
          </div>
        </div>
      </div>
    );
  };

  const steps = [
    { number: 1, title: 'Database Selection', component: Step1 },
    { number: 2, title: 'General Properties', component: Step2 },
    { number: 3, title: 'Database Connection', component: Step3 },
    { number: 4, title: 'Table Selection', component: Step4 },
    { number: 5, title: 'Schema Configuration', component: Step5 },
    { number: 6, title: 'Finish', component: Step6 }
  ];

  const canProceed = useCallback(() => {
    switch (currentStep) {
      case 1:
        return formData.dbType !== ''; // Database type must be selected
      case 2:
        return formData.name.trim() !== '' && formData.dbType !== '';
      case 3:
        return formData.connectionTested && isConnectionHealthy;
      case 4:
        return formData.selectedTables.length > 0;
      case 5:
        return true;
      case 6:
        return true;
      default:
        return false;
    }
  }, [currentStep, formData, isConnectionHealthy]);

  const handleSave = useCallback(async () => {
    try {
      // Only disconnect if we created a new connection
      if (formData.connectionId && !formData.useExistingConnection) {
        await disconnectDatabase();
      }
      
      // Prepare data for saving
      const saveData = {
        name: formData.name,
        purpose: formData.purpose,
        description: formData.description,
        dbType: formData.dbType,
        connection: formData.connection,
        selectedTables: formData.selectedTables,
        tableSelections: formData.tableSelections,
        schemaInference: formData.schemaInference,
        connectionTested: formData.connectionTested,
        previewData: formData.previewData,
        connectionId: formData.connectionId,
        tables: availableTables, // <-- ADDED: full table definitions
      };
      
      onSave(saveData);
      onClose();
      toast.success('Database metadata configuration saved successfully!');
    } catch (error: any) {
      toast.error(`Failed to save configuration: ${error.message}`);
    }
  }, [formData, availableTables, disconnectDatabase, onSave, onClose]);

  const handleClose = useCallback(async () => {
    try {
      // Only disconnect if we created a new connection
      if (formData.connectionId && !formData.useExistingConnection) {
        await disconnectDatabase();
      }
      onClose();
    } catch (error) {
      console.error('Error during close:', error);
      onClose();
    }
  }, [formData.connectionId, formData.useExistingConnection, disconnectDatabase, onClose]);

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-50">
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.9 }}
          className="bg-white dark:bg-gray-900 rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] flex flex-col"
        >
          {/* Header */}
          <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-800">
            <div className="flex items-center space-x-3">
              <Database className="h-6 w-6 text-blue-500" />
              <div>
                <h2 className="text-lg font-semibold">Database Metadata Wizard</h2>
                <p className="text-sm text-gray-500">
                  Step {currentStep} of {steps.length}: {steps[currentStep - 1].title}
                </p>
              </div>
            </div>
            <Button variant="ghost" size="sm" onClick={handleClose}>
              <X className="h-4 w-4" />
            </Button>
          </div>

          {/* Progress Steps */}
          <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-800">
            <div className="flex items-center justify-between">
              {steps.map((step, index) => (
                <React.Fragment key={step.number}>
                  <div className="flex items-center space-x-2">
                    <div className={`flex items-center justify-center w-8 h-8 rounded-full ${
                      currentStep >= step.number
                        ? 'bg-blue-500 text-white'
                        : 'bg-gray-200 dark:bg-gray-700 text-gray-500'
                    }`}>
                      {currentStep > step.number ? (
                        <CheckCircle className="h-4 w-4" />
                      ) : (
                        <span className="text-sm font-medium">{step.number}</span>
                      )}
                    </div>
                    <span className={`text-sm font-medium ${
                      currentStep >= step.number
                        ? 'text-blue-600 dark:text-blue-400'
                        : 'text-gray-500'
                    }`}>
                      {step.title}
                    </span>
                  </div>
                  {index < steps.length - 1 && (
                    <div className={`flex-1 h-0.5 mx-4 ${
                      currentStep > step.number ? 'bg-blue-500' : 'bg-gray-200 dark:bg-gray-700'
                    }`} />
                  )}
                </React.Fragment>
              ))}
            </div>
          </div>

          {/* Content */}
          <ScrollArea className="flex-1 p-6">
            {steps[currentStep - 1].component()}
          </ScrollArea>

          {/* Footer */}
          <div className="flex items-center justify-between p-6 border-t border-gray-200 dark:border-gray-800">
            <Button
              variant="outline"
              onClick={() => setCurrentStep(prev => Math.max(1, prev - 1))}
              disabled={currentStep === 1}
              className="flex items-center space-x-2"
            >
              <ChevronLeft className="h-4 w-4" />
              <span>Back</span>
            </Button>

            <div className="flex items-center space-x-3">
              {currentStep === steps.length ? (
                <>
                  <Button
                    variant="outline"
                    onClick={handleClose}
                  >
                    Cancel
                  </Button>
                  <Button
                    onClick={handleSave}
                    disabled={!formData.dbType}
                    className="flex items-center space-x-2 bg-green-600 hover:bg-green-700"
                  >
                    <CheckCircle className="h-4 w-4" />
                    <span>Save to Repository</span>
                  </Button>
                </>
              ) : (
                <Button
                  onClick={() => setCurrentStep(prev => Math.min(steps.length, prev + 1))}
                  disabled={!canProceed()}
                  className="flex items-center space-x-2"
                >
                  <span>Next</span>
                  <ChevronRight className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
};

export default DatabaseMetadataWizard;