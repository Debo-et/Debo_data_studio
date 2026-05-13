// src/components/wizards/MatchGroupWizard.tsx - UPDATED with onSave prop
import React, { useState } from 'react';
import {
  Dialog,
  DialogContent,
  Box,
  Button,
  Stepper,
  Step,
  StepLabel,
  Typography,
  IconButton,
  Paper,
  AppBar,
  Toolbar,
  Grid,
  TextField,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Card,
  CardContent,
  Chip,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  AlertTitle,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Switch,
  Tabs,
  Tab,
} from '@mui/material';
import {
  Close as CloseIcon,
  ArrowBack,
  ArrowForward,
  Search as SearchIcon,
  Refresh as RefreshIcon,
  Schema as SchemaIcon,
  DragIndicator as DragIcon,
  Delete as DeleteIcon,
  Add as AddIcon,
  Settings as SettingsIcon,
  CheckCircle as CheckCircleIcon,
  Warning as WarningIcon,
  Info as InfoIcon,
} from '@mui/icons-material';

// ==================== TYPES ====================
export type ColumnType = 'string' | 'number' | 'date' | 'boolean';
export type MatchType = 'exact' | 'fuzzy';
export type SurvivorshipFunction = 'MostFrequent' | 'Longest' | 'Min' | 'Max' | 'Custom';

export interface SchemaColumn {
  id: string;
  name: string;
  type: ColumnType;
  isKey: boolean;
  isNullable: boolean;
  description?: string;
}

export interface GroupingKey {
  id: string;
  columnId: string;
  columnName: string;
  matchType: MatchType;
  caseSensitive: boolean;
  preprocessFunction: string;
  weight?: number;
  threshold?: number;
}

export interface SurvivorshipRule {
  id: string;
  columnId: string;
  columnName: string;
  function: SurvivorshipFunction;
  customExpression: string;
  priority: number;
}

export interface WizardConfig {
  currentStep: number;
  inputFlow: string;
  inputFlowId?: string;
  schemaColumns: SchemaColumn[];
  groupingKeys: GroupingKey[];
  survivorshipRules: SurvivorshipRule[];
  outputMapping: Record<string, string>;
  outputTableName: string;
  previewData?: any[];
}

// UPDATED: Added onSave prop to the interface
export interface MatchGroupWizardProps {
  open: boolean;
  onClose: () => void;
  onSave?: (config: WizardConfig) => void; // New onSave prop
  initialConfig?: Partial<WizardConfig>;
}

export interface StepProps {
  config: WizardConfig;
  setConfig: (config: WizardConfig | ((prev: WizardConfig) => WizardConfig)) => void;
  onNext?: () => void;
  onBack?: () => void;
  onFinish?: () => void;
}

// ==================== STEP 1: INPUT SELECTION ====================
const Step1InputSelection: React.FC<StepProps> = ({ config, setConfig, onNext }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedFlow, setSelectedFlow] = useState(config.inputFlow);

  const inputFlows = [
    { id: 'flow_1', name: 'Customer Master Data', description: 'Primary customer information', updated: '2024-01-15' },
    { id: 'flow_2', name: 'Sales Transactions', description: 'Daily sales records', updated: '2024-01-14' },
    { id: 'flow_3', name: 'Product Catalog', description: 'Product master data', updated: '2024-01-13' },
  ];

  const filteredColumns = config.schemaColumns.filter(column =>
    column.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    column.type.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const handleInputFlowSelect = (flowId: string) => {
    setSelectedFlow(flowId);
    setConfig(prev => ({
      ...prev,
      inputFlow: flowId,
    }));
  };

  const handleRefreshSchema = () => {
    console.log('Refreshing schema...');
  };

  const handleNextStep = () => {
    if (!selectedFlow) {
      alert('Please select an input flow');
      return;
    }
    if (onNext) onNext();
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h5" gutterBottom fontWeight="medium">
        Input Selection
      </Typography>
      <Typography color="text.secondary" paragraph>
        Select the input data source and review its schema
      </Typography>

      <Grid container spacing={3} sx={{ flex: 1, minHeight: 0 }}>
        {/* Left Panel - Input Flow Selection */}
        <Grid item xs={12} md={4}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" gutterBottom>
                Available Input Flows
              </Typography>
              
              <Box sx={{ flex: 1, overflow: 'auto' }}>
                {inputFlows.map(flow => (
                  <Paper
                    key={flow.id}
                    elevation={selectedFlow === flow.id ? 4 : 0}
                    onClick={() => handleInputFlowSelect(flow.id)}
                    sx={{
                      p: 2,
                      mb: 2,
                      cursor: 'pointer',
                      border: 2,
                      borderColor: selectedFlow === flow.id ? 'primary.main' : 'divider',
                      bgcolor: selectedFlow === flow.id ? 'primary.light' : 'background.paper',
                      '&:hover': {
                        borderColor: 'primary.light',
                      },
                    }}
                  >
                    <Typography variant="subtitle1" fontWeight="medium">
                      {flow.name}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {flow.description}
                    </Typography>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 1 }}>
                      <Chip size="small" label={`${config.schemaColumns.length} columns`} />
                      <Typography variant="caption" color="text.secondary">
                        Updated: {flow.updated}
                      </Typography>
                    </Box>
                  </Paper>
                ))}
              </Box>

              <Button
                fullWidth
                variant="outlined"
                startIcon={<RefreshIcon />}
                onClick={handleRefreshSchema}
                sx={{ mt: 2 }}
              >
                Refresh Schema
              </Button>
            </CardContent>
          </Card>
        </Grid>

        {/* Right Panel - Schema Preview */}
        <Grid item xs={12} md={8}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">
                  Schema Preview
                </Typography>
                <TextField
                  size="small"
                  placeholder="Search columns..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  InputProps={{
                    startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
                  }}
                  sx={{ width: 250 }}
                />
              </Box>

              {!selectedFlow ? (
                <Alert severity="info" sx={{ mt: 2 }}>
                  <AlertTitle>No Input Flow Selected</AlertTitle>
                  Please select an input flow from the left panel to view its schema
                </Alert>
              ) : (
                <TableContainer sx={{ flex: 1, overflow: 'auto' }}>
                  <Table stickyHeader size="small">
                    <TableHead>
                      <TableRow>
                        <TableCell sx={{ width: 50 }}>#</TableCell>
                        <TableCell>Column Name</TableCell>
                        <TableCell align="center">Data Type</TableCell>
                        <TableCell align="center">Nullable</TableCell>
                        <TableCell align="center">Key Field</TableCell>
                        <TableCell>Description</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {filteredColumns.map((column, index) => (
                        <TableRow 
                          key={column.id}
                          hover
                          sx={{
                            bgcolor: column.isKey ? 'action.selected' : 'inherit',
                          }}
                        >
                          <TableCell>{index + 1}</TableCell>
                          <TableCell>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <SchemaIcon fontSize="small" color="action" />
                              <Typography variant="body2" fontWeight="medium">
                                {column.name}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell align="center">
                            <Chip
                              label={column.type}
                              size="small"
                              color={
                                column.type === 'string' ? 'primary' :
                                column.type === 'number' ? 'secondary' :
                                column.type === 'date' ? 'success' : 'default'
                              }
                              variant="outlined"
                            />
                          </TableCell>
                          <TableCell align="center">
                            {column.isNullable ? (
                              <Chip label="Yes" size="small" color="default" variant="outlined" />
                            ) : (
                              <Chip label="No" size="small" color="error" variant="outlined" />
                            )}
                          </TableCell>
                          <TableCell align="center">
                            {column.isKey ? (
                              <Chip label="Primary Key" size="small" color="primary" />
                            ) : '-'}
                          </TableCell>
                          <TableCell>
                            <Typography variant="body2" color="text.secondary">
                              {column.description || 'No description available'}
                            </Typography>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              )}

              <Box sx={{ display: 'flex', justifyContent: 'flex-end', mt: 3, pt: 2, borderTop: 1, borderColor: 'divider' }}>
                <Button
                  variant="contained"
                  onClick={handleNextStep}
                  disabled={!selectedFlow}
                  startIcon={<CheckCircleIcon />}
                >
                  Continue to Grouping Keys
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

// ==================== STEP 2: GROUPING KEYS ====================
const Step2GroupingKeys: React.FC<StepProps> = ({ config, setConfig, onNext, onBack }) => {
  const [draggedColumn, setDraggedColumn] = useState<string | null>(null);

  const availableColumns = config.schemaColumns.filter(
    column => !config.groupingKeys.some(key => key.columnId === column.id)
  );

  const handleDragStart = (e: React.DragEvent, columnId: string) => {
    setDraggedColumn(columnId);
    e.dataTransfer.setData('text/plain', columnId);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    if (draggedColumn) {
      const column = config.schemaColumns.find(c => c.id === draggedColumn);
      if (column) {
        const newKey: GroupingKey = {
          id: `key_${Date.now()}`,
          columnId: column.id,
          columnName: column.name,
          matchType: 'exact',
          caseSensitive: false,
          preprocessFunction: '',
          weight: 1,
          threshold: 0.8,
        };
        
        setConfig(prev => ({
          ...prev,
          groupingKeys: [...prev.groupingKeys, newKey]
        }));
      }
    }
    setDraggedColumn(null);
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
  };

  const handleRemoveKey = (keyId: string) => {
    setConfig(prev => ({
      ...prev,
      groupingKeys: prev.groupingKeys.filter(key => key.id !== keyId)
    }));
  };

  const handleUpdateKey = (keyId: string, updates: Partial<GroupingKey>) => {
    setConfig(prev => ({
      ...prev,
      groupingKeys: prev.groupingKeys.map(key =>
        key.id === keyId ? { ...key, ...updates } : key
      )
    }));
  };

  const addSampleKey = () => {
    if (availableColumns.length > 0) {
      const column = availableColumns[0];
      const newKey: GroupingKey = {
        id: `key_${Date.now()}`,
        columnId: column.id,
        columnName: column.name,
        matchType: 'exact',
        caseSensitive: false,
        preprocessFunction: 'TRIM(UPPER(?))',
        weight: 1,
        threshold: 0.9,
      };
      setConfig(prev => ({
        ...prev,
        groupingKeys: [...prev.groupingKeys, newKey]
      }));
    }
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h5" gutterBottom fontWeight="medium">
        Grouping Keys Configuration
      </Typography>
      <Typography color="text.secondary" paragraph>
        Define columns that identify matching records. Drag columns from the schema to add grouping keys.
      </Typography>

      <Grid container spacing={3} sx={{ flex: 1, minHeight: 0 }}>
        {/* Left Panel - Available Columns */}
        <Grid item xs={12} md={4}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" gutterBottom>
                Available Columns
                <Chip label={availableColumns.length} size="small" sx={{ ml: 1 }} />
              </Typography>
              
              {availableColumns.length === 0 ? (
                <Alert severity="success" sx={{ mt: 2 }}>
                  <AlertTitle>All Columns Used</AlertTitle>
                  All columns have been configured as grouping keys
                </Alert>
              ) : (
                <Paper
                  variant="outlined"
                  sx={{
                    flex: 1,
                    overflow: 'auto',
                    p: 1,
                    bgcolor: 'background.default',
                  }}
                >
                  <List dense>
                    {availableColumns.map(column => (
                      <ListItem
                        key={column.id}
                        draggable
                        onDragStart={(e) => handleDragStart(e, column.id)}
                        sx={{
                          mb: 1,
                          cursor: 'grab',
                          border: 1,
                          borderColor: 'divider',
                          borderRadius: 1,
                          bgcolor: 'background.paper',
                          '&:active': {
                            cursor: 'grabbing',
                          },
                          '&:hover': {
                            boxShadow: 1,
                          },
                        }}
                      >
                        <ListItemIcon>
                          <DragIcon color="action" />
                        </ListItemIcon>
                        <ListItemText
                          primary={
                            <Typography variant="body2" fontWeight="medium">
                              {column.name}
                            </Typography>
                          }
                          secondary={
                            <Box sx={{ display: 'flex', gap: 1, mt: 0.5 }}>
                              <Chip label={column.type} size="small" />
                              {column.isKey && (
                                <Chip label="Key" size="small" color="primary" variant="outlined" />
                              )}
                            </Box>
                          }
                        />
                      </ListItem>
                    ))}
                  </List>
                </Paper>
              )}

              <Typography variant="caption" color="text.secondary" sx={{ mt: 2, display: 'block' }}>
                <DragIcon sx={{ verticalAlign: 'middle', mr: 0.5 }} fontSize="small" />
                Drag columns to the right panel to add them as grouping keys
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        {/* Right Panel - Configured Grouping Keys */}
        <Grid item xs={12} md={8}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">
                  Configured Grouping Keys
                  <Chip label={config.groupingKeys.length} size="small" color="primary" sx={{ ml: 1 }} />
                </Typography>
                
                <Button
                  variant="outlined"
                  startIcon={<AddIcon />}
                  onClick={addSampleKey}
                  disabled={availableColumns.length === 0}
                  size="small"
                >
                  Add Sample Key
                </Button>
              </Box>

              <Paper
                variant="outlined"
                onDrop={handleDrop}
                onDragOver={handleDragOver}
                sx={{
                  flex: 1,
                  overflow: 'auto',
                  p: 2,
                  bgcolor: 'background.default',
                  minHeight: 300,
                  borderStyle: config.groupingKeys.length === 0 ? 'dashed' : 'solid',
                  borderWidth: 2,
                  borderColor: config.groupingKeys.length === 0 ? 'divider' : 'inherit',
                }}
              >
                {config.groupingKeys.length === 0 ? (
                  <Box
                    sx={{
                      height: '100%',
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      justifyContent: 'center',
                      color: 'text.secondary',
                      textAlign: 'center',
                      p: 4,
                    }}
                  >
                    <DragIcon sx={{ fontSize: 64, mb: 2, opacity: 0.3 }} />
                    <Typography variant="h6" gutterBottom>
                      No Grouping Keys Defined
                    </Typography>
                    <Typography variant="body2" color="text.secondary" paragraph>
                      Drag columns from the left panel or click "Add Sample Key" to create grouping keys
                    </Typography>
                    <Typography variant="caption">
                      Grouping keys determine which records are considered matches
                    </Typography>
                  </Box>
                ) : (
                  <Grid container spacing={2}>
                    {config.groupingKeys.map((key, index) => (
                      <Grid item xs={12} key={key.id}>
                        <Paper 
                          variant="outlined" 
                          sx={{ 
                            p: 2,
                            borderColor: 'primary.light',
                            '&:hover': {
                              borderColor: 'primary.main',
                            }
                          }}
                        >
                          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2 }}>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="subtitle1" fontWeight="medium">
                                {key.columnName}
                              </Typography>
                              <Chip
                                label={`Key ${index + 1}`}
                                size="small"
                                color="primary"
                              />
                              <Chip
                                label={key.matchType === 'exact' ? 'Exact Match' : 'Fuzzy Match'}
                                size="small"
                                variant="outlined"
                                color={key.matchType === 'exact' ? 'success' : 'warning'}
                              />
                            </Box>
                            <IconButton
                              size="small"
                              onClick={() => handleRemoveKey(key.id)}
                              color="error"
                              sx={{ mt: -0.5 }}
                            >
                              <DeleteIcon />
                            </IconButton>
                          </Box>

                          <Grid container spacing={2}>
                            <Grid item xs={12} md={4}>
                              <FormControl fullWidth size="small">
                                <InputLabel>Match Type</InputLabel>
                                <Select
                                  value={key.matchType}
                                  label="Match Type"
                                  onChange={(e) => handleUpdateKey(key.id, { 
                                    matchType: e.target.value as 'exact' | 'fuzzy' 
                                  })}
                                >
                                  <MenuItem value="exact">
                                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                      <CheckCircleIcon fontSize="small" sx={{ mr: 1 }} />
                                      Exact Match
                                    </Box>
                                  </MenuItem>
                                  <MenuItem value="fuzzy">
                                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                      <SettingsIcon fontSize="small" sx={{ mr: 1 }} />
                                      Fuzzy Match
                                    </Box>
                                  </MenuItem>
                                </Select>
                              </FormControl>
                            </Grid>

                            <Grid item xs={12} md={4}>
                              <TextField
                                fullWidth
                                size="small"
                                label="Preprocess Function"
                                value={key.preprocessFunction}
                                onChange={(e) => handleUpdateKey(key.id, { 
                                  preprocessFunction: e.target.value 
                                })}
                                placeholder="e.g., TRIM(UPPER(?))"
                                helperText="SQL expression to transform values"
                              />
                            </Grid>

                            <Grid item xs={12} md={4}>
                              <TextField
                                fullWidth
                                size="small"
                                label="Weight"
                                type="number"
                                value={key.weight || 1}
                                onChange={(e) => handleUpdateKey(key.id, { 
                                  weight: parseFloat(e.target.value) 
                                })}
                                inputProps={{ min: 0.1, max: 10, step: 0.1 }}
                                helperText="Relative importance"
                              />
                            </Grid>

                            <Grid item xs={12}>
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 3 }}>
                                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                  <Switch
                                    size="small"
                                    checked={key.caseSensitive}
                                    onChange={(e) => handleUpdateKey(key.id, { 
                                      caseSensitive: e.target.checked 
                                    })}
                                  />
                                  <Typography variant="body2">Case Sensitive</Typography>
                                </Box>

                                {key.matchType === 'fuzzy' && (
                                  <TextField
                                    size="small"
                                    label="Similarity Threshold"
                                    type="number"
                                    value={key.threshold || 0.8}
                                    onChange={(e) => handleUpdateKey(key.id, { 
                                      threshold: parseFloat(e.target.value) 
                                    })}
                                    inputProps={{ min: 0.1, max: 1, step: 0.05 }}
                                    sx={{ width: 150 }}
                                    helperText="0.0 to 1.0"
                                  />
                                )}
                              </Box>
                            </Grid>
                          </Grid>
                        </Paper>
                      </Grid>
                    ))}
                  </Grid>
                )}
              </Paper>

              <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 3, pt: 2, borderTop: 1, borderColor: 'divider' }}>
                <Button onClick={onBack} startIcon={<ArrowBack />}>
                  Back
                </Button>
                
                <Box sx={{ display: 'flex', gap: 2 }}>
                  <Alert severity="info" sx={{ py: 0.5, px: 1 }}>
                    <Typography variant="caption">
                      At least one grouping key is required
                    </Typography>
                  </Alert>
                  
                  <Button
                    variant="contained"
                    onClick={onNext}
                    disabled={config.groupingKeys.length === 0}
                    endIcon={<ArrowForward />}
                  >
                    Continue to Survivorship Rules
                  </Button>
                </Box>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

// ==================== STEP 3: SURVIVORSHIP RULES ====================
const Step3SurvivorshipRules: React.FC<StepProps> = ({ config, setConfig, onNext, onBack }) => {
  const [selectedColumn, setSelectedColumn] = useState<string | null>(null);

  const handleRuleUpdate = (columnId: string, rule: Partial<SurvivorshipRule>) => {
    setConfig(prev => {
      const existingRule = prev.survivorshipRules.find(r => r.columnId === columnId);
      if (existingRule) {
        return {
          ...prev,
          survivorshipRules: prev.survivorshipRules.map(r =>
            r.columnId === columnId ? { ...r, ...rule } : r
          )
        };
      } else {
        const column = prev.schemaColumns.find(c => c.id === columnId);
        const newRule: SurvivorshipRule = {
          id: `rule_${Date.now()}`,
          columnId,
          columnName: column?.name || '',
          function: 'MostFrequent',
          customExpression: '',
          priority: 1,
          ...rule
        };
        return {
          ...prev,
          survivorshipRules: [...prev.survivorshipRules, newRule]
        };
      }
    });
  };

  const getRuleForColumn = (columnId: string) => {
    return config.survivorshipRules.find(r => r.columnId === columnId);
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h5" gutterBottom fontWeight="medium">
        Survivorship Rules
      </Typography>
      <Typography color="text.secondary" paragraph>
        Define how conflicting values are resolved for each output column
      </Typography>

      <Grid container spacing={3} sx={{ flex: 1, minHeight: 0 }}>
        {/* Left Panel - Rule Configuration */}
        <Grid item xs={12} md={4}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" gutterBottom>
                Rule Configuration
              </Typography>
              
              {selectedColumn ? (
                <Box sx={{ flex: 1 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <Button onClick={() => setSelectedColumn(null)} size="small">
                      ← Back to list
                    </Button>
                  </Box>
                  
                  <Typography variant="subtitle1" gutterBottom>
                    Configure rule for: {config.schemaColumns.find(c => c.id === selectedColumn)?.name}
                  </Typography>
                  
                  <FormControl fullWidth size="small" sx={{ mb: 2 }}>
                    <InputLabel>Survivorship Function</InputLabel>
                    <Select
                      value={getRuleForColumn(selectedColumn)?.function || 'MostFrequent'}
                      label="Survivorship Function"
                      onChange={(e) => handleRuleUpdate(selectedColumn, { 
                        function: e.target.value as SurvivorshipFunction 
                      })}
                    >
                      <MenuItem value="MostFrequent">Most Frequent Value</MenuItem>
                      <MenuItem value="Longest">Longest Value</MenuItem>
                      <MenuItem value="Min">Minimum Value</MenuItem>
                      <MenuItem value="Max">Maximum Value</MenuItem>
                      <MenuItem value="Custom">Custom Expression</MenuItem>
                    </Select>
                  </FormControl>
                  
                  {getRuleForColumn(selectedColumn)?.function === 'Custom' && (
                    <TextField
                      fullWidth
                      multiline
                      rows={4}
                      label="Custom Expression"
                      value={getRuleForColumn(selectedColumn)?.customExpression || ''}
                      onChange={(e) => handleRuleUpdate(selectedColumn, { 
                        customExpression: e.target.value 
                      })}
                      placeholder="e.g., COALESCE(col1, col2, 'default')"
                      helperText="SQL expression to determine survivor value"
                    />
                  )}
                  
                  <TextField
                    fullWidth
                    type="number"
                    label="Priority"
                    value={getRuleForColumn(selectedColumn)?.priority || 1}
                    onChange={(e) => handleRuleUpdate(selectedColumn, { 
                      priority: parseInt(e.target.value) 
                    })}
                    sx={{ mt: 2 }}
                    helperText="Lower number = higher priority"
                  />
                </Box>
              ) : (
                <Alert severity="info" sx={{ mt: 2 }}>
                  <AlertTitle>Select a Column</AlertTitle>
                  Click on a column from the right panel to configure its survivorship rule
                </Alert>
              )}
            </CardContent>
          </Card>
        </Grid>

        {/* Right Panel - Column List */}
        <Grid item xs={12} md={8}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                <Typography variant="h6">
                  Available Columns
                  <Chip label={config.schemaColumns.length} size="small" sx={{ ml: 1 }} />
                </Typography>
              </Box>

              <TableContainer sx={{ flex: 1, overflow: 'auto' }}>
                <Table stickyHeader size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Column Name</TableCell>
                      <TableCell align="center">Type</TableCell>
                      <TableCell align="center">Survivorship Rule</TableCell>
                      <TableCell align="center">Status</TableCell>
                      <TableCell>Action</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {config.schemaColumns.map((column) => {
                      const rule = getRuleForColumn(column.id);
                      return (
                        <TableRow 
                          key={column.id}
                          hover
                          onClick={() => setSelectedColumn(column.id)}
                          selected={selectedColumn === column.id}
                          sx={{ cursor: 'pointer' }}
                        >
                          <TableCell>
                            <Typography variant="body2" fontWeight="medium">
                              {column.name}
                            </Typography>
                          </TableCell>
                          <TableCell align="center">
                            <Chip label={column.type} size="small" variant="outlined" />
                          </TableCell>
                          <TableCell align="center">
                            {rule ? (
                              <Chip 
                                label={rule.function} 
                                size="small" 
                                color="success" 
                                variant="outlined"
                              />
                            ) : (
                              <Chip label="Not Set" size="small" color="default" variant="outlined" />
                            )}
                          </TableCell>
                          <TableCell align="center">
                            {rule ? (
                              <CheckCircleIcon color="success" fontSize="small" />
                            ) : (
                              <WarningIcon color="warning" fontSize="small" />
                            )}
                          </TableCell>
                          <TableCell>
                            <Button size="small" variant="outlined">
                              {rule ? 'Edit Rule' : 'Add Rule'}
                            </Button>
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </TableContainer>

              <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 3, pt: 2, borderTop: 1, borderColor: 'divider' }}>
                <Button onClick={onBack} startIcon={<ArrowBack />}>
                  Back
                </Button>
                
                <Button
                  variant="contained"
                  onClick={onNext}
                  endIcon={<ArrowForward />}
                >
                  Continue to Output Mapping
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

// ==================== STEP 4: OUTPUT MAPPING ====================
const Step4OutputMapping: React.FC<StepProps> = ({ config, setConfig, onNext, onBack }) => {
  const [outputTableName, setOutputTableName] = useState(config.outputTableName);
  const [mappings, setMappings] = useState<Record<string, string>>(config.outputMapping);

  const handleMappingChange = (columnId: string, outputName: string) => {
    const newMappings = { ...mappings, [columnId]: outputName };
    setMappings(newMappings);
    setConfig(prev => ({
      ...prev,
      outputMapping: newMappings
    }));
  };

  const handleOutputTableNameChange = (name: string) => {
    setOutputTableName(name);
    setConfig(prev => ({
      ...prev,
      outputTableName: name
    }));
  };

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h5" gutterBottom fontWeight="medium">
        Output Mapping
      </Typography>
      <Typography color="text.secondary" paragraph>
        Configure output table name and column mappings
      </Typography>

      <Grid container spacing={3} sx={{ flex: 1, minHeight: 0 }}>
        {/* Left Panel - Output Configuration */}
        <Grid item xs={12} md={4}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" gutterBottom>
                Output Settings
              </Typography>
              
              <TextField
                fullWidth
                label="Output Table Name"
                value={outputTableName}
                onChange={(e) => handleOutputTableNameChange(e.target.value)}
                sx={{ mb: 3 }}
                helperText="Name for the resulting deduplicated table"
              />
              
              <Alert severity="info">
                <AlertTitle>Output Configuration</AlertTitle>
                <Typography variant="body2">
                  Map input columns to output columns. Leave blank to use original column names.
                </Typography>
              </Alert>
              
              <Box sx={{ flex: 1 }} />
              
              <Alert severity="success">
                <Typography variant="caption">
                  {Object.keys(mappings).filter(k => mappings[k]).length} of {config.schemaColumns.length} columns mapped
                </Typography>
              </Alert>
            </CardContent>
          </Card>
        </Grid>

        {/* Right Panel - Column Mapping */}
        <Grid item xs={12} md={8}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" gutterBottom>
                Column Mapping
              </Typography>

              <TableContainer sx={{ flex: 1, overflow: 'auto' }}>
                <Table stickyHeader size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Input Column</TableCell>
                      <TableCell align="center">Type</TableCell>
                      <TableCell>Output Column Name</TableCell>
                      <TableCell align="center">Mapping Status</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {config.schemaColumns.map((column) => (
                      <TableRow key={column.id}>
                        <TableCell>
                          <Typography variant="body2" fontWeight="medium">
                            {column.name}
                          </Typography>
                          <Typography variant="caption" color="text.secondary">
                            Original name
                          </Typography>
                        </TableCell>
                        <TableCell align="center">
                          <Chip label={column.type} size="small" />
                        </TableCell>
                        <TableCell>
                          <TextField
                            fullWidth
                            size="small"
                            value={mappings[column.id] || ''}
                            onChange={(e) => handleMappingChange(column.id, e.target.value)}
                            placeholder={column.name}
                            helperText="Leave blank to keep original name"
                          />
                        </TableCell>
                        <TableCell align="center">
                          {mappings[column.id] ? (
                            <Chip 
                              icon={<CheckCircleIcon fontSize="small" />}
                              label="Mapped" 
                              size="small" 
                              color="success" 
                              variant="outlined"
                            />
                          ) : (
                            <Chip 
                              icon={<InfoIcon fontSize="small" />}
                              label="Default" 
                              size="small" 
                              color="default" 
                              variant="outlined"
                            />
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>

              <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 3, pt: 2, borderTop: 1, borderColor: 'divider' }}>
                <Button onClick={onBack} startIcon={<ArrowBack />}>
                  Back
                </Button>
                
                <Button
                  variant="contained"
                  onClick={onNext}
                  endIcon={<ArrowForward />}
                >
                  Continue to Preview
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

// ==================== STEP 5: PREVIEW & VALIDATION ====================
const Step5PreviewValidation: React.FC<StepProps> = ({ config, onBack, onFinish }) => {
  const [activeTab, setActiveTab] = useState(0);

  const validationResults = {
    groupingKeys: config.groupingKeys.length > 0,
    survivorshipRules: config.survivorshipRules.length > 0,
    outputTableName: config.outputTableName.trim().length > 0,
  };

  const allValid = Object.values(validationResults).every(v => v);

  return (
    <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Typography variant="h5" gutterBottom fontWeight="medium">
        Preview & Validation
      </Typography>
      <Typography color="text.secondary" paragraph>
        Review configuration and validate before finalizing
      </Typography>

      <Grid container spacing={3} sx={{ flex: 1, minHeight: 0 }}>
        {/* Left Panel - Validation Summary */}
        <Grid item xs={12} md={4}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Typography variant="h6" gutterBottom>
                Validation Summary
              </Typography>
              
              <Box sx={{ flex: 1 }}>
                <Alert 
                  severity={allValid ? "success" : "warning"} 
                  sx={{ mb: 2 }}
                >
                  <AlertTitle>
                    {allValid ? "Configuration Complete" : "Configuration Incomplete"}
                  </AlertTitle>
                  {allValid ? 
                    "All required configurations are set" : 
                    "Some required configurations are missing"}
                </Alert>
                
                <List>
                  <ListItem>
                    <ListItemIcon>
                      {validationResults.groupingKeys ? 
                        <CheckCircleIcon color="success" /> : 
                        <WarningIcon color="warning" />}
                    </ListItemIcon>
                    <ListItemText
                      primary="Grouping Keys"
                      secondary={validationResults.groupingKeys ? 
                        `${config.groupingKeys.length} keys configured` : 
                        "At least one grouping key required"}
                    />
                  </ListItem>
                  
                  <ListItem>
                    <ListItemIcon>
                      {validationResults.survivorshipRules ? 
                        <CheckCircleIcon color="success" /> : 
                        <WarningIcon color="warning" />}
                    </ListItemIcon>
                    <ListItemText
                      primary="Survivorship Rules"
                      secondary={validationResults.survivorshipRules ? 
                        `${config.survivorshipRules.length} rules configured` : 
                        "Recommended for conflict resolution"}
                    />
                  </ListItem>
                  
                  <ListItem>
                    <ListItemIcon>
                      {validationResults.outputTableName ? 
                        <CheckCircleIcon color="success" /> : 
                        <WarningIcon color="warning" />}
                    </ListItemIcon>
                    <ListItemText
                      primary="Output Configuration"
                      secondary={validationResults.outputTableName ? 
                        `Output table: ${config.outputTableName}` : 
                        "Output table name required"}
                    />
                  </ListItem>
                </List>
              </Box>
              
              <Box sx={{ mt: 'auto' }}>
                <Button
                  fullWidth
                  variant="contained"
                  color="primary"
                  onClick={onFinish}
                  disabled={!allValid}
                  startIcon={<CheckCircleIcon />}
                  sx={{ mb: 1 }}
                >
                  Complete Configuration
                </Button>
                
                <Typography variant="caption" color="text.secondary" align="center" display="block">
                  This will create the match and group pipeline
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        {/* Right Panel - Configuration Preview */}
        <Grid item xs={12} md={8}>
          <Card variant="outlined" sx={{ height: '100%' }}>
            <CardContent sx={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
              <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
                <Tabs value={activeTab} onChange={(_, newValue) => setActiveTab(newValue)}>
                  <Tab label="Configuration Summary" />
                  <Tab label="Preview Data" />
                  <Tab label="Generated SQL" />
                </Tabs>
              </Box>
              
              {activeTab === 0 && (
                <Box sx={{ flex: 1, overflow: 'auto' }}>
                  <Typography variant="subtitle1" gutterBottom>
                    Input Flow: {config.inputFlow || 'Not selected'}
                  </Typography>
                  
                  <Typography variant="subtitle2" gutterBottom mt={2}>
                    Grouping Keys ({config.groupingKeys.length})
                  </Typography>
                  <List dense>
                    {config.groupingKeys.map((key, index) => (
                      <ListItem key={key.id}>
                        <ListItemText
                          primary={`${index + 1}. ${key.columnName}`}
                          secondary={`Match: ${key.matchType} | Weight: ${key.weight} | Case: ${key.caseSensitive ? 'Sensitive' : 'Insensitive'}`}
                        />
                      </ListItem>
                    ))}
                  </List>
                  
                  <Typography variant="subtitle2" gutterBottom mt={2}>
                    Survivorship Rules ({config.survivorshipRules.length})
                  </Typography>
                  <List dense>
                    {config.survivorshipRules.map((rule, index) => (
                      <ListItem key={rule.id}>
                        <ListItemText
                          primary={`${index + 1}. ${rule.columnName}`}
                          secondary={`Function: ${rule.function} | Priority: ${rule.priority}`}
                        />
                      </ListItem>
                    ))}
                  </List>
                </Box>
              )}
              
              {activeTab === 1 && (
                <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <Alert severity="info">
                    <AlertTitle>Preview Data</AlertTitle>
                    <Typography variant="body2">
                      Sample data preview will be shown here based on the configuration.
                    </Typography>
                  </Alert>
                </Box>
              )}
              
              {activeTab === 2 && (
                <Box sx={{ flex: 1, overflow: 'auto' }}>
                  <Paper variant="outlined" sx={{ p: 2, bgcolor: 'grey.50' }}>
                    <Typography variant="caption" component="pre" sx={{ whiteSpace: 'pre-wrap' }}>
                      {`-- Generated Match & Group Configuration
CREATE OR REPLACE TABLE ${config.outputTableName} AS
SELECT 
  -- Grouping key columns
  ${config.groupingKeys.map(k => k.columnName).join(',\n  ')}
  
  -- Survivorship columns
  ${config.schemaColumns
    .filter(c => !config.groupingKeys.some(k => k.columnId === c.id))
    .map(c => {
      const rule = config.survivorshipRules.find(r => r.columnId === c.id);
      return rule ? `  -- ${c.name}: ${rule.function}` : `  ${c.name}`;
    })
    .join(',\n  ')}
FROM input_data
GROUP BY ${config.groupingKeys.map(k => k.columnName).join(', ')};`}
                    </Typography>
                  </Paper>
                </Box>
              )}
              
              <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 3, pt: 2, borderTop: 1, borderColor: 'divider' }}>
                <Button onClick={onBack} startIcon={<ArrowBack />}>
                  Back
                </Button>
                
                <Button
                  variant="outlined"
                  onClick={() => console.log('Export configuration:', config)}
                >
                  Export Configuration
                </Button>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

// ==================== MAIN WIZARD COMPONENT ====================
const initialConfig: WizardConfig = {
  currentStep: 1,
  inputFlow: '',
  schemaColumns: [
    { id: '1', name: 'customer_id', type: 'string', isKey: true, isNullable: false, description: 'Unique customer identifier' },
    { id: '2', name: 'first_name', type: 'string', isKey: false, isNullable: true, description: 'Customer first name' },
    { id: '3', name: 'last_name', type: 'string', isKey: false, isNullable: true, description: 'Customer last name' },
    { id: '4', name: 'email', type: 'string', isKey: false, isNullable: true, description: 'Email address' },
    { id: '5', name: 'date_of_birth', type: 'date', isKey: false, isNullable: true, description: 'Date of birth' },
    { id: '6', name: 'total_purchases', type: 'number', isKey: false, isNullable: true, description: 'Total purchase amount' },
    { id: '7', name: 'phone_number', type: 'string', isKey: false, isNullable: true, description: 'Contact phone number' },
    { id: '8', name: 'address_line1', type: 'string', isKey: false, isNullable: true, description: 'Primary address' },
  ],
  groupingKeys: [],
  survivorshipRules: [],
  outputMapping: {},
  outputTableName: 'matched_customers',
};

const MatchGroupWizard: React.FC<MatchGroupWizardProps> = ({ 
  open, 
  onClose,
  onSave, // NEW: Added onSave prop
  initialConfig: userConfig 
}) => {
  const [config, setConfig] = useState<WizardConfig>({
    ...initialConfig,
    ...userConfig,
  });

  const handleNext = () => {
    if (config.currentStep < 5) {
      setConfig(prev => ({
        ...prev,
        currentStep: prev.currentStep + 1
      }));
    }
  };

  const handleBack = () => {
    if (config.currentStep > 1) {
      setConfig(prev => ({
        ...prev,
        currentStep: prev.currentStep - 1
      }));
    }
  };

  const handleStepClick = (stepIndex: number) => {
    // Only allow navigation to completed or current step
    const maxStep = Math.max(...[
      config.groupingKeys.length > 0 ? 2 : 1,
      config.survivorshipRules.length > 0 ? 3 : 2,
      4, // Output mapping always accessible
      5  // Preview always accessible
    ]);
    
    if (stepIndex + 1 <= maxStep) {
      setConfig(prev => ({
        ...prev,
        currentStep: stepIndex + 1
      }));
    }
  };

  // UPDATED: Modified handleFinish to call onSave if provided
  const handleFinish = () => {
    console.log('Final configuration:', config);
    
    if (onSave) {
      // Call onSave with the configuration
      onSave(config);
    } else {
      // Fallback to the original behavior
      alert('Configuration saved successfully!');
    }
    
    onClose();
  };

  const renderStepContent = () => {
    const stepProps = {
      config,
      setConfig,
      onNext: handleNext,
      onBack: handleBack,
      onFinish: handleFinish,
    };

    switch (config.currentStep) {
      case 1:
        return <Step1InputSelection {...stepProps} />;
      case 2:
        return <Step2GroupingKeys {...stepProps} />;
      case 3:
        return <Step3SurvivorshipRules {...stepProps} />;
      case 4:
        return <Step4OutputMapping {...stepProps} />;
      case 5:
        return <Step5PreviewValidation {...stepProps} />;
      default:
        return <Typography>Invalid step</Typography>;
    }
  };

  const steps = [
    { label: 'Input Selection', completed: config.currentStep > 1 },
    { label: 'Grouping Keys', completed: config.currentStep > 2 },
    { label: 'Survivorship Rules', completed: config.currentStep > 3 },
    { label: 'Output Mapping', completed: config.currentStep > 4 },
    { label: 'Preview & Validation', completed: false },
  ];

  return (
    <Dialog 
      open={open} 
      onClose={(_event, reason) => {
        if (reason !== 'backdropClick') {
          onClose();
        }
      }}
      maxWidth="xl"
      fullWidth
      PaperProps={{
        sx: {
          height: '90vh',
          maxHeight: '90vh',
          minHeight: '700px',
        }
      }}
    >
      <AppBar position="static" elevation={0} sx={{ bgcolor: 'primary.main', color: 'white' }}>
        <Toolbar>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Match & Group Configuration Wizard
          </Typography>
          <IconButton onClick={onClose} size="large" sx={{ color: 'white' }}>
            <CloseIcon />
          </IconButton>
        </Toolbar>
      </AppBar>

      <DialogContent sx={{ p: 0, display: 'flex', flexDirection: 'column', flex: 1 }}>
        <Box sx={{ flex: 1, display: 'flex', height: '100%' }}>
          {/* Navigation Panel */}
          <Paper 
            elevation={0} 
            sx={{ 
              width: 280, 
              borderRight: 1, 
              borderColor: 'divider',
              p: 3,
              display: 'flex',
              flexDirection: 'column'
            }}
          >
            <Typography variant="subtitle1" gutterBottom fontWeight="medium">
              Configuration Steps
            </Typography>
            
            <Stepper 
              activeStep={config.currentStep - 1} 
              orientation="vertical"
              nonLinear
              sx={{ mt: 2, flex: 1 }}
            >
              {steps.map((step, index) => (
                <Step 
                  key={step.label} 
                  completed={step.completed}
                  sx={{ 
                    cursor: config.currentStep > index ? 'pointer' : 'default',
                    '& .MuiStepLabel-root': {
                      '&:hover': config.currentStep > index ? {
                        bgcolor: 'action.hover',
                        borderRadius: 1,
                      } : {},
                    }
                  }}
                >
                  <StepLabel 
                    onClick={() => config.currentStep > index && handleStepClick(index)}
                    sx={{ 
                      '& .MuiStepLabel-label': {
                        fontWeight: config.currentStep === index + 1 ? 'bold' : 'normal',
                      }
                    }}
                  >
                    {step.label}
                  </StepLabel>
                </Step>
              ))}
            </Stepper>
            
            <Box sx={{ mt: 'auto', pt: 2, borderTop: 1, borderColor: 'divider' }}>
              <Typography variant="caption" color="text.secondary" display="block">
                Progress: {config.currentStep} of 5 steps
              </Typography>
              <Box sx={{ width: '100%', bgcolor: 'grey.200', borderRadius: 1, mt: 1 }}>
                <Box 
                  sx={{ 
                    width: `${(config.currentStep / 5) * 100}%`,
                    height: 4,
                    bgcolor: 'primary.main',
                    borderRadius: 1,
                  }} 
                />
              </Box>
            </Box>
          </Paper>

          {/* Main Panel */}
          <Box sx={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
            <Box sx={{ flex: 1, overflow: 'auto', p: 3 }}>
              {renderStepContent()}
            </Box>

            {/* Footer */}
            <Paper 
              elevation={0}
              sx={{ 
                borderTop: 1, 
                borderColor: 'divider',
                p: 2,
                bgcolor: 'grey.50'
              }}
            >
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Typography variant="body2" color="text.secondary">
                  Step {config.currentStep} of 5 • {steps[config.currentStep - 1]?.label}
                </Typography>
                
                <Box sx={{ display: 'flex', gap: 2 }}>
                  <Button
                    onClick={handleBack}
                    disabled={config.currentStep === 1}
                    startIcon={<ArrowBack />}
                    variant="outlined"
                  >
                    Back
                  </Button>
                  
                  {config.currentStep === 5 ? (
                    <Button
                      variant="contained"
                      onClick={handleFinish}
                      color="success"
                      startIcon={<CheckCircleIcon />}
                    >
                      Finish & Deploy
                    </Button>
                  ) : (
                    <Button
                      variant="contained"
                      onClick={handleNext}
                      endIcon={<ArrowForward />}
                    >
                      Next Step
                    </Button>
                  )}
                </Box>
              </Box>
            </Paper>
          </Box>
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default MatchGroupWizard;