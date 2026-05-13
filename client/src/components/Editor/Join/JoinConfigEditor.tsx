// JoinConfigEditor.tsx
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  CardContent,
  Grid,
  Typography,
  Box,
  Alert,
  Snackbar,
  IconButton,
  Tooltip} from '@mui/material';
import UndoIcon from '@mui/icons-material/Undo';
import RedoIcon from '@mui/icons-material/Redo';
import DownloadIcon from '@mui/icons-material/Download';
import UploadIcon from '@mui/icons-material/Upload';
import SchemaIcon from '@mui/icons-material/Schema';
import JoinTypeSelector from './JoinTypeSelector';
import KeyMatchingSection from './KeyMatchingSection';
import ExpressionEditor from './ExpressionEditor';
import PrefixConfig from './PrefixConfig';
import AdvancedOptions from './AdvancedOptions';
import JoinVisualization from './JoinVisualization';
import { validateConfiguration, exportConfiguration, importConfiguration } from './joinUtils';

export interface JoinConfiguration {
  joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS' | 'SEMI' | 'ANTI';
  leftKeys: string[];
  rightKeys: string[];
  filterExpression?: string;
  prefixAliases?: boolean;
  prefixLeft?: string;
  prefixRight?: string;
  nullEquality?: boolean;
}

export interface FieldSchema {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date' | 'timestamp' | 'array' | 'object';
  nullable?: boolean;
}

export interface InputSchemas {
  left: FieldSchema[];
  right: FieldSchema[];
}

interface JoinConfigEditorProps {
  configuration: JoinConfiguration;
  inputSchemas: InputSchemas;
  onChange: (config: JoinConfiguration) => void;
  mode?: 'controlled' | 'uncontrolled';
  initialConfig?: JoinConfiguration;
}

interface HistoryState {
  past: JoinConfiguration[];
  present: JoinConfiguration;
  future: JoinConfiguration[];
}

// Move getDefaultConfig outside the component or declare it before use
const getDefaultConfig = (): JoinConfiguration => ({
  joinType: 'INNER',
  leftKeys: [],
  rightKeys: [],
  filterExpression: '',
  prefixAliases: false,
  prefixLeft: 'left_',
  prefixRight: 'right_',
  nullEquality: false
});

const JoinConfigEditor: React.FC<JoinConfigEditorProps> = ({
  configuration,
  inputSchemas,
  onChange,
  mode = 'controlled',
  initialConfig
}) => {
  const [internalConfig, setInternalConfig] = useState<JoinConfiguration>(
    mode === 'uncontrolled' ? initialConfig || getDefaultConfig() : configuration
  );
  
  const [history, setHistory] = useState<HistoryState>({
    past: [],
    present: mode === 'uncontrolled' ? initialConfig || getDefaultConfig() : configuration,
    future: []
  });
  
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [snackbar, setSnackbar] = useState<{ open: boolean; message: string; severity: 'success' | 'error' }>({
    open: false,
    message: '',
    severity: 'success'
  });

  const maxHistory = 50;

  useEffect(() => {
    if (mode === 'controlled') {
      setInternalConfig(configuration);
    }
  }, [configuration, mode]);

  const updateConfig = useCallback((updates: Partial<JoinConfiguration>) => {
    setInternalConfig(prev => {
      const newConfig = { ...prev, ...updates };
      
      // Validate
      const validation = validateConfiguration(newConfig, inputSchemas);
      setErrors(validation.errors);
      
      // Add to history
      setHistory(hist => {
        const newPast = [...hist.past.slice(-maxHistory + 1), hist.present];
        return {
          past: newPast,
          present: newConfig,
          future: []
        };
      });
      
      // Notify parent
      if (Object.keys(validation.errors).length === 0) {
        onChange(newConfig);
      }
      
      return newConfig;
    });
  }, [onChange, inputSchemas]);

  const handleUndo = () => {
    setHistory(hist => {
      if (hist.past.length === 0) return hist;
      
      const previous = hist.past[hist.past.length - 1];
      const newPast = hist.past.slice(0, -1);
      
      setInternalConfig(previous);
      onChange(previous);
      
      return {
        past: newPast,
        present: previous,
        future: [hist.present, ...hist.future]
      };
    });
  };

  const handleRedo = () => {
    setHistory(hist => {
      if (hist.future.length === 0) return hist;
      
      const next = hist.future[0];
      const newFuture = hist.future.slice(1);
      
      setInternalConfig(next);
      onChange(next);
      
      return {
        past: [...hist.past, hist.present],
        present: next,
        future: newFuture
      };
    });
  };

  const handleExport = () => {
    const json = exportConfiguration(internalConfig);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'join-configuration.json';
    a.click();
    URL.revokeObjectURL(url);
    
    setSnackbar({
      open: true,
      message: 'Configuration exported successfully',
      severity: 'success'
    });
  };

  const handleImport = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const content = e.target?.result as string;
        const importedConfig = importConfiguration(content, getDefaultConfig());
        updateConfig(importedConfig);
        
        setSnackbar({
          open: true,
          message: 'Configuration imported successfully',
          severity: 'success'
        });
      } catch (error) {
        setSnackbar({
          open: true,
          message: `Import failed: ${error instanceof Error ? error.message : 'Invalid file'}`,
          severity: 'error'
        });
      }
    };
    reader.readAsText(file);
  };

  return (
    <Box sx={{ p: 2 }}>
      {/* Header with Actions */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <SchemaIcon color="primary" />
          <Typography variant="h5">Join Configuration</Typography>
        </Box>
        
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Tooltip title="Undo">
            <span>
              <IconButton 
                onClick={handleUndo} 
                disabled={history.past.length === 0}
              >
                <UndoIcon />
              </IconButton>
            </span>
          </Tooltip>
          
          <Tooltip title="Redo">
            <span>
              <IconButton 
                onClick={handleRedo} 
                disabled={history.future.length === 0}
              >
                <RedoIcon />
              </IconButton>
            </span>
          </Tooltip>
          
          <Tooltip title="Export Configuration">
            <IconButton onClick={handleExport}>
              <DownloadIcon />
            </IconButton>
          </Tooltip>
          
          <Tooltip title="Import Configuration">
            <IconButton component="label">
              <UploadIcon />
              <input
                type="file"
                accept=".json"
                hidden
                onChange={handleImport}
              />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {Object.keys(errors).length > 0 && (
        <Alert severity="error" sx={{ mb: 2 }}>
          <ul style={{ margin: 0, paddingLeft: '20px' }}>
            {Object.values(errors).map((error, idx) => (
              <li key={idx}>{error}</li>
            ))}
          </ul>
        </Alert>
      )}

      <Grid container spacing={3}>
        {/* Left Column */}
        <Grid item xs={12} md={8}>
          <Card sx={{ mb: 3 }}>
            <CardContent>
              <JoinTypeSelector
                value={internalConfig.joinType}
                onChange={(joinType) => updateConfig({ joinType })}
              />
            </CardContent>
          </Card>

          <Card sx={{ mb: 3 }}>
            <CardContent>
              <KeyMatchingSection
                leftKeys={internalConfig.leftKeys}
                rightKeys={internalConfig.rightKeys}
                leftFields={inputSchemas.left}
                rightFields={inputSchemas.right}
                onChange={(leftKeys, rightKeys) => updateConfig({ leftKeys, rightKeys })}
              />
            </CardContent>
          </Card>

          <Card sx={{ mb: 3 }}>
            <CardContent>
              <ExpressionEditor
                value={internalConfig.filterExpression || ''}
                onChange={(filterExpression) => updateConfig({ filterExpression })}
                leftFields={inputSchemas.left}
                rightFields={inputSchemas.right}
              />
            </CardContent>
          </Card>
        </Grid>

        {/* Right Column */}
        <Grid item xs={12} md={4}>
          <Card sx={{ mb: 3, height: '100%' }}>
            <CardContent>
              <JoinVisualization
                joinType={internalConfig.joinType}
                leftCount={inputSchemas.left.length}
                rightCount={inputSchemas.right.length}
              />
            </CardContent>
          </Card>

          <Card sx={{ mb: 3 }}>
            <CardContent>
              <PrefixConfig
                prefixAliases={internalConfig.prefixAliases || false}
                prefixLeft={internalConfig.prefixLeft || 'left_'}
                prefixRight={internalConfig.prefixRight || 'right_'}
                onChange={(updates) => updateConfig(updates)}
              />
            </CardContent>
          </Card>

          <Card>
            <CardContent>
              <AdvancedOptions
                nullEquality={internalConfig.nullEquality || false}
                onChange={(updates) => updateConfig(updates)}
              />
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      <Snackbar
        open={snackbar.open}
        autoHideDuration={3000}
        onClose={() => setSnackbar(prev => ({ ...prev, open: false }))}
        message={snackbar.message}
      />
    </Box>
  );
};

export default JoinConfigEditor;