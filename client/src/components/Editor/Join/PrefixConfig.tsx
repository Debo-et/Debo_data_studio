// PrefixConfig.tsx
import React from 'react';
import {
  Box,
  Typography,
  Switch,
  FormControlLabel,
  TextField,
  Grid,
  Alert,
  Divider
} from '@mui/material';
import {
  FormatQuote as QuoteIcon,
  Warning as WarningIcon
} from '@mui/icons-material';

interface PrefixConfigProps {
  prefixAliases: boolean;
  prefixLeft: string;
  prefixRight: string;
  onChange: (updates: {
    prefixAliases?: boolean;
    prefixLeft?: string;
    prefixRight?: string;
  }) => void;
}

const PrefixConfig: React.FC<PrefixConfigProps> = ({
  prefixAliases,
  prefixLeft,
  prefixRight,
  onChange
}) => {
  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Field Prefixes
      </Typography>
      
      <Typography variant="body2" color="text.secondary" gutterBottom>
        Add prefixes to avoid column name conflicts
      </Typography>

      <FormControlLabel
        control={
          <Switch
            checked={prefixAliases}
            onChange={(e) => onChange({ prefixAliases: e.target.checked })}
          />
        }
        label="Enable prefix aliases"
        sx={{ mb: 2 }}
      />

      {prefixAliases && (
        <Box>
          <Divider sx={{ mb: 3 }} />
          
          <Grid container spacing={2}>
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Left table prefix"
                value={prefixLeft}
                onChange={(e) => onChange({ prefixLeft: e.target.value })}
                helperText="Added to all left table columns"
                InputProps={{
                  startAdornment: <QuoteIcon fontSize="small" sx={{ mr: 1, color: 'text.secondary' }} />
                }}
              />
            </Grid>
            
            <Grid item xs={12} md={6}>
              <TextField
                fullWidth
                label="Right table prefix"
                value={prefixRight}
                onChange={(e) => onChange({ prefixRight: e.target.value })}
                helperText="Added to all right table columns"
                InputProps={{
                  startAdornment: <QuoteIcon fontSize="small" sx={{ mr: 1, color: 'text.secondary' }} />
                }}
              />
            </Grid>
          </Grid>

          <Alert 
            severity="info" 
            icon={<WarningIcon />}
            sx={{ mt: 2 }}
          >
            <Typography variant="caption">
              Example: Column "id" becomes "{prefixLeft}id" and "{prefixRight}id"
            </Typography>
          </Alert>

          <Box sx={{ mt: 2, p: 1.5, bgcolor: '#f5f5f5', borderRadius: 1 }}>
            <Typography variant="caption" color="text.secondary">
              <strong>Best Practices:</strong>
              <ul style={{ margin: '4px 0', paddingLeft: '16px' }}>
                <li>Use short, meaningful prefixes (e.g., "l_", "r_")</li>
                <li>Add underscore for readability</li>
                <li>Keep prefixes consistent across queries</li>
                <li>Disable if column names are already unique</li>
              </ul>
            </Typography>
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default PrefixConfig;