import React from 'react';
import {
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Typography,
  TextField,
  FormControlLabel,
  Switch,
  Grid,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
} from '@mui/material';
import { ExpandMore as ExpandMoreIcon } from '@mui/icons-material';
import { AdvancedOptions } from '../../../types/types';

interface AdvancedOptionsProps {
  options: AdvancedOptions;
  onChange: (options: Partial<AdvancedOptions>) => void;
}

export const AdvancedOptionsSection: React.FC<AdvancedOptionsProps> = ({
  options,
  onChange,
}) => {
  const dateFormats = [
    'YYYY-MM-DD',
    'DD/MM/YYYY',
    'MM/DD/YYYY',
    'YYYY-MM-DD HH:mm:ss',
    'DD-MM-YYYY',
    'MM-DD-YYYY',
  ];

  const handleSwitchChange = (key: keyof AdvancedOptions) => (
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    onChange({ [key]: e.target.checked } as Partial<AdvancedOptions>);
  };

  return (
    <Accordion defaultExpanded={false} sx={{ mt: 2 }}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
        <Typography variant="subtitle1">Advanced Options</Typography>
      </AccordionSummary>
      <AccordionDetails>
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <FormControl fullWidth size="small">
              <InputLabel>Date Format</InputLabel>
              <Select
                value={options.dateFormat}
                label="Date Format"
                onChange={(e) => onChange({ dateFormat: e.target.value })}
              >
                {dateFormats.map((format) => (
                  <MenuItem key={format} value={format}>
                    {format}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Grid>

          <Grid item xs={12} md={6}>
            <TextField
              fullWidth
              size="small"
              label="Number Pattern"
              value={options.numberPattern}
              onChange={(e) => onChange({ numberPattern: e.target.value })}
              placeholder="#,##0.##"
              helperText="e.g., #,##0.## for thousand separators"
            />
          </Grid>

          <Grid item xs={12}>
            <FormControlLabel
              control={
                <Switch
                  checked={options.enforceLengthPrecision}
                  onChange={handleSwitchChange('enforceLengthPrecision')}
                />
              }
              label="Enforce Length/Precision Constraints"
            />
          </Grid>

          <Grid item xs={12}>
            <FormControlLabel
              control={
                <Switch
                  checked={options.enforceNullable}
                  onChange={handleSwitchChange('enforceNullable')}
                />
              }
              label="Enforce Nullable Constraints"
            />
          </Grid>

          <Grid item xs={12}>
            <Typography variant="caption" color="textSecondary">
              Note: These options apply to all columns and affect validation rules.
            </Typography>
          </Grid>
        </Grid>
      </AccordionDetails>
    </Accordion>
  );
};