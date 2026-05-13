// AdvancedOptions.tsx
import React from 'react';
import {
  Box,
  Typography,
  Switch,
  FormControlLabel,
  FormGroup,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  ListItemText,
  ListItemIcon
} from '@mui/material';
import {
  ExpandMore as ExpandMoreIcon,
  Settings as SettingsIcon,
  Warning as WarningIcon,
  Speed as PerformanceIcon,
  DataObject as DataIcon
} from '@mui/icons-material';

interface AdvancedOptionsProps {
  nullEquality: boolean;
  onChange: (updates: {
    nullEquality?: boolean;
  }) => void;
}

const AdvancedOptions: React.FC<AdvancedOptionsProps> = ({
  nullEquality,
  onChange
}) => {
  return (
    <Box>
      <Typography variant="h6" gutterBottom sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <SettingsIcon fontSize="small" />
        Advanced Options
      </Typography>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle1">Join Behavior</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <FormGroup>
            <FormControlLabel
              control={
                <Switch
                  checked={nullEquality}
                  onChange={(e) => onChange({ nullEquality: e.target.checked })}
                />
              }
              label={
                <Box>
                  <Typography>NULL = NULL equality</Typography>
                  <Typography variant="caption" color="text.secondary">
                    Treat NULL values as equal in join conditions
                  </Typography>
                </Box>
              }
            />
          </FormGroup>

          <Alert severity="warning" sx={{ mt: 2 }}>
            <Typography variant="caption">
              <strong>Note:</strong> Most SQL dialects treat NULL != NULL by default. 
              Enabling this may produce different results than standard SQL.
            </Typography>
          </Alert>
        </AccordionDetails>
      </Accordion>

      <Accordion>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="subtitle1">Performance Options</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <List dense>
            <ListItem>
              <ListItemIcon>
                <PerformanceIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText
                primary="Broadcast Join"
                secondary="Enable for small tables"
              />
            </ListItem>
            <ListItem>
              <ListItemIcon>
                <DataIcon fontSize="small" />
              </ListItemIcon>
              <ListItemText
                primary="Sort-Merge Join"
                secondary="For pre-sorted or large datasets"
              />
            </ListItem>
          </List>
        </AccordionDetails>
      </Accordion>

      <Alert 
        severity="info" 
        icon={<WarningIcon />}
        sx={{ mt: 2 }}
      >
        <Typography variant="caption">
          Advanced options are engine-specific. Check your SQL engine's documentation 
          for implementation details and performance implications.
        </Typography>
      </Alert>
    </Box>
  );
};

export default AdvancedOptions;