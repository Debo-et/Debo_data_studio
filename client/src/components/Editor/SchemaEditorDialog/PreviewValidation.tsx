// components/SchemaEditorDialog/PreviewValidation.tsx
import React from 'react';
import {
  Paper,
  Typography,
  Box,
  Alert,
  AlertTitle,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Chip,
  Button,
  Collapse,
} from '@mui/material';
import {
  Error as ErrorIcon,
  Warning as WarningIcon,
  CheckCircle as CheckIcon,
  Visibility as PreviewIcon,
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { ValidationResult } from '../../../types/types';

const ValidationContainer = styled(Paper)(({ theme }) => ({
  padding: theme.spacing(2),
  marginTop: theme.spacing(2),
  backgroundColor: theme.palette.grey[50],
}));

interface PreviewValidationProps {
  validation: ValidationResult;
  onPreview: () => void;
}

export const PreviewValidationSection: React.FC<PreviewValidationProps> = ({
  validation,
  onPreview,
}) => {
  const [expanded, setExpanded] = React.useState(false);

  const hasErrors = validation.errors.length > 0;
  const hasWarnings = validation.warnings.length > 0;
  const hasDuplicates = validation.duplicateColumns.length > 0;

  return (
    <ValidationContainer variant="outlined">
      <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
        <Typography variant="subtitle1" fontWeight="medium">
          Validation & Preview
        </Typography>
        <Box display="flex" gap={1}>
          {hasDuplicates && (
            <Chip
              label={`${validation.duplicateColumns.length} duplicate(s)`}
              color="warning"
              size="small"
            />
          )}
          <Button
            startIcon={<PreviewIcon />}
            variant="outlined"
            size="small"
            onClick={onPreview}
            disabled={!validation.isValid}
          >
            Preview Schema
          </Button>
        </Box>
      </Box>

      {validation.isValid && !hasWarnings ? (
        <Alert severity="success" icon={<CheckIcon />}>
          Schema is valid and ready to use
        </Alert>
      ) : (
        <>
          {hasErrors && (
            <Alert
              severity="error"
              sx={{ mb: 1 }}
              action={
                <Button
                  size="small"
                  onClick={() => setExpanded(!expanded)}
                >
                  {expanded ? 'Hide' : 'Show'} Details
                </Button>
              }
            >
              <AlertTitle>Schema Validation Errors</AlertTitle>
              Found {validation.errors.length} error(s) that must be fixed
            </Alert>
          )}

          {hasWarnings && !hasErrors && (
            <Alert severity="warning" sx={{ mb: 1 }}>
              <AlertTitle>Schema Warnings</AlertTitle>
              Found {validation.warnings.length} warning(s)
            </Alert>
          )}

          <Collapse in={expanded}>
            {hasErrors && (
              <List dense>
                {validation.errors.map((error, index) => (
                  <ListItem key={index}>
                    <ListItemIcon>
                      <ErrorIcon color="error" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText primary={error} />
                  </ListItem>
                ))}
              </List>
            )}

            {hasWarnings && (
              <List dense>
                {validation.warnings.map((warning, index) => (
                  <ListItem key={index}>
                    <ListItemIcon>
                      <WarningIcon color="warning" fontSize="small" />
                    </ListItemIcon>
                    <ListItemText primary={warning} />
                  </ListItem>
                ))}
              </List>
            )}
          </Collapse>

          {hasDuplicates && (
            <Box mt={2}>
              <Typography variant="body2" color="textSecondary" gutterBottom>
                Duplicate column names:
              </Typography>
              <Box display="flex" gap={1} flexWrap="wrap">
                {validation.duplicateColumns.map((name, index) => (
                  <Chip
                    key={index}
                    label={name}
                    color="warning"
                    variant="outlined"
                    size="small"
                  />
                ))}
              </Box>
            </Box>
          )}
        </>
      )}
    </ValidationContainer>
  );
};