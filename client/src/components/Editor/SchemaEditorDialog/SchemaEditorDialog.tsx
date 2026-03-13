// components/SchemaEditorDialog/SchemaEditorDialog.tsx
import React, { useEffect, useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  IconButton,
  Typography,
  Box,
  Snackbar,
  Alert,
} from '@mui/material';
import {
  Close as CloseIcon,
  Minimize as MinimizeIcon,
  CropSquare as MaximizeIcon,
  Check as OkIcon,
  Clear as CancelIcon,
  Done as ApplyIcon,
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';
import { SchemaEditorDialogProps, SchemaColumn } from '../../../types/types';
import { useSchemaEditor } from '../../../hooks/useSchemaEditor';
import { SchemaTable } from './SchemaTable';
import { SchemaToolbar } from './Toolbar';
import { AdvancedOptionsSection } from './AdvancedOptions';
import { PreviewValidationSection } from './PreviewValidation';

const StyledDialog = styled(Dialog)(({ theme }) => ({
  '& .MuiDialog-paper': {
    minWidth: 1200,
    maxWidth: '90vw',
    maxHeight: '90vh',
    [theme.breakpoints.down('lg')]: {
      minWidth: 'auto',
      width: '95vw',
    },
  },
}));

const DialogHeader = styled(DialogTitle)(({ theme }) => ({
  padding: theme.spacing(1, 2),
  backgroundColor: theme.palette.grey[100],
  borderBottom: `1px solid ${theme.palette.divider}`,
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
}));

const DialogContentStyled = styled(DialogContent)(({ theme }) => ({
  padding: 0,
  display: 'flex',
  flexDirection: 'column',
  gap: theme.spacing(2),
  '&:first-of-type': {
    paddingTop: theme.spacing(2),
  },
}));

const ActionButtons = styled(DialogActions)(({ theme }) => ({
  padding: theme.spacing(2),
  borderTop: `1px solid ${theme.palette.divider}`,
  gap: theme.spacing(1),
}));

export const SchemaEditorDialog: React.FC<SchemaEditorDialogProps> = ({
  open,
  componentName,
  initialSchema,
  onSave,
  onCancel,
  onSchemaChange,
}) => {
  const {
    state,
    actions,
    validation,
  } = useSchemaEditor(initialSchema);

  const [isMaximized, setIsMaximized] = useState(false);
  const [notification, setNotification] = useState<{
    open: boolean;
    message: string;
    severity: 'success' | 'error' | 'info' | 'warning';
  }>({ open: false, message: '', severity: 'info' });

  const selectedCount = state.selectedRows.size;

  useEffect(() => {
    if (onSchemaChange) {
      onSchemaChange(state.columns);
    }
  }, [state.columns, onSchemaChange]);

  const handleCellDoubleClick = (rowIndex: number, columnKey: keyof SchemaColumn, value: any) => {
    actions.setEditingCell({ rowIndex, columnKey, value });
  };

  const handleCellEdit = (rowId: string, field: keyof SchemaColumn, value: any) => {
    actions.updateRow(rowId, field, value);
  };

  const handleSave = () => {
    if (!validation.isValid) {
      setNotification({
        open: true,
        message: 'Cannot save schema with validation errors',
        severity: 'error',
      });
      return;
    }

    onSave(state.columns);
    setNotification({
      open: true,
      message: 'Schema saved successfully',
      severity: 'success',
    });
  };

  const handleApply = () => {
    if (!validation.isValid) {
      setNotification({
        open: true,
        message: 'Cannot apply schema with validation errors',
        severity: 'error',
      });
      return;
    }

    onSave(state.columns);
    setNotification({
      open: true,
      message: 'Schema changes applied',
      severity: 'success',
    });
  };

  const handlePreview = () => {
    const sampleData = state.columns.map(col => ({
      name: col.name,
      type: col.type,
      nullable: col.nullable,
      defaultValue: col.defaultValue,
    }));

    console.log('Schema Preview:', sampleData);
    setNotification({
      open: true,
      message: 'Schema preview generated in console',
      severity: 'info',
    });
  };

  const handleMoveSelected = (direction: 'up' | 'down') => {
    if (selectedCount === 1) {
      const selectedId = Array.from(state.selectedRows)[0];
      actions.moveRow(selectedId, direction);
    }
  };

  const handleExpressionBuilder = () => {
    // In a real implementation, this would open an expression builder modal
    setNotification({
      open: true,
      message: 'Expression builder would open here',
      severity: 'info',
    });
  };

  const handleSyncColumns = () => {
    // In a real implementation, this would sync with external schema
    setNotification({
      open: true,
      message: 'Columns synchronized successfully',
      severity: 'success',
    });
  };

  const handleCloseNotification = () => {
    setNotification(prev => ({ ...prev, open: false }));
  };

  return (
    <>
      <StyledDialog
        open={open}
        onClose={onCancel}
        fullScreen={isMaximized}
        maxWidth={false}
      >
        <DialogHeader>
          <Box display="flex" alignItems="center" gap={1}>
            <Typography variant="h6" component="div">
              {componentName} – Edit Schema
            </Typography>
            {state.hasChanges && (
              <Typography
                variant="caption"
                sx={{
                  ml: 1,
                  px: 1,
                  py: 0.5,
                  bgcolor: 'warning.light',
                  borderRadius: 1,
                  color: 'warning.contrastText',
                }}
              >
                UNSAVED CHANGES
              </Typography>
            )}
          </Box>
          <Box>
            <IconButton size="small" title="Minimize" disabled>
              <MinimizeIcon fontSize="small" />
            </IconButton>
            <IconButton
              size="small"
              title={isMaximized ? 'Restore' : 'Maximize'}
              onClick={() => setIsMaximized(!isMaximized)}
            >
              <MaximizeIcon fontSize="small" />
            </IconButton>
            <IconButton size="small" title="Close" onClick={onCancel}>
              <CloseIcon fontSize="small" />
            </IconButton>
          </Box>
        </DialogHeader>

        <DialogContentStyled>
          <Box px={2}>
            <SchemaToolbar
              selectedCount={selectedCount}
              onAddRow={actions.addRow}
              onRemoveSelected={actions.removeSelectedRows}
              onClearAll={actions.clearAll}
              onMoveUp={() => handleMoveSelected('up')}
              onMoveDown={() => handleMoveSelected('down')}
              onGuessSchema={actions.guessSchema}
              onImport={actions.importSchema}
              onExport={actions.exportSchema}
              onSync={handleSyncColumns}
              onExpressionBuilder={handleExpressionBuilder}
            />
          </Box>

          <Box px={2}>
            <SchemaTable
              columns={state.columns}
              selectedRows={state.selectedRows}
              editingCell={state.editingCell}
              onCellDoubleClick={handleCellDoubleClick}
              onCellEdit={handleCellEdit}
              onRowSelect={actions.toggleRowSelection}
              onMoveRow={actions.moveRow}
              onBlur={() => actions.setEditingCell(null)}
            />
          </Box>

          <Box px={2}>
            <AdvancedOptionsSection
              options={state.advancedOptions}
              onChange={actions.setAdvancedOptions}
            />
          </Box>

          <Box px={2}>
            <PreviewValidationSection
              validation={validation}
              onPreview={handlePreview}
            />
          </Box>
        </DialogContentStyled>

        <ActionButtons>
          <Button
            startIcon={<CancelIcon />}
            onClick={onCancel}
            color="inherit"
            disabled={state.hasChanges}
          >
            Cancel
          </Button>
          <Button
            startIcon={<ApplyIcon />}
            onClick={handleApply}
            color="secondary"
            disabled={!state.hasChanges || !validation.isValid}
          >
            Apply
          </Button>
          <Button
            startIcon={<OkIcon />}
            onClick={handleSave}
            variant="contained"
            color="primary"
            disabled={!validation.isValid}
          >
            OK
          </Button>
        </ActionButtons>
      </StyledDialog>

      <Snackbar
        open={notification.open}
        autoHideDuration={6000}
        onClose={handleCloseNotification}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert
          onClose={handleCloseNotification}
          severity={notification.severity}
          sx={{ width: '100%' }}
        >
          {notification.message}
        </Alert>
      </Snackbar>
    </>
  );
};