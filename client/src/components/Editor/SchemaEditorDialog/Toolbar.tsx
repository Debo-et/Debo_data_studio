// components/SchemaEditorDialog/Toolbar.tsx
import React from 'react';
import {
  Toolbar as MuiToolbar,
  Button,
  IconButton,
  Tooltip,
  Divider,
  Badge,
} from '@mui/material';
import {
  Add as AddIcon,
  Remove as RemoveIcon,
  Delete as ClearIcon,
  ArrowUpward as UpIcon,
  ArrowDownward as DownIcon,
  AutoFixHigh as GuessIcon,
  Upload as ImportIcon,
  Download as ExportIcon,
  Sync as SyncIcon,
  Functions as FunctionsIcon,
  CloudUpload as CloudUploadIcon,
} from '@mui/icons-material';
import { styled } from '@mui/material/styles';

const StyledToolbar = styled(MuiToolbar)(({ theme }) => ({
  backgroundColor: theme.palette.grey[50],
  borderBottom: `1px solid ${theme.palette.divider}`,
  minHeight: '56px',
  gap: theme.spacing(1),
  flexWrap: 'wrap',
}));

interface SchemaToolbarProps {
  selectedCount: number;
  onAddRow: () => void;
  onRemoveSelected: () => void;
  onClearAll: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  onGuessSchema: () => void;
  onImport: (file: File) => void;
  onExport: () => void;
  onSync: () => void;
  onExpressionBuilder: () => void;
}

export const SchemaToolbar: React.FC<SchemaToolbarProps> = ({
  selectedCount,
  onAddRow,
  onRemoveSelected,
  onClearAll,
  onMoveUp,
  onMoveDown,
  onGuessSchema,
  onImport,
  onExport,
  onSync,
  onExpressionBuilder,
}) => {
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleImportClick = () => {
    fileInputRef.current?.click();
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      onImport(file);
      event.target.value = ''; // Reset input
    }
  };

  return (
    <>
      <StyledToolbar variant="dense">
        <Tooltip title="Add Row">
          <IconButton onClick={onAddRow}>
            <AddIcon />
          </IconButton>
        </Tooltip>

        <Tooltip title={`Remove Selected (${selectedCount})`}>
          <span>
            <IconButton onClick={onRemoveSelected} disabled={selectedCount === 0}>
              <Badge badgeContent={selectedCount} color="error" max={99}>
                <RemoveIcon />
              </Badge>
            </IconButton>
          </span>
        </Tooltip>

        <Tooltip title="Clear All">
          <IconButton onClick={onClearAll}>
            <ClearIcon />
          </IconButton>
        </Tooltip>

        <Divider orientation="vertical" flexItem />

        <Tooltip title="Move Up">
          <span>
            <IconButton onClick={onMoveUp} disabled={selectedCount !== 1}>
              <UpIcon />
            </IconButton>
          </span>
        </Tooltip>

        <Tooltip title="Move Down">
          <span>
            <IconButton onClick={onMoveDown} disabled={selectedCount !== 1}>
              <DownIcon />
            </IconButton>
          </span>
        </Tooltip>

        <Divider orientation="vertical" flexItem />

        <Tooltip title="Guess Schema">
          <IconButton onClick={onGuessSchema}>
            <GuessIcon />
          </IconButton>
        </Tooltip>

        <input
          type="file"
          ref={fileInputRef}
          style={{ display: 'none' }}
          accept=".json,.yaml,.yml"
          onChange={handleFileChange}
        />
        <Tooltip title="Import Schema">
          <IconButton onClick={handleImportClick}>
            <ImportIcon />
          </IconButton>
        </Tooltip>

        <Tooltip title="Export Schema">
          <IconButton onClick={onExport}>
            <ExportIcon />
          </IconButton>
        </Tooltip>

        <Tooltip title="Synchronize Columns">
          <IconButton onClick={onSync}>
            <SyncIcon />
          </IconButton>
        </Tooltip>

        <Tooltip title="Expression Builder">
          <IconButton onClick={onExpressionBuilder}>
            <FunctionsIcon />
          </IconButton>
        </Tooltip>

        <Divider orientation="vertical" flexItem />

        <Button
          startIcon={<CloudUploadIcon />}
          size="small"
          variant="outlined"
          onClick={handleImportClick}
        >
          Import Schema
        </Button>
      </StyledToolbar>
    </>
  );
};