// components/SchemaTable/SchemaTable.tsx
import React, { useState, useRef, useEffect } from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Checkbox,
  TextField,
  Select,
  MenuItem,
  IconButton,
  Tooltip,
  Paper,
} from '@mui/material';
import {
  KeyboardArrowUp,
  KeyboardArrowDown,
  Edit as EditIcon,
} from '@mui/icons-material';
import { SchemaColumn, DataType }from '../../../types/types';
import { styled } from '@mui/material/styles';

const StyledTableRow = styled(TableRow)<{ selected: boolean; haserror?: boolean }>(({ theme, selected, haserror }) => ({
  backgroundColor: selected
    ? theme.palette.action.selected
    : haserror
    ? theme.palette.error.light
    : 'inherit',
  '&:hover': {
    backgroundColor: theme.palette.action.hover,
  },
  '& td': {
    padding: theme.spacing(0.75, 1),
  },
}));

const EditableCell = styled('div')(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  width: '100%',
  cursor: 'text',
  '&:hover': {
    backgroundColor: theme.palette.action.hover,
  },
}));

interface SchemaTableProps {
  columns: SchemaColumn[];
  selectedRows: Set<string>;
  editingCell: { rowIndex: number; columnKey: string; value: any } | null;
  onCellDoubleClick: (rowIndex: number, columnKey: keyof SchemaColumn, value: any) => void;
  onCellEdit: (rowId: string, field: keyof SchemaColumn, value: any) => void;
  onRowSelect: (rowId: string) => void;
  onMoveRow: (rowId: string, direction: 'up' | 'down') => void;
  onBlur: () => void;
}

const dataTypeOptions: DataType[] = [
  'String', 'Integer', 'Float', 'Boolean', 'Date', 'DateTime', 'Decimal', 'Object', 'Array'
];

export const SchemaTable: React.FC<SchemaTableProps> = ({
  columns,
  selectedRows,
  editingCell,
  onCellDoubleClick,
  onCellEdit,
  onRowSelect,
  onMoveRow,
  onBlur,
}) => {
  const [focusedCell, setFocusedCell] = useState<{ rowIndex: number; columnKey: string } | null>(null);
  const tableRef = useRef<HTMLTableElement>(null);

  const handleKeyDown = (e: React.KeyboardEvent, _rowId: string, _field: keyof SchemaColumn) => {
    if (e.key === 'Enter') {
      onBlur();
    } else if (e.key === 'Escape') {
      onBlur();
    }
  };

  const renderCellContent = (column: SchemaColumn, rowIndex: number, field: keyof SchemaColumn, value: any) => {
    const isEditing = editingCell?.rowIndex === rowIndex && editingCell?.columnKey === field;
    const isFocused = focusedCell?.rowIndex === rowIndex && focusedCell?.columnKey === field;

    if (isEditing) {
      switch (field) {
        case 'type':
          return (
            <Select
              value={value}
              onChange={(e) => onCellEdit(column.id, field, e.target.value)}
              onBlur={onBlur}
              autoFocus
              fullWidth
              size="small"
            >
              {dataTypeOptions.map((type) => (
                <MenuItem key={type} value={type}>
                  {type}
                </MenuItem>
              ))}
            </Select>
          );

        case 'nullable':
        case 'isKey':
        case 'isPrimary':
        case 'isUnique':
          return (
            <Checkbox
              checked={Boolean(value)}
              onChange={(e) => onCellEdit(column.id, field, e.target.checked)}
              onBlur={onBlur}
              autoFocus
            />
          );

        case 'length':
        case 'precision':
          return (
            <TextField
              type="number"
              value={value || ''}
              onChange={(e) => onCellEdit(column.id, field, e.target.value ? Number(e.target.value) : undefined)}
              onBlur={onBlur}
              autoFocus
              fullWidth
              size="small"
              inputProps={{ min: 0 }}
            />
          );

        default:
          return (
            <TextField
              value={value || ''}
              onChange={(e) => onCellEdit(column.id, field, e.target.value)}
              onBlur={onBlur}
              autoFocus
              fullWidth
              size="small"
              onKeyDown={(e) => handleKeyDown(e, column.id, field)}
            />
          );
      }
    }

    return (
      <EditableCell
        onDoubleClick={() => onCellDoubleClick(rowIndex, field, value)}
        onClick={() => setFocusedCell({ rowIndex, columnKey: field })}
      >
        <span style={{ flex: 1 }}>
          {field === 'type' ? value : 
           ['nullable', 'isKey', 'isPrimary', 'isUnique'].includes(field) ? 
             (value ? '✓' : '') : 
             value || '-'}
        </span>
        {isFocused && <EditIcon fontSize="small" sx={{ opacity: 0.5 }} />}
      </EditableCell>
    );
  };

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (tableRef.current && !tableRef.current.contains(event.target as Node)) {
        onBlur();
        setFocusedCell(null);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [onBlur]);

  return (
    <TableContainer component={Paper} sx={{ maxHeight: 400, overflow: 'auto' }}>
      <Table stickyHeader size="small" ref={tableRef}>
        <TableHead>
          <TableRow>
            <TableCell padding="checkbox">
              <Checkbox
                indeterminate={selectedRows.size > 0 && selectedRows.size < columns.length}
                checked={columns.length > 0 && selectedRows.size === columns.length}
                onChange={() => {
                  if (selectedRows.size === columns.length) {
                    columns.forEach(col => onRowSelect(col.id));
                  } else {
                    columns.forEach(col => onRowSelect(col.id));
                  }
                }}
              />
            </TableCell>
            <TableCell>Column Name</TableCell>
            <TableCell>Type</TableCell>
            <TableCell>Length/Precision</TableCell>
            <TableCell>Nullable</TableCell>
            <TableCell>Key</TableCell>
            <TableCell>Primary</TableCell>
            <TableCell>Unique</TableCell>
            <TableCell>Default Value</TableCell>
            <TableCell>Comment</TableCell>
            <TableCell>Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {columns.map((column, index) => (
            <StyledTableRow
              key={column.id}
              selected={selectedRows.has(column.id)}
              haserror={!column.validation?.isValid}
            >
              <TableCell padding="checkbox">
                <Checkbox
                  checked={selectedRows.has(column.id)}
                  onChange={() => onRowSelect(column.id)}
                />
              </TableCell>
              <TableCell>
                {renderCellContent(column, index, 'name', column.name)}
              </TableCell>
              <TableCell>
                {renderCellContent(column, index, 'type', column.type)}
              </TableCell>
              <TableCell>
                {renderCellContent(column, index, column.type === 'Decimal' ? 'precision' : 'length', 
                  column.type === 'Decimal' ? column.precision : column.length)}
              </TableCell>
              <TableCell align="center">
                {renderCellContent(column, index, 'nullable', column.nullable)}
              </TableCell>
              <TableCell align="center">
                {renderCellContent(column, index, 'isKey', column.isKey)}
              </TableCell>
              <TableCell align="center">
                {renderCellContent(column, index, 'isPrimary', column.isPrimary)}
              </TableCell>
              <TableCell align="center">
                {renderCellContent(column, index, 'isUnique', column.isUnique)}
              </TableCell>
              <TableCell>
                {renderCellContent(column, index, 'defaultValue', column.defaultValue)}
              </TableCell>
              <TableCell>
                {renderCellContent(column, index, 'comment', column.comment)}
              </TableCell>
              <TableCell>
                <Tooltip title="Move up">
                  <span>
                    <IconButton
                      size="small"
                      onClick={() => onMoveRow(column.id, 'up')}
                      disabled={index === 0}
                    >
                      <KeyboardArrowUp />
                    </IconButton>
                  </span>
                </Tooltip>
                <Tooltip title="Move down">
                  <span>
                    <IconButton
                      size="small"
                      onClick={() => onMoveRow(column.id, 'down')}
                      disabled={index === columns.length - 1}
                    >
                      <KeyboardArrowDown />
                    </IconButton>
                  </span>
                </Tooltip>
              </TableCell>
            </StyledTableRow>
          ))}
        </TableBody>
      </Table>
    </TableContainer>
  );
};