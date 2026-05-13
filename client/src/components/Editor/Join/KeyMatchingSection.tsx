// KeyMatchingSection.tsx
import React, { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Chip,
  TextField,
  InputAdornment,
  Paper,
   Grid 
} from '@mui/material';
import {
  DragIndicator as DragIcon,
  Delete as DeleteIcon,
  Link as LinkIcon,
  Search as SearchIcon,
  DataObject as StringIcon,
  Numbers as NumberIcon,
  CalendarToday as DateIcon,
  CheckBox as BooleanIcon,
  Code as ArrayIcon,
  AccountTree as ObjectIcon
} from '@mui/icons-material';
import { DragDropContext, Droppable, Draggable } from '@hello-pangea/dnd';

interface FieldSchema {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date' | 'timestamp' | 'array' | 'object';
  nullable?: boolean;
}

interface KeyMatchingSectionProps {
  leftKeys: string[];
  rightKeys: string[];
  leftFields: FieldSchema[];
  rightFields: FieldSchema[];
  onChange: (leftKeys: string[], rightKeys: string[]) => void;
}

const getTypeIcon = (type: string) => {
  switch (type) {
    case 'string': return <StringIcon fontSize="small" />;
    case 'number': return <NumberIcon fontSize="small" />;
    case 'boolean': return <BooleanIcon fontSize="small" />;
    case 'date':
    case 'timestamp': return <DateIcon fontSize="small" />;
    case 'array': return <ArrayIcon fontSize="small" />;
    case 'object': return <ObjectIcon fontSize="small" />;
    default: return <StringIcon fontSize="small" />;
  }
};

const KeyMatchingSection: React.FC<KeyMatchingSectionProps> = ({
  leftKeys,
  rightKeys,
  leftFields,
  rightFields,
  onChange
}) => {
  const [selectedLeft, setSelectedLeft] = useState<string>('');
  const [selectedRight, setSelectedRight] = useState<string>('');
  const [leftSearch, setLeftSearch] = useState('');
  const [rightSearch, setRightSearch] = useState('');

  const filteredLeftFields = leftFields.filter(field =>
    field.name.toLowerCase().includes(leftSearch.toLowerCase())
  );

  const filteredRightFields = rightFields.filter(field =>
    field.name.toLowerCase().includes(rightSearch.toLowerCase())
  );

  const handlePairKeys = () => {
    if (selectedLeft && selectedRight) {
      const newLeftKeys = [...leftKeys, selectedLeft];
      const newRightKeys = [...rightKeys, selectedRight];
      onChange(newLeftKeys, newRightKeys);
      setSelectedLeft('');
      setSelectedRight('');
    }
  };

  const handleRemovePair = (index: number) => {
    const newLeftKeys = leftKeys.filter((_, i) => i !== index);
    const newRightKeys = rightKeys.filter((_, i) => i !== index);
    onChange(newLeftKeys, newRightKeys);
  };

  const handleDragEnd = (result: any) => {
    if (!result.destination) return;

    if (result.source.droppableId === 'leftFields' && result.destination.droppableId === 'rightFields') {
      // Dragging from left to right - create pair
      const leftField = filteredLeftFields[result.source.index];
      const rightField = filteredRightFields[result.destination.index];
      
      if (leftField && rightField) {
        const newLeftKeys = [...leftKeys, leftField.name];
        const newRightKeys = [...rightKeys, rightField.name];
        onChange(newLeftKeys, newRightKeys);
      }
    }
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Key Matching
      </Typography>
      
      <Typography variant="body2" color="text.secondary" gutterBottom>
        Select fields from left and right inputs to create join key pairs
      </Typography>

      {leftKeys.length > 0 && (
        <Paper sx={{ p: 2, mb: 3, bgcolor: '#f8f9fa' }}>
          <Typography variant="subtitle1" gutterBottom>
            Current Key Pairs ({leftKeys.length})
          </Typography>
          <List>
            {leftKeys.map((leftKey, index) => (
              <ListItem
                key={index}
                secondaryAction={
                  <IconButton edge="end" onClick={() => handleRemovePair(index)}>
                    <DeleteIcon />
                  </IconButton>
                }
              >
                <ListItemIcon>
                  <LinkIcon color="primary" />
                </ListItemIcon>
                <ListItemText
                  primary={
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                      <Chip
                        label={leftKey}
                        color="primary"
                        variant="outlined"
                        size="small"
                        icon={getTypeIcon(leftFields.find(f => f.name === leftKey)?.type || 'string')}
                      />
                      <Typography variant="body2">→</Typography>
                      <Chip
                        label={rightKeys[index]}
                        color="success"
                        variant="outlined"
                        size="small"
                        icon={getTypeIcon(rightFields.find(f => f.name === rightKeys[index])?.type || 'string')}
                      />
                    </Box>
                  }
                />
              </ListItem>
            ))}
          </List>
        </Paper>
      )}

      <DragDropContext onDragEnd={handleDragEnd}>
        <Grid container spacing={3}>
          {/* Left Fields */}
          <Grid item xs={12} md={5}>
            <Paper sx={{ p: 2, border: '1px solid #e0e0e0' }}>
              <Typography variant="subtitle1" gutterBottom color="primary">
                Left Input Fields ({leftFields.length})
              </Typography>
              
              <TextField
                fullWidth
                size="small"
                placeholder="Search left fields..."
                value={leftSearch}
                onChange={(e) => setLeftSearch(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" />
                    </InputAdornment>
                  ),
                }}
                sx={{ mb: 2 }}
              />

              <Droppable droppableId="leftFields">
                {(provided) => (
                  <List
                    {...provided.droppableProps}
                    ref={provided.innerRef}
                    sx={{ maxHeight: 300, overflow: 'auto' }}
                  >
                    {filteredLeftFields.map((field, index) => (
                      <Draggable key={field.name} draggableId={`left-${field.name}`} index={index}>
                        {(provided) => (
                          <ListItem
                            ref={provided.innerRef}
                            {...provided.draggableProps}
                            secondaryAction={
                              <Box {...provided.dragHandleProps}>
                                <DragIcon />
                              </Box>
                            }
                            selected={selectedLeft === field.name}
                            onClick={() => setSelectedLeft(field.name)}
                            sx={{
                              cursor: 'pointer',
                              '&:hover': { bgcolor: '#f0f7ff' },
                              border: selectedLeft === field.name ? '1px solid #1976d2' : 'none'
                            }}
                          >
                            <ListItemIcon>
                              {getTypeIcon(field.type)}
                            </ListItemIcon>
                            <ListItemText
                              primary={field.name}
                              secondary={
                                <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                                  <Chip label={field.type} size="small" />
                                  {field.nullable && (
                                    <Chip label="nullable" size="small" color="warning" variant="outlined" />
                                  )}
                                </Box>
                              }
                            />
                          </ListItem>
                        )}
                      </Draggable>
                    ))}
                    {provided.placeholder}
                  </List>
                )}
              </Droppable>
              
              {selectedLeft && (
                <Chip
                  label={`Selected: ${selectedLeft}`}
                  color="primary"
                  onDelete={() => setSelectedLeft('')}
                  sx={{ mt: 1 }}
                />
              )}
            </Paper>
          </Grid>

          {/* Center Controls */}
          <Grid item xs={12} md={2}>
            <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', height: '100%', justifyContent: 'center' }}>
              <Button
                variant="contained"
                startIcon={<LinkIcon />}
                onClick={handlePairKeys}
                disabled={!selectedLeft || !selectedRight}
                sx={{ mb: 2 }}
              >
                Pair Keys
              </Button>
              
              <Typography variant="caption" color="text.secondary" align="center">
                OR
              </Typography>
              
              <Typography variant="caption" color="text.secondary" align="center" sx={{ mt: 1 }}>
                Drag from left to right
              </Typography>
            </Box>
          </Grid>

          {/* Right Fields */}
          <Grid item xs={12} md={5}>
            <Paper sx={{ p: 2, border: '1px solid #e0e0e0' }}>
              <Typography variant="subtitle1" gutterBottom color="success">
                Right Input Fields ({rightFields.length})
              </Typography>
              
              <TextField
                fullWidth
                size="small"
                placeholder="Search right fields..."
                value={rightSearch}
                onChange={(e) => setRightSearch(e.target.value)}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" />
                    </InputAdornment>
                  ),
                }}
                sx={{ mb: 2 }}
              />

              <Droppable droppableId="rightFields">
                {(provided) => (
                  <List
                    {...provided.droppableProps}
                    ref={provided.innerRef}
                    sx={{ maxHeight: 300, overflow: 'auto' }}
                  >
                    {filteredRightFields.map((field) => (
                      <ListItem
                        key={field.name}
                        selected={selectedRight === field.name}
                        onClick={() => setSelectedRight(field.name)}
                        sx={{
                          cursor: 'pointer',
                          '&:hover': { bgcolor: '#f0f9f0' },
                          border: selectedRight === field.name ? '1px solid #4caf50' : 'none'
                        }}
                      >
                        <ListItemIcon>
                          {getTypeIcon(field.type)}
                        </ListItemIcon>
                        <ListItemText
                          primary={field.name}
                          secondary={
                            <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                              <Chip label={field.type} size="small" />
                              {field.nullable && (
                                <Chip label="nullable" size="small" color="warning" variant="outlined" />
                              )}
                            </Box>
                          }
                        />
                      </ListItem>
                    ))}
                    {provided.placeholder}
                  </List>
                )}
              </Droppable>
              
              {selectedRight && (
                <Chip
                  label={`Selected: ${selectedRight}`}
                  color="success"
                  onDelete={() => setSelectedRight('')}
                  sx={{ mt: 1 }}
                />
              )}
            </Paper>
          </Grid>
        </Grid>
      </DragDropContext>
    </Box>
  );
};

export default KeyMatchingSection;