// JoinTypeSelector.tsx
import React from 'react';
import {
  FormControl,
  FormLabel,
  RadioGroup,
  FormControlLabel,
  Radio,
  Box,
  Typography,
  Chip,
  Grid,
  Paper // Add Paper import here
} from '@mui/material';
import {
  MergeType as InnerIcon,
  ArrowBack as LeftIcon,
  ArrowForward as RightIcon,
  CompareArrows as FullIcon,
  Close as CrossIcon,
  FilterCenterFocus as SemiIcon,
  Block as AntiIcon
} from '@mui/icons-material';

const JOIN_TYPES = [
  {
    value: 'INNER',
    label: 'Inner Join',
    description: 'Returns only matching rows from both tables',
    icon: <InnerIcon />,
    color: '#4caf50'
  },
  {
    value: 'LEFT',
    label: 'Left Outer Join',
    description: 'Returns all rows from left table and matching rows from right',
    icon: <LeftIcon />,
    color: '#2196f3'
  },
  {
    value: 'RIGHT',
    label: 'Right Outer Join',
    description: 'Returns all rows from right table and matching rows from left',
    icon: <RightIcon />,
    color: '#4caf50'
  },
  {
    value: 'FULL',
    label: 'Full Outer Join',
    description: 'Returns all rows when there is a match in either table',
    icon: <FullIcon />,
    color: '#ff9800'
  },
  {
    value: 'CROSS',
    label: 'Cross Join',
    description: 'Returns Cartesian product of both tables',
    icon: <CrossIcon />,
    color: '#9c27b0'
  },
  {
    value: 'SEMI',
    label: 'Semi Join',
    description: 'Returns rows from left that have matches in right',
    icon: <SemiIcon />,
    color: '#00bcd4'
  },
  {
    value: 'ANTI',
    label: 'Anti Join',
    description: 'Returns rows from left that have no matches in right',
    icon: <AntiIcon />,
    color: '#f44336'
  }
];

// Define a type for the join values
export type JoinType = 'INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS' | 'SEMI' | 'ANTI';

interface JoinTypeSelectorProps {
  value: JoinType;
  onChange: (value: JoinType) => void;
}

const JoinTypeSelector: React.FC<JoinTypeSelectorProps> = ({ value, onChange }) => {
  return (
    <Box>
      <FormControl component="fieldset" fullWidth>
        <FormLabel component="legend" sx={{ mb: 2 }}>
          <Typography variant="h6">Join Type</Typography>
        </FormLabel>
        
        <RadioGroup
          value={value}
          onChange={(e) => onChange(e.target.value as JoinType)}
        >
          <Grid container spacing={2}>
            {JOIN_TYPES.map((type) => (
              <Grid item xs={12} sm={6} key={type.value}>
                <Paper
                  sx={{
                    p: 2,
                    border: value === type.value ? `2px solid ${type.color}` : '1px solid #e0e0e0',
                    borderRadius: 1,
                    cursor: 'pointer',
                    '&:hover': {
                      backgroundColor: '#f5f5f5'
                    }
                  }}
                  onClick={() => onChange(type.value as JoinType)}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                    <Box sx={{ color: type.color, mr: 1 }}>
                      {type.icon}
                    </Box>
                    <FormControlLabel
                      value={type.value}
                      control={<Radio />}
                      label={type.label}
                      sx={{ flex: 1 }}
                    />
                  </Box>
                  <Typography variant="body2" color="text.secondary">
                    {type.description}
                  </Typography>
                  
                  {type.value === 'CROSS' && value === 'CROSS' && (
                    <Chip
                      label="Warning: May produce large result sets"
                      color="warning"
                      size="small"
                      sx={{ mt: 1 }}
                    />
                  )}
                </Paper>
              </Grid>
            ))}
          </Grid>
        </RadioGroup>
      </FormControl>
    </Box>
  );
};

export default JoinTypeSelector;