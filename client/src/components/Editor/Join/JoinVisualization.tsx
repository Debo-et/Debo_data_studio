// JoinVisualization.tsx
import React from 'react';
import { Box, Typography, Paper } from '@mui/material';

interface JoinVisualizationProps {
  joinType: string;
  leftCount: number;
  rightCount: number;
}

const JoinVisualization: React.FC<JoinVisualizationProps> = ({ joinType, leftCount, rightCount }) => {
  const getVennConfig = () => {
    switch (joinType) {
      case 'INNER':
        return {
          leftFill: 'rgba(33, 150, 243, 0.3)',
          rightFill: 'rgba(76, 175, 80, 0.3)',
          overlapFill: 'rgba(156, 39, 176, 0.5)',
          showOverlap: true,
          leftOnly: false,
          rightOnly: false
        };
      case 'LEFT':
        return {
          leftFill: 'rgba(33, 150, 243, 0.5)',
          rightFill: 'rgba(76, 175, 80, 0.2)',
          overlapFill: 'rgba(156, 39, 176, 0.5)',
          showOverlap: true,
          leftOnly: true,
          rightOnly: false
        };
      case 'RIGHT':
        return {
          leftFill: 'rgba(33, 150, 243, 0.2)',
          rightFill: 'rgba(76, 175, 80, 0.5)',
          overlapFill: 'rgba(156, 39, 176, 0.5)',
          showOverlap: true,
          leftOnly: false,
          rightOnly: true
        };
      case 'FULL':
        return {
          leftFill: 'rgba(33, 150, 243, 0.3)',
          rightFill: 'rgba(76, 175, 80, 0.3)',
          overlapFill: 'rgba(156, 39, 176, 0.5)',
          showOverlap: true,
          leftOnly: true,
          rightOnly: true
        };
      case 'CROSS':
        return {
          leftFill: 'rgba(33, 150, 243, 0.3)',
          rightFill: 'rgba(76, 175, 80, 0.3)',
          overlapFill: 'transparent',
          showOverlap: false,
          leftOnly: false,
          rightOnly: false
        };
      case 'SEMI':
        return {
          leftFill: 'rgba(33, 150, 243, 0.3)',
          rightFill: 'transparent',
          overlapFill: 'rgba(33, 150, 243, 0.5)',
          showOverlap: true,
          leftOnly: false,
          rightOnly: false
        };
      case 'ANTI':
        return {
          leftFill: 'rgba(33, 150, 243, 0.5)',
          rightFill: 'transparent',
          overlapFill: 'rgba(33, 150, 243, 0.2)',
          showOverlap: false,
          leftOnly: true,
          rightOnly: false
        };
      default:
        return {
          leftFill: 'rgba(33, 150, 243, 0.3)',
          rightFill: 'rgba(76, 175, 80, 0.3)',
          overlapFill: 'rgba(156, 39, 176, 0.5)',
          showOverlap: true,
          leftOnly: false,
          rightOnly: false
        };
    }
  };

  const config = getVennConfig();

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Join Visualization
      </Typography>
      
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', my: 3 }}>
        <Box sx={{ position: 'relative', width: 250, height: 150 }}>
          {/* Left Circle */}
          <Box
            sx={{
              position: 'absolute',
              width: 100,
              height: 100,
              borderRadius: '50%',
              bgcolor: config.leftFill,
              border: '2px solid #1976d2',
              left: 30,
              top: 25,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 1
            }}
          >
            <Typography variant="caption" fontWeight="bold">
              Left: {leftCount}
            </Typography>
          </Box>

          {/* Right Circle */}
          <Box
            sx={{
              position: 'absolute',
              width: 100,
              height: 100,
              borderRadius: '50%',
              bgcolor: config.rightFill,
              border: '2px solid #4caf50',
              right: 30,
              top: 25,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              zIndex: 2
            }}
          >
            <Typography variant="caption" fontWeight="bold">
              Right: {rightCount}
            </Typography>
          </Box>

          {/* Overlap Area */}
          {config.showOverlap && (
            <Box
              sx={{
                position: 'absolute',
                width: 60,
                height: 60,
                borderRadius: '50%',
                bgcolor: config.overlapFill,
                border: '2px dashed #9c27b0',
                left: 80,
                top: 45,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                zIndex: 3
              }}
            >
              <Typography variant="caption" fontSize="10px">
                Match
              </Typography>
            </Box>
          )}

          {/* Left Only Indicator */}
          {config.leftOnly && (
            <Box
              sx={{
                position: 'absolute',
                left: 10,
                top: 60,
                bgcolor: '#1976d2',
                color: 'white',
                borderRadius: '50%',
                width: 20,
                height: 20,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            >
              <Typography variant="caption" fontSize="10px">
                +
              </Typography>
            </Box>
          )}

          {/* Right Only Indicator */}
          {config.rightOnly && (
            <Box
              sx={{
                position: 'absolute',
                right: 10,
                top: 60,
                bgcolor: '#4caf50',
                color: 'white',
                borderRadius: '50%',
                width: 20,
                height: 20,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
              }}
            >
              <Typography variant="caption" fontSize="10px">
                +
              </Typography>
            </Box>
          )}
        </Box>
      </Box>

      {/* Legend */}
      <Box sx={{ mt: 3 }}>
        <Typography variant="subtitle2" gutterBottom>
          Legend:
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Box sx={{ width: 12, height: 12, bgcolor: '#1976d2', borderRadius: '50%' }} />
            <Typography variant="caption">Left Input</Typography>
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Box sx={{ width: 12, height: 12, bgcolor: '#4caf50', borderRadius: '50%' }} />
            <Typography variant="caption">Right Input</Typography>
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Box sx={{ width: 12, height: 12, bgcolor: '#9c27b0', borderRadius: '50%' }} />
            <Typography variant="caption">Matching Rows</Typography>
          </Box>
        </Box>
      </Box>

      {/* Join Type Description */}
      <Paper sx={{ p: 2, mt: 2, bgcolor: '#f5f5f5' }}>
        <Typography variant="body2">
          <strong>{joinType} Join:</strong> {getJoinDescription(joinType)}
        </Typography>
      </Paper>
    </Box>
  );
};

const getJoinDescription = (joinType: string): string => {
  switch (joinType) {
    case 'INNER': return 'Returns only rows with matching keys in both inputs';
    case 'LEFT': return 'Returns all rows from left input and matching rows from right';
    case 'RIGHT': return 'Returns all rows from right input and matching rows from left';
    case 'FULL': return 'Returns all rows from both inputs, with matches where available';
    case 'CROSS': return 'Returns Cartesian product (all combinations) of both inputs';
    case 'SEMI': return 'Returns rows from left that have matches in right (no right columns)';
    case 'ANTI': return 'Returns rows from left that have no matches in right';
    default: return '';
  }
};

export default JoinVisualization;