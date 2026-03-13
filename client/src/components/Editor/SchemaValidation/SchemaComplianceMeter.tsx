// components/Visualization/SchemaComplianceMeter.tsx
import React from 'react';

interface SchemaComplianceMeterProps {
  coverage: number; // 0-1
  threshold: number; // 0-1
}

const SchemaComplianceMeter: React.FC<SchemaComplianceMeterProps> = ({ 
  coverage, 
  threshold 
}) => {
  const getColor = (value: number) => {
    if (value >= 0.8) return 'text-green-600';
    if (value >= 0.5) return 'text-yellow-600';
    return 'text-red-600';
  };

  const getBarColor = (value: number) => {
    if (value >= 0.8) return 'bg-green-500';
    if (value >= 0.5) return 'bg-yellow-500';
    return 'bg-red-500';
  };

  return (
    <div className="space-y-2">
      <div className="flex justify-between text-sm">
        <span className={getColor(coverage)}>
          {Math.round(coverage * 100)}% Covered
        </span>
        <span className="text-gray-500">
          Threshold: {Math.round(threshold * 100)}%
        </span>
      </div>
      <div className="h-2 bg-gray-200 rounded-full overflow-hidden">
        <div 
          className={`h-full ${getBarColor(coverage)} transition-all duration-300`}
          style={{ width: `${coverage * 100}%` }}
        />
        {threshold > 0 && (
          <div 
            className="h-full w-1 bg-gray-800 ml-[-1px] relative"
            style={{ 
              marginLeft: `${threshold * 100}%`,
              left: '-0.5px'
            }}
          />
        )}
      </div>
    </div>
  );
};

export default SchemaComplianceMeter;