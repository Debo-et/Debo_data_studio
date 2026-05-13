// src/components/NodeModuleFallback.tsx
import React from 'react';

interface Props {
  componentName: string;
  error: Error;
}

const NodeModuleFallback: React.FC<Props> = ({ componentName, error }) => {
  return (
    <div className="p-4 border border-red-300 bg-red-50 rounded-lg">
      <h3 className="text-lg font-semibold text-red-800 mb-2">
        Component Load Error: {componentName}
      </h3>
      <p className="text-red-600 mb-2">
        This component cannot be loaded due to missing dependencies.
      </p>
      <details className="text-sm text-red-700">
        <summary>Technical Details</summary>
        <pre className="mt-2 whitespace-pre-wrap">
          {error.message}
        </pre>
      </details>
    </div>
  );
};

export default NodeModuleFallback;