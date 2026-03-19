import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import {
  X,
  Save,
  AlertCircle,
  Info,
  Copy,
  Columns,
  Hash,
} from 'lucide-react';
import {
  ReplicateComponentConfiguration,
  SchemaDefinition,
  FieldSchema,
} from '../../../types/unified-pipeline.types';

interface ReplicateEditorProps {
  nodeId: string;
  nodeMetadata: any;
  inputColumns: Array<{ name: string; type?: string; id?: string }>;
  initialConfig?: ReplicateComponentConfiguration;
  onClose: () => void;
  onSave: (config: ReplicateComponentConfiguration) => void;
}

const ReplicateEditor: React.FC<ReplicateEditorProps> = ({
  nodeId,
  nodeMetadata,
  inputColumns,
  initialConfig,
  onClose,
  onSave,
}) => {
  // State for configuration
  const [addBranchIdentifier, setAddBranchIdentifier] = useState<boolean>(
    initialConfig?.addBranchIdentifier || false
  );
  const [branchColumnName, setBranchColumnName] = useState<string>(
    initialConfig?.branchIdentifierColumnName || 'branch_id'
  );
  const [error, setError] = useState<string | null>(null);

  // Reset state when initialConfig changes
  useEffect(() => {
    if (initialConfig) {
      setAddBranchIdentifier(initialConfig.addBranchIdentifier || false);
      setBranchColumnName(initialConfig.branchIdentifierColumnName || 'branch_id');
    }
  }, [initialConfig]);

  // Build output schema based on input columns and branch identifier
  const buildOutputSchema = (): SchemaDefinition => {
    const fields: FieldSchema[] = inputColumns.map((col, idx) => ({
      id: col.id || `${nodeId}_in_${idx}`,
      name: col.name,
      type: (col.type as any) || 'STRING',
      nullable: true,
      isKey: false,
      description: `Input column from source`,
    }));

    if (addBranchIdentifier) {
      fields.push({
        id: `${nodeId}_branch_id`,
        name: branchColumnName,
        type: 'STRING',
        nullable: false,
        isKey: false,
        description: 'Identifier of the replication branch',
      });
    }

    return {
      id: `${nodeId}_output_schema`,
      name: `${nodeMetadata?.name || 'Replicate'} Output Schema`,
      fields,
      isTemporary: false,
      isMaterialized: false,
      metadata: {},
    };
  };

  // Validation before saving
  const validate = (): boolean => {
    if (addBranchIdentifier && !branchColumnName.trim()) {
      setError('Branch identifier column name cannot be empty');
      return false;
    }
    if (addBranchIdentifier && inputColumns.some((col) => col.name === branchColumnName)) {
      setError(`Column "${branchColumnName}" already exists in input. Choose a different name.`);
      return false;
    }
    setError(null);
    return true;
  };

  // Handle save
  const handleSave = () => {
    if (!validate()) return;

    const config: ReplicateComponentConfiguration = {
      version: '1.0',
      addBranchIdentifier,
      branchIdentifierColumnName: addBranchIdentifier ? branchColumnName : undefined,
      outputSchema: buildOutputSchema(),
      sqlGeneration: {
        passthrough: !addBranchIdentifier,
        estimatedRowMultiplier: addBranchIdentifier ? 1.0 : 1.0, // no change in row count
      },
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: 'canvas',
        validationStatus: 'VALID',
        warnings: [],
      },
    };

    onSave(config);
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-4xl max-h-[90vh] overflow-hidden flex flex-col"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b bg-gradient-to-r from-blue-50 to-indigo-50">
          <div>
            <h2 className="text-xl font-bold flex items-center text-gray-800">
              <Copy className="w-6 h-6 mr-2 text-indigo-600" />
              tReplicate Configuration
              <span className="ml-3 text-xs bg-indigo-100 text-indigo-800 px-2 py-0.5 rounded">
                Replication
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Node: <span className="font-semibold">{nodeMetadata?.name || nodeId}</span>
              <span className="ml-3 text-xs bg-gray-100 px-2 py-0.5 rounded">
                {inputColumns.length} input columns
              </span>
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-200 rounded-full transition-colors"
            title="Close"
          >
            <X className="w-5 h-5 text-gray-600" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6 space-y-6">
          {/* Info Card */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 flex items-start">
            <Info className="w-5 h-5 text-blue-600 mr-3 flex-shrink-0 mt-0.5" />
            <div className="text-sm text-blue-800">
              <p className="font-medium">About tReplicate</p>
              <p className="mt-1">
                This component duplicates the input data stream to multiple output connections.
                All columns are passed through unchanged. You can optionally add a branch
                identifier column to distinguish which output branch processed each row.
              </p>
            </div>
          </div>

          {/* Input Schema Preview */}
          <div className="border rounded-lg overflow-hidden">
            <div className="bg-gray-50 px-4 py-2 border-b flex items-center justify-between">
              <div className="flex items-center">
                <Columns className="w-4 h-4 mr-2 text-gray-600" />
                <span className="font-medium text-gray-700">Input Schema</span>
              </div>
              <span className="text-xs text-gray-500">{inputColumns.length} columns</span>
            </div>
            <div className="max-h-60 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-100 sticky top-0">
                  <tr>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-600 uppercase">#</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-600 uppercase">Column Name</th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-600 uppercase">Data Type</th>
                  </tr>
                </thead>
                <tbody>
                  {inputColumns.map((col, idx) => (
                    <tr key={col.id || idx} className="border-t hover:bg-gray-50">
                      <td className="px-4 py-2 text-gray-500">{idx + 1}</td>
                      <td className="px-4 py-2 font-mono text-gray-800">{col.name}</td>
                      <td className="px-4 py-2">
                        <span className="bg-blue-100 text-blue-800 px-2 py-0.5 rounded text-xs">
                          {col.type || 'STRING'}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Branch Identifier Options */}
          <div className="border rounded-lg p-4">
            <div className="flex items-center mb-4">
              <input
                type="checkbox"
                id="addBranchId"
                checked={addBranchIdentifier}
                onChange={(e) => setAddBranchIdentifier(e.target.checked)}
                className="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded"
              />
              <label htmlFor="addBranchId" className="ml-2 block text-sm font-medium text-gray-700">
                Add branch identifier column
              </label>
            </div>

            {addBranchIdentifier && (
              <motion.div
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 1, height: 'auto' }}
                className="ml-6 space-y-3"
              >
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Column Name
                  </label>
                  <div className="flex items-center">
                    <Hash className="w-4 h-4 text-gray-400 mr-2" />
                    <input
                      type="text"
                      value={branchColumnName}
                      onChange={(e) => setBranchColumnName(e.target.value)}
                      className="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 text-sm"
                      placeholder="e.g., branch_id"
                    />
                  </div>
                  <p className="text-xs text-gray-500 mt-1">
                    This column will contain a value identifying the output branch (e.g., 'branch_1', 'branch_2').
                  </p>
                </div>
              </motion.div>
            )}
          </div>

          {/* Error Display */}
          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 flex items-start">
              <AlertCircle className="w-4 h-4 text-red-600 mr-2 flex-shrink-0 mt-0.5" />
              <span className="text-sm text-red-700">{error}</span>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 px-6 py-4 border-t bg-gray-50">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm border border-gray-300 rounded-lg hover:bg-gray-100 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            className="px-4 py-2 text-sm bg-gradient-to-r from-indigo-600 to-purple-600 text-white rounded-lg hover:from-indigo-700 hover:to-purple-700 transition-all flex items-center"
          >
            <Save className="w-4 h-4 mr-2" />
            Save Configuration
          </button>
        </div>
      </motion.div>
    </div>
  );
};

export default ReplicateEditor;