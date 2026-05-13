// src/pages/canvas.modals.tsx
import React, { Suspense } from 'react';
import { motion } from 'framer-motion';

// Import all editor components
import MapEditor from '../components/Editor/Mapping/MapEditor';
import { SortEditor } from '../components/Editor/JoinsAndLookups/SortEditor';
import ReplaceEditor from '../components/Editor/Mapping/ReplaceEditor';
import JoinEditor from '../components/Editor/JoinsAndLookups/JoinEditor';
import { FilterRowConfigModal } from '../components/Editor/JoinsAndLookups/FilterRowConfigModal';
import ExtractXMLFieldEditor from '../components/Editor/Parsing/ExtractXMLFieldEditor';
import { ExtractJSONFieldsEditor } from '../components/Editor/Parsing/ExtractJSONFieldsEditor';
import { ExtractDelimitedFieldsConfigModal } from '../components/Editor/Parsing/ExtractDelimitedFieldsConfigModal';
import { AggregateEditor } from '../components/Editor/Aggregates/AggregateEditor';
import { ConvertTypeEditor } from '../components/Editor/Mapping/ConvertTypeEditor';
import ReplaceListEditor from '../components/Editor/Mapping/ReplaceListEditor';
import NormalizeNumberEditor from '../components/Editor/Mapping/NormalizeNumberEditor';
import NormalizeEditor from '../components/Editor/Mapping/NormalizeEditor';
import ReplicateEditor from '../components/Editor/JoinsAndLookups/ReplicateEditor';
import { RecordMatchingEditor } from '../components/Editor/JoinsAndLookups/RecordMatchingEditor';
import { MatchGroupEditor } from '../components/Editor/JoinsAndLookups/MatchGroupEditor';
import FilterColumnsEditor from '../components/Editor/JoinsAndLookups/FilterColumnsEditor';
import { FileLookupEditor } from '../components/Editor/JoinsAndLookups/FileLookupEditor';
import UnpivotRowEditor from '../components/Editor/Aggregates/UnpivotRowEditor';
import { UniteEditor } from '../components/Editor/Aggregates/UniteEditor';
import UniqRowEditor from '../components/Editor/Aggregates/UniqRowEditor';
import SplitRowEditor from '../components/Editor/Aggregates/SplitRowEditor';
import { PivotToColumnsDelimitedEditor } from '../components/Editor/Aggregates/PivotToColumnsDelimitedEditor';
import { DenormalizeSortedRowEditor } from '../components/Editor/Aggregates/DenormalizeSortedRowEditor';
import { DenormalizeEditor } from '../components/Editor/Aggregates/DenormalizeEditor';
import { ExtractRegexFieldsEditor } from '../components/Editor/Parsing/ExtractRegexFieldsEditor';
import { ParseRecordSetEditor } from '../components/Editor/Parsing/ParseRecordSetEditor';
import SampleRowEditor from '../components/Editor/Aggregates/SampleRowEditor';
import { SchemaComplianceCheckEditor } from '../components/Editor/Parsing/SchemaComplianceCheckEditor';
import AddCRCRowEditor from '../components/Editor/Parsing/AddCRCRowEditor';
import DataMaskingEditor from '../components/Editor/Parsing/DataMaskingEditor';

import RoleSelectionModal from './RoleSelectionPopup';
import {
  ActiveEditor,
  ConnectionFeedback as ConnectionFeedbackType,
  PendingRoleSelection,
  CanvasNodeData,
  MapEditorPayload,
  SimpleColumn,
} from './canvas.types';

// ----------------------------------------------------------------------
// Connection feedback popup
// ----------------------------------------------------------------------
interface ConnectionFeedbackProps {
  feedback: ConnectionFeedbackType;
}

export const ConnectionFeedback: React.FC<ConnectionFeedbackProps> = ({ feedback }) => {
  if (!feedback.isVisible) return null;
  const { message, type, position } = feedback;
  const colors = {
    success: 'bg-green-100 border-green-400 text-green-700',
    error: 'bg-red-100 border-red-400 text-red-700',
    info: 'bg-blue-100 border-blue-400 text-blue-700',
    warning: 'bg-yellow-100 border-yellow-400 text-yellow-700'
  };
  return (
    <div className="fixed z-[10000]" style={{ left: position.x, top: position.y }}>
      <div className={`px-3 py-2 rounded-lg border ${colors[type]} shadow-lg animate-fadeIn`}>
        <div className="text-sm font-medium">{message}</div>
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Auto-save status indicator
// ----------------------------------------------------------------------
interface AutoSaveStatusProps {
  status: 'idle' | 'saving' | 'saved' | 'error';
  lastSavedAt?: string;
  enabled: boolean;
}

export const AutoSaveStatus: React.FC<AutoSaveStatusProps> = ({ status, lastSavedAt, enabled }) => {
  if (!enabled || status === 'idle') return null;
  const statusConfig = {
    saving: { text: 'Saving...', color: 'bg-yellow-400', icon: '⏳' },
    saved: { text: 'Saved', color: 'bg-green-400', icon: '✅' },
    error: { text: 'Save failed', color: 'bg-red-400', icon: '❌' }
  };
  const config = statusConfig[status];
  return (
    <div className="fixed bottom-4 right-4 z-50 flex items-center space-x-2">
      <div className={`${config.color} text-white px-3 py-2 rounded-lg shadow-lg flex items-center space-x-2`}>
        <span>{config.icon}</span>
        <span className="text-sm font-medium">{config.text}</span>
        {lastSavedAt && status === 'saved' && (
          <span className="text-xs opacity-80">
            {new Date(lastSavedAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </span>
        )}
      </div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Role selection popup wrapper
// ----------------------------------------------------------------------
interface RoleSelectionPopupProps {
  pending: PendingRoleSelection | null;
  onSelect: (role: 'INPUT' | 'OUTPUT') => void;
  onCancel: () => void;
}

export const RoleSelectionPopup: React.FC<RoleSelectionPopupProps> = ({ pending, onSelect, onCancel }) => {
  if (!pending) return null;
  return (
    <RoleSelectionModal
      componentType={pending.componentId}
      displayName={pending.displayName}
      position={pending.dropPosition}
      onSelect={onSelect}
      onCancel={onCancel}
    />
  );
};

// ----------------------------------------------------------------------
// Map Editor Modal (old version)
// ----------------------------------------------------------------------
interface MapEditorModalProps {
  isOpen: boolean;
  selectedNode: any;
  onClose: () => void;
  onNodeUpdate: (nodeId: string, updates: Partial<CanvasNodeData>) => void;
}

export const MapEditorModal: React.FC<MapEditorModalProps> = ({ isOpen, selectedNode, onClose }) => {
  if (!isOpen || !selectedNode) return null;
  const sourceTables = selectedNode.sourceTables || [];
  const targetTables = selectedNode.targetTables || [];
  return (
    <div className="map-editor-modal fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: -20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 20 }}
        transition={{ duration: 0.2 }}
        className="map-editor-content relative w-full h-full max-w-[95vw] max-h-[95vh] bg-white rounded-xl shadow-2xl overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-4 right-4 z-50 bg-red-500 hover:bg-red-600 text-white rounded-full w-8 h-8 flex items-center justify-center shadow-lg transition-all"
          title="Close Map Editor"
          aria-label="Close Map Editor"
        >
          ✕
        </button>
        <Suspense fallback={<div>Loading Map Editor...</div>}>
          <MapEditor
            sourceTables={sourceTables}
            targetTables={targetTables}
            initialConfig={selectedNode.metadata?.mapConfiguration}
            onClose={onClose}
          />
        </Suspense>
      </motion.div>
    </div>
  );
};

// ----------------------------------------------------------------------
// tMap Editor Modal (new, with metadata)
// ----------------------------------------------------------------------
interface TMapEditorModalProps {
  isOpen: boolean;
  data: MapEditorPayload | null;
  nodeMetadata?: CanvasNodeData;
  onClose: () => void;
  onSave: (config: any) => void;
}

export const TMapEditorModal: React.FC<TMapEditorModalProps> = ({
  isOpen,
  data,
  nodeMetadata,
  onClose,
  onSave
}) => {
  if (!isOpen || !data) return null;
  const { nodeId, inputColumns, outputColumns } = data;
  const nodeLabel = nodeMetadata?.name || nodeId;
  const initialConfig = nodeMetadata && nodeMetadata.metadata?.configuration?.type === 'MAP'
    ? nodeMetadata.metadata.configuration.config
    : undefined;

  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-6xl h-[90vh] overflow-hidden"
      >
        <div className="flex justify-between items-center p-6 border-b bg-gradient-to-r from-blue-50 to-gray-50">
          <div>
            <h2 className="text-xl font-bold flex items-center">
              <span className="mr-2">🗺️</span>
              Map Editor
              <span className="ml-2 text-xs bg-purple-100 text-purple-800 px-2 py-0.5 rounded">
                Metadata Strategy v1.0
              </span>
            </h2>
            <p className="text-sm text-gray-600 mt-1">
              Editing tMap node: <span className="font-semibold text-blue-600">{nodeLabel}</span>
              <span className="ml-3 text-xs bg-gray-100 text-gray-800 px-2 py-0.5 rounded">
                {inputColumns.length} input columns • {outputColumns.length} output columns
              </span>
              {initialConfig && (
                <span className="ml-3 text-xs bg-green-100 text-green-800 px-2 py-0.5 rounded">
                  {initialConfig.transformations.length} existing transformations
                </span>
              )}
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded-full transition-colors"
            title="Close Map Editor"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div className="h-full overflow-hidden">
          <Suspense fallback={<div>Loading Map Editor...</div>}>
            <MapEditor
              nodeId={nodeId}
              nodeMetadata={nodeMetadata}
              inputColumns={inputColumns}
              outputColumns={outputColumns}
              initialConfig={initialConfig}
              onClose={onClose}
              onSave={onSave}
            />
          </Suspense>
        </div>
      </motion.div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Match Group Wizard (placeholder)
// ----------------------------------------------------------------------
interface MatchGroupWizardProps {
  isOpen: boolean;
  selectedNode: any;
  onClose: () => void;
  onNodeUpdate: (nodeId: string, updates: Partial<CanvasNodeData>) => void;
}

export const MatchGroupWizard: React.FC<MatchGroupWizardProps> = ({
  isOpen,
  selectedNode,
  onClose,
  onNodeUpdate
}) => {
  if (!isOpen || !selectedNode) return null;
  return (
    <div className="fixed inset-0 z-[9999] bg-black bg-opacity-70 flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white rounded-xl shadow-2xl w-full max-w-4xl h-[80vh]"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="p-6">
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-xl font-bold">Match Group Configuration Wizard</h2>
            <button onClick={onClose} className="text-gray-500 hover:text-gray-700">✕</button>
          </div>
          <p className="text-gray-600 mb-4">
            Configure match group settings for: {selectedNode.name}
          </p>
          <div className="text-center py-8">
            <p className="text-gray-500">Match Group Wizard UI would be implemented here</p>
            <button
              onClick={() => {
                const config = {
                  currentStep: 1,
                  inputFlow: 'default',
                  schemaColumns: [],
                  groupingKeys: [],
                  survivorshipRules: [],
                  outputMapping: {},
                  outputTableName: 'matched_output'
                };
                const existingMetadata = selectedNode.metadata || {};
                onNodeUpdate(selectedNode.id, {
                  metadata: {
                    ...existingMetadata,
                    matchGroupConfig: config,
                    lastConfigured: new Date().toISOString()
                  }
                });
                onClose();
              }}
              className="mt-4 px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-700"
            >
              Save Configuration
            </button>
          </div>
        </div>
      </motion.div>
    </div>
  );
};

// ----------------------------------------------------------------------
// Active Editor Renderer
// ----------------------------------------------------------------------
interface ActiveEditorRendererProps {
  editor: ActiveEditor;
  onClose: () => void;
  onSaveSort: (config: any) => void;
  onSaveReplace: (config: any) => void;
  onSaveJoin: (config: any) => void;
  onSaveFilter: (config: any) => void;
  onSaveExtractXML: (config: any) => void;
  onSaveExtractJSON: (config: any) => void;
  onSaveExtractDelimited: (config: any) => void;
  onSaveConvert: (config: any) => void;
  onSaveAggregate: (config: any) => void;
  onSaveReplaceList: (config: any) => void;
  onSaveNormalizeNumber: (config: any) => void;
  onSaveNormalize: (config: any) => void;
  onSaveReplicate: (config: any) => void;
  onSaveRecordMatching: (config: any) => void;
  onSaveMatchGroup: (config: any) => void;
  onSaveFilterColumns: (config: any) => void;
  onSaveFileLookup: (config: any) => void;
  onSaveUnpivotRow: (config: any) => void;
  onSaveUnite: (config: any) => void;
  onSaveUniqRow: (config: any) => void;
  onSaveSplitRow: (config: any) => void;
  onSavePivotToColumnsDelimited: (config: any) => void;
  onSaveDenormalizeSortedRow: (config: any) => void;
  onSaveDenormalize: (config: any) => void;
  // New callbacks for added editors
  onSaveExtractRegex: (config: any) => void;
  onSaveParseRecordSet: (config: any) => void;
  onSaveSampleRow: (config: any) => void;
  onSaveSchemaComplianceCheck: (config: any) => void;
  onSaveAddCRCRow: (config: any) => void;
  onSaveDataMasking: (config: any) => void;
}

const normalizeColumns = (columns: SimpleColumn[]): Array<{ name: string; type: string; id?: string }> =>
  columns.map(col => ({
    ...col,
    type: col.type || 'STRING',
  }));
  
export const ActiveEditorRenderer: React.FC<ActiveEditorRendererProps> = ({
  editor,
  onClose,
  onSaveSort,
  onSaveReplace,
  onSaveJoin,
  onSaveFilter,
  onSaveExtractXML,
  onSaveExtractJSON,
  onSaveExtractDelimited,
  onSaveConvert,
  onSaveAggregate,
  onSaveReplaceList,
  onSaveNormalizeNumber,
  onSaveNormalize,
  onSaveReplicate,
  onSaveRecordMatching,
  onSaveMatchGroup,
  onSaveFilterColumns,
  onSaveFileLookup,
  onSaveUnpivotRow,
  onSaveUnite,
  onSaveUniqRow,
  onSaveSplitRow,
  onSavePivotToColumnsDelimited,
  onSaveDenormalizeSortedRow,
  onSaveDenormalize,
  onSaveExtractRegex,
  onSaveParseRecordSet,
  onSaveSampleRow,
  onSaveSchemaComplianceCheck,
  onSaveAddCRCRow,
  onSaveDataMasking,
}) => {
  if (!editor) return null;

  switch (editor.type) {
case 'sort':
  return (
    <SortEditor
      nodeId={editor.nodeId}
      nodeMetadata={editor.nodeMetadata}
      inputColumns={normalizeColumns(editor.inputColumns)}
      initialConfig={editor.initialConfig}
      onClose={onClose}
      onSave={onSaveSort}
    />
  );
    case 'replace':
      return (
        <ReplaceEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          outputColumns={editor.outputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveReplace}
        />
      );
    case 'join':
      return (
        <JoinEditor
          nodeId={editor.nodeId}
          nodeName={editor.nodeMetadata.name}
          leftSchema={editor.leftSchema}
          rightSchema={editor.rightSchema}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveJoin}
        />
      );
case 'filter':
  return (
    <FilterRowConfigModal
      isOpen={true}
      onClose={onClose}
      nodeId={editor.nodeId}
      nodeName={editor.nodeMetadata.name}
      inputColumns={normalizeColumns(editor.inputColumns)}
      initialConfig={editor.initialConfig}
      onSave={onSaveFilter}
    />
  );
case 'extractXML':
  return (
    <ExtractXMLFieldEditor
      nodeId={editor.nodeId}
      nodeName={editor.nodeMetadata.name}
      inputColumns={normalizeColumns(editor.inputColumns)}
      initialConfig={editor.initialConfig}
      onClose={onClose}
      onSave={onSaveExtractXML}
    />
  );
    case 'extractJSON':
      return (
        <ExtractJSONFieldsEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveExtractJSON}
        />
      );
case 'extractDelimited':
  return (
    <ExtractDelimitedFieldsConfigModal
      isOpen={true}
      onClose={onClose}
      nodeId={editor.nodeId}
      nodeName={editor.nodeMetadata.name}
      inputColumns={normalizeColumns(editor.inputColumns)}
      initialConfig={editor.initialConfig}
      onSave={onSaveExtractDelimited}
    />
  );
case 'convert':
  return (
    <ConvertTypeEditor
      nodeId={editor.nodeId}
      nodeMetadata={editor.nodeMetadata}
      inputColumns={normalizeColumns(editor.inputColumns)}
      outputColumns={normalizeColumns(editor.outputColumns)}
      initialConfig={editor.initialConfig}
      onClose={onClose}
      onSave={onSaveConvert}
    />
  );
    case 'aggregate':
      return (
        <AggregateEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveAggregate}
        />
      );
    case 'replaceList':
      return (
        <ReplaceListEditor
          nodeId={editor.nodeId}
          initialConfig={editor.initialConfig}
          inputSchema={editor.inputSchema}
          onSave={onSaveReplaceList}
          onClose={onClose}
        />
      );
    case 'normalizeNumber':
      return (
        <NormalizeNumberEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveNormalizeNumber}
        />
      );
    case 'normalize':
      return (
        <NormalizeEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveNormalize}
        />
      );
    case 'replicate':
      return (
        <ReplicateEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveReplicate}
        />
      );
case 'recordMatching':
  return (
    <RecordMatchingEditor
      nodeId={editor.nodeId}
      inputFields={editor.inputFields}
      initialConfig={editor.initialConfig}
      onClose={onClose}
      onSave={onSaveRecordMatching}
    />
  );
    case 'matchGroup':
      return (
        <MatchGroupEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveMatchGroup}
        />
      );
    case 'filterColumns':
      return (
        <FilterColumnsEditor
          isOpen={true}
          onClose={onClose}
          onSave={onSaveFilterColumns}
          nodeId={editor.nodeId}
          initialConfig={editor.initialConfig}
          inputSchema={editor.inputSchema}
        />
      );
    case 'fileLookup':
      return (
        <FileLookupEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveFileLookup}
        />
      );
    case 'unpivotRow':
      return (
        <UnpivotRowEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveUnpivotRow}
        />
      );
    case 'unite':
      return (
        <UniteEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputSchemas={editor.inputSchemas}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveUnite}
        />
      );
    case 'uniqRow':
      return (
        <UniqRowEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveUniqRow}
        />
      );
    case 'splitRow':
      return (
        <SplitRowEditor
          nodeId={editor.nodeId}
          nodeName={editor.nodeMetadata.name}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveSplitRow}
        />
      );
    case 'pivotToColumnsDelimited':
      return (
        <PivotToColumnsDelimitedEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSavePivotToColumnsDelimited}
        />
      );
    case 'denormalizeSortedRow':
      return (
        <DenormalizeSortedRowEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveDenormalizeSortedRow}
        />
      );
    case 'denormalize':
      return (
        <DenormalizeEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveDenormalize}
        />
      );
    case 'extractRegex':
      return (
        <ExtractRegexFieldsEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveExtractRegex}
        />
      );
    case 'parseRecordSet':
      return (
        <ParseRecordSetEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveParseRecordSet}
        />
      );
    case 'sampleRow':
      return (
        <SampleRowEditor
          nodeId={editor.nodeId}
          nodeName={editor.nodeMetadata.name}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveSampleRow}
        />
      );
    case 'schemaComplianceCheck':
      return (
        <SchemaComplianceCheckEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveSchemaComplianceCheck}
        />
      );
    case 'addCRCRow':
      return (
        <AddCRCRowEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveAddCRCRow}
        />
      );
    case 'dataMasking':
      return (
        <DataMaskingEditor
          nodeId={editor.nodeId}
          nodeMetadata={editor.nodeMetadata}
          inputColumns={editor.inputColumns}
          initialConfig={editor.initialConfig}
          onClose={onClose}
          onSave={onSaveDataMasking}
        />
      );
    default:
      return null;
  }
};