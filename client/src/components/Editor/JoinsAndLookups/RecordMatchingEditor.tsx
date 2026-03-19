// src/components/Editor/RecordMatchingEditor.tsx
import React, { useState, useReducer, useCallback, useEffect } from "react";
import {
  X,
  Save,
  Plus,
  Trash2,
  GripVertical,
  AlertCircle,
} from "lucide-react";
import { Button } from "../../ui/Button";
import { Input } from "../../ui/input";
import { Label } from "../../ui/label";
import { Checkbox } from "../../ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../../ui/select";
import { Slider } from "../../ui/slider";
import { Badge } from "../../ui/badge";
import {
  MatchGroupComponentConfiguration,
  MatchType,
  SurvivorshipRuleType,
  FieldSchema,
  MatchKey,
  SurvivorshipRule,
} from "../../../types/unified-pipeline.types";

// ----------------------------------------------------------------------
// Types & Constants
// ----------------------------------------------------------------------

export interface RecordMatchingEditorProps {
  /** Input schema fields (from incoming connection) */
  inputFields: FieldSchema[];
  /** Optional existing configuration to load */
  initialConfig?: MatchGroupComponentConfiguration;
  /** Callback when user saves */
  onSave: (config: MatchGroupComponentConfiguration) => void;
  /** Callback when user cancels */
  onClose: () => void;
  /** Optional node ID for display */
  nodeId?: string;
}

// Default empty configuration
const DEFAULT_CONFIG: MatchGroupComponentConfiguration = {
  version: "1.0",
  matchKeys: [],
  survivorshipRules: [],
  outputFields: [],
  globalOptions: {
    matchThreshold: 0.8,
    maxMatchesPerRecord: 1,
    nullHandling: "no_match",
    outputMode: "best_match",
    includeMatchDetails: false,
    parallelization: false,
    batchSize: 10000,
  },
  sqlGeneration: {},
  compilerMetadata: {
    lastModified: new Date().toISOString(),
    createdBy: "user",
    matchKeyCount: 0,
    ruleCount: 0,
    validationStatus: "WARNING",
    warnings: [],
    dependencies: [],
  },
};

// ----------------------------------------------------------------------
// Reducer for managing editor state
// ----------------------------------------------------------------------

type EditorAction =
  | { type: "SET_INPUT_FIELDS"; fields: FieldSchema[] }
  | { type: "ADD_MATCH_KEY"; key?: Partial<MatchKey> }
  | { type: "UPDATE_MATCH_KEY"; id: string; updates: Partial<MatchKey> }
  | { type: "REMOVE_MATCH_KEY"; id: string }
  | { type: "REORDER_MATCH_KEYS"; sourceIndex: number; targetIndex: number }
  | { type: "ADD_SURVIVORSHIP_RULE"; rule?: Partial<SurvivorshipRule> }
  | { type: "UPDATE_SURVIVORSHIP_RULE"; id: string; updates: Partial<SurvivorshipRule> }
  | { type: "REMOVE_SURVIVORSHIP_RULE"; id: string }
  | { type: "SET_OUTPUT_FIELDS"; fields: string[] }
  | { type: "TOGGLE_OUTPUT_FIELD"; fieldName: string; checked: boolean }
  | { type: "UPDATE_GLOBAL_OPTION"; key: keyof MatchGroupComponentConfiguration["globalOptions"]; value: any }
  | { type: "SET_VALIDATION"; status: "VALID" | "WARNING" | "ERROR"; warnings: string[] }
  | { type: "LOAD_CONFIG"; config: MatchGroupComponentConfiguration };

interface EditorState {
  inputFields: FieldSchema[];
  matchKeys: MatchKey[];
  survivorshipRules: SurvivorshipRule[];
  outputFields: string[]; // list of output field names
  globalOptions: MatchGroupComponentConfiguration["globalOptions"];
  validationStatus: "VALID" | "WARNING" | "ERROR";
  warnings: string[];
}

const generateId = () => `rule-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;

const editorReducer = (state: EditorState, action: EditorAction): EditorState => {
  switch (action.type) {
    case "SET_INPUT_FIELDS":
      return { ...state, inputFields: action.fields };

    case "ADD_MATCH_KEY": {
      const newKey: MatchKey = {
        id: generateId(),
        field: "",
        matchType: MatchType.EXACT,
        threshold: 0.8,
        caseSensitive: false,
        ignoreNull: true,
        weight: 1.0,
        blockingKey: false,
        ...action.key,
      };
      return { ...state, matchKeys: [...state.matchKeys, newKey] };
    }

    case "UPDATE_MATCH_KEY":
      return {
        ...state,
        matchKeys: state.matchKeys.map((k) =>
          k.id === action.id ? { ...k, ...action.updates } : k
        ),
      };

    case "REMOVE_MATCH_KEY":
      return {
        ...state,
        matchKeys: state.matchKeys.filter((k) => k.id !== action.id),
      };

    case "REORDER_MATCH_KEYS": {
      const newKeys = [...state.matchKeys];
      const [removed] = newKeys.splice(action.sourceIndex, 1);
      newKeys.splice(action.targetIndex, 0, removed);
      return { ...state, matchKeys: newKeys };
    }

    case "ADD_SURVIVORSHIP_RULE": {
      const newRule: SurvivorshipRule = {
        id: generateId(),
        field: "",
        ruleType: SurvivorshipRuleType.FIRST,
        params: {},
        sourceField: "",
        ...action.rule,
      };
      return { ...state, survivorshipRules: [...state.survivorshipRules, newRule] };
    }

    case "UPDATE_SURVIVORSHIP_RULE":
      return {
        ...state,
        survivorshipRules: state.survivorshipRules.map((r) =>
          r.id === action.id ? { ...r, ...action.updates } : r
        ),
      };

    case "REMOVE_SURVIVORSHIP_RULE":
      return {
        ...state,
        survivorshipRules: state.survivorshipRules.filter((r) => r.id !== action.id),
      };

    case "SET_OUTPUT_FIELDS":
      return { ...state, outputFields: action.fields };

    case "TOGGLE_OUTPUT_FIELD": {
      const { fieldName, checked } = action;
      const newFields = checked
        ? [...state.outputFields, fieldName]
        : state.outputFields.filter((f) => f !== fieldName);
      return { ...state, outputFields: newFields };
    }

    case "UPDATE_GLOBAL_OPTION":
      return {
        ...state,
        globalOptions: { ...state.globalOptions, [action.key]: action.value },
      };

    case "SET_VALIDATION":
      return {
        ...state,
        validationStatus: action.status,
        warnings: action.warnings,
      };

    case "LOAD_CONFIG": {
      const config = action.config;
      return {
        inputFields: state.inputFields, // keep existing input fields
        matchKeys: config.matchKeys || [],
        survivorshipRules: config.survivorshipRules || [],
        outputFields: config.outputFields || [],
        globalOptions: {
          matchThreshold: config.globalOptions?.matchThreshold ?? DEFAULT_CONFIG.globalOptions.matchThreshold,
          maxMatchesPerRecord: config.globalOptions?.maxMatchesPerRecord ?? DEFAULT_CONFIG.globalOptions.maxMatchesPerRecord,
          nullHandling: config.globalOptions?.nullHandling ?? DEFAULT_CONFIG.globalOptions.nullHandling,
          outputMode: config.globalOptions?.outputMode ?? DEFAULT_CONFIG.globalOptions.outputMode,
          includeMatchDetails: config.globalOptions?.includeMatchDetails ?? DEFAULT_CONFIG.globalOptions.includeMatchDetails,
          parallelization: config.globalOptions?.parallelization ?? DEFAULT_CONFIG.globalOptions.parallelization,
          batchSize: config.globalOptions?.batchSize ?? DEFAULT_CONFIG.globalOptions.batchSize,
        },
        validationStatus: config.compilerMetadata?.validationStatus || "WARNING",
        warnings: config.compilerMetadata?.warnings || [],
      };
    }

    default:
      return state;
  }
};

// ----------------------------------------------------------------------
// Helper Components
// ----------------------------------------------------------------------

interface MatchKeyRowProps {
  keyDef: MatchKey;
  inputFields: FieldSchema[];
  onUpdate: (updates: Partial<MatchKey>) => void;
  onRemove: () => void;
}

const MatchKeyRow: React.FC<MatchKeyRowProps> = ({
  keyDef,
  inputFields,
  onUpdate,
  onRemove,
}) => {
  const fieldOptions = inputFields.map((f) => (
    <SelectItem key={f.name} value={f.name}>
      {f.name} ({f.type})
    </SelectItem>
  ));

  return (
    <div className="flex items-center gap-2 p-2 border rounded hover:bg-gray-50 group">
      <GripVertical className="h-4 w-4 text-gray-400 cursor-move" />
      <Select
        value={keyDef.field}
        onValueChange={(val) => onUpdate({ field: val })}
      >
        <SelectTrigger className="w-40">
          <SelectValue placeholder="Select field" />
        </SelectTrigger>
        <SelectContent>{fieldOptions}</SelectContent>
      </Select>

      <Select
        value={keyDef.matchType}
        onValueChange={(val: MatchType) => onUpdate({ matchType: val })}
      >
        <SelectTrigger className="w-36">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {Object.values(MatchType).map((type) => (
            <SelectItem key={type} value={type}>
              {type.replace(/_/g, " ")}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {keyDef.matchType !== MatchType.EXACT &&
        keyDef.matchType !== MatchType.EXACT_IGNORE_CASE && (
          <div className="w-32 flex items-center gap-1">
            <Slider
              value={[keyDef.threshold ?? 0.8]}
              min={0}
              max={1}
              step={0.05}
              onValueChange={([val]) => onUpdate({ threshold: val })}
              className="flex-1"
            />
            <span className="text-xs w-10">{(keyDef.threshold ?? 0.8).toFixed(2)}</span>
          </div>
        )}

      <Checkbox
        checked={keyDef.caseSensitive}
        onChange={(e) => onUpdate({ caseSensitive: e.target.checked })}
        title="Case sensitive"
      />
      <Checkbox
        checked={keyDef.ignoreNull}
        onChange={(e) => onUpdate({ ignoreNull: e.target.checked })}
        title="Ignore null"
      />
      <Input
        type="number"
        value={keyDef.weight ?? 1.0}
        onChange={(e) => onUpdate({ weight: parseFloat(e.target.value) || 1.0 })}
        className="w-16 h-8"
        min={0}
        step={0.1}
        title="Weight"
      />
      <Checkbox
        checked={keyDef.blockingKey}
        onChange={(e) => onUpdate({ blockingKey: e.target.checked })}
        title="Blocking key"
      />
      <Button variant="ghost" size="sm" onClick={onRemove} className="opacity-0 group-hover:opacity-100">
        <Trash2 className="h-4 w-4 text-red-500" />
      </Button>
    </div>
  );
};

interface SurvivorshipRuleRowProps {
  rule: SurvivorshipRule;
  inputFields: FieldSchema[];
  outputFields: string[];
  onUpdate: (updates: Partial<SurvivorshipRule>) => void;
  onRemove: () => void;
}

const SurvivorshipRuleRow: React.FC<SurvivorshipRuleRowProps> = ({
  rule,
  inputFields,
  outputFields,
  onUpdate,
  onRemove,
}) => {
  const fieldOptions = inputFields.map((f) => (
    <SelectItem key={f.name} value={f.name}>
      {f.name}
    </SelectItem>
  ));

  const outputFieldOptions = outputFields.map((name) => (
    <SelectItem key={name} value={name}>
      {name}
    </SelectItem>
  ));

  if (outputFields.length === 0) {
    return (
      <div className="p-2 border rounded bg-yellow-50 text-yellow-800 text-sm">
        No output fields defined. Please select output fields first.
      </div>
    );
  }

  return (
    <div className="flex items-center gap-2 p-2 border rounded hover:bg-gray-50 group">
      <GripVertical className="h-4 w-4 text-gray-400 cursor-move" />
      <Select
        value={rule.field}
        onValueChange={(val) => onUpdate({ field: val })}
      >
        <SelectTrigger className="w-40">
          <SelectValue placeholder="Output field" />
        </SelectTrigger>
        <SelectContent>{outputFieldOptions}</SelectContent>
      </Select>

      <Select
        value={rule.ruleType}
        onValueChange={(val: SurvivorshipRuleType) => onUpdate({ ruleType: val })}
      >
        <SelectTrigger className="w-32">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {Object.values(SurvivorshipRuleType).map((type) => (
            <SelectItem key={type} value={type}>
              {type}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={rule.sourceField || rule.field}
        onValueChange={(val) => onUpdate({ sourceField: val })}
      >
        <SelectTrigger className="w-40">
          <SelectValue placeholder="Source field" />
        </SelectTrigger>
        <SelectContent>{fieldOptions}</SelectContent>
      </Select>

      {rule.ruleType === SurvivorshipRuleType.CONCAT && (
        <Input
          placeholder="Separator"
          value={rule.params?.separator || ""}
          onChange={(e) =>
            onUpdate({ params: { ...rule.params, separator: e.target.value } })
          }
          className="w-20"
        />
      )}

      {(rule.ruleType === SurvivorshipRuleType.FIRST ||
        rule.ruleType === SurvivorshipRuleType.LAST) && (
        <>
          <Select
            value={rule.params?.orderBy || ""}
            onValueChange={(val) =>
              onUpdate({ params: { ...rule.params, orderBy: val } })
            }
          >
            <SelectTrigger className="w-32">
              <SelectValue placeholder="Order by" />
            </SelectTrigger>
            <SelectContent>
              {inputFields.map((f) => (
                <SelectItem key={f.name} value={f.name}>
                  {f.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={rule.params?.orderDirection || "ASC"}
            onValueChange={(val: "ASC" | "DESC") =>
              onUpdate({ params: { ...rule.params, orderDirection: val } })
            }
          >
            <SelectTrigger className="w-20">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ASC">ASC</SelectItem>
              <SelectItem value="DESC">DESC</SelectItem>
            </SelectContent>
          </Select>
        </>
      )}

      <Button variant="ghost" size="sm" onClick={onRemove} className="opacity-0 group-hover:opacity-100">
        <Trash2 className="h-4 w-4 text-red-500" />
      </Button>
    </div>
  );
};

// ----------------------------------------------------------------------
// Main Editor Component
// ----------------------------------------------------------------------

export const RecordMatchingEditor: React.FC<RecordMatchingEditorProps> = ({
  inputFields,
  initialConfig,
  onSave,
  onClose,
  nodeId,
}) => {
  const [state, dispatch] = useReducer(editorReducer, {
    inputFields,
    matchKeys: [],
    survivorshipRules: [],
    outputFields: [],
    globalOptions: DEFAULT_CONFIG.globalOptions,
    validationStatus: "WARNING",
    warnings: [],
  });

  const [activeTab, setActiveTab] = useState<"keys" | "rules" | "advanced">("keys");

  // Load initial config if provided
  useEffect(() => {
    if (initialConfig) {
      dispatch({ type: "LOAD_CONFIG", config: initialConfig });
    }
  }, [initialConfig]);

  // Validate configuration and update status
  useEffect(() => {
    const warnings: string[] = [];

    if (state.matchKeys.length === 0) {
      warnings.push("No matching keys defined.");
    }
    if (state.outputFields.length === 0) {
      warnings.push("No output fields selected.");
    } else {
      const fieldsWithRules = new Set(state.survivorshipRules.map((r) => r.field));
      const missing = state.outputFields.filter((f) => !fieldsWithRules.has(f));
      if (missing.length > 0) {
        warnings.push(`Output fields missing survivorship rules: ${missing.join(", ")}`);
      }
    }

    const status = warnings.length === 0 ? "VALID" : warnings.length > 2 ? "ERROR" : "WARNING";
    dispatch({ type: "SET_VALIDATION", status, warnings });
  }, [state.matchKeys, state.outputFields, state.survivorshipRules]);

  // Auto‑configure: guess match keys from common field names
  const handleAutoConfigure = useCallback(() => {
    const commonKeyNames = ["id", "ssn", "email", "phone", "customer_id", "account_id"];
    // Add match keys
    inputFields.forEach((field) => {
      if (commonKeyNames.includes(field.name.toLowerCase())) {
        dispatch({
          type: "ADD_MATCH_KEY",
          key: {
            field: field.name,
            matchType: field.name === "email" ? MatchType.EXACT_IGNORE_CASE : MatchType.EXACT,
            threshold: 0.8,
            caseSensitive: false,
            ignoreNull: true,
            weight: 1.0,
            blockingKey: field.name === "id",
          },
        });
      }
    });
    // Select all input fields as output
    dispatch({ type: "SET_OUTPUT_FIELDS", fields: inputFields.map((f) => f.name) });
    // Create default survivorship rules (FIRST) for each output field
    inputFields.forEach((field) => {
      dispatch({
        type: "ADD_SURVIVORSHIP_RULE",
        rule: {
          field: field.name,
          ruleType: SurvivorshipRuleType.FIRST,
          sourceField: field.name,
        },
      });
    });
  }, [inputFields]);

  // Build final configuration object on save
  const handleSave = useCallback(() => {
    const config: MatchGroupComponentConfiguration = {
      version: "1.0",
      matchKeys: state.matchKeys,
      survivorshipRules: state.survivorshipRules,
      outputFields: state.outputFields,
      globalOptions: {
        matchThreshold: state.globalOptions.matchThreshold ?? 0.8,
        maxMatchesPerRecord: state.globalOptions.maxMatchesPerRecord ?? 1,
        nullHandling: state.globalOptions.nullHandling ?? "no_match",
        outputMode: state.globalOptions.outputMode ?? "best_match",
        includeMatchDetails: state.globalOptions.includeMatchDetails ?? false,
        parallelization: state.globalOptions.parallelization ?? false,
        batchSize: state.globalOptions.batchSize ?? 10000,
      },
      sqlGeneration: {},
      compilerMetadata: {
        lastModified: new Date().toISOString(),
        createdBy: "user",
        matchKeyCount: state.matchKeys.length,
        ruleCount: state.survivorshipRules.length,
        validationStatus: state.validationStatus,
        warnings: state.warnings,
        dependencies: [],
      },
    };
    onSave(config);
  }, [state, onSave]);

  // Toggle output field inclusion
  const handleToggleOutputField = (fieldName: string, checked: boolean) => {
    dispatch({ type: "TOGGLE_OUTPUT_FIELD", fieldName, checked });
  };

  return (
    <div className="fixed inset-0 z-[9999] bg-gray-900/80 flex flex-col">
      {/* Toolbar */}
      <div className="flex items-center justify-between px-4 py-3 border-b bg-gradient-to-r from-blue-50 to-gray-50">
        <div className="flex items-center space-x-3">
          <div className="text-lg font-bold text-blue-700 flex items-center">
            <span className="mr-2">🔍</span>
            Record Matching Editor
            {nodeId && (
              <span className="ml-2 text-xs bg-gray-200 text-gray-700 px-2 py-0.5 rounded">
                Node: {nodeId}
              </span>
            )}
          </div>
          <Badge
            variant={
              state.validationStatus === "VALID"
                ? "success"
                : state.validationStatus === "WARNING"
                ? "warning"
                : "destructive"
            }
          >
            {state.validationStatus}
          </Badge>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={handleAutoConfigure}>
            Auto‑configure
          </Button>
          <Button variant="ghost" size="sm" onClick={onClose}>
            <X className="h-4 w-4 mr-1" />
            Cancel
          </Button>
          <Button
            size="sm"
            onClick={handleSave}
            disabled={state.validationStatus === "ERROR"}
            className="bg-green-600 hover:bg-green-700"
          >
            <Save className="h-4 w-4 mr-1" />
            Save & Compile
          </Button>
        </div>
      </div>

      {/* Main 3‑column layout */}
      <div className="flex flex-1 overflow-hidden">
        {/* Left: Input Schema */}
        <div className="w-1/4 border-r bg-white overflow-auto p-4">
          <h3 className="text-sm font-semibold mb-3 flex items-center">
            <span className="mr-2">📥</span> Input Schema
          </h3>
          <div className="space-y-2">
            {inputFields.map((field) => (
              <div key={field.name} className="flex items-center justify-between text-sm">
                <div className="flex items-center gap-2">
                  <Checkbox
                    id={`input-${field.name}`}
                    checked={state.outputFields.includes(field.name)}
                    onChange={(e) =>
                      handleToggleOutputField(field.name, e.target.checked)
                    }
                  />
                  <Label htmlFor={`input-${field.name}`} className="font-medium">
                    {field.name}
                  </Label>
                </div>
                <Badge variant="outline" className="text-xs">
                  {field.type}
                </Badge>
              </div>
            ))}
          </div>
        </div>

        {/* Middle: Output Fields (collapsed summary) */}
        <div className="w-1/4 border-r bg-white overflow-auto p-4">
          <h3 className="text-sm font-semibold mb-3 flex items-center">
            <span className="mr-2">📤</span> Output Fields
          </h3>
          <div className="space-y-4">
            {state.outputFields.map((fieldName) => {
              const rule = state.survivorshipRules.find((r) => r.field === fieldName);
              return (
                <div key={fieldName} className="border rounded p-2">
                  <div className="flex items-center justify-between">
                    <span className="font-medium">{fieldName}</span>
                    <Badge variant="outline">{rule?.ruleType || "No rule"}</Badge>
                  </div>
                  {rule && (
                    <div className="text-xs text-gray-500 mt-1">
                      {rule.sourceField && `from ${rule.sourceField}`}
                      {rule.params?.orderBy && ` ordered by ${rule.params.orderBy}`}
                    </div>
                  )}
                </div>
              );
            })}
            <Button
              variant="outline"
              size="sm"
              className="w-full"
              onClick={() => {
                // Could open a dialog, but for now rely on toggling in left panel
              }}
            >
              <Plus className="h-4 w-4 mr-1" /> Add Output Field
            </Button>
          </div>
        </div>

        {/* Right: Tabs area */}
        <div className="w-1/2 bg-gray-50 border-l flex flex-col">
          {/* Tabs */}
          <div className="flex border-b bg-white">
            <button
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === "keys"
                  ? "text-blue-600 border-b-2 border-blue-600"
                  : "text-gray-600 hover:text-gray-800"
              }`}
              onClick={() => setActiveTab("keys")}
            >
              Matching Keys
            </button>
            <button
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === "rules"
                  ? "text-blue-600 border-b-2 border-blue-600"
                  : "text-gray-600 hover:text-gray-800"
              }`}
              onClick={() => setActiveTab("rules")}
            >
              Survivorship Rules
            </button>
            <button
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === "advanced"
                  ? "text-blue-600 border-b-2 border-blue-600"
                  : "text-gray-600 hover:text-gray-800"
              }`}
              onClick={() => setActiveTab("advanced")}
            >
              Advanced
            </button>
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-auto p-4">
            {activeTab === "keys" && (
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <h4 className="text-sm font-semibold">Define Matching Keys</h4>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => dispatch({ type: "ADD_MATCH_KEY" })}
                  >
                    <Plus className="h-4 w-4 mr-1" /> Add Key
                  </Button>
                </div>
                <div className="space-y-2">
                  {state.matchKeys.map((key, _index) => (
                    <MatchKeyRow
                      key={key.id}
                      keyDef={key}
                      inputFields={inputFields}
                      onUpdate={(updates) =>
                        dispatch({ type: "UPDATE_MATCH_KEY", id: key.id, updates })
                      }
                      onRemove={() => dispatch({ type: "REMOVE_MATCH_KEY", id: key.id })}
                    />
                  ))}
                  {state.matchKeys.length === 0 && (
                    <p className="text-sm text-gray-500 italic">No matching keys defined.</p>
                  )}
                </div>
              </div>
            )}

            {activeTab === "rules" && (
              <div className="space-y-2">
                <div className="flex justify-between items-center">
                  <h4 className="text-sm font-semibold">Survivorship Rules</h4>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => dispatch({ type: "ADD_SURVIVORSHIP_RULE" })}
                  >
                    <Plus className="h-4 w-4 mr-1" /> Add Rule
                  </Button>
                </div>
                <div className="space-y-2">
                  {state.survivorshipRules.map((rule) => (
                    <SurvivorshipRuleRow
                      key={rule.id}
                      rule={rule}
                      inputFields={inputFields}
                      outputFields={state.outputFields}
                      onUpdate={(updates) =>
                        dispatch({ type: "UPDATE_SURVIVORSHIP_RULE", id: rule.id, updates })
                      }
                      onRemove={() => dispatch({ type: "REMOVE_SURVIVORSHIP_RULE", id: rule.id })}
                    />
                  ))}
                  {state.survivorshipRules.length === 0 && (
                    <p className="text-sm text-gray-500 italic">No survivorship rules defined.</p>
                  )}
                </div>
              </div>
            )}

            {activeTab === "advanced" && (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label>Global Match Threshold</Label>
                    <div className="flex items-center gap-2">
                      <Slider
                        value={[state.globalOptions.matchThreshold ?? 0.8]}
                        min={0}
                        max={1}
                        step={0.05}
                        onValueChange={([val]) =>
                          dispatch({ type: "UPDATE_GLOBAL_OPTION", key: "matchThreshold", value: val })
                        }
                        className="flex-1"
                      />
                      <span className="text-sm w-12">
                        {(state.globalOptions.matchThreshold ?? 0.8).toFixed(2)}
                      </span>
                    </div>
                  </div>
                  <div>
                    <Label>Max Matches Per Record</Label>
                    <Input
                      type="number"
                      value={state.globalOptions.maxMatchesPerRecord ?? 1}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_GLOBAL_OPTION",
                          key: "maxMatchesPerRecord",
                          value: parseInt(e.target.value) || 1,
                        })
                      }
                      min={0}
                    />
                  </div>
                </div>

                <div>
                  <Label>Null Handling</Label>
                  <Select
                    value={state.globalOptions.nullHandling ?? "no_match"}
                    onValueChange={(val: any) =>
                      dispatch({ type: "UPDATE_GLOBAL_OPTION", key: "nullHandling", value: val })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="match">Match</SelectItem>
                      <SelectItem value="no_match">No Match</SelectItem>
                      <SelectItem value="ignore">Ignore</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div>
                  <Label>Output Mode</Label>
                  <Select
                    value={state.globalOptions.outputMode ?? "best_match"}
                    onValueChange={(val: any) =>
                      dispatch({ type: "UPDATE_GLOBAL_OPTION", key: "outputMode", value: val })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all_matches">All Matches</SelectItem>
                      <SelectItem value="best_match">Best Match Only</SelectItem>
                      <SelectItem value="groups_only">Groups Only</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="includeDetails"
                      checked={state.globalOptions.includeMatchDetails ?? false}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_GLOBAL_OPTION",
                          key: "includeMatchDetails",
                          value: e.target.checked,
                        })
                      }
                    />
                    <Label htmlFor="includeDetails">Include Match Details</Label>
                  </div>
                  <div className="flex items-center gap-2">
                    <Checkbox
                      id="parallel"
                      checked={state.globalOptions.parallelization ?? false}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_GLOBAL_OPTION",
                          key: "parallelization",
                          value: e.target.checked,
                        })
                      }
                    />
                    <Label htmlFor="parallel">Parallelize</Label>
                  </div>
                </div>

                {state.globalOptions.parallelization && (
                  <div>
                    <Label>Batch Size</Label>
                    <Input
                      type="number"
                      value={state.globalOptions.batchSize ?? 10000}
                      onChange={(e) =>
                        dispatch({
                          type: "UPDATE_GLOBAL_OPTION",
                          key: "batchSize",
                          value: parseInt(e.target.value) || 10000,
                        })
                      }
                      min={1}
                    />
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Status Bar */}
      <div className="flex items-center justify-between px-4 py-2 border-t bg-gray-50 text-xs text-gray-600">
        <div className="flex items-center space-x-4">
          <div className="flex items-center">
            <span className="font-medium mr-2">Status:</span>
            <Badge
              variant={
                state.validationStatus === "VALID"
                  ? "success"
                  : state.validationStatus === "WARNING"
                  ? "warning"
                  : "destructive"
              }
            >
              {state.validationStatus}
            </Badge>
          </div>
          {state.warnings.length > 0 && (
            <div className="flex items-center text-yellow-700">
              <AlertCircle className="h-3 w-3 mr-1" />
              <span>{state.warnings[0]}</span>
              {state.warnings.length > 1 && (
                <span className="ml-1">(+{state.warnings.length - 1} more)</span>
              )}
            </div>
          )}
        </div>
        <div className="flex items-center space-x-3">
          <span>Match Keys: {state.matchKeys.length}</span>
          <span>Output Fields: {state.outputFields.length}</span>
          <span>Rules: {state.survivorshipRules.length}</span>
          <span className="text-gray-400">|</span>
          <kbd className="px-1.5 py-0.5 bg-white border rounded">Ctrl+S</kbd>
          <span>Save</span>
        </div>
      </div>
    </div>
  );
};