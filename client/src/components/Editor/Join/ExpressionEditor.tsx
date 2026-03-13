// ExpressionEditor.tsx
import React, { useState, useRef } from 'react';
import {
  Box,
  Typography,
  Button,
  Chip,
  Popover,
  List,
  ListItem,
  ListItemText,
  IconButton
} from '@mui/material';
import {
  Code as CodeIcon,
  Functions as FunctionsIcon,
  Help as HelpIcon
} from '@mui/icons-material';
import Editor, { Monaco } from '@monaco-editor/react';

interface FieldSchema {
  name: string;
  type: string;
}

interface ExpressionEditorProps {
  value: string;
  onChange: (value: string) => void;
  leftFields: FieldSchema[];
  rightFields: FieldSchema[];
}

const SQL_FUNCTIONS = [
  { name: 'CONCAT', description: 'Concatenate strings: CONCAT(str1, str2)' },
  { name: 'UPPER', description: 'Convert to uppercase: UPPER(string)' },
  { name: 'LOWER', description: 'Convert to lowercase: LOWER(string)' },
  { name: 'TRIM', description: 'Remove spaces: TRIM(string)' },
  { name: 'COALESCE', description: 'Return first non-null: COALESCE(val1, val2)' },
  { name: 'NULLIF', description: 'Return null if equal: NULLIF(a, b)' },
  { name: 'CASE', description: 'Conditional: CASE WHEN condition THEN result END' },
  { name: 'SUBSTRING', description: 'Extract substring: SUBSTRING(str, start, length)' },
  { name: 'LENGTH', description: 'String length: LENGTH(string)' },
  { name: 'ROUND', description: 'Round number: ROUND(number, decimals)' },
  { name: 'ABS', description: 'Absolute value: ABS(number)' },
  { name: 'DATE_ADD', description: 'Add to date: DATE_ADD(date, INTERVAL 1 DAY)' },
  { name: 'DATE_DIFF', description: 'Date difference: DATE_DIFF(date1, date2)' }
];

const ExpressionEditor: React.FC<ExpressionEditorProps> = ({
  value,
  onChange,
  leftFields,
  rightFields
}) => {
  const [showFunctions, setShowFunctions] = useState(false);
  const [anchorEl, setAnchorEl] = useState<HTMLElement | null>(null);
  const editorRef = useRef<any>(null);
  const monacoRef = useRef<Monaco | null>(null);

  const handleInsertField = (field: string, prefix?: string) => {
    const fullFieldName = prefix ? `${prefix}.${field}` : field;
    const editor = editorRef.current;
    
    if (editor && monacoRef.current) {
      const position = editor.getPosition();
      editor.executeEdits('', [{
        range: new monacoRef.current.Range(
          position.lineNumber, 
          position.column, 
          position.lineNumber, 
          position.column
        ),
        text: fullFieldName
      }]);
    } else {
      onChange(value + fullFieldName);
    }
  };

  const handleInsertFunction = (funcName: string) => {
    const editor = editorRef.current;
    const template = `${funcName}()`;
    
    if (editor && monacoRef.current) {
      const position = editor.getPosition();
      editor.executeEdits('', [{
        range: new monacoRef.current.Range(
          position.lineNumber, 
          position.column, 
          position.lineNumber, 
          position.column
        ),
        text: template
      }]);
      // Move cursor inside parentheses
      editor.setPosition({
        lineNumber: position.lineNumber,
        column: position.column + funcName.length + 1
      });
    } else {
      onChange(value + template);
    }
    setShowFunctions(false);
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6">Filter Expression</Typography>
        <IconButton onClick={(e) => {
          setShowFunctions(true);
          setAnchorEl(e.currentTarget);
        }}>
          <HelpIcon />
        </IconButton>
      </Box>

      <Typography variant="body2" color="text.secondary" gutterBottom>
        Optional SQL-like expression to filter joined results
      </Typography>

      {/* Field Quick Insert */}
      <Box sx={{ mb: 2 }}>
        <Typography variant="subtitle2" gutterBottom>
          Quick Insert:
        </Typography>
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mb: 2 }}>
          <Chip
            icon={<CodeIcon />}
            label="Left Fields"
            color="primary"
            variant="outlined"
          />
          {leftFields.slice(0, 5).map(field => (
            <Chip
              key={`left-${field.name}`}
              label={field.name}
              size="small"
              onClick={() => handleInsertField(field.name, 'left')}
            />
          ))}
          
          <Chip
            icon={<CodeIcon />}
            label="Right Fields"
            color="success"
            variant="outlined"
            sx={{ ml: 1 }}
          />
          {rightFields.slice(0, 5).map(field => (
            <Chip
              key={`right-${field.name}`}
              label={field.name}
              size="small"
              onClick={() => handleInsertField(field.name, 'right')}
            />
          ))}
          
          <Button
            size="small"
            startIcon={<FunctionsIcon />}
            onClick={() => setShowFunctions(true)}
          >
            Functions
          </Button>
        </Box>
      </Box>

      {/* Monaco Editor */}
      <Box sx={{ border: '1px solid #e0e0e0', borderRadius: 1, overflow: 'hidden' }}>
        <Editor
          height="200px"
          language="sql"
          value={value}
          onChange={(val) => onChange(val || '')}
          onMount={(editor, monaco) => {
            editorRef.current = editor;
            monacoRef.current = monaco;
            
            // Add custom SQL completion
            monaco.languages.registerCompletionItemProvider('sql', {
              provideCompletionItems: (model: { getWordUntilPosition: (arg0: any) => any; }, position: { lineNumber: any; }) => {
                const word = model.getWordUntilPosition(position);
                const range = {
                  startLineNumber: position.lineNumber,
                  endLineNumber: position.lineNumber,
                  startColumn: word.startColumn,
                  endColumn: word.endColumn
                };

                const suggestions = [
                  ...leftFields.map(field => ({
                    label: `left.${field.name}`,
                    kind: monaco.languages.CompletionItemKind.Field,
                    insertText: `left.${field.name}`,
                    detail: `Left field: ${field.type}`,
                    range
                  })),
                  ...rightFields.map(field => ({
                    label: `right.${field.name}`,
                    kind: monaco.languages.CompletionItemKind.Field,
                    insertText: `right.${field.name}`,
                    detail: `Right field: ${field.type}`,
                    range
                  })),
                  ...SQL_FUNCTIONS.map(func => ({
                    label: func.name,
                    kind: monaco.languages.CompletionItemKind.Function,
                    insertText: func.name,
                    detail: func.description,
                    range
                  }))
                ];

                return { suggestions };
              }
            });
          }}
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            lineNumbers: 'off',
            scrollBeyondLastLine: false,
            automaticLayout: true,
            folding: false,
            lineDecorationsWidth: 0,
            lineNumbersMinChars: 0,
            glyphMargin: false
          }}
        />
      </Box>

      {/* Example Expressions */}
      <Box sx={{ mt: 2 }}>
        <Typography variant="caption" color="text.secondary">
          Examples:
        </Typography>
        <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mt: 0.5 }}>
          <Chip
            label="left.status = 'active'"
            size="small"
            variant="outlined"
            onClick={() => onChange("left.status = 'active'")}
          />
          <Chip
            label="UPPER(left.name) = UPPER(right.name)"
            size="small"
            variant="outlined"
            onClick={() => onChange('UPPER(left.name) = UPPER(right.name)')}
          />
          <Chip
            label="left.date > DATE_ADD(right.date, INTERVAL -7 DAY)"
            size="small"
            variant="outlined"
            onClick={() => onChange('left.date > DATE_ADD(right.date, INTERVAL -7 DAY)')}
          />
        </Box>
      </Box>

      {/* Functions Popover */}
      <Popover
        open={showFunctions}
        anchorEl={anchorEl}
        onClose={() => setShowFunctions(false)}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
      >
        <Box sx={{ p: 2, width: 300 }}>
          <Typography variant="subtitle2" gutterBottom>
            SQL Functions
          </Typography>
          <List dense>
            {SQL_FUNCTIONS.map(func => (
              <ListItem
                key={func.name}
                button
                onClick={() => handleInsertFunction(func.name)}
              >
                <ListItemText
                  primary={func.name}
                  secondary={func.description}
                  secondaryTypographyProps={{ variant: 'caption' }}
                />
              </ListItem>
            ))}
          </List>
        </Box>
      </Popover>
    </Box>
  );
};

export default ExpressionEditor;