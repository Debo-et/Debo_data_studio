// src/components/development/DevResetButton.tsx
import React from 'react';
import { Button } from '../ui/Button';
import { RefreshCw, AlertTriangle } from 'lucide-react';
import { useHotkeys } from 'react-hotkeys-hook';

interface DevResetButtonProps {
  onReset: () => void;
}

const DevResetButton: React.FC<DevResetButtonProps> = ({ onReset }) => {
  // Only show in development
  if (process.env.NODE_ENV === 'production') {
    return null;
  }

  // Add keyboard shortcut: Ctrl+Shift+R
  useHotkeys('ctrl+shift+r', (e) => {
    e.preventDefault();
    if (confirm('Reset application state for development?')) {
      onReset();
    }
  }, { enableOnFormTags: true });

  const handleReset = () => {
    if (confirm('🚨 DEVELOPMENT RESET 🚨\n\nThis will:\n• Clear all localStorage data\n• Reset current job\n• Start fresh canvas\n\nAre you sure?')) {
      onReset();
    }
  };

  return (
    <div className="fixed bottom-4 left-4 z-50">
      <Button
        variant="destructive"
        size="sm"
        onClick={handleReset}
        className="flex items-center gap-2 shadow-lg"
      >
        <RefreshCw className="h-4 w-4" />
        <span>Reset Dev State</span>
        <AlertTriangle className="h-4 w-4" />
      </Button>
      <div className="text-xs text-gray-500 mt-1 text-center">
        Ctrl+Shift+R
      </div>
    </div>
  );
};

export default DevResetButton;