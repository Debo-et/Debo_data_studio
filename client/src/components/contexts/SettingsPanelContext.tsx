// contexts/SettingsPanelContext.tsx
import React, { createContext, useContext, useState, ReactNode } from 'react';
import { ComponentType, BasicSettingsState } from '../../components/Editor/BasicSettingsPanel';

interface SettingsPanelContextType {
  isOpen: boolean;
  selectedComponent: {
    id: string;
    type: ComponentType;
    metadata: any;
  } | null;
  openPanel: (componentId: string, componentType: ComponentType, metadata: any) => void;
  closePanel: () => void;
  updateComponent: (config: BasicSettingsState) => void;
}

const SettingsPanelContext = createContext<SettingsPanelContextType | undefined>(undefined);

export const useSettingsPanel = () => {
  const context = useContext(SettingsPanelContext);
  if (!context) {
    throw new Error('useSettingsPanel must be used within SettingsPanelProvider');
  }
  return context;
};

interface SettingsPanelProviderProps {
  children: ReactNode;
  onSave?: (config: BasicSettingsState) => void;
  onApply?: (config: BasicSettingsState) => void;
}

export const SettingsPanelProvider: React.FC<SettingsPanelProviderProps> = ({ 
  children}) => {
  const [state, setState] = useState<{
    isOpen: boolean;
    selectedComponent: { id: string; type: ComponentType; metadata: any } | null;
  }>({
    isOpen: false,
    selectedComponent: null
  });

  const openPanel = (componentId: string, componentType: ComponentType, metadata: any) => {
    setState({
      isOpen: true,
      selectedComponent: { id: componentId, type: componentType, metadata }
    });
  };

  const closePanel = () => {
    setState(prev => ({ ...prev, isOpen: false }));
  };

  const updateComponent = (config: BasicSettingsState) => {
    // This would update the component in your global state
    console.log('Updating component with config:', config);
  };

  return (
    <SettingsPanelContext.Provider
      value={{
        isOpen: state.isOpen,
        selectedComponent: state.selectedComponent,
        openPanel,
        closePanel,
        updateComponent
      }}
    >
      {children}
    </SettingsPanelContext.Provider>
  );
};