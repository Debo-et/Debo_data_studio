// src/components/layout/RightPanel.tsx - STREAMLINED (COMPONENT PALETTE ONLY)
import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Card, CardContent, CardHeader } from '../ui/card';
import { Badge } from '../ui/badge';
import { Button } from '../ui/Button';
import { 
  ChevronDown,
  ChevronRight,
  Minimize2,
  Maximize2,
  Cpu,
  GitMerge,
  Filter,
  Map
} from 'lucide-react';

// Import UNIFIED Component Registry
import { 
  getCategoryColor, 
  getComponentsBySource,
  ComponentDefinition,
  ComponentCategory 
} from '../../pages/ComponentRegistry';

// ==================== REACT FLOW DRAG-AND-DROP INTERFACES ====================
export interface ReactFlowDragData {
  type: 'reactflow-component';
  componentId: string;
  source: 'sidebar' | 'rightPanel';
  metadata?: Record<string, any>;
}

// ==================== COMPONENT PALETTE DATA ====================
interface PaletteComponent {
  id: string;
  name: string;
  icon: React.ReactElement;
  description: string;
  compactIcon: React.ReactElement;
  iconType?: string;
  category: ComponentCategory;
  definition: ComponentDefinition;
}

interface ComponentCategoryData {
  title: string;
  icon: React.ReactElement;
  components: PaletteComponent[];
}

interface ComponentCategories {
  [key: string]: ComponentCategoryData;
}

// ==================== REACT FLOW DRAG HANDLERS ====================
const handleReactFlowDragStart = (event: React.DragEvent, component: PaletteComponent) => {
  const definition = component.definition;
  const categoryColor = getCategoryColor(definition.category);
  
  // Create unified React Flow drag data
  const dragData: ReactFlowDragData = {
    type: 'reactflow-component',
    componentId: component.id,
    source: 'rightPanel',
    metadata: {
      description: definition.description,
      category: definition.category,
      createdAt: new Date().toISOString(),
      version: '1.0',
      isRepositoryNode: false,
      componentCategory: definition.category,
      originalNodeName: definition.displayName,
      originalNodeType: component.id,
      talendDefinition: definition,
      defaultWidth: definition.defaultDimensions.width,
      defaultHeight: definition.defaultDimensions.height,
      defaultRole: definition.defaultRole
    }
  };
  
  console.log('📤 RightPanel drag started:', {
    component: definition.displayName,
    componentId: component.id,
    dragData
  });
  
  // Clear previous data
  event.dataTransfer.clearData();
  
  // Set unified React Flow data format
  event.dataTransfer.setData('application/reactflow', JSON.stringify(dragData));
  event.dataTransfer.setData('text/plain', definition.displayName);
  
  // Set drag effect
  event.dataTransfer.effectAllowed = 'copy';
  
  // Create Talend-style drag image
  const dragImage = document.createElement('div');
  
  dragImage.style.cssText = `
    position: absolute;
    top: -1000px;
    left: -1000px;
    background: linear-gradient(135deg, ${categoryColor}15 0%, ${categoryColor}08 100%);
    border: 2px solid ${categoryColor}40;
    color: #374151;
    padding: 8px 12px;
    border-radius: 12px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    font-size: 12px;
    font-weight: 600;
    z-index: 10000;
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 120px;
  `;
  
  dragImage.innerHTML = `
    <div style="
      width: 24px;
      height: 24px;
      border-radius: 50%;
      background: linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      color: white;
      flex-shrink: 0;
    ">
      ${definition.displayName.charAt(0)}
    </div>
    <span style="white-space: nowrap;">${definition.displayName}</span>
  `;
  
  document.body.appendChild(dragImage);
  event.dataTransfer.setDragImage(dragImage, 60, 15);
  setTimeout(() => document.body.removeChild(dragImage), 0);
};

// ==================== COMPONENT PALETTE (TALEND STYLE) ====================
interface ComponentPaletteProps {
  expandedCategories: { [key: string]: boolean };
  toggleCategory: (categoryKey: string) => void;
}

const ComponentPalette: React.FC<ComponentPaletteProps> = ({ expandedCategories, toggleCategory }) => {
  // Get all components from the unified registry with source 'rightPanel'
  const allComponents = getComponentsBySource('rightPanel');
  
  // Create palette components from unified registry with proper type safety
  const paletteComponents: PaletteComponent[] = allComponents.map((definition) => {
    const iconElement = React.isValidElement(definition.icon) 
      ? definition.icon 
      : React.createElement('div', {}, definition.displayName.charAt(0));
    
    const compactIcon = React.isValidElement(definition.icon)
      ? React.cloneElement(definition.icon, { className: 'w-4 h-4' })
      : iconElement;
    
    return {
      id: definition.id,
      name: definition.displayName,
      icon: iconElement,
      compactIcon,
      description: definition.description,
      iconType: definition.category,
      category: definition.category,
      definition
    };
  });

  // Group components by category – updated to include all requested components
  const componentCategories: ComponentCategories = {
    mappingTransformation: {
      title: 'Mapping & Transformation',
      icon: React.createElement(Map, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        [
          'tMap', 'tConvertType', 'tReplace', 'tReplaceList', 'tParseRecordSet',
          'tPivotToColumnsDelimited', 'tUnpivotRow', 'tDenormalizeSortedRow',
          'tNormalizeNumber', 'tExtractDelimitedFields', 'tExtractRegexFields',
          'tExtractJSONFields', 'tExtractXMLField'
        ].includes(c.id)
      )
    },
    rowProcessing: {
      title: 'Row Processing',
      icon: React.createElement(Filter, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        [
          'tFilterRow', 'tSortRow', 'tAggregateRow', 'tNormalize', 'tDenormalize',
          'tFilterColumns', 'tUniqRow', 'tSampleRow', 'tSchemaComplianceCheck',
          'tAddCRCRow', 'tStandardizeRow', 'tSurvivorshipRule',
          'tDataMasking',  
        ].includes(c.id)
      )
    },
    dataCombination: {
      title: 'Data Combination',
      icon: React.createElement(GitMerge, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        [
          'tJoin', 'tSplitRow', 'tMatchGroup', 'tReplicate', 'tUnite', 'tRecordMatching',
          'tFileLookup'
        ].includes(c.id)
      )
    },
    advancedProcessing: {
      title: 'Advanced Processing',
      icon: React.createElement(Cpu, { className: "w-4 h-4" }),
      components: paletteComponents.filter(c => 
        [
          'tPivot', 'tUnpivot', 'tRowGenerator','tLookup',
          'tCache', 'tCacheIn', 'tCacheOut'
        ].includes(c.id)
      )
    }
  };

  return (
    <div className="h-full overflow-y-auto">
      <Card className="bg-gray-800/50 border-gray-700 backdrop-blur-sm rounded-none border-b-0 h-full">
        <CardHeader className="pb-3">
        </CardHeader>
        <CardContent className="space-y-2">
          <div className="space-y-1">
            {Object.entries(componentCategories).map(([key, category]) => (
              <div key={key} className="border border-gray-600 rounded-lg overflow-hidden">
                <button
                  onClick={() => toggleCategory(key)}
                  className="w-full flex items-center justify-between p-3 bg-gray-700/50 hover:bg-gray-600/50 transition-colors text-white"
                >
                  <div className="flex items-center space-x-3">
                    <div className="text-gray-300">{category.icon}</div>
                    <span className="text-sm font-medium">{category.title}</span>
                    <Badge variant="secondary" className="bg-gray-600 text-gray-300">
                      {category.components.length}
                    </Badge>
                  </div>
                  {expandedCategories[key] ? (
                    <ChevronDown className="h-4 w-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="h-4 w-4 text-gray-400" />
                  )}
                </button>
                
                {expandedCategories[key] && (
                  <motion.div
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    className="bg-gray-800/30 p-2 border-t border-gray-600"
                  >
                    <div className="grid grid-cols-1 gap-0.5 max-h-60 overflow-y-auto">
                      {category.components.map(component => {
                        const categoryColor = getCategoryColor(component.category);
                        return (
                          <div
                            key={component.id}
                            draggable
                            onDragStart={(e) => handleReactFlowDragStart(e, component)}
                            className="flex items-center space-x-2 p-2 rounded-lg border border-transparent hover:bg-gray-600/30 hover:border-gray-500 transition-all cursor-grab active:cursor-grabbing group"
                            title={component.description}
                          >
                            <div 
                              className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-white shadow-sm"
                              style={{ 
                                background: `linear-gradient(135deg, ${categoryColor} 0%, ${categoryColor}CC 100%)` 
                              }}
                            >
                              {component.compactIcon}
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="text-xs font-semibold text-white truncate">
                                {component.name}
                              </div>
                              <div className="text-xs text-gray-400 truncate">
                                {component.id}
                              </div>
                            </div>
                            <div className="text-xs text-gray-500">
                              {component.definition.defaultDimensions.width}x{component.definition.defaultDimensions.height}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </motion.div>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

// ==================== MAIN RIGHT PANEL (STREAMLINED) ====================
const RightPanel: React.FC = () => {
  const [isExpanded, setIsExpanded] = useState(false);
  
  const [expandedCategories, setExpandedCategories] = useState<{ [key: string]: boolean }>({
    mappingTransformation: true,
    rowProcessing: false,
    dataCombination: false,
    dataQuality: false,
    advancedProcessing: false
  });

  const toggleCategory = (categoryKey: string) => {
    setExpandedCategories(prev => ({
      ...prev,
      [categoryKey]: !prev[categoryKey]
    }));
  };

  const toggleExpand = () => {
    setIsExpanded(!isExpanded);
  };

  const panelWidth = isExpanded ? 'w-96' : 'w-80';

  return (
    <motion.div 
      initial={{ opacity: 0, x: 50 }}
      animate={{ opacity: 1, x: 0 }}
      className={`${panelWidth} bg-gradient-to-b from-gray-900 to-gray-800 border-l border-gray-700 shadow-2xl flex flex-col transition-all duration-300`}
    >
      {/* Panel Header */}
      <div className="flex items-center justify-between p-3 border-b border-gray-700 bg-gray-800/80">
        <div className="flex items-center space-x-2">
          <h3 className="text-sm font-medium text-white">Component Palette</h3>
        </div>
        <div className="flex items-center space-x-1">
          <Button
            variant="ghost"
            size="sm"
            onClick={toggleExpand}
            className="h-7 w-7 p-0 text-gray-400 hover:text-white hover:bg-gray-700/50"
            title={isExpanded ? "Collapse Panel" : "Expand Panel"}
          >
            {isExpanded ? <Minimize2 className="h-3.5 w-3.5" /> : <Maximize2 className="h-3.5 w-3.5" />}
          </Button>
        </div>
      </div>

      {/* Component Palette */}
      <div className="flex-1 overflow-hidden">
        <ComponentPalette 
          expandedCategories={expandedCategories}
          toggleCategory={toggleCategory}
        />
      </div>
    </motion.div>
  );
};

export default RightPanel;