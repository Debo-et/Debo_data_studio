import React, { useEffect, useRef, useState, useCallback } from 'react';
import { useAppDispatch, useAppSelector } from '../../hooks';
import { clearLogs, setAutoScroll, setFilterLevel } from '../../store/slices/logsSlice';
import { ScrollArea } from '../ui/scroll-area';
import { Button } from '../ui/Button';
import { Badge } from '../ui/badge';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../ui/select';
import { Maximize2, X, Move, GripHorizontal } from 'lucide-react';

interface ConsoleProps {
  isFloating?: boolean;
  onToggleFloating?: () => void;
}

// Debounce function
const debounce = <T extends (...args: any[]) => any>(
  func: T,
  wait: number
): ((...args: Parameters<T>) => void) => {
  let timeout: NodeJS.Timeout | null = null;
  return (...args: Parameters<T>) => {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
};

const Console: React.FC<ConsoleProps> = ({ 
  isFloating: externalIsFloating = false,
  onToggleFloating 
}) => {
  const dispatch = useAppDispatch();
  const { entries, isAutoScrollEnabled, filterLevel } = useAppSelector((state) => state.logs);
  const { isWebSocketConnected, executionId } = useAppSelector((state) => state.api);
  
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const consoleRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
  const resizeBottomRef = useRef<HTMLDivElement>(null);
  const resizeRightRef = useRef<HTMLDivElement>(null);
  const resizeCornerRef = useRef<HTMLDivElement>(null);
  
  // Local state
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isCollapsed, setIsCollapsed] = useState<boolean>(false);
  
  // Floating panel state
  const [isFloating, setIsFloating] = useState<boolean>(() => {
    try {
      const saved = localStorage.getItem('console_floating');
      return saved ? JSON.parse(saved) : externalIsFloating;
    } catch (error) {
      console.warn('Failed to load floating state from localStorage:', error);
      return externalIsFloating;
    }
  });
  
  // Use sessionStorage for temporary UI state
  const [position, setPosition] = useState<{x: number, y: number}>(() => {
    try {
      const saved = sessionStorage.getItem('console_position');
      return saved ? JSON.parse(saved) : { x: 20, y: window.innerHeight - 320 };
    } catch (error) {
      console.warn('Failed to load position from sessionStorage:', error);
      return { x: 20, y: window.innerHeight - 320 };
    }
  });
  
  const [size, setSize] = useState<{width: number, height: number}>(() => {
    try {
      const saved = sessionStorage.getItem('console_size');
      return saved ? JSON.parse(saved) : { width: 800, height: 300 };
    } catch (error) {
      console.warn('Failed to load size from sessionStorage:', error);
      return { width: 800, height: 300 };
    }
  });
  
  const [isDragging, setIsDragging] = useState<boolean>(false);
  const [isResizing, setIsResizing] = useState<boolean>(false);
  const [, setResizeDirection] = useState<'bottom' | 'right' | 'corner' | null>(null);
  const [dragStart, setDragStart] = useState<{x: number, y: number}>({ x: 0, y: 0 });
  const [sizeStart, setSizeStart] = useState<{width: number, height: number}>({ width: 0, height: 0 });
  const [positionStart, setPositionStart] = useState<{x: number, y: number}>({ x: 0, y: 0 });

  // Debounced save functions
  const debouncedSavePosition = useCallback(
    debounce((pos: {x: number, y: number}) => {
      try {
        sessionStorage.setItem('console_position', JSON.stringify(pos));
      } catch (error) {
        console.warn('Failed to save position to sessionStorage:', error);
        setErrorMessage('Storage limit reached. UI preferences may not be saved.');
      }
    }, 500),
    []
  );

  const debouncedSaveSize = useCallback(
    debounce((size: {width: number, height: number}) => {
      try {
        sessionStorage.setItem('console_size', JSON.stringify(size));
      } catch (error) {
        console.warn('Failed to save size to sessionStorage:', error);
        setErrorMessage('Storage limit reached. UI preferences may not be saved.');
      }
    }, 500),
    []
  );

  // Auto-scroll to bottom
  useEffect(() => {
    if (isAutoScrollEnabled && scrollAreaRef.current && !isCollapsed) {
      const scrollContainer = scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]');
      if (scrollContainer) {
        scrollContainer.scrollTop = scrollContainer.scrollHeight;
      }
    }
  }, [entries, isAutoScrollEnabled, isCollapsed]);

  // Save position and size (with debouncing)
  useEffect(() => {
    debouncedSavePosition(position);
  }, [position, debouncedSavePosition]);

  useEffect(() => {
    debouncedSaveSize(size);
  }, [size, debouncedSaveSize]);

  // Save floating state (not frequent, no debounce needed)
  useEffect(() => {
    try {
      localStorage.setItem('console_floating', JSON.stringify(isFloating));
    } catch (error) {
      console.warn('Failed to save floating state to localStorage:', error);
      setErrorMessage('Storage limit reached. Some preferences may not be saved.');
    }
  }, [isFloating]);

  // Drag handling
  const handleDragStart = useCallback((e: React.MouseEvent) => {
    if (!isFloating || isCollapsed) return;
    
    e.preventDefault();
    setIsDragging(true);
    setDragStart({
      x: e.clientX - position.x,
      y: e.clientY - position.y
    });
    
    // Add global class to disable canvas interactions
    document.body.classList.add('console-interaction-active');
    
    const handleMouseMove = (e: MouseEvent) => {
      if (!isDragging) return;
      
      const newX = e.clientX - dragStart.x;
      const newY = e.clientY - dragStart.y;
      
      const maxX = window.innerWidth - size.width;
      const maxY = window.innerHeight - size.height;
      
      setPosition({
        x: Math.max(0, Math.min(newX, maxX)),
        y: Math.max(0, Math.min(newY, maxY))
      });
    };
    
    const handleMouseUp = () => {
      setIsDragging(false);
      document.body.classList.remove('console-interaction-active');
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
    
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  }, [isFloating, isCollapsed, position, size, dragStart, isDragging]);

  // Resize handling
  const handleResizeStart = useCallback((direction: 'bottom' | 'right' | 'corner', e: React.MouseEvent) => {
    if (!isFloating || isCollapsed) return;
    
    e.preventDefault();
    e.stopPropagation();
    
    setIsResizing(true);
    setResizeDirection(direction);
    setSizeStart(size);
    setPositionStart(position);
    
    const startX = e.clientX;
    const startY = e.clientY;
    
    // Add global class to disable canvas interactions
    document.body.classList.add('console-interaction-active');
    
    const handleMouseMove = (e: MouseEvent) => {
      if (!isResizing) return;
      
      const deltaX = e.clientX - startX;
      const deltaY = e.clientY - startY;
      
      let newWidth = size.width;
      let newHeight = size.height;
      
      if (direction === 'right' || direction === 'corner') {
        newWidth = Math.max(400, Math.min(window.innerWidth - position.x, sizeStart.width + deltaX));
      }
      if (direction === 'bottom' || direction === 'corner') {
        newHeight = Math.max(200, Math.min(window.innerHeight - position.y, sizeStart.height + deltaY));
      }
      
      setSize({
        width: newWidth,
        height: newHeight
      });
    };
    
    const handleMouseUp = () => {
      setIsResizing(false);
      setResizeDirection(null);
      document.body.classList.remove('console-interaction-active');
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
    
    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  }, [isFloating, isCollapsed, size, position, isResizing, sizeStart, positionStart]);

  // Panel controls
  const handleToggleFloating = useCallback(() => {
    const newState = !isFloating;
    setIsFloating(newState);
    
    if (onToggleFloating) {
      onToggleFloating();
    }
    
    if (!newState) {
      setPosition({ x: 20, y: window.innerHeight - 320 });
      setSize({ width: 800, height: 300 });
      try {
        sessionStorage.removeItem('console_position');
        sessionStorage.removeItem('console_size');
      } catch (error) {
        console.warn('Failed to clear sessionStorage:', error);
      }
    }
  }, [isFloating, onToggleFloating]);

  const handleClear = () => {
    dispatch(clearLogs());
  };

  const handleAutoScrollToggle = () => {
    dispatch(setAutoScroll(!isAutoScrollEnabled));
  };

  const handleFilterChange = (value: string) => {
    dispatch(setFilterLevel(value as any));
  };

  const handleToggleCollapse = () => {
    setIsCollapsed(!isCollapsed);
  };

  // Show error message temporarily
  useEffect(() => {
    if (errorMessage) {
      const timer = setTimeout(() => {
        setErrorMessage(null);
      }, 5000);
      return () => clearTimeout(timer);
    }
  }, [errorMessage]);

  // Utility functions
  const getLogLevelColor = (level: string) => {
    switch (level) {
      case 'ERROR': return 'text-red-400 bg-red-400/10 border-red-400/20';
      case 'WARN': return 'text-yellow-400 bg-yellow-400/10 border-yellow-400/20';
      case 'INFO': return 'text-blue-400 bg-blue-400/10 border-blue-400/20';
      case 'DEBUG': return 'text-gray-400 bg-gray-400/10 border-gray-400/20';
      case 'SUCCESS': return 'text-green-400 bg-green-400/10 border-green-400/20';
      default: return 'text-gray-400 bg-gray-400/10 border-gray-400/20';
    }
  };

  const formatTimestamp = (timestamp: number) => {
    const date = new Date(timestamp);
    const timeString = date.toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
    const milliseconds = date.getMilliseconds().toString().padStart(3, '0');
    return `${timeString}.${milliseconds}`;
  };

  const filteredEntries = entries.filter(entry => {
    if (filterLevel === 'ALL') return true;
    return entry.level === filterLevel;
  });

  // Render floating panel
  if (isFloating) {
    return (
      <>
        {/* Error message */}
        {errorMessage && (
          <div className="fixed top-4 right-4 bg-red-600 text-white text-sm px-4 py-2 rounded-lg shadow-lg z-[10001]">
            {errorMessage}
          </div>
        )}
        
        <div
          ref={consoleRef}
          className={`fixed bg-gray-900 border border-gray-700 rounded-lg shadow-2xl flex flex-col z-[10000] overflow-hidden floating-console-overlay ${
            isDragging ? 'cursor-grabbing' : 'cursor-default'
          }`}
          style={{
            left: `${position.x}px`,
            top: `${position.y}px`,
            width: `${size.width}px`,
            height: isCollapsed ? 'auto' : `${size.height}px`,
            minWidth: '400px',
            minHeight: isCollapsed ? 'auto' : '200px',
            maxWidth: `${window.innerWidth - 20}px`,
            maxHeight: `${window.innerHeight - 60}px`,
            pointerEvents: 'auto'
          }}
        >
          {/* Header */}
          <div
            ref={headerRef}
            className="flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700 cursor-move select-none console-header"
            onMouseDown={handleDragStart}
            style={{ cursor: isDragging ? 'grabbing' : 'grab' }}
          >
            <div className="flex items-center space-x-3">
              <Move className="h-4 w-4 text-gray-400" />
              <h3 className="text-sm font-medium text-gray-200">Console</h3>
              <Badge 
                variant="outline" 
                className={
                  isWebSocketConnected ? "text-green-400 border-green-400/50" :
                  "text-red-400 border-red-400/50"
                }
              >
                <div className={`w-2 h-2 rounded-full mr-1 ${
                  isWebSocketConnected ? 'bg-green-400' : 'bg-red-400'
                }`} />
                {isWebSocketConnected ? 'Connected' : 'Disconnected'}
              </Badge>
            </div>
            
            <div className="flex items-center space-x-1">
              <Button
                variant="ghost"
                size="sm"
                onClick={handleToggleCollapse}
                className="h-6 w-6 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
                title={isCollapsed ? "Expand" : "Collapse"}
              >
                {isCollapsed ? '▸' : '▾'}
              </Button>
              
              <Button
                variant="ghost"
                size="sm"
                onClick={handleToggleFloating}
                className="h-6 w-6 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
                title="Dock to bottom"
              >
                <X className="h-4 w-4" />
              </Button>
            </div>
          </div>
          
          {/* Content */}
          {!isCollapsed && (
            <>
              {/* Controls */}
              <div className="flex items-center justify-between px-4 py-2 bg-gray-800/50 border-b border-gray-700">
                <div className="flex items-center space-x-2">
                  <Badge variant="outline" className="text-gray-300 border-gray-600">
                    {filteredEntries.length} entries
                  </Badge>
                  
                  {executionId && (
                    <Badge variant="outline" className="text-blue-400 border-blue-400/50">
                      Exec: {executionId.slice(0, 8)}...
                    </Badge>
                  )}
                </div>
                
                <div className="flex items-center space-x-2">
                  <Select value={filterLevel} onValueChange={handleFilterChange}>
                    <SelectTrigger className="w-28 h-7 text-xs bg-gray-700 border-gray-600">
                      <SelectValue placeholder="Filter" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="ALL">All Levels</SelectItem>
                      <SelectItem value="DEBUG">Debug</SelectItem>
                      <SelectItem value="INFO">Info</SelectItem>
                      <SelectItem value="WARN">Warning</SelectItem>
                      <SelectItem value="ERROR">Error</SelectItem>
                      <SelectItem value="SUCCESS">Success</SelectItem>
                    </SelectContent>
                  </Select>

                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleAutoScrollToggle}
                    className={`h-7 text-xs ${
                      isAutoScrollEnabled 
                        ? 'bg-blue-600 text-white border-blue-600' 
                        : 'bg-gray-700 text-gray-300 border-gray-600'
                    }`}
                  >
                    Auto-scroll {isAutoScrollEnabled ? 'ON' : 'OFF'}
                  </Button>

                  <Button
                    variant="outline"
                    size="sm"
                    onClick={handleClear}
                    className="h-7 text-xs bg-gray-700 text-gray-300 border-gray-600 hover:bg-gray-600"
                  >
                    Clear
                  </Button>
                </div>
              </div>
              
              {/* Logs */}
              <ScrollArea className="flex-1" ref={scrollAreaRef}>
                <div className="p-4 font-mono text-sm">
                  {filteredEntries.length === 0 ? (
                    <div className="text-gray-500 text-center py-8">
                      No log entries to display.
                    </div>
                  ) : (
                    <div className="space-y-1">
                      {filteredEntries.map((entry) => (
                        <div 
                          key={entry.id} 
                          className="flex items-start space-x-3 hover:bg-gray-800/50 px-2 py-1 rounded"
                        >
                          <span className="text-gray-500 text-xs whitespace-nowrap mt-0.5 flex-shrink-0">
                            [{formatTimestamp(entry.timestamp)}]
                          </span>
                          
                          <Badge 
                            variant="outline" 
                            className={`text-xs px-2 py-0 h-4 ${getLogLevelColor(entry.level)}`}
                          >
                            {entry.level}
                          </Badge>
                          
                          {entry.source && (
                            <span className="text-gray-400 text-xs mt-0.5 flex-shrink-0">
                              [{entry.source}]
                            </span>
                          )}
                          
                          <span 
                            className={`text-gray-300 flex-1 ${
                              entry.level === 'ERROR' ? 'text-red-300' :
                              entry.level === 'WARN' ? 'text-yellow-300' :
                              entry.level === 'SUCCESS' ? 'text-green-300' :
                              'text-gray-300'
                            }`}
                          >
                            {entry.message}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </ScrollArea>
              
              {/* Resize handles */}
              <div
                ref={resizeBottomRef}
                className="absolute bottom-0 left-0 right-0 h-2 cursor-row-resize hover:bg-blue-500/20 active:bg-blue-500/40"
                onMouseDown={(e) => handleResizeStart('bottom', e)}
                title="Drag to resize height"
              >
                <div className="absolute bottom-0 left-1/2 transform -translate-x-1/2">
                  <GripHorizontal className="h-2 w-8 text-gray-500 opacity-50" />
                </div>
              </div>
              
              <div
                ref={resizeRightRef}
                className="absolute top-0 right-0 bottom-0 w-2 cursor-col-resize hover:bg-blue-500/20 active:bg-blue-500/40"
                onMouseDown={(e) => handleResizeStart('right', e)}
                title="Drag to resize width"
              >
                <div className="absolute right-0 top-1/2 transform -translate-y-1/2 rotate-90">
                  <GripHorizontal className="h-2 w-8 text-gray-500 opacity-50" />
                </div>
              </div>
              
              <div
                ref={resizeCornerRef}
                className="absolute bottom-0 right-0 w-4 h-4 cursor-nwse-resize hover:bg-blue-500/30 active:bg-blue-500/50"
                onMouseDown={(e) => handleResizeStart('corner', e)}
                title="Drag to resize both dimensions"
              >
                <div className="absolute bottom-0 right-0 w-3 h-3 border-r-2 border-b-2 border-gray-500"></div>
              </div>
            </>
          )}
          
          {/* Size indicator during resize */}
          {isResizing && (
            <div className="fixed bottom-4 left-1/2 transform -translate-x-1/2 bg-blue-600 text-white text-xs px-3 py-1 rounded-lg shadow-lg z-[10000] pointer-events-none">
              {Math.round(size.width)} × {Math.round(size.height)}
            </div>
          )}
        </div>
      </>
    );
  }

  // Render docked console - AS OVERLAY (updated for App.tsx changes)
  return (
    <div 
      ref={consoleRef}
      className={`docked-console-overlay flex flex-col bg-gray-900 border-t border-gray-700 ${isCollapsed ? 'h-10' : 'h-48'}`}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700">
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={handleToggleCollapse}
              className="h-6 w-6 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
              title={isCollapsed ? "Expand" : "Collapse"}
            >
              {isCollapsed ? '▸' : '▾'}
            </Button>
            
            <h3 className="text-sm font-medium text-gray-200">Console</h3>
          </div>
          
          <div className="flex items-center space-x-2">
            <Badge 
              variant="outline" 
              className={
                isWebSocketConnected ? "text-green-400 border-green-400/50" :
                "text-red-400 border-red-400/50"
              }
            >
              <div className={`w-2 h-2 rounded-full mr-1 ${
                isWebSocketConnected ? 'bg-green-400' : 'bg-red-400'
              }`} />
              {isWebSocketConnected ? 'Connected' : 'Disconnected'}
            </Badge>
            
            {executionId && (
              <Badge variant="outline" className="text-blue-400 border-blue-400/50">
                Execution: {executionId.slice(0, 8)}...
              </Badge>
            )}
            
            <Badge variant="outline" className="text-gray-300 border-gray-600">
              {filteredEntries.length} {filteredEntries.length === 1 ? 'entry' : 'entries'}
            </Badge>
          </div>
        </div>
        
        <div className="flex items-center space-x-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={handleToggleFloating}
            className="h-6 w-6 p-0 text-gray-400 hover:text-white hover:bg-gray-700"
            title="Make floating panel"
          >
            <Maximize2 className="h-4 w-4" />
          </Button>
          
          <Select value={filterLevel} onValueChange={handleFilterChange}>
            <SelectTrigger className="w-28 h-7 text-xs bg-gray-700 border-gray-600">
              <SelectValue placeholder="Filter" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">All Levels</SelectItem>
              <SelectItem value="DEBUG">Debug</SelectItem>
              <SelectItem value="INFO">Info</SelectItem>
              <SelectItem value="WARN">Warning</SelectItem>
              <SelectItem value="ERROR">Error</SelectItem>
              <SelectItem value="SUCCESS">Success</SelectItem>
            </SelectContent>
          </Select>

          <Button
            variant="outline"
            size="sm"
            onClick={handleAutoScrollToggle}
            className={`h-7 text-xs ${
              isAutoScrollEnabled 
                ? 'bg-blue-600 text-white border-blue-600' 
                : 'bg-gray-700 text-gray-300 border-gray-600'
            }`}
          >
            Auto-scroll {isAutoScrollEnabled ? 'ON' : 'OFF'}
          </Button>

          <Button
            variant="outline"
            size="sm"
            onClick={handleClear}
            className="h-7 text-xs bg-gray-700 text-gray-300 border-gray-600 hover:bg-gray-600"
          >
            Clear
          </Button>
        </div>
      </div>
      
      {/* Log content */}
      {!isCollapsed && (
        <ScrollArea className="flex-1" ref={scrollAreaRef}>
          <div className="p-4 font-mono text-sm">
            {filteredEntries.length === 0 ? (
              <div className="text-gray-500 text-center py-8">
                No log entries to display.
              </div>
            ) : (
              <div className="space-y-1">
                {filteredEntries.map((entry) => (
                  <div 
                    key={entry.id} 
                    className="flex items-start space-x-3 hover:bg-gray-800/50 px-2 py-1 rounded"
                  >
                    <span className="text-gray-500 text-xs whitespace-nowrap mt-0.5 flex-shrink-0">
                      [{formatTimestamp(entry.timestamp)}]
                    </span>
                    
                    <Badge 
                      variant="outline" 
                      className={`text-xs px-2 py-0 h-4 ${getLogLevelColor(entry.level)}`}
                    >
                      {entry.level}
                    </Badge>
                    
                    {entry.source && (
                      <span className="text-gray-400 text-xs mt-0.5 flex-shrink-0">
                        [{entry.source}]
                      </span>
                    )}
                    
                    <span 
                      className={`text-gray-300 flex-1 ${
                        entry.level === 'ERROR' ? 'text-red-300' :
                        entry.level === 'WARN' ? 'text-yellow-300' :
                        entry.level === 'SUCCESS' ? 'text-green-300' :
                        'text-gray-300'
                      }`}
                    >
                      {entry.message}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </ScrollArea>
      )}
    </div>
  );
};

export default Console;