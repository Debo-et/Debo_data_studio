// src/components/ui/Tooltip.tsx

import React, { useState, useRef, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

export interface TooltipProps {
  children: React.ReactElement;
  content: React.ReactNode;
  position?: 'top' | 'bottom' | 'left' | 'right';
  delay?: number;
  disabled?: boolean;
  className?: string;
  maxWidth?: number;
}

export const Tooltip: React.FC<TooltipProps> = ({
  children,
  content,
  position = 'top',
  delay = 300,
  disabled = false,
  className = '',
  maxWidth = 250
}) => {
  const [isVisible, setIsVisible] = useState(false);
  const [coords, setCoords] = useState({ x: 0, y: 0 });
  const timeoutRef = useRef<NodeJS.Timeout>();
  const triggerRef = useRef<HTMLDivElement>(null);

  const showTooltip = () => {
    if (disabled) return;
    
    timeoutRef.current = setTimeout(() => {
      if (triggerRef.current) {
        const rect = triggerRef.current.getBoundingClientRect();
        setCoords({
          x: rect.left + rect.width / 2,
          y: rect.top + rect.height / 2
        });
      }
      setIsVisible(true);
    }, delay);
  };

  const hideTooltip = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }
    setIsVisible(false);
  };

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const getTooltipPosition = () => {
    const offset = 10;
    switch (position) {
      case 'top':
        return {
          left: coords.x,
          top: coords.y - offset,
          transform: 'translate(-50%, -100%)'
        };
      case 'bottom':
        return {
          left: coords.x,
          top: coords.y + offset,
          transform: 'translate(-50%, 0)'
        };
      case 'left':
        return {
          left: coords.x - offset,
          top: coords.y,
          transform: 'translate(-100%, -50%)'
        };
      case 'right':
        return {
          left: coords.x + offset,
          top: coords.y,
          transform: 'translate(0, -50%)'
        };
      default:
        return {
          left: coords.x,
          top: coords.y - offset,
          transform: 'translate(-50%, -100%)'
        };
    }
  };

  const getArrowPosition = () => {
    switch (position) {
      case 'top':
        return 'bottom-0 left-1/2 transform -translate-x-1/2 translate-y-1/2 rotate-45';
      case 'bottom':
        return 'top-0 left-1/2 transform -translate-x-1/2 -translate-y-1/2 rotate-45';
      case 'left':
        return 'right-0 top-1/2 transform translate-x-1/2 -translate-y-1/2 rotate-45';
      case 'right':
        return 'left-0 top-1/2 transform -translate-x-1/2 -translate-y-1/2 rotate-45';
      default:
        return 'bottom-0 left-1/2 transform -translate-x-1/2 translate-y-1/2 rotate-45';
    }
  };

  return (
    <>
      <div
        ref={triggerRef}
        onMouseEnter={showTooltip}
        onMouseLeave={hideTooltip}
        onFocus={showTooltip}
        onBlur={hideTooltip}
        className="inline-block"
      >
        {children}
      </div>

      <AnimatePresence>
        {isVisible && content && (
          <motion.div
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.9 }}
            transition={{ duration: 0.15 }}
            className={`fixed z-50 ${className}`}
            style={{ ...getTooltipPosition(), maxWidth }}
            role="tooltip"
          >
            <div className="relative">
              <div className="bg-gray-900 text-white text-sm rounded-lg px-3 py-2 shadow-xl">
                {content}
              </div>
              <div className={`absolute w-2 h-2 bg-gray-900 ${getArrowPosition()}`} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
};

Tooltip.displayName = 'Tooltip';

export default Tooltip;