// components/ui/slider.tsx
import * as React from "react"
import { cn } from "@/lib/utils"

interface SliderProps {
  value?: number[]
  defaultValue?: number[]
  min?: number
  max?: number
  step?: number
  disabled?: boolean
  minStepsBetweenThumbs?: number
  onValueChange?: (value: number[]) => void
  onValueCommit?: (value: number[]) => void
  orientation?: "horizontal" | "vertical"
  className?: string
  // Additional HTML div element props (excluding conflicting ones)
  style?: React.CSSProperties
  id?: string
  "aria-label"?: string
  "aria-labelledby"?: string
  "aria-describedby"?: string
  "aria-valuetext"?: string
}

const Slider = React.forwardRef<
  HTMLDivElement,
  SliderProps
>(({ 
  value,
  defaultValue = [0],
  min = 0,
  max = 100,
  step = 1,
  disabled = false,
  orientation = "horizontal",
  className,
  onValueChange,
  onValueCommit,
  // HTML props
  style,
  id,
  "aria-label": ariaLabel,
  "aria-labelledby": ariaLabelledBy,
  "aria-describedby": ariaDescribedBy,
  "aria-valuetext": ariaValueText,
  ...props 
}, ref) => {
  const [internalValue, setInternalValue] = React.useState(value || defaultValue);
  const sliderRef = React.useRef<HTMLDivElement>(null);
  const isControlled = value !== undefined;
  
  const currentValue = isControlled ? value : internalValue;
  const currentThumbValue = currentValue[0] || 0;
  
  const handleSliderClick = React.useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (disabled || !sliderRef.current) return;
    
    const rect = sliderRef.current.getBoundingClientRect();
    const isVertical = orientation === "vertical";
    
    let percentage;
    if (isVertical) {
      percentage = (rect.bottom - e.clientY) / rect.height;
    } else {
      percentage = (e.clientX - rect.left) / rect.width;
    }
    
    percentage = Math.max(0, Math.min(1, percentage));
    const newValue = min + percentage * (max - min);
    const steppedValue = Math.round(newValue / step) * step;
    const clampedValue = Math.max(min, Math.min(max, steppedValue));
    
    const newValues = [clampedValue];
    
    if (!isControlled) {
      setInternalValue(newValues);
    }
    
    onValueChange?.(newValues);
    onValueCommit?.(newValues);
  }, [disabled, min, max, step, orientation, isControlled, onValueChange, onValueCommit]);

  const handleKeyDown = React.useCallback((e: React.KeyboardEvent) => {
    if (disabled) return;
    
    let newValue = currentThumbValue;
    
    switch (e.key) {
      case 'ArrowRight':
      case 'ArrowUp':
        e.preventDefault();
        newValue = Math.min(max, currentThumbValue + step);
        break;
      case 'ArrowLeft':
      case 'ArrowDown':
        e.preventDefault();
        newValue = Math.max(min, currentThumbValue - step);
        break;
      case 'Home':
        e.preventDefault();
        newValue = min;
        break;
      case 'End':
        e.preventDefault();
        newValue = max;
        break;
      default:
        return;
    }
    
    const newValues = [newValue];
    
    if (!isControlled) {
      setInternalValue(newValues);
    }
    
    onValueChange?.(newValues);
    if (['ArrowRight', 'ArrowLeft', 'ArrowUp', 'ArrowDown', 'Home', 'End'].includes(e.key)) {
      onValueCommit?.(newValues);
    }
  }, [currentThumbValue, disabled, isControlled, min, max, step, onValueChange, onValueCommit]);

  const percentage = ((currentThumbValue - min) / (max - min)) * 100;
  
  // Filter out any remaining props that might cause conflicts
  const safeProps = { ...props } as any;
  delete safeProps.defaultValue;
  delete safeProps.value;
  delete safeProps.onChange;
  
  return (
    <div
      ref={(node) => {
        if (typeof ref === 'function') {
          ref(node);
        } else if (ref) {
          ref.current = node;
        }
        // @ts-ignore
        sliderRef.current = node;
      }}
      className={cn(
        "relative flex w-full touch-none select-none items-center cursor-pointer",
        orientation === "vertical" && "h-full w-auto flex-col",
        disabled && "cursor-not-allowed opacity-50",
        className
      )}
      onClick={handleSliderClick}
      onKeyDown={handleKeyDown}
      role="slider"
      aria-valuemin={min}
      aria-valuemax={max}
      aria-valuenow={currentThumbValue}
      aria-disabled={disabled}
      aria-label={ariaLabel}
      aria-labelledby={ariaLabelledBy}
      aria-describedby={ariaDescribedBy}
      aria-valuetext={ariaValueText}
      tabIndex={disabled ? -1 : 0}
      style={style}
      id={id}
      {...safeProps}
    >
      <div className="relative h-2 w-full grow overflow-hidden rounded-full bg-gray-200 dark:bg-gray-800">
        <div 
          className="absolute h-full bg-blue-600 dark:bg-blue-500"
          style={orientation === "horizontal" 
            ? { width: `${percentage}%` }
            : { height: `${percentage}%`, bottom: 0 }
          }
        />
      </div>
      <div
        className="absolute block h-5 w-5 rounded-full border-2 border-blue-600 bg-white ring-2 ring-blue-600/20 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-600/50 focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 dark:border-blue-500 dark:bg-gray-950 dark:ring-blue-500/20 dark:focus-visible:ring-blue-500/50"
        style={orientation === "horizontal"
          ? { left: `${percentage}%`, transform: 'translateX(-50%)' }
          : { bottom: `${percentage}%`, transform: 'translateY(50%)' }
        }
      />
    </div>
  );
});

Slider.displayName = "Slider";

export { Slider };
export type { SliderProps };