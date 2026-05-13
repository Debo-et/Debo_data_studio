// src/utils/performance-utils.ts

/**
 * Performance optimization utilities for canvas and connection management
 */

// ==================== DEBOUNCE UTILITIES ====================

export function createDebouncedFunction<T extends (...args: any[]) => any>(
  func: T,
  wait: number,
  immediate = false
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout | null = null;
  
  return function executedFunction(this: any, ...args: Parameters<T>) {
    const context = this;
    
    const later = () => {
      timeout = null;
      if (!immediate) func.apply(context, args);
    };
    
    const callNow = immediate && !timeout;
    
    if (timeout) {
      clearTimeout(timeout);
    }
    
    timeout = setTimeout(later, wait);
    
    if (callNow) {
      func.apply(context, args);
    }
  };
}

// ==================== THROTTLE UTILITIES ====================

export function createThrottledFunction<T extends (...args: any[]) => any>(
  func: T,
  limit: number
): (...args: Parameters<T>) => void {
  let inThrottle: boolean;
  
  return function executedFunction(this: any, ...args: Parameters<T>) {
    const context = this;
    
    if (!inThrottle) {
      func.apply(context, args);
      inThrottle = true;
      setTimeout(() => (inThrottle = false), limit);
    }
  };
}

// ==================== BATCH UPDATE UTILITIES ====================

export class BatchUpdateManager {
  private updates: Map<string, any> = new Map();
  private timeout: NodeJS.Timeout | null = null;
  private readonly batchSize: number;
  private readonly debounceMs: number;
  private readonly callback: (updates: Map<string, any>) => void;
  
  constructor(
    callback: (updates: Map<string, any>) => void,
    options: { batchSize?: number; debounceMs?: number } = {}
  ) {
    this.callback = callback;
    this.batchSize = options.batchSize || 50;
    this.debounceMs = options.debounceMs || 100;
  }
  
  addUpdate(key: string, value: any): void {
    this.updates.set(key, value);
    
    if (this.updates.size >= this.batchSize) {
      this.flush();
    } else if (!this.timeout) {
      this.timeout = setTimeout(() => this.flush(), this.debounceMs);
    }
  }
  
  flush(): void {
    if (this.timeout) {
      clearTimeout(this.timeout);
      this.timeout = null;
    }
    
    if (this.updates.size > 0) {
      this.callback(new Map(this.updates));
      this.updates.clear();
    }
  }
  
  dispose(): void {
    if (this.timeout) {
      clearTimeout(this.timeout);
      this.timeout = null;
    }
    this.flush();
  }
}

// ==================== MEMORY MANAGEMENT ====================

export class ConnectionCache {
  private cache: Map<string, any> = new Map();
  private accessTimes: Map<string, number> = new Map();
  private maxSize: number;
  
  constructor(maxSize = 1000) {
    this.maxSize = maxSize;
  }
  
  set(key: string, value: any): void {
    // If cache is full, remove least recently used items
    if (this.cache.size >= this.maxSize) {
      this.cleanup();
    }
    
    this.cache.set(key, value);
    this.accessTimes.set(key, Date.now());
  }
  
  get(key: string): any | undefined {
    const value = this.cache.get(key);
    if (value) {
      this.accessTimes.set(key, Date.now());
    }
    return value;
  }
  
  has(key: string): boolean {
    return this.cache.has(key);
  }
  
  delete(key: string): void {
    this.cache.delete(key);
    this.accessTimes.delete(key);
  }
  
  clear(): void {
    this.cache.clear();
    this.accessTimes.clear();
  }
  
  private cleanup(): void {
    // Remove oldest 20% of items
    const entries = Array.from(this.accessTimes.entries())
      .sort(([, timeA], [, timeB]) => timeA - timeB);
    
    const itemsToRemove = Math.floor(this.maxSize * 0.2);
    
    for (let i = 0; i < Math.min(itemsToRemove, entries.length); i++) {
      const [key] = entries[i];
      this.cache.delete(key);
      this.accessTimes.delete(key);
    }
  }
  
  get size(): number {
    return this.cache.size;
  }
}

// ==================== RENDER OPTIMIZATION ====================

export function shouldComponentUpdate(
  prevProps: any,
  nextProps: any,
  keys: string[]
): boolean {
  for (const key of keys) {
    if (prevProps[key] !== nextProps[key]) {
      return true;
    }
  }
  return false;
}

export function memoizeRender<T extends (...args: any[]) => React.ReactNode>(
  renderFunction: T,
  dependencyKeys: string[]
): T {
  let lastProps: any = {};
  let lastResult: React.ReactNode;
  
  return ((props: any) => {
    if (shouldComponentUpdate(lastProps, props, dependencyKeys)) {
      lastResult = renderFunction(props);
      lastProps = props;
    }
    return lastResult;
  }) as T;
}

// ==================== CANVAS OPTIMIZATION ====================

export class CanvasOptimizer {
  private static instance: CanvasOptimizer;
  private renderQueue: Set<string> = new Set();
  private isRendering: boolean = false;
  
  static getInstance(): CanvasOptimizer {
    if (!CanvasOptimizer.instance) {
      CanvasOptimizer.instance = new CanvasOptimizer();
    }
    return CanvasOptimizer.instance;
  }
  
  setVisibleRect(): void {
  }
  
  scheduleRender(itemId: string): void {
    this.renderQueue.add(itemId);
    this.processRenderQueue();
  }
  
  private processRenderQueue(): void {
    if (this.isRendering || this.renderQueue.size === 0) return;
    
    this.isRendering = true;
    
    // Use requestAnimationFrame for smooth rendering
    requestAnimationFrame(() => {
      const items = Array.from(this.renderQueue);
      this.renderQueue.clear();
      
      // In a real implementation, this would trigger actual rendering
      // For now, we just log for debugging
      if (process.env.NODE_ENV === 'development') {
        console.log(`CanvasOptimizer: Rendering ${items.length} items`);
      }
      
      this.isRendering = false;
      
      // If more items were added while rendering, process them
      if (this.renderQueue.size > 0) {
        this.processRenderQueue();
      }
    });
  }
  
  cancelRender(itemId: string): void {
    this.renderQueue.delete(itemId);
  }
  
  clearQueue(): void {
    this.renderQueue.clear();
  }
}

// ==================== PERFORMANCE MONITORING ====================

export class PerformanceMonitor {
  private measurements: Map<string, number[]> = new Map();
  private startTimes: Map<string, number> = new Map();
  
  start(operation: string): void {
    this.startTimes.set(operation, performance.now());
  }
  
  end(operation: string): void {
    const startTime = this.startTimes.get(operation);
    if (startTime === undefined) return;
    
    const duration = performance.now() - startTime;
    this.startTimes.delete(operation);
    
    const measurements = this.measurements.get(operation) || [];
    measurements.push(duration);
    
    // Keep only last 100 measurements
    if (measurements.length > 100) {
      measurements.shift();
    }
    
    this.measurements.set(operation, measurements);
  }
  
  getAverage(operation: string): number {
    const measurements = this.measurements.get(operation);
    if (!measurements || measurements.length === 0) return 0;
    
    return measurements.reduce((sum, val) => sum + val, 0) / measurements.length;
  }
  
  getStats(operation: string): { avg: number; min: number; max: number; count: number } {
    const measurements = this.measurements.get(operation) || [];
    if (measurements.length === 0) {
      return { avg: 0, min: 0, max: 0, count: 0 };
    }
    
    const min = Math.min(...measurements);
    const max = Math.max(...measurements);
    const avg = this.getAverage(operation);
    
    return { avg, min, max, count: measurements.length };
  }
  
  logStats(operation: string): void {
    const stats = this.getStats(operation);
    console.log(
      `Performance - ${operation}: ` +
      `Avg: ${stats.avg.toFixed(2)}ms, ` +
      `Min: ${stats.min.toFixed(2)}ms, ` +
      `Max: ${stats.max.toFixed(2)}ms, ` +
      `Count: ${stats.count}`
    );
  }
  
  clear(): void {
    this.measurements.clear();
    this.startTimes.clear();
  }
}