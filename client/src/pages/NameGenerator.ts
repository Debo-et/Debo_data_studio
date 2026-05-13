// src/components/canvas/NameGenerator.ts
type ComponentRole = 'INPUT' | 'TRANSFORM' | 'OUTPUT';

class NameGenerator {
  private counters: Map<string, Map<ComponentRole, number>> = new Map();

  private getCurrentCount(componentName: string, role: ComponentRole): number {
    if (!this.counters.has(componentName)) {
      this.counters.set(componentName, new Map());
    }
    
    const roleCounters = this.counters.get(componentName)!;
    if (!roleCounters.has(role)) {
      roleCounters.set(role, 0);
    }
    
    return roleCounters.get(role)!;
  }

  generate(componentName: string, role: ComponentRole): string {
    const currentCount = this.getCurrentCount(componentName, role);
    const newCount = currentCount + 1;
    
    const roleCounters = this.counters.get(componentName)!;
    roleCounters.set(role, newCount);
    
    return `${componentName}_${role}_${newCount}`;
  }

  peekNext(componentName: string, role: ComponentRole): string {
    const currentCount = this.getCurrentCount(componentName, role);
    return `${componentName}_${role}_${currentCount + 1}`;
  }

  getCurrentCountForComponent(componentName: string, role: ComponentRole): number {
    return this.getCurrentCount(componentName, role);
  }

  resetCounter(componentName: string, role?: ComponentRole): void {
    if (role) {
      const roleCounters = this.counters.get(componentName);
      if (roleCounters) {
        roleCounters.set(role, 0);
      }
    } else {
      this.counters.delete(componentName);
    }
  }

  resetAll(): void {
    this.counters.clear();
  }

  // Helper to extract base component name from generated name
  static extractBaseName(generatedName: string): { componentName: string; role: ComponentRole; instance: number } | null {
    const parts = generatedName.split('_');
    if (parts.length < 3) return null;
    
    const instancePart = parts.pop();
    const rolePart = parts.pop() as ComponentRole;
    const componentName = parts.join('_');
    
    const instance = parseInt(instancePart || '0', 10);
    
    if (isNaN(instance) || !['INPUT', 'TRANSFORM', 'OUTPUT'].includes(rolePart)) {
      return null;
    }
    
    return {
      componentName,
      role: rolePart as ComponentRole,
      instance
    };
  }

  // Instance method wrapper for convenience
  extractBaseName(generatedName: string): { componentName: string; role: ComponentRole; instance: number } | null {
    return NameGenerator.extractBaseName(generatedName);
  }

  // Initialize from existing nodes
  initializeFromExistingNodes(nodeLabels: string[]): void {
    this.resetAll();
    
    nodeLabels.forEach(label => {
      const extracted = NameGenerator.extractBaseName(label);
      if (extracted) {
        const { componentName, role, instance } = extracted;
        
        if (!this.counters.has(componentName)) {
          this.counters.set(componentName, new Map());
        }
        
        const roleCounters = this.counters.get(componentName)!;
        if (!roleCounters.has(role) || roleCounters.get(role)! < instance) {
          roleCounters.set(role, instance);
        }
      }
    });
  }

  // Get all registered component names
  getRegisteredComponents(): string[] {
    return Array.from(this.counters.keys());
  }

  // Get counts for all roles of a component
  getComponentCounts(componentName: string): Map<ComponentRole, number> | undefined {
    return this.counters.get(componentName);
  }

  // Export current state (useful for debugging or persistence)
  exportState(): Record<string, Record<ComponentRole, number>> {
    const state: Record<string, Record<ComponentRole, number>> = {};
    
    for (const [componentName, roleCounters] of this.counters) {
      const roleState: Record<ComponentRole, number> = {
          INPUT: 0,
          TRANSFORM: 0,
          OUTPUT: 0
      };
      for (const [role, count] of roleCounters) {
        roleState[role] = count;
      }
      state[componentName] = roleState;
    }
    
    return state;
  }

  // Import state (useful for restoring from persistence)
  importState(state: Record<string, Record<ComponentRole, number>>): void {
    this.resetAll();
    
    for (const [componentName, roleState] of Object.entries(state)) {
      if (!this.counters.has(componentName)) {
        this.counters.set(componentName, new Map());
      }
      
      const roleCounters = this.counters.get(componentName)!;
      for (const [role, count] of Object.entries(roleState)) {
        roleCounters.set(role as ComponentRole, count);
      }
    }
  }

  // Bulk generate names for testing or batch operations
  generateMultiple(componentName: string, role: ComponentRole, count: number): string[] {
    const names: string[] = [];
    for (let i = 0; i < count; i++) {
      names.push(this.generate(componentName, role));
    }
    return names;
  }

  // Decrement counter (useful for undo operations)
  decrementCounter(componentName: string, role: ComponentRole): boolean {
    if (!this.counters.has(componentName)) {
      return false;
    }
    
    const roleCounters = this.counters.get(componentName)!;
    if (!roleCounters.has(role) || roleCounters.get(role)! <= 0) {
      return false;
    }
    
    const currentCount = roleCounters.get(role)!;
    roleCounters.set(role, currentCount - 1);
    return true;
  }

  // Check if a name matches the pattern
  static isValidGeneratedName(name: string): boolean {
    return NameGenerator.extractBaseName(name) !== null;
  }

  // Parse name without affecting counters
  static parseName(name: string): { componentName: string; role: ComponentRole; instance: number } | null {
    return NameGenerator.extractBaseName(name);
  }
}

// Singleton instance
export const nameGenerator = new NameGenerator();

// Helper function to create a new instance (useful for testing)
export const createNameGenerator = (): NameGenerator => {
  return new NameGenerator();
};