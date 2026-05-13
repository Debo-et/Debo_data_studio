// src/utils/sql-storage.ts
/**
 * Utility for storing and retrieving generated SQL scripts
 */

export interface SavedSQL {
  id: string;
  name: string;
  sql: string;
  jobName: string;
  timestamp: string;
  metadata?: {
    nodeCount: number;
    edgeCount: number;
    warnings: string[];
  };
}

export class SQLStorage {
  private static readonly STORAGE_KEY = 'generated_sql_scripts';

  /**
   * Save SQL script to storage
   */
  static saveScript(sql: string, jobName: string, metadata?: any): string {
    const id = `sql_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    const script: SavedSQL = {
      id,
      name: `SQL for ${jobName} - ${new Date().toLocaleString()}`,
      sql,
      jobName,
      timestamp: new Date().toISOString(),
      metadata
    };
    
    // Get existing scripts
    const existingScripts = this.getAllScripts();
    
    // Add new script (limit to 50 most recent)
    const updatedScripts = [script, ...existingScripts].slice(0, 50);
    
    // Save to localStorage
    localStorage.setItem(this.STORAGE_KEY, JSON.stringify(updatedScripts));
    
    return id;
  }

  /**
   * Get all saved SQL scripts
   */
  static getAllScripts(): SavedSQL[] {
    try {
      const stored = localStorage.getItem(this.STORAGE_KEY);
      return stored ? JSON.parse(stored) : [];
    } catch (error) {
      console.error('Failed to load saved SQL scripts:', error);
      return [];
    }
  }

  /**
   * Get a specific SQL script by ID
   */
  static getScript(id: string): SavedSQL | null {
    const scripts = this.getAllScripts();
    return scripts.find(script => script.id === id) || null;
  }

  /**
   * Delete a SQL script
   */
  static deleteScript(id: string): boolean {
    const scripts = this.getAllScripts();
    const filtered = scripts.filter(script => script.id !== id);
    
    if (filtered.length < scripts.length) {
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(filtered));
      return true;
    }
    
    return false;
  }

  /**
   * Export all scripts as a JSON file
   */
  static exportAllScripts(): void {
    const scripts = this.getAllScripts();
    const data = JSON.stringify(scripts, null, 2);
    
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `sql_scripts_export_${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  /**
   * Clear all saved scripts
   */
  static clearAllScripts(): void {
    localStorage.removeItem(this.STORAGE_KEY);
  }
}