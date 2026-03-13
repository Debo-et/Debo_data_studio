// src/utils/backup.utils.ts

import { JobDesignState } from "@/types/types";

export class BackupUtils {
  static exportAllData(): string {
    const allData: Record<string, any> = {};
    
    // Collect all localStorage items with app prefixes
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && (
        key.startsWith('app_') || 
        key.startsWith('job_') || 
        key.startsWith('rightpanel_') ||
        key.includes('repository')
      )) {
        try {
          const value = localStorage.getItem(key);
          if (value) {
            allData[key] = JSON.parse(value);
          }
        } catch (error) {
          console.warn(`Could not parse data for key ${key}`, error);
        }
      }
    }
    
    return JSON.stringify(allData, null, 2);
  }

  static importAllData(jsonString: string): boolean {
    try {
      const data = JSON.parse(jsonString);
      
      // Clear existing app data
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.startsWith('app_')) {
          localStorage.removeItem(key);
        }
      }
      
      // Import new data
      Object.entries(data).forEach(([key, value]) => {
        localStorage.setItem(key, JSON.stringify(value));
      });
      
      return true;
    } catch (error) {
      console.error('Failed to import data:', error);
      return false;
    }
  }

  static createBackupFile(_backupData: { jobs: any[]; jobDesigns: Record<string, JobDesignState>; timestamp: string; }): void {
    const data = this.exportAllData();
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = url;
    a.download = `debo-backup-${new Date().toISOString().split('T')[0]}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  static restoreFromFile(file: File): Promise<boolean> {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = e.target?.result as string;
          const success = this.importAllData(data);
          if (success) {
            alert('✅ Backup restored successfully! Please refresh the page.');
          }
          resolve(success);
        } catch (error) {
          console.error('Restore failed:', error);
          resolve(false);
        }
      };
      reader.readAsText(file);
    });
  }
}