// src/services/migration.service.ts
export class MigrationService {
  static migrateIfNeeded() {
    const versionKey = 'app_data_version';
    const currentVersion = '1.2.0'; // Increment when data format changes
    
    const savedVersion = localStorage.getItem(versionKey);
    
    if (!savedVersion) {
      // First time setup
      localStorage.setItem(versionKey, currentVersion);
      return;
    }
    
    if (savedVersion !== currentVersion) {
      console.log(`Migrating from ${savedVersion} to ${currentVersion}`);
      
      // Perform migrations based on version
      this.migrateFromTo(savedVersion, currentVersion);
      
      localStorage.setItem(versionKey, currentVersion);
    }
  }
  
  private static migrateFromTo(fromVersion: string, toVersion: string) {
    // Add migration logic here as your data format evolves
    if (fromVersion === '1.0.0' && toVersion === '1.1.0') {
      // Example: Add new fields to repository data
      this.migrateRepositoryData();
    }
    
    if (fromVersion === '1.1.0' && toVersion === '1.2.0') {
      // Example: Restructure drag state
      this.migrateDragState();
    }
  }
  
  private static migrateRepositoryData() {
    const repoData = localStorage.getItem('app_repository_data');
    if (repoData) {
      try {
        const parsed = JSON.parse(repoData);
        // Add new fields or restructure
        const migrated = parsed.map((node: any) => ({
          ...node,
          version: '1.1.0',
          createdAt: node.createdAt || new Date().toISOString()
        }));
        localStorage.setItem('app_repository_data', JSON.stringify(migrated));
      } catch (error) {
        console.error('Migration failed:', error);
      }
    }
  }
  
  private static migrateDragState() {
    // Similar migration for drag state
  }
}

// Call this early in your app initialization
MigrationService.migrateIfNeeded();