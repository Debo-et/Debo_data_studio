// backend/src/database/types/index.ts

// Export all database types
export * from './database.types';
export * from './inspection.types';

// Common type exports
export {
  DatabaseError,
  ConnectionError,
  QueryError,
  DatabaseErrorCategory
} from './database.types';

export {
  ArchiveHandle,
  Trivalue
} from './inspection.types';