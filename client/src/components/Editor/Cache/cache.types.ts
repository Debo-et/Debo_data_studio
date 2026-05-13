// types/cache.types.ts
export type PersistenceType = 'memory' | 'disk' | 'redis';
export type EvictionPolicy = 'lru' | 'fifo' | 'lfu';
export type LookupType = 'exact' | 'prefix' | 'range';
export type MissHandling = 'null' | 'error' | 'skip';

export interface CacheInConfig {
  cacheName: string;
  keyFields: string[];
  ttl?: number;
  maxSize?: number;
  evictionPolicy?: EvictionPolicy;
  compression?: boolean;
  persistence?: PersistenceType;
  redisConfig?: RedisConfig;
  diskConfig?: DiskConfig;
  writeThrough?: boolean;
  batchSize?: number;
}

export interface CacheOutConfig {
  cacheName: string;
  keyFields: string[];
  lookupType: LookupType;
  defaultValues?: Record<string, any>;
  onMiss?: MissHandling;
  cacheWarming?: boolean;
  warmupStrategy?: 'eager' | 'lazy';
}

export interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  db?: number;
}

export interface DiskConfig {
  path: string;
  maxFileSize?: number;
}

export interface CacheStats {
  hitRate: number;
  missRate: number;
  memoryUsage: number;
  entryCount: number;
  averageAge: number;
  sizeInBytes: number;
}

export interface FieldSchema {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'date';
  sampleValue: any;
}