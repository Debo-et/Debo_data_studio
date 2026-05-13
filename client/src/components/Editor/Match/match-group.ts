// types/match-group.ts
export type MatchingAlgorithm = 'levenshtein' | 'jaro-winkler' | 'cosine' | 'jaccard' | 'exact';

export interface FieldSchema {
  name: string;
  type: 'string' | 'number' | 'date' | 'boolean';
  nullable?: boolean;
}

export interface MatchGroupConfiguration {
  keys: Array<{
    name: string;
    weight?: number;
    type: FieldSchema['type'];
  }>;
  threshold: number;
  algorithm: MatchingAlgorithm;
  blockingFields?: string[];
  outputGroupId: boolean;
  maxGroupSize?: number;
}

export interface AlgorithmInfo {
  name: string;
  description: string;
  speed: number; // 1-5
  accuracy: number; // 1-5
  icon: string;
  useCases: string[];
  configuration?: {
    nGramSize?: number;
    caseSensitive?: boolean;
  };
}

export interface MatchPreview {
  groupId: number;
  records: Array<{
    id: string;
    data: Record<string, any>;
  }>;
  similarity: number;
}