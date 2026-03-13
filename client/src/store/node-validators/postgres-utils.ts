// src/utils/node-validators/postgres-utils.ts
/**
 * PostgreSQL-specific validation utilities
 */

import { PostgreSQLDataType } from '../../types/pipeline-types';

export class PostgresValidationUtils {
  static readonly RESERVED_KEYWORDS = new Set([
    'ALL', 'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC',
    'ASYMMETRIC', 'AUTHORIZATION', 'BINARY', 'BOTH', 'CASE', 'CAST',
    'CHECK', 'COLLATE', 'COLUMN', 'CONCURRENTLY', 'CONSTRAINT', 'CREATE',
    'CROSS', 'CURRENT_CATALOG', 'CURRENT_DATE', 'CURRENT_ROLE',
    'CURRENT_SCHEMA', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER',
    'DEFAULT', 'DEFERRABLE', 'DESC', 'DISTINCT', 'DO', 'ELSE', 'END',
    'EXCEPT', 'FALSE', 'FETCH', 'FOR', 'FOREIGN', 'FREEZE', 'FROM',
    'FULL', 'GRANT', 'GROUP', 'HAVING', 'ILIKE', 'IN', 'INITIALLY',
    'INNER', 'INTERSECT', 'INTO', 'IS', 'ISNULL', 'JOIN', 'LATERAL',
    'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME', 'LOCALTIMESTAMP',
    'NATURAL', 'NOT', 'NOTNULL', 'NULL', 'OFFSET', 'ON', 'ONLY', 'OR',
    'ORDER', 'OUTER', 'OVER', 'OVERLAPS', 'PLACING', 'PRIMARY',
    'REFERENCES', 'RETURNING', 'RIGHT', 'SELECT', 'SESSION_USER',
    'SIMILAR', 'SOME', 'SYMMETRIC', 'TABLE', 'THEN', 'TO', 'TRAILING',
    'TRUE', 'UNION', 'UNIQUE', 'USER', 'USING', 'VARIADIC', 'VERBOSE',
    'WHEN', 'WHERE', 'WINDOW', 'WITH'
  ]);

  static isReservedKeyword(word: string): boolean {
    return this.RESERVED_KEYWORDS.has(word.toUpperCase());
  }

  static isValidIdentifier(identifier: string): boolean {
    // PostgreSQL identifier rules
    const regex = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;
    return regex.test(identifier) && !this.isReservedKeyword(identifier);
  }

  static getTypeCompatibility(source: PostgreSQLDataType, target: PostgreSQLDataType): {
    compatible: boolean;
    requiresCast: boolean;
    castFunction?: string;
    risk: 'none' | 'low' | 'medium' | 'high';
  } {
    // Simplified compatibility matrix
    const matrix: Record<string, Record<string, any>> = {
      'INTEGER': {
        'BIGINT': { compatible: true, requiresCast: true, castFunction: '::bigint', risk: 'none' },
        'DECIMAL': { compatible: true, requiresCast: true, castFunction: '::decimal', risk: 'none' },
        'TEXT': { compatible: true, requiresCast: true, castFunction: '::text', risk: 'none' }
      },
      'TEXT': {
        'VARCHAR': { compatible: true, requiresCast: true, castFunction: '::varchar', risk: 'low' },
        'INTEGER': { compatible: false, requiresCast: false, risk: 'high' }
      }
    };

    const sourceMatrix = matrix[source];
    if (!sourceMatrix) {
      return { compatible: false, requiresCast: false, risk: 'high' };
    }

    const compatibility = sourceMatrix[target];
    if (!compatibility) {
      return { compatible: false, requiresCast: false, risk: 'high' };
    }

    return compatibility;
  }

  static validateSqlExpression(expression: string): {
    valid: boolean;
    issues: string[];
    suggestedFix?: string;
  } {
    const issues: string[] = [];

    // Check for common SQL injection patterns
    const dangerousPatterns = [
      { pattern: /;\s*--/, description: 'SQL comment injection' },
      { pattern: /DROP\s+TABLE/i, description: 'DROP TABLE command' },
      { pattern: /DELETE\s+FROM/i, description: 'DELETE command' },
      { pattern: /UPDATE\s+.+\s+SET/i, description: 'UPDATE command' },
      { pattern: /INSERT\s+INTO/i, description: 'INSERT command' },
      { pattern: /UNION\s+SELECT/i, description: 'UNION injection' },
      { pattern: /xp_cmdshell/i, description: 'Dangerous extended procedure' }
    ];

    dangerousPatterns.forEach(({ pattern, description }) => {
      if (pattern.test(expression)) {
        issues.push(`Potential security issue: ${description}`);
      }
    });

    // Check for unbalanced parentheses
    const openParen = (expression.match(/\(/g) || []).length;
    const closeParen = (expression.match(/\)/g) || []).length;
    if (openParen !== closeParen) {
      issues.push(`Unbalanced parentheses: ${openParen} opening, ${closeParen} closing`);
    }

    // Check for missing operators in expressions
    const operatorPattern = /[a-zA-Z_][a-zA-Z0-9_]*\s*[a-zA-Z_][a-zA-Z0-9_]*/;
    if (operatorPattern.test(expression.replace(/\s+/g, ' '))) {
      issues.push('Missing operator between identifiers');
    }

    return {
      valid: issues.length === 0,
      issues,
      suggestedFix: issues.length > 0 ? 'Use parameterized queries or validate input' : undefined
    };
  }

  static suggestColumnMapping(sourceColumns: string[], targetColumns: string[]): Array<{
    source: string;
    target: string;
    confidence: number;
    reason: string;
  }> {
    const suggestions: Array<{
      source: string;
      target: string;
      confidence: number;
      reason: string;
    }> = [];

    sourceColumns.forEach(source => {
      targetColumns.forEach(target => {
        let confidence = 0;
        let reason = '';

        // Exact match
        if (source.toLowerCase() === target.toLowerCase()) {
          confidence = 100;
          reason = 'Exact name match';
        }
        // Common abbreviations
        else if (
          (source === 'id' && target === 'identifier') ||
          (source === 'addr' && target === 'address') ||
          (source === 'desc' && target === 'description')
        ) {
          confidence = 80;
          reason = 'Common abbreviation';
        }
        // Similar names (Levenshtein distance)
        else if (this.similarityScore(source, target) > 0.7) {
          confidence = Math.round(this.similarityScore(source, target) * 100);
          reason = 'Similar name';
        }

        if (confidence > 50) {
          suggestions.push({ source, target, confidence, reason });
        }
      });
    });

    return suggestions.sort((a, b) => b.confidence - a.confidence);
  }

  private static similarityScore(a: string, b: string): number {
    // Simple similarity calculation
    const aLower = a.toLowerCase();
    const bLower = b.toLowerCase();

    if (aLower.includes(bLower) || bLower.includes(aLower)) {
      return 0.8;
    }

    // Count common characters
    const setA = new Set(aLower);
    const setB = new Set(bLower);
    const intersection = new Set([...setA].filter(x => setB.has(x)));
    const union = new Set([...setA, ...setB]);

    return intersection.size / union.size;
  }
}