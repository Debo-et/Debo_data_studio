// src/test/utils/sqlComparator.ts
export interface ComparisonResult {
  success: boolean;
  diff?: string;
}

export function compareSQL(actual: string, expected: string): ComparisonResult {
  const normalize = (sql: string) =>
    sql
      .trim()
      .replace(/\s+/g, ' ')
      .replace(/\(/g, '(') // keep parentheses formatting
      .toLowerCase();

  const actualNorm = normalize(actual);
  const expectedNorm = normalize(expected);

  if (actualNorm === expectedNorm) {
    return { success: true };
  }

  // Generate a line-by-line diff
  const actualLines = actual.split('\n');
  const expectedLines = expected.split('\n');
  const diffLines: string[] = [];

  for (let i = 0; i < Math.max(actualLines.length, expectedLines.length); i++) {
    const actualLine = actualLines[i] || '';
    const expectedLine = expectedLines[i] || '';
    if (actualLine !== expectedLine) {
      diffLines.push(`Line ${i + 1}: expected "${expectedLine}"`);
      diffLines.push(`          actual   "${actualLine}"`);
    }
  }

  return {
    success: false,
    diff: diffLines.join('\n'),
  };
}