import { UniqueRowSQLGenerator } from '../../generators/UniqueRowSQLGenerator';
import { UnifiedCanvasNode, NodeType } from '../../types/unified-pipeline.types';
import { SQLGenerationContext, SQLGenerationOptions } from '../../generators/BaseSQLGenerator';
import { compareSQL } from '../utils/sqlComparator';

// Helper to create a basic context
function createContext(node: UnifiedCanvasNode): SQLGenerationContext {
  const options: SQLGenerationOptions = {
    includeComments: false,
    formatSQL: false,
    targetDialect: 'POSTGRESQL',
    postgresVersion: '14.0',
    useCTEs: false,
    optimizeForReadability: false,
    includeExecutionPlan: false,
    parameterizeValues: false,
    maxLineLength: 80,
  };
  return {
    node,
    indentLevel: 0,
    parameters: new Map(),
    options,
  };
}

describe('UniqueRowSQLGenerator', () => {
  let generator: UniqueRowSQLGenerator;

  beforeEach(() => {
    generator = new UniqueRowSQLGenerator({ postgresVersion: '14.0', includeComments: false, formatSQL: false });
  });

  it('generates DISTINCT ON SQL when key fields are specified', () => {
    const node: UnifiedCanvasNode = {
      id: 'uniq1',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'OTHER',
          config: {
            columns: ['first_name', 'last_name'],
            keep: 'first',
          },
        },
      },
    };

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT DISTINCT ON (first_name, last_name) * FROM source_table ORDER BY first_name, last_name, id ASC`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('generates DISTINCT ON with LAST keep strategy (DESC order)', () => {
    const node: UnifiedCanvasNode = {
      id: 'uniq2',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'OTHER',
          config: {
            columns: ['email'],
            keep: 'last',
          },
        },
      },
    };

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT DISTINCT ON (email) * FROM source_table ORDER BY email, id DESC`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('falls back to SELECT DISTINCT when no key fields are configured', () => {
    const node: UnifiedCanvasNode = {
      id: 'uniq3',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'OTHER',
          config: {
            columns: [],
            keep: 'first',
          },
        },
      },
    };

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT DISTINCT * FROM source_table`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('uses default config when metadata.configuration is missing', () => {
    // Provide an empty configuration (no key fields) to simulate missing config
    const node: UnifiedCanvasNode = {
      id: 'uniq4',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'OTHER',
          config: {}, // empty config -> no key fields -> fallback to SELECT DISTINCT
        },
      },
    };

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT DISTINCT * FROM source_table`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('handles legacy config stored in node.metadata.config (backward compatibility)', () => {
    // Legacy nodes may store config directly under metadata.config.
    // We provide a dummy configuration to satisfy the type, but the generator
    // should still read from metadata.config for backward compatibility.
    const node = {
      id: 'uniq5',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        config: {
          columns: ['category', 'subcategory'],
          keep: 'first',
        },
        configuration: { type: 'OTHER', config: {} }, // dummy to satisfy type
      },
    } as unknown as UnifiedCanvasNode; // cast to bypass strict type check for legacy test

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT DISTINCT ON (category, subcategory) * FROM source_table ORDER BY category, subcategory, id ASC`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('sanitizes column names (quotes identifiers with spaces/reserved words)', () => {
    const node: UnifiedCanvasNode = {
      id: 'uniq6',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'OTHER',
          config: {
            columns: ['group', 'order by'], // "order by" is a reserved phrase
            keep: 'first',
          },
        },
      },
    };

    const context = createContext(node);
    const fragment = generator.generateSQL(context);
    const expected = `SELECT DISTINCT ON ("group", "order by") * FROM source_table ORDER BY "group", "order by", id ASC`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
  });

  it('includes warnings when no incoming connection is present (placeholder source)', () => {
    const node: UnifiedCanvasNode = {
      id: 'uniq7',
      name: 'Uniq Row',
      type: NodeType.UNIQ_ROW,
      position: { x: 0, y: 0 },
      size: { width: 200, height: 100 },
      metadata: {
        configuration: {
          type: 'OTHER',
          config: { columns: ['id'], keep: 'first' },
        },
      },
    };

    const context = createContext(node);
    // Simulate no connection by not providing a connection in context
    const fragment = generator.generateSQL(context);
    // The generator still uses 'source_table' placeholder
    const expected = `SELECT DISTINCT ON (id) * FROM source_table ORDER BY id, id ASC`;
    const result = compareSQL(fragment.sql, expected);
    expect(result.success).toBe(true);
    // Warnings should indicate no incoming connection
    expect(fragment.warnings).toContainEqual(expect.stringContaining('No incoming connection found'));
  });
});