// mockData.ts
import { RuleSet, SchemaField, LookupReference, TestCase, Rule } from '../../../types/types';

export const mockSchemaFields: SchemaField[] = [
  { name: 'customer_id', type: 'string', nullable: false, description: 'Unique customer identifier' },
  { name: 'first_name', type: 'string', nullable: false, description: 'Customer first name' },
  { name: 'last_name', type: 'string', nullable: false, description: 'Customer last name' },
  { name: 'email', type: 'string', nullable: false, description: 'Customer email address' },
  { name: 'phone', type: 'string', nullable: true, description: 'Customer phone number' },
  { name: 'address', type: 'string', nullable: true, description: 'Customer address' },
  { name: 'city', type: 'string', nullable: true, description: 'City' },
  { name: 'state', type: 'string', nullable: true, description: 'State/Province' },
  { name: 'zip_code', type: 'string', nullable: true, description: 'Postal code' },
  { name: 'country', type: 'string', nullable: true, description: 'Country' },
  { name: 'date_of_birth', type: 'date', nullable: true, description: 'Date of birth' },
  { name: 'registration_date', type: 'timestamp', nullable: false, description: 'Registration timestamp' },
  { name: 'account_status', type: 'string', nullable: false, description: 'Account status' },
  { name: 'total_purchases', type: 'decimal', nullable: false, description: 'Total purchase amount' },
  { name: 'last_purchase_date', type: 'date', nullable: true, description: 'Last purchase date' }
];

export const mockLookups: LookupReference[] = [
  { 
    id: 'lookup_1', 
    name: 'US States', 
    type: 'local', 
    source: '/data/lookups/us_states.csv', 
    description: 'US states and territories' 
  },
  { 
    id: 'lookup_2', 
    name: 'Country Codes', 
    type: 'talend-dq', 
    source: 'TALEND_DQ_COUNTRY_CODES', 
    description: 'ISO country codes' 
  },
  { 
    id: 'lookup_3', 
    name: 'Title Standardization', 
    type: 'custom', 
    source: '/data/lookups/titles.json', 
    description: 'Standard titles (Mr, Ms, Dr, etc.)' 
  },
  { 
    id: 'lookup_4', 
    name: 'Product Categories', 
    type: 'database', 
    source: 'product_categories', 
    description: 'Product category hierarchy' 
  },
  { 
    id: 'lookup_5', 
    name: 'Email Providers', 
    type: 'local', 
    source: '/data/lookups/email_providers.txt', 
    description: 'Common email service providers' 
  }
];

export const initialTestCases: TestCase[] = [
  { 
    id: 'test_1', 
    inputColumn: 'email', 
    testValue: 'john.doe@gmail.com', 
    expectedOutput: 'john.doe@gmail.com',
    description: 'Valid email should remain unchanged'
  },
  { 
    id: 'test_2', 
    inputColumn: 'phone', 
    testValue: '(123) 456-7890', 
    expectedOutput: '1234567890',
    description: 'Phone number should be cleansed to digits only'
  },
  { 
    id: 'test_3', 
    inputColumn: 'first_name', 
    testValue: 'JOHN', 
    expectedOutput: 'John',
    description: 'First name should be converted to proper case'
  },
  { 
    id: 'test_4', 
    inputColumn: 'state', 
    testValue: 'california', 
    expectedOutput: 'CA',
    description: 'State name should be converted to abbreviation'
  },
  { 
    id: 'test_5', 
    inputColumn: 'zip_code', 
    testValue: '12345', 
    expectedOutput: '12345',
    description: 'Valid ZIP code should remain unchanged'
  },
  { 
    id: 'test_6', 
    inputColumn: 'country', 
    testValue: 'united states', 
    expectedOutput: 'UNITED STATES',
    description: 'Country should be converted to uppercase'
  }
];

// Helper function to create rules with all required properties
const createRule = (
  id: string, 
  name: string, 
  inputColumn: string, 
  operation: string, 
  matchPattern: string, 
  enabled: boolean, 
  priority?: number, 
  replacement?: string, 
  description?: string, 
  patternType?: 'regex' | 'contains' | 'startsWith' | 'endsWith' | 'exactMatch' | 'dictionary' | 'custom',
  lookup?: LookupReference
): Rule => ({
  id,
  name,
  inputColumn,
  operation,
  matchPattern,
  enabled,
  priority,
  replacement,
  description,
  patternType,
  lookup
});

export const mockRuleSets: RuleSet[] = [
  {
    id: 'ruleset_1',
    name: 'Customer Data Standardization',
    description: 'Standardization rules for customer demographic data including names, addresses, and contact information.',
    version: '2.1.0',
    author: 'Data Quality Team',
    created: new Date('2024-01-15'),
    lastModified: new Date('2024-03-10'),
    inputSchema: mockSchemaFields,
    rules: [
      createRule(
        'rule_1',
        'Email Normalization',
        'email',
        'Normalize',
        '^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$',
        true,
        90,
        '${LOWER(email)}',
        'Convert email to lowercase',
        'regex'
      ),
      createRule(
        'rule_2',
        'Phone Number Cleanse',
        'phone',
        'Cleanse',
        '[^\\d]',
        true,
        85,
        '',
        'Remove non-numeric characters from phone numbers',
        'regex'
      ),
      createRule(
        'rule_3',
        'First Name Standardization',
        'first_name',
        'Title Case',
        '.*',
        true,
        80,
        '${PROPER(first_name)}',
        'Convert first name to proper case',
        'contains'
      ),
      createRule(
        'rule_4',
        'Last Name Standardization',
        'last_name',
        'Title Case',
        '.*',
        true,
        80,
        '${PROPER(last_name)}',
        'Convert last name to proper case',
        'contains'
      ),
      createRule(
        'rule_5',
        'State Abbreviation',
        'state',
        'Abbreviation expansion',
        'california|calif|ca',
        true,
        75,
        'CA',
        'Standardize state abbreviations',
        'dictionary',
        mockLookups[0]
      ),
      createRule(
        'rule_6',
        'Country Uppercase',
        'country',
        'UPPERCASE',
        '.*',
        true,
        70,
        '${UPPER(country)}',
        'Convert country to uppercase',
        'contains'
      ),
      createRule(
        'rule_7',
        'Address Cleanse',
        'address',
        'Remove noise characters',
        '[#&*@]',
        true,
        60,
        '',
        'Remove special characters from addresses',
        'regex'
      ),
      createRule(
        'rule_8',
        'ZIP Code Validation',
        'zip_code',
        'Format',
        '^\\d{5}(-\\d{4})?$',
        false,
        85,
        '${zip_code}',
        'Validate ZIP code format',
        'regex'
      )
    ]
  },
  {
    id: 'ruleset_2',
    name: 'Product Data Cleansing',
    description: 'Rules for standardizing product catalog data including names, categories, and prices.',
    version: '1.0.0',
    author: 'Catalog Team',
    created: new Date('2024-02-01'),
    lastModified: new Date('2024-02-28'),
    inputSchema: [
      { name: 'product_id', type: 'string', nullable: false, description: 'Product identifier' },
      { name: 'product_name', type: 'string', nullable: false, description: 'Product name' },
      { name: 'category', type: 'string', nullable: true, description: 'Product category' },
      { name: 'price', type: 'decimal', nullable: false, description: 'Product price' },
      { name: 'sku', type: 'string', nullable: false, description: 'Stock keeping unit' },
      { name: 'in_stock', type: 'boolean', nullable: false, description: 'Availability status' }
    ],
    rules: [
      createRule(
        'rule_9',
        'Product Name Trim',
        'product_name',
        'Trim whitespace',
        '^\\s+|\\s+$',
        true,
        95,
        '',
        'Trim leading/trailing whitespace',
        'regex'
      ),
      createRule(
        'rule_10',
        'Price Formatting',
        'price',
        'Format',
        '^\\$?(\\d+(?:\\.\\d{2})?)$',
        true,
        90,
        '${1}',
        'Extract numeric price',
        'regex'
      ),
      createRule(
        'rule_11',
        'SKU Standardization',
        'sku',
        'UPPERCASE',
        '.*',
        true,
        85,
        '${UPPER(sku)}',
        'Convert SKU to uppercase',
        'contains'
      )
    ]
  },
  {
    id: 'ruleset_3',
    name: 'Financial Data Validation',
    description: 'Rules for validating and standardizing financial transaction data.',
    version: '1.2.0',
    author: 'Finance Team',
    created: new Date('2024-03-01'),
    lastModified: new Date('2024-03-20'),
    inputSchema: [
      { name: 'transaction_id', type: 'string', nullable: false, description: 'Transaction identifier' },
      { name: 'amount', type: 'decimal', nullable: false, description: 'Transaction amount' },
      { name: 'currency', type: 'string', nullable: false, description: 'Currency code' },
      { name: 'transaction_date', type: 'date', nullable: false, description: 'Transaction date' },
      { name: 'account_number', type: 'string', nullable: false, description: 'Account number' }
    ],
    rules: [
      createRule(
        'rule_12',
        'Currency Code Standardization',
        'currency',
        'UPPERCASE',
        '.*',
        true,
        90,
        '${UPPER(currency)}',
        'Convert currency codes to uppercase',
        'contains'
      ),
      createRule(
        'rule_13',
        'Date Standardization',
        'transaction_date',
        'Standardize date',
        '\\d{4}-\\d{2}-\\d{2}',
        true,
        85,
        '${transaction_date}',
        'Ensure date format is YYYY-MM-DD',
        'regex'
      ),
      createRule(
        'rule_14',
        'Account Number Masking',
        'account_number',
        'Format',
        '(\\d{4})(\\d{4})(\\d{4})(\\d{4})',
        true,
        80,
        '****-****-****-${4}',
        'Mask all but last 4 digits of account number',
        'regex'
      )
    ]
  }
];

// Additional mock data for testing
export const mockTestResults = [
  { 
    ruleId: 'rule_1',
    testCaseId: 'test_1',
    input: 'john.doe@gmail.com',
    output: 'john.doe@gmail.com',
    passed: true,
    executionTime: 12
  },
  { 
    ruleId: 'rule_2',
    testCaseId: 'test_2',
    input: '(123) 456-7890',
    output: '1234567890',
    passed: true,
    executionTime: 8
  },
  { 
    ruleId: 'rule_3',
    testCaseId: 'test_3',
    input: 'JOHN',
    output: 'John',
    passed: true,
    executionTime: 5
  },
  { 
    ruleId: 'rule_5',
    testCaseId: 'test_4',
    input: 'california',
    output: 'CA',
    passed: true,
    executionTime: 10
  },
  { 
    ruleId: 'rule_6',
    testCaseId: 'test_6',
    input: 'united states',
    output: 'UNITED STATES',
    passed: true,
    executionTime: 6
  }
];

export const mockRuleExecutionStats = {
  totalRules: 14,
  enabledRules: 13,
  disabledRules: 1,
  averageExecutionTime: 8.5,
  totalExecutions: 1245,
  successRate: 98.7
};

export const mockDataQualityMetrics = {
  completeness: 96.5,
  accuracy: 97.8,
  consistency: 95.2,
  timeliness: 99.1,
  validity: 98.4,
  uniqueness: 99.7
};