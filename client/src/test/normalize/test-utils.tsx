// src/test/test-utils.tsx
import React from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { SimpleColumn } from '../../pages/canvas.types';

// Mock data for NormalizeEditor
export const mockInputColumns: SimpleColumn[] = [
  { name: 'id', type: 'integer', id: 'col1' },
  { name: 'name', type: 'string', id: 'col2' },
  { name: 'tags', type: 'string', id: 'col3' },
  { name: 'department', type: 'string', id: 'col4' },
];

// Mock sample rows (as they would appear in preview)
export const mockSampleData = [
  { id: 1, name: 'Alice', tags: 'a,b,c', department: 'Engineering' },
  { id: 2, name: 'Bob', tags: 'x,y', department: 'Marketing' },
  { id: 3, name: 'Charlie', tags: 'p,q,r,s', department: 'Sales' },
  { id: 4, name: 'David', tags: '', department: 'Finance' },        // empty string
  { id: 5, name: 'Eve', tags: null, department: 'HR' },             // null
];

// Mock for DenormalizeEditor input columns (similar structure)
export const mockDenormalizeInputColumns: SimpleColumn[] = [
  { name: 'order_id', type: 'integer', id: 'c1' },
  { name: 'product', type: 'string', id: 'c2' },
  { name: 'quantity', type: 'integer', id: 'c3' },
];

// Mock denormalize sample data (multiple rows per order)
export const mockDenormalizeSampleData = [
  { order_id: 101, product: 'Laptop', quantity: 1 },
  { order_id: 101, product: 'Mouse', quantity: 2 },
  { order_id: 102, product: 'Keyboard', quantity: 1 },
];

// Custom render with providers if needed
const AllProviders = ({ children }: { children: React.ReactNode }) => <>{children}</>;

export function customRender(ui: React.ReactElement, options?: RenderOptions) {
  return render(ui, { wrapper: AllProviders, ...options });
}