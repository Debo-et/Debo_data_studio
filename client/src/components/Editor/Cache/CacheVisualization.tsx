// components/CacheVisualization.tsx
import React from 'react';
import { Card } from 'antd';

interface CacheEntry {
  key: string;
  value: any;
  age: number;
  hits: number;
}

interface CacheVisualizationProps {
  entries: CacheEntry[];
  maxEntries: number;
}

export const CacheVisualization: React.FC<CacheVisualizationProps> = ({ entries, maxEntries }) => {
  return (
    <Card title="Cache Visualization" bordered={false}>
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: 8,
        minHeight: 200,
        padding: 16,
        backgroundColor: '#fafafa',
        borderRadius: 4,
      }}>
        {entries.map((entry, index) => (
          <div
            key={index}
            style={{
              padding: 8,
              border: '1px solid #d9d9d9',
              borderRadius: 4,
              backgroundColor: entry.hits > 10 ? '#d6e4ff' : '#ffffff',
              transition: 'all 0.3s',
              cursor: 'pointer',
            }}
            title={`Key: ${entry.key}\nHits: ${entry.hits}\nAge: ${entry.age}s`}
          >
            <div style={{ fontWeight: 'bold', fontSize: 12 }}>{entry.key}</div>
            <div style={{ fontSize: 10, color: '#666' }}>
              {typeof entry.value === 'object' 
                ? JSON.stringify(entry.value).substring(0, 20) + '...'
                : String(entry.value).substring(0, 20)
              }
            </div>
            <div style={{ fontSize: 9, color: '#999', marginTop: 2 }}>
              ♥ {entry.hits} • ⏱ {entry.age}s
            </div>
          </div>
        ))}
        
        {Array(maxEntries - entries.length).fill(0).map((_, index) => (
          <div
            key={`empty-${index}`}
            style={{
              width: 100,
              height: 60,
              border: '1px dashed #d9d9d9',
              borderRadius: 4,
              backgroundColor: '#f5f5f5',
            }}
          />
        ))}
      </div>
    </Card>
  );
};