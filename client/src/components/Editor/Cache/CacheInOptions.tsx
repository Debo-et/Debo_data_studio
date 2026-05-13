// components/CacheInOptions.tsx
import React from 'react';
import { Card, Switch, InputNumber, Form, Row, Col, Tag } from 'antd'; // Added Tag here

interface CacheInOptionsProps {
  config: any;
  onConfigChange: (key: string, value: any) => void;
}

export const CacheInOptions: React.FC<CacheInOptionsProps> = ({ config, onConfigChange }) => {
  return (
    <Card title="Cache IN (Write) Options" bordered={false}>
      <Row gutter={16}>
        <Col span={12}>
          <Form layout="vertical">
            <Form.Item label="Write Through">
              <Switch
                checked={config.writeThrough}
                onChange={(checked) => onConfigChange('writeThrough', checked)}
              />
              <span style={{ marginLeft: 8 }}>
                {config.writeThrough ? 'Enabled' : 'Disabled'}
              </span>
              <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>
                Writes to cache and backing store simultaneously
              </div>
            </Form.Item>

            <Form.Item label="Batch Size">
              <InputNumber
                style={{ width: '100%' }}
                value={config.batchSize}
                onChange={(value) => onConfigChange('batchSize', value)}
                min={1}
                max={1000}
                placeholder="Number of entries per batch"
              />
              <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>
                Number of entries to write in a single batch operation
              </div>
            </Form.Item>
          </Form>
        </Col>
        <Col span={12}>
          <div style={{ padding: '16px', backgroundColor: '#fafafa', borderRadius: 4 }}>
            <h4>Write Strategy</h4>
            <div style={{ marginTop: 8 }}>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                <div style={{ width: 100 }}>Write Through:</div>
                <Tag color={config.writeThrough ? 'green' : 'default'}>
                  {config.writeThrough ? 'ON' : 'OFF'}
                </Tag>
              </div>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <div style={{ width: 100 }}>Batch Size:</div>
                <Tag color="blue">{config.batchSize || 1}</Tag>
              </div>
            </div>
          </div>
        </Col>
      </Row>
    </Card>
  );
};