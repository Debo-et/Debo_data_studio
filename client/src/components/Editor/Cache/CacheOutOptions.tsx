// components/CacheOutOptions.tsx
import React from 'react';
import { Card, Select, Input, Form, Row, Col, Tag, Switch } from 'antd';

const { Option } = Select;
const { TextArea } = Input;

interface CacheOutOptionsProps {
  config: any;
  onConfigChange: (key: string, value: any) => void;
}

export const CacheOutOptions: React.FC<CacheOutOptionsProps> = ({ config, onConfigChange }) => {
  const lookupTypes = [
    { value: 'exact', label: 'Exact Match', desc: 'Match complete key' },
    { value: 'prefix', label: 'Prefix Match', desc: 'Match beginning of key' },
    { value: 'range', label: 'Range Match', desc: 'Match within key range' },
  ];

  const missHandlingOptions = [
    { value: 'null', label: 'Return Null' },
    { value: 'error', label: 'Throw Error' },
    { value: 'skip', label: 'Skip Entry' },
  ];

  const handleDefaultValuesChange = (value: string) => {
    try {
      const parsed = JSON.parse(value);
      onConfigChange('defaultValues', parsed);
    } catch {
      // Keep as is if invalid JSON
    }
  };

  return (
    <Card title="Cache OUT (Read) Options" bordered={false}>
      <Row gutter={16}>
        <Col span={12}>
          <Form layout="vertical">
            <Form.Item label="Lookup Type">
              <Select
                value={config.lookupType}
                onChange={(value) => onConfigChange('lookupType', value)}
              >
                {lookupTypes.map(type => (
                  <Option key={type.value} value={type.value}>
                    <div>
                      <strong>{type.label}</strong>
                      <div style={{ fontSize: 12, color: '#666' }}>{type.desc}</div>
                    </div>
                  </Option>
                ))}
              </Select>
            </Form.Item>

            <Form.Item label="Miss Handling">
              <Select
                value={config.onMiss}
                onChange={(value) => onConfigChange('onMiss', value)}
              >
                {missHandlingOptions.map(option => (
                  <Option key={option.value} value={option.value}>
                    {option.label}
                  </Option>
                ))}
              </Select>
            </Form.Item>

            <Form.Item label="Default Values (JSON)">
              <TextArea
                rows={3}
                value={JSON.stringify(config.defaultValues || {}, null, 2)}
                onChange={(e) => handleDefaultValuesChange(e.target.value)}
                placeholder='{"field": "default_value"}'
              />
            </Form.Item>

            <Form.Item label="Cache Warming">
              <Switch
                checked={config.cacheWarming}
                onChange={(checked) => onConfigChange('cacheWarming', checked)}
              />
              <span style={{ marginLeft: 8 }}>
                {config.cacheWarming ? 'Enabled' : 'Disabled'}
              </span>
            </Form.Item>

            {config.cacheWarming && (
              <Form.Item label="Warmup Strategy">
                <Select
                  value={config.warmupStrategy}
                  onChange={(value) => onConfigChange('warmupStrategy', value)}
                >
                  <Option value="eager">Eager (pre-load all)</Option>
                  <Option value="lazy">Lazy (load on access)</Option>
                </Select>
              </Form.Item>
            )}
          </Form>
        </Col>
        <Col span={12}>
          <div style={{ padding: '16px', backgroundColor: '#fafafa', borderRadius: 4 }}>
            <h4>Read Behavior</h4>
            <div style={{ marginTop: 16 }}>
              <div style={{ marginBottom: 12 }}>
                <div style={{ fontWeight: 'bold' }}>Lookup Type:</div>
                <Tag color={
                  config.lookupType === 'exact' ? 'blue' :
                  config.lookupType === 'prefix' ? 'green' : 'orange'
                }>
                  {config.lookupType?.toUpperCase() || 'NOT SET'}
                </Tag>
              </div>
              <div style={{ marginBottom: 12 }}>
                <div style={{ fontWeight: 'bold' }}>On Miss:</div>
                <Tag color={
                  config.onMiss === 'error' ? 'red' :
                  config.onMiss === 'skip' ? 'orange' : 'gray'
                }>
                  {config.onMiss?.toUpperCase() || 'NOT SET'}
                </Tag>
              </div>
              <div style={{ marginBottom: 12 }}>
                <div style={{ fontWeight: 'bold' }}>Cache Warming:</div>
                <Tag color={config.cacheWarming ? 'green' : 'default'}>
                  {config.cacheWarming ? 'ENABLED' : 'DISABLED'}
                </Tag>
              </div>
            </div>
          </div>
        </Col>
      </Row>
    </Card>
  );
};