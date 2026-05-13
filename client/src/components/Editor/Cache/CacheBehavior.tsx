// components/CacheBehavior.tsx
import React from 'react';
import { Card, InputNumber, Select, Switch, Form, Row, Col, Progress } from 'antd';

const { Option } = Select;

interface CacheBehaviorProps {
  config: any;
  onConfigChange: (key: string, value: any) => void;
}

export const CacheBehavior: React.FC<CacheBehaviorProps> = ({ config, onConfigChange }) => {
  const evictionPolicies = [
    { value: 'lru', label: 'LRU', desc: 'Least Recently Used' },
    { value: 'fifo', label: 'FIFO', desc: 'First In First Out' },
    { value: 'lfu', label: 'LFU', desc: 'Least Frequently Used' },
  ];

  const timeUnits = [
    { value: 'seconds', label: 'Seconds' },
    { value: 'minutes', label: 'Minutes' },
    { value: 'hours', label: 'Hours' },
    { value: 'days', label: 'Days' },
  ];

  const estimateMemoryUsage = () => {
    const avgEntrySize = 1024; // Assume 1KB per entry
    const entries = config.maxSize || 1000;
    return (entries * avgEntrySize) / (1024 * 1024); // Convert to MB
  };

  const estimateCompressionRatio = () => {
    return config.compression ? 0.7 : 1.0; // 30% compression
  };

  const getProgressStatus = (usage: number): 'success' | 'exception' | 'normal' | 'active' | undefined => {
    if (usage > 80) return 'exception';
    if (usage > 60) return 'active';
    return 'normal';
  };

  const getProgressColor = (usage: number): string | undefined => {
    if (usage > 60 && usage <= 80) return '#faad14'; // Yellow color for warning
    return undefined;
  };

  return (
    <Card title="Cache Behavior" bordered={false}>
      <Row gutter={16}>
        <Col span={12}>
          <Form layout="vertical">
            <Form.Item label="Time-to-Live (TTL)">
              <Row gutter={8}>
                <Col span={16}>
                  <InputNumber
                    style={{ width: '100%' }}
                    value={config.ttl}
                    onChange={(value) => onConfigChange('ttl', value)}
                    min={0}
                    placeholder="No expiration"
                  />
                </Col>
                <Col span={8}>
                  <Select
                    value={config.ttlUnit || 'seconds'}
                    onChange={(value) => onConfigChange('ttlUnit', value)}
                  >
                    {timeUnits.map(unit => (
                      <Option key={unit.value} value={unit.value}>
                        {unit.label}
                      </Option>
                    ))}
                  </Select>
                </Col>
              </Row>
            </Form.Item>

            <Form.Item label="Maximum Entries">
              <InputNumber
                style={{ width: '100%' }}
                value={config.maxSize}
                onChange={(value) => onConfigChange('maxSize', value)}
                min={1}
                placeholder="Unlimited"
              />
            </Form.Item>

            <Form.Item label="Eviction Policy">
              <Select
                value={config.evictionPolicy}
                onChange={(value) => onConfigChange('evictionPolicy', value)}
              >
                {evictionPolicies.map(policy => (
                  <Option key={policy.value} value={policy.value}>
                    <div>
                      <strong>{policy.label}</strong>
                      <div style={{ fontSize: 12, color: '#666' }}>{policy.desc}</div>
                    </div>
                  </Option>
                ))}
              </Select>
            </Form.Item>

            <Form.Item label="Compression">
              <Switch
                checked={config.compression}
                onChange={(checked) => onConfigChange('compression', checked)}
              />
              <span style={{ marginLeft: 8 }}>
                {config.compression ? 'Enabled' : 'Disabled'}
              </span>
            </Form.Item>
          </Form>
        </Col>

        <Col span={12}>
          <div style={{ padding: '0 16px' }}>
            <h4>Memory Estimation</h4>
            <Progress
              percent={Math.min((estimateMemoryUsage() / 100) * 100, 100)}
              status={getProgressStatus(estimateMemoryUsage())}
              strokeColor={getProgressColor(estimateMemoryUsage())}
              format={() => `${estimateMemoryUsage().toFixed(1)} MB`}
            />
            <div style={{ fontSize: 12, color: '#666', marginTop: 4 }}>
              Estimated for {config.maxSize || 1000} entries
            </div>

            <div style={{ marginTop: 24 }}>
              <h4>Compression Impact</h4>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <div style={{ flex: 1, backgroundColor: '#e6f7ff', padding: 8 }}>
                  <div>Original: 100%</div>
                  <div style={{ height: 20, backgroundColor: '#1890ff', marginTop: 4 }} />
                </div>
                <div style={{ margin: '0 8px' }}>→</div>
                <div style={{ flex: estimateCompressionRatio(), backgroundColor: '#f6ffed', padding: 8 }}>
                  <div>Compressed: {Math.round(estimateCompressionRatio() * 100)}%</div>
                  <div
                    style={{
                      height: 20,
                      backgroundColor: '#52c41a',
                      marginTop: 4,
                      width: `${estimateCompressionRatio() * 100}%`
                    }}
                  />
                </div>
              </div>
            </div>

            {config.evictionPolicy && (
              <div style={{ marginTop: 24 }}>
                <h4>Eviction Visualization</h4>
                <div style={{
                  height: 60,
                  backgroundColor: '#f5f5f5',
                  borderRadius: 4,
                  padding: 8,
                  position: 'relative'
                }}>
                  {/* Simplified visualization of cache entries */}
                  {[1, 2, 3, 4, 5].map(i => (
                    <div
                      key={i}
                      style={{
                        position: 'absolute',
                        left: `${i * 15}%`,
                        bottom: 8,
                        width: 20,
                        height: 20,
                        backgroundColor: i === 1 ? '#ff4d4f' : '#1890ff',
                        borderRadius: 2,
                        transition: 'all 0.3s',
                      }}
                    />
                  ))}
                  <div style={{ position: 'absolute', bottom: 32, fontSize: 12 }}>
                    {config.evictionPolicy === 'lru' && '← Least Recently Used'}
                    {config.evictionPolicy === 'fifo' && '← First In'}
                    {config.evictionPolicy === 'lfu' && '← Least Frequently Used'}
                  </div>
                </div>
              </div>
            )}
          </div>
        </Col>
      </Row>
    </Card>
  );
};