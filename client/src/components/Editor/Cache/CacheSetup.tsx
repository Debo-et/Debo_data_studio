// components/CacheSetup.tsx
import React from 'react';
import { Card, Input, Select, Form, Row, Col, Tag, Statistic } from 'antd';
import { DatabaseOutlined, CloudServerOutlined, HddOutlined } from '@ant-design/icons';
import { CacheStats } from './cache.types';

const { Option } = Select;

interface CacheSetupProps {
  config: any;
  onConfigChange: (key: string, value: any) => void;
  stats?: CacheStats;
}

export const CacheSetup: React.FC<CacheSetupProps> = ({ config, onConfigChange, stats }) => {
  const persistenceOptions = [
    { value: 'memory', label: 'Memory', icon: <DatabaseOutlined />, color: 'green' },
    { value: 'disk', label: 'Disk', icon: <HddOutlined />, color: 'blue' },
    { value: 'redis', label: 'Redis', icon: <CloudServerOutlined />, color: 'red' },
  ];

  return (
    <Card title="Cache Setup" bordered={false}>
      <Row gutter={16}>
        <Col span={12}>
          <Form layout="vertical">
            <Form.Item
              label="Cache Name"
              required
              rules={[{ required: true, message: 'Cache name is required' }]}
            >
              <Input
                value={config.cacheName}
                onChange={(e) => onConfigChange('cacheName', e.target.value)}
                placeholder="Enter cache name"
              />
            </Form.Item>

            <Form.Item label="Persistence Layer">
              <Select
                value={config.persistence}
                onChange={(value) => onConfigChange('persistence', value)}
              >
                {persistenceOptions.map(opt => (
                  <Option key={opt.value} value={opt.value}>
                    {opt.icon} {opt.label}
                  </Option>
                ))}
              </Select>
            </Form.Item>

            {config.persistence === 'redis' && (
              <>
                <Form.Item label="Redis Host">
                  <Input
                    value={config.redisConfig?.host}
                    onChange={(e) => onConfigChange('redisConfig', {
                      ...config.redisConfig,
                      host: e.target.value
                    })}
                    placeholder="localhost"
                  />
                </Form.Item>
                <Row gutter={8}>
                  <Col span={12}>
                    <Form.Item label="Port">
                      <Input
                        type="number"
                        value={config.redisConfig?.port}
                        onChange={(e) => onConfigChange('redisConfig', {
                          ...config.redisConfig,
                          port: parseInt(e.target.value)
                        })}
                        placeholder="6379"
                      />
                    </Form.Item>
                  </Col>
                  <Col span={12}>
                    <Form.Item label="Database">
                      <Input
                        type="number"
                        value={config.redisConfig?.db}
                        onChange={(e) => onConfigChange('redisConfig', {
                          ...config.redisConfig,
                          db: parseInt(e.target.value)
                        })}
                        placeholder="0"
                      />
                    </Form.Item>
                  </Col>
                </Row>
              </>
            )}

            {config.persistence === 'disk' && (
              <Form.Item label="Storage Path">
                <Input
                  value={config.diskConfig?.path}
                  onChange={(e) => onConfigChange('diskConfig', {
                    ...config.diskConfig,
                    path: e.target.value
                  })}
                  placeholder="/var/cache"
                />
              </Form.Item>
            )}
          </Form>
        </Col>

        <Col span={12}>
          <Card title="Cache Statistics" size="small">
            {stats ? (
              <Row gutter={16}>
                <Col span={12}>
                  <Statistic title="Hit Rate" value={stats.hitRate} suffix="%" precision={1} />
                  <div style={{ height: 4, backgroundColor: '#f0f0f0', marginTop: 8 }}>
                    <div
                      style={{
                        width: `${stats.hitRate}%`,
                        height: '100%',
                        backgroundColor: '#52c41a'
                      }}
                    />
                  </div>
                </Col>
                <Col span={12}>
                  <Statistic title="Miss Rate" value={stats.missRate} suffix="%" precision={1} />
                  <div style={{ height: 4, backgroundColor: '#f0f0f0', marginTop: 8 }}>
                    <div
                      style={{
                        width: `${stats.missRate}%`,
                        height: '100%',
                        backgroundColor: '#ff4d4f'
                      }}
                    />
                  </div>
                </Col>
                <Col span={12}>
                  <Statistic title="Entries" value={stats.entryCount} />
                </Col>
                <Col span={12}>
                  <Statistic title="Memory Usage" 
                    value={stats.memoryUsage} 
                    suffix="MB" 
                    precision={1}
                  />
                </Col>
              </Row>
            ) : (
              <div style={{ textAlign: 'center', padding: 20 }}>
                No statistics available
              </div>
            )}
          </Card>

          <div style={{ marginTop: 16 }}>
            <Tag color={config.persistence === 'memory' ? 'green' : 
                       config.persistence === 'disk' ? 'blue' : 'red'}>
              {config.persistence ? config.persistence.toUpperCase() : 'NOT SET'}
            </Tag>
            <span style={{ marginLeft: 8 }}>Persistence</span>
          </div>
        </Col>
      </Row>
    </Card>
  );
};