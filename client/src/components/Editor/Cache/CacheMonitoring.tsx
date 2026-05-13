// components/CacheMonitoring.tsx
import React from 'react';
import { Card, Button, Progress, Row, Col, Statistic, Timeline, Space } from 'antd';
import { ClearOutlined, ReloadOutlined, ExportOutlined } from '@ant-design/icons';
import { CacheStats } from './cache.types';

interface CacheMonitoringProps {
  stats?: CacheStats;
  onClearCache: () => void;
  onRefresh: () => void;
  onExport: () => void;
}

export const CacheMonitoring: React.FC<CacheMonitoringProps> = ({
  stats,
  onClearCache,
  onRefresh,
  onExport
}) => {
  const ageDistribution = [
    { time: '< 1m', percentage: 30 },
    { time: '1m-10m', percentage: 40 },
    { time: '10m-1h', percentage: 20 },
    { time: '> 1h', percentage: 10 },
  ];

  return (
    <Card
      title="Cache Monitoring"
      bordered={false}
      extra={
        <Space>
          <Button
            icon={<ReloadOutlined />}
            onClick={onRefresh}
            size="small"
          >
            Refresh
          </Button>
          <Button
            icon={<ExportOutlined />}
            onClick={onExport}
            size="small"
          >
            Export
          </Button>
          <Button
            danger
            icon={<ClearOutlined />}
            onClick={onClearCache}
            size="small"
          >
            Clear Cache
          </Button>
        </Space>
      }
    >
      <Row gutter={16}>
        <Col span={8}>
          <div style={{ marginBottom: 16 }}>
            <div style={{ marginBottom: 8 }}>Hit/Miss Ratio</div>
            <Progress
              type="dashboard"
              percent={stats?.hitRate || 0}
              strokeColor={{
                '0%': '#108ee9',
                '100%': '#87d068',
              }}
            />
            <div style={{ textAlign: 'center', marginTop: 8 }}>
              <Statistic title="Hits" value={stats?.hitRate || 0} suffix="%" />
              <Statistic title="Misses" value={stats?.missRate || 0} suffix="%" />
            </div>
          </div>
        </Col>

        <Col span={8}>
          <div style={{ marginBottom: 16 }}>
            <div style={{ marginBottom: 8 }}>Memory Usage</div>
            <Progress
              type="circle"
              percent={Math.min(((stats?.memoryUsage || 0) / 100) * 100, 100)}
              strokeColor={
                (stats?.memoryUsage || 0) > 80 ? '#ff4d4f' :
                (stats?.memoryUsage || 0) > 60 ? '#faad14' : '#52c41a'
              }
            />
            <div style={{ textAlign: 'center', marginTop: 8 }}>
              <Statistic
                title="Used"
                value={stats?.memoryUsage || 0}
                suffix="MB"
                precision={1}
              />
              <Statistic
                title="Entries"
                value={stats?.entryCount || 0}
              />
            </div>
          </div>
        </Col>

        <Col span={8}>
          <div style={{ marginBottom: 16 }}>
            <h4>Age Distribution</h4>
            <Timeline>
              {ageDistribution.map((item, index) => (
                <Timeline.Item key={index} color={
                  item.percentage > 50 ? 'green' :
                  item.percentage > 30 ? 'blue' : 'gray'
                }>
                  {item.time}: {item.percentage}%
                </Timeline.Item>
              ))}
            </Timeline>
            <div style={{ marginTop: 16 }}>
              <Statistic
                title="Average Age"
                value={stats?.averageAge || 0}
                suffix="seconds"
              />
            </div>
          </div>
        </Col>
      </Row>

      <div style={{ marginTop: 16 }}>
        <h4>Connection Status</h4>
        <div style={{ display: 'flex', alignItems: 'center' }}>
          <div
            style={{
              width: 12,
              height: 12,
              borderRadius: '50%',
              backgroundColor: stats ? '#52c41a' : '#ff4d4f',
              marginRight: 8,
            }}
          />
          <span>{stats ? 'Connected' : 'Disconnected'}</span>
        </div>
      </div>
    </Card>
  );
};