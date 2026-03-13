// FileLookupConfigEditor.tsx
import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Upload, Table, Select, InputNumber, Switch, Button,
  Card, Row, Col, Divider, Typography, Progress, Tag,
  Form, Alert, Space, Radio} from 'antd';
import {
  UploadOutlined, LinkOutlined, CheckOutlined,
  CloseOutlined, PlayCircleOutlined,
  DragOutlined, DatabaseOutlined, FileExcelOutlined,
  FileTextOutlined, CodeOutlined, HddOutlined
} from '@ant-design/icons';
import { 
  LookupConfig, FileInfo, SchemaField, ConnectionLine, TestResult 
} from './types';
import './FileLookupConfigEditor.css';

const { Title, Text } = Typography;
const { Option } = Select;

// Create a type alias for file types to ensure consistency
type FileType = 'csv' | 'json' | 'excel' | 'parquet';

const FileLookupConfigEditor: React.FC = () => {
  // State management
  const [config, setConfig] = useState<LookupConfig>({
    keyField: '',
    filePath: '',
    fieldMapping: {},
    fileType: 'csv',
    cacheSize: 1000,
    reloadOnChange: false,
    joinType: 'left'
  });

  const [inputSchema] = useState<SchemaField[]>([
    { name: 'customer_id', type: 'string', sampleValues: ['C001', 'C002', 'C003'] },
    { name: 'order_date', type: 'date', sampleValues: ['2024-01-15', '2024-01-16'] },
    { name: 'amount', type: 'number', sampleValues: [150.50, 299.99] }
  ]);

  const [fileInfo, setFileInfo] = useState<FileInfo | null>(null);
  const [connectionLines, setConnectionLines] = useState<ConnectionLine[]>([]);
  const [isDragging, setIsDragging] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<TestResult | null>(null);
  const [isTesting, setIsTesting] = useState(false);
  const [memoryUsage, setMemoryUsage] = useState<number>(0);

  // Refs for drag-and-drop connections
  const fieldRefs = useRef<Record<string, HTMLDivElement>>({});
  const containerRef = useRef<HTMLDivElement>(null);

  // Calculate memory usage based on cache size
  useEffect(() => {
    const estimate = (config.cacheSize || 0) * 1024; // Rough estimate: 1KB per record
    setMemoryUsage(estimate);
  }, [config.cacheSize]);

  // File upload handler
  const handleFileUpload = async (file: File) => {
    const fileType = detectFileType(file.name);
    
    // Simulate file parsing and preview
    const mockColumns = ['customer_id', 'name', 'email', 'segment', 'region'];
    const mockData = Array.from({ length: 10 }, (_, i) => ({
      customer_id: `C${String(i+1).padStart(3, '0')}`,
      name: `Customer ${i+1}`,
      email: `customer${i+1}@example.com`,
      segment: ['Premium', 'Standard', 'Basic'][i % 3],
      region: ['North', 'South', 'East', 'West'][i % 4]
    }));

    // Create the fileInfo object with proper typing
    const newFileInfo: FileInfo = {
      name: file.name,
      size: file.size,
      type: fileType,
      lastModified: new Date(file.lastModified),
      previewData: mockData,
      columns: mockColumns,
      rowCount: 1000 // Mock value
    };

    setFileInfo(newFileInfo);

    setConfig(prev => ({
      ...prev,
      filePath: `uploads/${file.name}`,
      fileType: fileType // This is now correctly typed
    }));
  };

  const detectFileType = (fileName: string): FileType => {
    const ext = fileName.split('.').pop()?.toLowerCase() || '';
    
    switch (ext) {
      case 'csv':
        return 'csv';
      case 'json':
        return 'json';
      case 'xlsx':
      case 'xls':
        return 'excel';
      case 'parquet':
        return 'parquet';
      default:
        return 'csv'; // Default to csv
    }
  };

  // Field mapping handlers
  const handleDragStart = (fieldName: string) => {
    setIsDragging(fieldName);
  };

  const handleDragOver = (e: React.DragEvent, _columnName: string) => {
    e.preventDefault();
  };

  const handleDrop = (e: React.DragEvent, columnName: string) => {
    e.preventDefault();
    if (isDragging) {
      const newMapping = { ...config.fieldMapping, [isDragging]: columnName };
      setConfig(prev => ({ ...prev, fieldMapping: newMapping }));
      
      // Create visual connection
      updateConnectionLine(isDragging, columnName);
    }
    setIsDragging(null);
  };

  const updateConnectionLine = useCallback((fromField: string, toField: string) => {
    const fromEl = fieldRefs.current[fromField];
    const toEl = fieldRefs.current[toField];
    const container = containerRef.current;

    if (fromEl && toEl && container) {
      const fromRect = fromEl.getBoundingClientRect();
      const toRect = toEl.getBoundingClientRect();
      const containerRect = container.getBoundingClientRect();

      const newLine: ConnectionLine = {
        from: {
          x: fromRect.right - containerRect.left,
          y: fromRect.top + fromRect.height / 2 - containerRect.top
        },
        to: {
          x: toRect.left - containerRect.left,
          y: toRect.top + toRect.height / 2 - containerRect.top
        },
        fromField,
        toField
      };

      setConnectionLines(prev => [...prev.filter(l => l.fromField !== fromField), newLine]);
    }
  }, []);

  // Auto-map fields by name
  const handleAutoMap = () => {
    const newMapping: Record<string, string> = {};
    
    inputSchema.forEach(inputField => {
      const matchingColumn = fileInfo?.columns.find(col => 
        col.toLowerCase() === inputField.name.toLowerCase() ||
        col.toLowerCase().includes(inputField.name.toLowerCase())
      );
      if (matchingColumn) {
        newMapping[inputField.name] = matchingColumn;
        updateConnectionLine(inputField.name, matchingColumn);
      }
    });

    setConfig(prev => ({ ...prev, fieldMapping: newMapping }));
  };

  // Test lookup functionality
  const runTestLookup = async () => {
    setIsTesting(true);
    
    // Simulate API call
    setTimeout(() => {
      setTestResults({
        matched: 85,
        unmatched: 15,
        total: 100,
        sampleOutput: [
          { customer_id: 'C001', name: 'John Doe', email: 'john@example.com', amount: 150.50 },
          { customer_id: 'C002', name: 'Jane Smith', email: 'jane@example.com', amount: 299.99 }
        ],
        executionTime: 124,
        cacheHitRate: 67
      });
      setIsTesting(false);
    }, 1500);
  };

  // Render file type icon
  const renderFileIcon = (type: FileType) => {
    const icons = {
      csv: <FileTextOutlined style={{ color: '#1890ff' }} />,
      json: <CodeOutlined style={{ color: '#52c41a' }} />,
      excel: <FileExcelOutlined style={{ color: '#52c41a' }} />,
      parquet: <DatabaseOutlined style={{ color: '#722ed1' }} />
    };
    return icons[type];
  };

  return (
    <div className="file-lookup-editor">
      <Title level={2}>📁 File Lookup Configuration</Title>
      
      <Row gutter={[24, 24]}>
        {/* File Selection Section */}
        <Col span={24}>
          <Card 
            title={
              <Space>
                <UploadOutlined />
                File Selection & Preview
              </Space>
            }
            extra={
              fileInfo && (
                <Tag icon={renderFileIcon(fileInfo.type)}>
                  {fileInfo.type.toUpperCase()}
                </Tag>
              )
            }
          >
            <Row gutter={16}>
              <Col span={8}>
                <Upload
                  accept=".csv,.json,.xlsx,.xls,.parquet"
                  beforeUpload={(file) => {
                    handleFileUpload(file);
                    return false;
                  }}
                  showUploadList={false}
                >
                  <Button icon={<UploadOutlined />} block size="large">
                    Select Lookup File
                  </Button>
                </Upload>
                
                {fileInfo && (
                  <div className="file-info">
                    <Divider />
                    <Text strong>File Info:</Text>
                    <div className="file-stats">
                      <div>Name: {fileInfo.name}</div>
                      <div>Size: {(fileInfo.size / 1024).toFixed(2)} KB</div>
                      <div>Rows: {fileInfo.rowCount.toLocaleString()}</div>
                      <div>Columns: {fileInfo.columns.length}</div>
                    </div>
                  </div>
                )}
              </Col>
              
              <Col span={16}>
                {fileInfo ? (
                  <>
                    <Text strong>File Preview (First 10 rows):</Text>
                    <div className="table-preview">
                      <Table
                        dataSource={fileInfo.previewData}
                        columns={fileInfo.columns.map(col => ({
                          title: col,
                          dataIndex: col,
                          key: col,
                          ellipsis: true
                        }))}
                        pagination={false}
                        size="small"
                        scroll={{ x: true }}
                      />
                    </div>
                    <div className="schema-info">
                      <Text type="secondary">
                        Detected {fileInfo.columns.length} columns: {fileInfo.columns.join(', ')}
                      </Text>
                    </div>
                  </>
                ) : (
                  <div className="empty-preview">
                    <Text type="secondary">
                      Select a file to preview its contents
                    </Text>
                  </div>
                )}
              </Col>
            </Row>
          </Card>
        </Col>

        {/* Key Field Configuration */}
        <Col span={12}>
          <Card title="🔑 Key Field Configuration">
            <Form layout="vertical">
              <Form.Item
                label="Select Key Field (from Input)"
                help="This field will be used to match records between input and lookup file"
              >
                <Select
                  value={config.keyField}
                  onChange={(value) => setConfig({ ...config, keyField: value })}
                  placeholder="Choose key field"
                >
                  {inputSchema.map(field => (
                    <Option key={field.name} value={field.name}>
                      <div className="field-option">
                        <Text strong>{field.name}</Text>
                        <Text type="secondary" style={{ fontSize: '12px' }}>
                          Type: {field.type} | Sample: {field.sampleValues.slice(0, 2).join(', ')}
                        </Text>
                      </div>
                    </Option>
                  ))}
                </Select>
              </Form.Item>

              {config.keyField && fileInfo && (
                <Alert
                  message={
                    fileInfo.columns.includes(config.keyField) 
                      ? `✓ Key field "${config.keyField}" exists in lookup file`
                      : `⚠ Key field "${config.keyField}" NOT found in lookup file columns`
                  }
                  type={fileInfo.columns.includes(config.keyField) ? 'success' : 'warning'}
                  showIcon
                />
              )}

              <Form.Item
                label="Lookup File Key Column"
                help="Column in the lookup file that matches the input key field"
              >
                <Select
                  value={config.fieldMapping[config.keyField]}
                  onChange={(value) => {
                    const newMapping = { ...config.fieldMapping, [config.keyField]: value };
                    setConfig({ ...config, fieldMapping: newMapping });
                    updateConnectionLine(config.keyField, value);
                  }}
                  placeholder="Select matching column"
                  disabled={!config.keyField}
                >
                  {fileInfo?.columns.map(col => (
                    <Option key={col} value={col}>{col}</Option>
                  ))}
                </Select>
              </Form.Item>
            </Form>
          </Card>
        </Col>

        {/* Cache Settings */}
        <Col span={12}>
          <Card title="⚡ Lookup Settings">
            <Form layout="vertical">
              <Form.Item label="Cache Size (records)">
                <InputNumber
                  min={0}
                  max={100000}
                  value={config.cacheSize}
                  onChange={(value) => setConfig({ ...config, cacheSize: value || 0 })}
                  style={{ width: '100%' }}
                />
                <div className="cache-visualization">
                  <Progress
                    percent={Math.min((config.cacheSize || 0) / 10000 * 100, 100)}
                    size="small"
                    status="active"
                  />
                  <Text type="secondary">
                    Memory usage: {(memoryUsage / 1024).toFixed(2)} MB estimated
                  </Text>
                </div>
              </Form.Item>

              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item label="Join Type">
                    <Radio.Group
                      value={config.joinType}
                      onChange={(e) => setConfig({ ...config, joinType: e.target.value })}
                    >
                      <Radio value="left">Left Join</Radio>
                      <Radio value="inner">Inner Join</Radio>
                    </Radio.Group>
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item label="Reload on Change">
                    <Switch
                      checked={config.reloadOnChange}
                      onChange={(checked) => setConfig({ ...config, reloadOnChange: checked })}
                    />
                    <Text type="secondary" style={{ marginLeft: 8 }}>
                      {config.reloadOnChange ? 'Watching file changes' : 'Manual reload only'}
                    </Text>
                  </Form.Item>
                </Col>
              </Row>
            </Form>
          </Card>
        </Col>

        {/* Field Mapping Matrix */}
        <Col span={24}>
          <Card 
            title={
              <Space>
                <LinkOutlined />
                Field Mapping Matrix
                <Button 
                  size="small" 
                  onClick={handleAutoMap}
                  icon={<DragOutlined />}
                >
                  Auto-Map by Name
                </Button>
              </Space>
            }
          >
            <div ref={containerRef} className="mapping-container">
              {/* Connection lines SVG */}
              <svg className="connection-lines">
                {connectionLines.map((line, index) => (
                  <line
                    key={index}
                    x1={line.from.x}
                    y1={line.from.y}
                    x2={line.to.x}
                    y2={line.to.y}
                    stroke="#1890ff"
                    strokeWidth="2"
                    strokeDasharray="5,5"
                  />
                ))}
              </svg>

              <Row gutter={48}>
                {/* Input Fields */}
                <Col span={12}>
                  <div className="field-section">
                    <Title level={4}>Input Fields (Upstream)</Title>
                    {inputSchema.map(field => (
                      <div
                        key={field.name}
                        ref={el => { if (el) fieldRefs.current[field.name] = el; }}
                        className={`field-item ${
                          isDragging === field.name ? 'dragging' : ''
                        } ${config.fieldMapping[field.name] ? 'mapped' : ''}`}
                        draggable
                        onDragStart={() => handleDragStart(field.name)}
                      >
                        <Space>
                          <DragOutlined className="drag-handle" />
                          <div className="field-info">
                            <Text strong>{field.name}</Text>
                            <br />
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                              {field.type} • {field.sampleValues.slice(0, 3).join(', ')}
                            </Text>
                          </div>
                          {config.fieldMapping[field.name] && (
                            <CheckOutlined className="mapped-indicator" />
                          )}
                        </Space>
                      </div>
                    ))}
                  </div>
                </Col>

                {/* Lookup File Columns */}
                <Col span={12}>
                  <div className="field-section">
                    <Title level={4}>Lookup File Columns</Title>
                    {fileInfo ? (
                      fileInfo.columns.map(column => (
                        <div
                          key={column}
                          ref={el => { if (el) fieldRefs.current[column] = el; }}
                          className="field-item drop-target"
                          onDragOver={(e) => handleDragOver(e, column)}
                          onDrop={(e) => handleDrop(e, column)}
                        >
                          <Space>
                            <div className="field-info">
                              <Text strong>{column}</Text>
                              <br />
                              <Text type="secondary" style={{ fontSize: '12px' }}>
                                {fileInfo.previewData[0]?.[column]?.toString().substring(0, 30) || 'No sample'}
                              </Text>
                            </div>
                            {Object.entries(config.fieldMapping).find(([_, col]) => col === column) && (
                              <Tag color="blue">Mapped</Tag>
                            )}
                          </Space>
                        </div>
                      ))
                    ) : (
                      <Text type="secondary">Upload a file to see columns</Text>
                    )}
                  </div>
                </Col>
              </Row>
            </div>

            {/* Mapping Summary */}
            <Divider />
            <div className="mapping-summary">
              <Text strong>Current Mappings:</Text>
              {Object.entries(config.fieldMapping).map(([inputField, lookupColumn]) => (
                <Tag key={inputField} color="blue">
                  {inputField} → {lookupColumn}
                </Tag>
              ))}
              {Object.keys(config.fieldMapping).length === 0 && (
                <Text type="secondary">No mappings configured</Text>
              )}
            </div>
          </Card>
        </Col>

        {/* Test Lookup Section */}
        <Col span={24}>
          <Card 
            title={
              <Space>
                <PlayCircleOutlined />
                Test Lookup Configuration
              </Space>
            }
            extra={
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={runTestLookup}
                loading={isTesting}
              >
                Run Test
              </Button>
            }
          >
            <Row gutter={24}>
              <Col span={8}>
                <Upload
                  accept=".csv,.json"
                  beforeUpload={() => false}
                  showUploadList={false}
                >
                  <Button icon={<UploadOutlined />} block>
                    Upload Sample Input Data
                  </Button>
                </Upload>
                
                {testResults && (
                  <div className="test-results">
                    <Divider />
                    <Text strong>Test Results:</Text>
                    <div className="match-stats">
                      <Progress
                        type="circle"
                        percent={(testResults.matched / testResults.total) * 100}
                        format={() => (
                          <div>
                            <div style={{ fontSize: '24px', fontWeight: 'bold' }}>
                              {Math.round((testResults.matched / testResults.total) * 100)}%
                            </div>
                            <div style={{ fontSize: '12px' }}>Match Rate</div>
                          </div>
                        )}
                      />
                      <div className="stats-details">
                        <div>
                          <CheckOutlined style={{ color: '#52c41a' }} />
                          Matched: {testResults.matched}
                        </div>
                        <div>
                          <CloseOutlined style={{ color: '#ff4d4f' }} />
                          Unmatched: {testResults.unmatched}
                        </div>
                        <div>Total: {testResults.total}</div>
                        <div>Time: {testResults.executionTime}ms</div>
                        {testResults.cacheHitRate && (
                          <div>Cache Hit: {testResults.cacheHitRate}%</div>
                        )}
                      </div>
                    </div>
                  </div>
                )}
              </Col>
              
              <Col span={16}>
                {testResults ? (
                  <>
                    <Text strong>Enriched Output Preview:</Text>
                    <div className="output-preview">
                      <Table
                        dataSource={testResults.sampleOutput}
                        columns={[
                          ...inputSchema.map(f => ({
                            title: f.name,
                            dataIndex: f.name,
                            key: f.name
                          })),
                          ...Object.values(config.fieldMapping).map(col => ({
                            title: col,
                            dataIndex: col,
                            key: col
                          }))
                        ]}
                        pagination={false}
                        size="small"
                        scroll={{ x: true }}
                      />
                    </div>
                  </>
                ) : (
                  <div className="test-placeholder">
                    <Text type="secondary">
                      Run a test to see lookup results and enriched output
                    </Text>
                  </div>
                )}
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>

      {/* Configuration Summary */}
      <Card className="config-summary">
        <Row align="middle" justify="space-between">
          <Col>
            <Space direction="vertical" size="small">
              <Text strong>Configuration Summary</Text>
              <Space>
                <Tag icon={<HddOutlined />}>
                  Cache: {config.cacheSize} records
                </Tag>
                <Tag icon={renderFileIcon(config.fileType)}>
                  {config.fileType.toUpperCase()}
                </Tag>
                <Tag>{config.joinType} Join</Tag>
                <Tag color={config.reloadOnChange ? 'green' : 'default'}>
                  {config.reloadOnChange ? 'Auto-reload' : 'Manual reload'}
                </Tag>
              </Space>
            </Space>
          </Col>
          <Col>
            <Space>
              <Button>Save Configuration</Button>
              <Button type="primary">Deploy Lookup</Button>
            </Space>
          </Col>
        </Row>
      </Card>
    </div>
  );
};

export default FileLookupConfigEditor;