// components/KeyConfiguration.tsx
import React, { useState } from 'react';
import { Card, Select, Tag, Button, Table, Space } from 'antd';
import { PlusOutlined, DeleteOutlined, EyeOutlined } from '@ant-design/icons';
import { FieldSchema } from './cache.types';

const { Option } = Select;

interface KeyConfigurationProps {
  keyFields: string[];
  onKeyFieldsChange: (fields: string[]) => void;
  availableFields: FieldSchema[];
  sampleData?: Record<string, any>[];
}

export const KeyConfiguration: React.FC<KeyConfigurationProps> = ({
  keyFields,
  onKeyFieldsChange,
  availableFields,
  sampleData
}) => {
  const [selectedField, setSelectedField] = useState<string>('');

  const addKeyField = () => {
    if (selectedField && !keyFields.includes(selectedField)) {
      onKeyFieldsChange([...keyFields, selectedField]);
      setSelectedField('');
    }
  };

  const removeKeyField = (field: string) => {
    onKeyFieldsChange(keyFields.filter(f => f !== field));
  };

  const getFieldType = (fieldName: string) => {
    const field = availableFields.find(f => f.name === fieldName);
    return field?.type || 'unknown';
  };

  const columns = [
    {
      title: 'Field Name',
      dataIndex: 'field',
      key: 'field',
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      render: (type: string) => <Tag color={
        type === 'string' ? 'blue' :
        type === 'number' ? 'green' :
        type === 'boolean' ? 'orange' : 'purple'
      }>{type}</Tag>
    },
    {
      title: 'Action',
      key: 'action',
      render: (_: any, record: { field: string }) => (
        <Button
          type="text"
          danger
          icon={<DeleteOutlined />}
          onClick={() => removeKeyField(record.field)}
        />
      ),
    },
  ];

  const dataSource = keyFields.map(field => ({
    key: field,
    field,
    type: getFieldType(field)
  }));

  return (
    <Card title="Key Configuration" bordered={false}>
      <Space direction="vertical" style={{ width: '100%' }} size="large">
        <div>
          <h4>Composite Key Builder</h4>
          <Space>
            <Select
              style={{ width: 200 }}
              value={selectedField}
              onChange={setSelectedField}
              placeholder="Select field to add"
            >
              {availableFields
                .filter(field => !keyFields.includes(field.name))
                .map(field => (
                  <Option key={field.name} value={field.name}>
                    {field.name} ({field.type})
                  </Option>
                ))}
            </Select>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={addKeyField}
              disabled={!selectedField}
            >
              Add to Key
            </Button>
          </Space>
        </div>

        <div>
          <h4>Selected Key Fields</h4>
          <Table
            columns={columns}
            dataSource={dataSource}
            pagination={false}
            size="small"
          />
        </div>

        {keyFields.length > 0 && sampleData && (
          <div>
            <h4>
              Key Preview <EyeOutlined style={{ marginLeft: 8 }} />
            </h4>
            <Card size="small">
              {sampleData.slice(0, 3).map((data, index) => (
                <div key={index} style={{ marginBottom: 8 }}>
                  <Tag color="geekblue">
                    {keyFields.map(field => data[field]).join(':')}
                  </Tag>
                  <span style={{ marginLeft: 8, fontSize: 12, color: '#666' }}>
                    {JSON.stringify(
                      Object.fromEntries(
                        Object.entries(data).filter(([key]) => keyFields.includes(key))
                      )
                    )}
                  </span>
                </div>
              ))}
            </Card>
          </div>
        )}

        {keyFields.length > 1 && (
          <div style={{ padding: 8, backgroundColor: '#f6ffed', borderRadius: 4 }}>
            ⓘ Composite key formed by concatenating: {keyFields.join(' + ')}
          </div>
        )}
      </Space>
    </Card>
  );
};