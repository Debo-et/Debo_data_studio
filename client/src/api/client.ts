// src/api/client.ts
import axios, { AxiosResponse, AxiosError } from 'axios';

export interface NodeType {
  id: string;
  name: string;
  category: 'input' | 'output' | 'transform' | 'filter' | 'join';
  description: string;
  parameters: ParameterDefinition[];
}

export interface ParameterDefinition {
  name: string;
  type: 'string' | 'number' | 'boolean' | 'select';
  required: boolean;
  defaultValue?: any;
  options?: string[];
}

export interface Workflow {
  id?: string;
  name: string;
  nodes: any[];
  edges: any[];
  createdAt?: string;
  updatedAt?: string;
  metadata?: {
    nodeCount: number;
    connectionCount: number;
    timestamp: number;
  };
}

export interface RunWorkflowResponse {
  executionId: string;
  status: 'running' | 'completed' | 'failed';
  message: string;
}

export interface LogMessage {
  timestamp: string;
  level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG';
  message: string;
  source?: string;
  executionId?: string;
}

// Safe axios instance creation
const createApiClient = () => {
  return axios.create({
    baseURL: 'http://localhost:8080',
    timeout: 30000,
    headers: {
      'Content-Type': 'application/json',
    },
  });
};

const apiClient = createApiClient();

// Request interceptor
apiClient.interceptors.request.use(
  (config) => {
    console.log(`Making ${config.method?.toUpperCase()} request to ${config.url}`);
    return config;
  },
  (error) => {
    console.error('Request error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor
apiClient.interceptors.response.use(
  (response: AxiosResponse) => {
    return response;
  },
  (error: AxiosError) => {
    if (error.code === 'ECONNREFUSED') {
      console.error('Backend server is not running. Please start the server on port 8080.');
    } else if (error.response) {
      console.error('API Error Response:', {
        status: error.response.status,
        statusText: error.response.statusText,
        data: error.response.data,
        url: error.config?.url
      });
    } else if (error.request) {
      console.error('API No Response:', {
        message: error.message,
        url: error.config?.url
      });
    } else {
      console.error('API Unknown Error:', error.message);
    }
    
    return Promise.reject(error);
  }
);

export const api = {
  async getNodeTypes(): Promise<NodeType[]> {
    try {
      const response = await apiClient.get<NodeType[]>('/nodes/list');
      return response.data;
    } catch (error) {
      console.error('Failed to fetch node types:', error);
      return [];
    }
  },

  async runWorkflow(workflow: Workflow): Promise<RunWorkflowResponse> {
    try {
      const response = await apiClient.post<RunWorkflowResponse>('/workflow/run', workflow);
      return response.data;
    } catch (error) {
      console.error('Failed to run workflow:', error);
      return {
        executionId: 'fallback-' + Date.now(),
        status: 'failed',
        message: 'Failed to connect to backend server'
      };
    }
  },

  async saveWorkflow(workflow: Workflow): Promise<{ success: boolean; id: string }> {
    try {
      const response = await apiClient.post<{ success: boolean; id: string }>('/workflow/save', workflow);
      return response.data;
    } catch (error) {
      console.error('Failed to save workflow:', error);
      return {
        success: false,
        id: 'failed-' + Date.now()
      };
    }
  },

  async healthCheck(): Promise<boolean> {
    try {
      await apiClient.get('/health');
      return true;
    } catch {
      return false;
    }
  }
};

export default apiClient;