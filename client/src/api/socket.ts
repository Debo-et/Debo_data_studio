// src/api/socket.ts
export interface ExecutionEvent {
  type: 'NODE_STARTED' | 'NODE_COMPLETED' | 'NODE_FAILED' | 'NODE_SKIPPED' | 
        'EXECUTION_STARTED' | 'EXECUTION_COMPLETED' | 'EXECUTION_FAILED' |
        'PROGRESS_UPDATE' | 'LOG_MESSAGE';
  data: {
    nodeId?: string;
    executionId?: string;
    status?: string;
    error?: string;
    progress?: number;
    level?: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG' | 'SUCCESS';
    message?: string;
    source?: string;
    timestamp?: string;
  };
}

export type WebSocketMessage = ExecutionEvent;

export interface WebSocketCallbacks {
  onMessage: (message: WebSocketMessage) => void;
  onOpen: () => void;
  onClose: (event: CloseEvent) => void;
  onError: (error: Event) => void;
}

class WebSocketService {
  private socket: WebSocket | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  private reconnectInterval = 3000;
  private isConnecting = false;
  private callbacks: WebSocketCallbacks | null = null;

  connect(callbacks: WebSocketCallbacks): void {
    if (this.isConnecting || this.socket?.readyState === WebSocket.OPEN) {
      return;
    }

    this.callbacks = callbacks;
    this.isConnecting = true;

    try {
      // Use localhost for WebSocket connection
      const wsUrl = 'ws://localhost:8081/logs';
      
      this.socket = new WebSocket(wsUrl);

      this.socket.onopen = () => {
        console.log('WebSocket connected');
        this.isConnecting = false;
        this.reconnectAttempts = 0;
        this.callbacks?.onOpen();
      };

      this.socket.onmessage = (event: MessageEvent) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data);
          this.callbacks?.onMessage(message);
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
        }
      };

      this.socket.onclose = (event: CloseEvent) => {
        console.log('WebSocket disconnected:', event.code, event.reason);
        this.isConnecting = false;
        this.callbacks?.onClose(event);
        this.attemptReconnect();
      };

      this.socket.onerror = (error: Event) => {
        console.error('WebSocket error:', error);
        this.isConnecting = false;
        this.callbacks?.onError(error);
      };
    } catch (error) {
      console.error('Failed to create WebSocket connection:', error);
      this.isConnecting = false;
    }
  }

  private attemptReconnect(): void {
    if (this.reconnectAttempts < this.maxReconnectAttempts && this.callbacks) {
      this.reconnectAttempts++;
      console.log(`Attempting to reconnect... (${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
      
      setTimeout(() => {
        this.connect(this.callbacks!);
      }, this.reconnectInterval);
    } else if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error('Max reconnection attempts reached');
    }
  }

  disconnect(): void {
    if (this.socket) {
      this.socket.close(1000, 'Manual disconnect');
      this.socket = null;
    }
    this.isConnecting = false;
    this.reconnectAttempts = 0;
  }

  isConnected(): boolean {
    return this.socket?.readyState === WebSocket.OPEN;
  }
}

export const webSocketService = new WebSocketService();