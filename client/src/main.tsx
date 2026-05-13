// main.tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import { Provider } from 'react-redux'
import { store } from './store'
import App from './App.tsx'
import './index.css'

// Enhanced error boundary
class RootErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; errorInfo: string }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props)
    this.state = { hasError: false, errorInfo: '' }
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, errorInfo: error.message }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
    console.error('Root error boundary caught error:', error, errorInfo)
    this.setState({ errorInfo: `${error.message}\n${errorInfo.componentStack}` })
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ 
          padding: '20px', 
          fontFamily: 'Arial, sans-serif',
          textAlign: 'center',
          marginTop: '50px'
        }}>
          <h1>Application Error</h1>
          <p>There was an issue starting the application.</p>
          <div style={{ 
            backgroundColor: '#f8f9fa', 
            padding: '15px', 
            margin: '20px 0',
            borderRadius: '5px',
            textAlign: 'left',
            maxWidth: '600px',
            marginLeft: 'auto',
            marginRight: 'auto'
          }}>
            <pre style={{ 
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              fontSize: '12px',
              color: '#dc3545'
            }}>
              {this.state.errorInfo}
            </pre>
          </div>
          <button 
            onClick={() => window.location.reload()}
            style={{
              padding: '10px 20px',
              backgroundColor: '#3b82f6',
              color: 'white',
              border: 'none',
              borderRadius: '5px',
              cursor: 'pointer',
              margin: '5px'
            }}
          >
            Reload Application
          </button>
        </div>
      )
    }

    return this.props.children
  }
}

const LoadingFallback = () => (
  <div style={{
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: '100vh',
    fontFamily: 'Arial, sans-serif'
  }}>
    <div style={{ textAlign: 'center' }}>
      <h2>Loading Debo Data Studio...</h2>
      <p>Initializing application components</p>
      <div style={{ 
        width: '40px', 
        height: '40px', 
        border: '4px solid #f3f3f3',
        borderTop: '4px solid #3b82f6',
        borderRadius: '50%',
        animation: 'spin 1s linear infinite',
        margin: '20px auto'
      }}></div>
    </div>
    <style>
      {`
        @keyframes spin {
          0% { transform: rotate(0deg); }
          100% { transform: rotate(360deg); }
        }
      `}
    </style>
  </div>
)

const RootComponent = () => (
  <React.StrictMode>
    <RootErrorBoundary>
      <Provider store={store}>
        <React.Suspense fallback={<LoadingFallback />}>
          <App />
        </React.Suspense>
      </Provider>
    </RootErrorBoundary>
  </React.StrictMode>
)

const rootElement = document.getElementById('root')
if (!rootElement) {
  throw new Error('Failed to find the root element')
}

try {
  const root = ReactDOM.createRoot(rootElement)
  root.render(<RootComponent />)
} catch (error) {
  console.error('Failed to render React app:', error)
  
  // Simple fallback without PostgreSQL references
  const fallbackHTML = `
    <div style="padding: 40px; font-family: Arial, sans-serif; text-align: center;">
      <h1 style="color: #dc2626;">Application Failed to Load</h1>
      <p style="color: #666; margin: 20px 0;">
        Error: ${error instanceof Error ? error.message : String(error)}
      </p>
      <button 
        onclick="window.location.reload()" 
        style="
          padding: 10px 20px;
          background-color: #3b82f6;
          color: white;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          margin: 10px;
        "
      >
        Reload Application
      </button>
    </div>
  `
  
  rootElement.innerHTML = fallbackHTML
}