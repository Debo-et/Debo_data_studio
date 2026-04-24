// client/vite.config.ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'

// Use dynamic import.meta.url to get __dirname equivalent in ESM
const __dirname = new URL('.', import.meta.url).pathname

// Detect if we're building for Electron production
const isElectron = process.env.ELECTRON === 'true' || process.env.NODE_ENV === 'production'

export default defineConfig({
  plugins: [react()],
  
  // Use relative base path for Electron (file:// protocol compatibility)
  base: './',
  
  server: {
    port: 3001,
    strictPort: true,
    host: true, // Expose to network for Electron dev
  },
  
  build: {
    outDir: 'dist',
    sourcemap: true,
    target: 'es2020',
    rollupOptions: {
      external: [
        // Node.js built-in modules that shouldn't be bundled
        'fs', 'path', 'url', 'child_process', 'net', 'os', 'crypto',
        'stream', 'util', 'events', 'buffer', 'querystring', 'http',
        'https', 'zlib', 'tls', 'dns', 'module', 'assert', 'constants',
        // Database drivers (if they're used directly in renderer, better to keep external)
        'pg', 'mysql2', 'node-sybase', 'better-sqlite3', 'oracledb',
        'sqlite3', 'tedious',
      ],
    },
  },
  
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      '@xyflow/react',
      '@reduxjs/toolkit',
      'react-redux',
      'lucide-react',
      '@dnd-kit/core',
      '@dnd-kit/sortable',
      '@dnd-kit/utilities',
    ],
    exclude: [
      // Exclude Node.js modules from optimization (they won't be used in browser)
      'pg', 'mysql2', 'better-sqlite3', 'oracledb', 'node-sybase',
      'fs', 'path', 'child_process', 'net', 'os', 'crypto',
    ]
  },
  
  resolve: {
    alias: {
      '@': resolve(__dirname, 'src'),
      '@components': resolve(__dirname, 'src/components'),
      '@store': resolve(__dirname, 'src/store'),
      '@hooks': resolve(__dirname, 'src/hooks'),
      '@api': resolve(__dirname, 'src/api'),
      '@types': resolve(__dirname, 'src/types')
    }
  },
  
  // Define environment variables for the client
  define: {
    // Provide a global flag so the app knows it's running in Electron
    'import.meta.env.VITE_IS_ELECTRON': JSON.stringify(isElectron),
    // You can also set the API base URL conditionally
    'import.meta.env.VITE_API_BASE_URL': JSON.stringify(
      isElectron ? 'http://localhost:3000' : 'http://localhost:3000'
    ),
  },
})