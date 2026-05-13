// client/vite.config.ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { nodePolyfills } from 'vite-plugin-node-polyfills'
import { resolve } from 'path'

// Detect Electron environment
const isElectron = process.env.ELECTRON === 'true' || process.env.NODE_ENV === 'production'

export default defineConfig({
  plugins: [
    react(),
    nodePolyfills({
      include: [
        'util', 'buffer', 'crypto', 'stream', 'events',
        'process', 'path', 'url', 'querystring', 'http', 'https',
      ],
    })
  ],

  base: './',

  server: {
    port: 3001,
    strictPort: true,
    host: true,
  },

  build: {
    outDir: 'dist',
    sourcemap: true,
    target: 'es2020',
    rollupOptions: {
      external: [
        'fs', 'child_process', 'net', 'os',
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
      'xlsx',
    ],
    exclude: [
      'fs', 'child_process', 'net', 'os',
      'pg', 'mysql2', 'better-sqlite3', 'oracledb', 'node-sybase',
    ],
  },

  resolve: {
    alias: {
      '@': resolve(__dirname, 'src'),
      '@components': resolve(__dirname, 'src/components'),
      '@store': resolve(__dirname, 'src/store'),
      '@hooks': resolve(__dirname, 'src/hooks'),
      '@api': resolve(__dirname, 'src/api'),
      '@types': resolve(__dirname, 'src/types'),
    },
  },

  define: {
    'import.meta.env.VITE_IS_ELECTRON': JSON.stringify(isElectron),
    'import.meta.env.VITE_API_BASE_URL': JSON.stringify(
      isElectron ? 'http://localhost:3000' : 'http://localhost:3000'
    ),
    // Mock process.env
    'process.env': JSON.stringify({
      NODE_ENV: process.env.NODE_ENV || 'development',
      REACT_APP_BACKEND_URL: process.env.REACT_APP_BACKEND_URL || 'http://localhost:3000',
      REACT_APP_DB_USER: process.env.REACT_APP_DB_USER || 'postgres',
      USER: process.env.USER || 'postgres',
      USERNAME: process.env.USERNAME || 'postgres',
    }),
    'process.platform': JSON.stringify('browser'),
    // ✅ ADD THESE TWO LINES to prevent "process.version is undefined"
    'process.version': JSON.stringify('v18.0.0'),
    'process.versions': JSON.stringify({ node: '18.0.0' }),
  },
})