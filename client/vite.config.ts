// vite.config.ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'

// 使用动态 import.meta.url 和 URL 来获取 __dirname
const __dirname = new URL('.', import.meta.url).pathname

// 配置 vite
export default defineConfig({
  plugins: [react()],
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
        'fs', 'path', 'url', 'child_process', 'net', 'os', 'crypto',
        'stream', 'util', 'events', 'buffer', 'querystring', 'http',
        'https', 'zlib', 'tls', 'dns', 'module', 'assert', 'constants',
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
  }
})