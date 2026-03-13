// vite.config.ts
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { dirname, resolve } from 'path'
import { fileURLToPath } from 'url'

// Use import.meta.url safely
const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

export default defineConfig({
  plugins: [react()],
  base: '',
  define: {
    'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'development'),
    // 移除直接定义 global 的方式，改为在 rollup 外部处理
  },
  server: {
    port: 3001, // 改为 3001 以匹配 package.json 中的 dev 脚本
    strictPort: true,
    host: true,
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      external: (id) => {
        const externals = [
          'fs', 'path', 'url', 'child_process', 'net', 'os', 'crypto',
          'stream', 'util', 'events', 'buffer', 'querystring', 'http',
          'https', 'zlib', 'tls', 'dns', 'module', 'assert', 'constants',
          'pg', 'mysql2', 'node-sybase', 'better-sqlite3', 'oracledb',
          'sqlite3', 'tedious',
        ]

        if (externals.includes(id)) {
          return true
        }

        const pathPatterns = [
          /.*[\\/]node_modules[\\/](pg|mysql2|node-sybase|better-sqlite3|oracledb)[\\/].*/,
          /.*[\\/]embedded-pg[\\/].*/,
          /.*[\\/]electron[\\/].*/,
          /.*[\\/]lib[\\/]db[\\/].*/,
          /.*databaseManager.*/,
        ]

        return pathPatterns.some(pattern => pattern.test(id))
      },
      output: {
        manualChunks: {
          'vendor': ['react', 'react-dom'],
          'ui': ['@xyflow/react', 'lucide-react'],
          'state': ['@reduxjs/toolkit', 'react-redux'],
        }
      }
    },
    target: 'es2020',
    commonjsOptions: {
      transformMixedEsModules: true,
      exclude: [
        '**/embedded-pg/**',
        '**/electron/**', 
        '**/lib/db/**',
        '**/node_modules/pg/**',
        '**/node_modules/mysql2/**',
        '**/node_modules/better-sqlite3/**',
        '**/node_modules/oracledb/**',
        '**/node_modules/node-sybase/**'
      ]
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
      '@': resolve(__dirname, './src'),
      '@components': resolve(__dirname, './src/components'),
      '@store': resolve(__dirname, './src/store'),
      '@hooks': resolve(__dirname, './src/hooks'),
      '@api': resolve(__dirname, './src/api'),
      '@types': resolve(__dirname, './src/types')
    }
  }
})