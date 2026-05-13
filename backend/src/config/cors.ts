// backend/src/config/cors.ts

import { CorsOptions } from 'cors';

const corsOptions: CorsOptions = {
  origin: (origin, callback) => {
    // Allow requests with no origin (like mobile apps, curl, Postman)
    if (!origin) return callback(null, true);

    // List of allowed origins for local development
    const allowedOrigins = [
      'http://localhost:3000',
      'http://localhost:3001',
      'http://127.0.0.1:3000',
      'http://127.0.0.1:3001',
      'http://0.0.0.0:3000',
      'http://0.0.0.0:3001',
    ];

    if (allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      console.warn(`CORS blocked request from origin: ${origin}`);
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: false,               // Keep false unless you need cookies / authentication headers
  optionsSuccessStatus: 200,        // For old browsers
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: [
    'Content-Type',
    'Authorization',
    'Accept',
    'X-Requested-With',
    'X-Request-ID',
    'X-CSRF-Token',
    // 'Origin' is automatically sent by the browser – do NOT add it here
  ],
  exposedHeaders: [
    'Content-Length',
    'Content-Type',
    'Date',
    'ETag',
    'X-Request-ID',
    'X-Powered-By',
    'X-CSRF-Token',
  ],
};

export default corsOptions;