// backend/src/config/cors.ts

import { CorsOptions } from 'cors';

const corsOptions: CorsOptions = {
  origin: (origin, callback) => {
    // Allow requests with no origin (like mobile apps, curl, postman)
    if (!origin) return callback(null, true);
    
    // Allow all local development origins
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
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  optionsSuccessStatus: 200,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
  allowedHeaders: [
    'Content-Type',
    'Authorization',
    'Accept',
    'Origin',
    'X-Requested-With',
    'X-Request-ID',
    'X-CSRF-Token',
    'Access-Control-Allow-Headers',
    'Access-Control-Allow-Origin',
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