// backend/src/routes/upload.routes.ts

import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import config from '../config';
import {
  uploadCSV,
  convertPositional,
  convertXml,
  convertStructured,
  convertRegex,
  convertLdif,
  convertSchema,
} from '../controllers/upload.controller';

// Ensure upload directory exists
const uploadDir = config.uploadDir;
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true, mode: 0o755 });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    cb(null, uploadDir);
  },
  filename: (_req, file, cb) => {
    const safeName = file.originalname.replace(/[^a-z0-9.]/gi, '_');
    cb(null, safeName);
  },
});

const upload = multer({ storage });
const router = Router();

// 🚫 REMOVED the cors() middleware block here

router.post('/upload-csv', upload.single('file'), uploadCSV);
router.post('/convert/positional', upload.single('file'), convertPositional);
router.post('/convert/xml', upload.single('file'), convertXml);
router.post('/convert/structured', upload.single('file'), convertStructured);
router.post('/convert/regex', upload.single('file'), convertRegex);
router.post('/convert/ldif', upload.single('file'), convertLdif);
router.post(
  '/convert/schema',
  upload.fields([
    { name: 'schemaFile', maxCount: 1 },
    { name: 'dataFile', maxCount: 1 },
  ]),
  convertSchema
);

export default router;