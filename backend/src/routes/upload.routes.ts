import { Router } from 'express';
import multer from 'multer';
import fs from 'fs';
import config from '../config';
import { uploadCSV } from '../controllers/upload.controller';
import { convertPositional, convertXml, convertStructured, convertRegex, convertLdif, convertSchema } from '../controllers/upload.controller';


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
    // Sanitize filename (remove unsafe characters) but keep the original name – NO timestamp prefix
    const safeName = file.originalname.replace(/[^a-z0-9.]/gi, '_');
    cb(null, safeName);
  }
});

const upload = multer({ storage });
const router = Router();

router.post('/upload-csv', upload.single('file'), uploadCSV);
router.post('/convert/positional', upload.single('file'), convertPositional);
router.post('/convert/xml', upload.single('file'), convertXml);
router.post('/convert/structured', upload.single('file'), convertStructured);
router.post('/convert/regex', upload.single('file'), convertRegex);
router.post('/convert/ldif', upload.single('file'), convertLdif);
router.post('/convert/schema', upload.fields([{ name: 'schemaFile' }, { name: 'dataFile' }]), convertSchema);
export default router;