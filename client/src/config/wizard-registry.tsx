// src/config/wizard-registry.tsx
import { ReactNode } from 'react';
import {
  FileSpreadsheet,
  FileCode,
  FileText,
  Ruler,
  FileJson,
  Database,
  Search,
  Network,
  Globe,
  Briefcase,
} from 'lucide-react';

export interface WizardConfig {
  /** Match by exact node.id (single or array) */
  id?: string | string[];
  /** Match by node.type (single or array) */
  nodeType?: string | string[];
  /** Match by node.metadata.wizardType (single or array) */
  wizardType?: string | string[];
  /** Display label in context menu */
  label: string;
  /** Icon element (ReactNode, not component class) */
  icon: ReactNode;
  /** Name of the handler prop passed to ContextMenu (e.g. 'onOpenExcelWizard') */
  handlerProp: string;
  /** Whether this wizard creates a new item (appears under "New" section) */
  isCreation?: boolean;
}

export const WIZARD_CONFIG: WizardConfig[] = [
  // ----- System folders (creation) -----
  {
    id: 'job-designs',
    label: 'Create Job',
    icon: <Briefcase className="h-4 w-4" />,
    handlerProp: 'onCreateJobWizard',
    isCreation: true,
  },

  // ----- Metadata categories (under "metadata" folder) -----
  {
    id: 'file-excel',
    label: 'Excel Metadata',
    icon: <FileSpreadsheet className="h-4 w-4" />,
    handlerProp: 'onOpenExcelWizard',
    isCreation: true,
  },
  {
    id: 'file-xml',
    label: 'XML Metadata',
    icon: <FileCode className="h-4 w-4" />,
    handlerProp: 'onOpenXMLWizard',
    isCreation: true,
  },
  {
    id: 'file-delimited',
    label: 'Delimited File Metadata',
    icon: <FileText className="h-4 w-4" />,
    handlerProp: 'onOpenDelimitedWizard',
    isCreation: true,
  },
  {
    id: 'file-positional',
    label: 'Positional File Metadata',
    icon: <Ruler className="h-4 w-4" />,
    handlerProp: 'onOpenPositionalWizard',
    isCreation: true,
  },
  {
    id: 'file-json-avro-parquet',
    label: 'JSON/Avro/Parquet Metadata',
    icon: <FileJson className="h-4 w-4" />,
    handlerProp: 'onOpenJsonAvroParquetWizard',
    isCreation: true,
  },
  {
    id: 'file-schema',
    label: 'File Schema Metadata',
    icon: <Database className="h-4 w-4" />,
    handlerProp: 'onOpenFileSchemaWizard',
    isCreation: true,
  },
  {
    id: 'file-regex',
    label: 'Regex Metadata',
    icon: <Search className="h-4 w-4" />,
    handlerProp: 'onOpenRegexWizard',
    isCreation: true,
  },
  {
    id: 'file-ldif',
    label: 'LDIF Metadata',
    icon: <Network className="h-4 w-4" />,
    handlerProp: 'onOpenLDIFWizard',
    isCreation: true,
  },
  {
    id: 'web-service',
    label: 'Web Service Metadata',
    icon: <Globe className="h-4 w-4" />,
    handlerProp: 'onOpenWebServiceWizard',
    isCreation: true,
  },
  {
    id: 'ldap',
    label: 'LDAP Metadata',
    icon: <Network className="h-4 w-4" />,
    handlerProp: 'onOpenLDAPWizard',
    isCreation: true,
  },
  {
    id: 'db-connections',
    label: 'Database Connection',
    icon: <Database className="h-4 w-4" />,
    handlerProp: 'onOpenDatabaseWizard',
    isCreation: true,
  },

  // ----- 👇 YOUR CUSTOM NODE -----
  {
    id: 'repository-data-source', // <-- replace with your actual node.id
    label: 'Database Connection',
    icon: <Database className="h-4 w-4" />,
    handlerProp: 'onOpenDatabaseWizard',
    isCreation: true,
  },

  // ----- Match by metadata.wizardType (more flexible) -----
  {
    wizardType: 'postgresql',
    label: 'PostgreSQL Connection',
    icon: <Database className="h-4 w-4" />,
    handlerProp: 'onOpenDatabaseWizard',
  },
  // Add more wizardType mappings as needed...
];