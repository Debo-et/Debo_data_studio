import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  CheckCircle,
  AlertCircle,
  Wifi,
} from 'lucide-react';
import { toast } from 'react-toastify';

// Types for the form data
export interface SAPConnectionFormData {
  name: string;
  purpose: string;
  description: string;
  host: string;
  port: number;
  client: string;
  username: string;
  password: string;
  language: string;
  poolSize: number;
  systemId?: string;
  instanceNumber?: string;
}

interface SAPConnectionWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (data: SAPConnectionFormData) => Promise<void>; // or void
}

const SAPConnectionWizard: React.FC<SAPConnectionWizardProps> = ({
  isOpen,
  onClose,
  onSave,
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<SAPConnectionFormData>({
    name: '',
    purpose: '',
    description: '',
    host: '',
    port: 3300,
    client: '100',
    username: '',
    password: '',
    language: 'EN',
    poolSize: 5,
    systemId: '',
    instanceNumber: '',
  });
  const [isTesting, setIsTesting] = useState(false);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const totalSteps = 4;

  // Helper to update form data
  const updateFormData = (updates: Partial<SAPConnectionFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  // Validation for each step
  const validateStep = (step: number): boolean => {
    setError(null);
    switch (step) {
      case 1:
        if (!formData.name.trim()) {
          setError('Name is required.');
          return false;
        }
        return true;
      case 2:
        if (!formData.host.trim()) {
          setError('Host is required.');
          return false;
        }
        if (!formData.port || formData.port <= 0 || formData.port > 65535) {
          setError('Port must be a valid number between 1 and 65535.');
          return false;
        }
        if (!formData.client.trim()) {
          setError('Client is required.');
          return false;
        }
        if (!formData.username.trim()) {
          setError('Username is required.');
          return false;
        }
        if (!formData.password.trim()) {
          setError('Password is required.');
          return false;
        }
        if (!formData.language.trim()) {
          setError('Language is required.');
          return false;
        }
        if (!formData.poolSize || formData.poolSize < 1 || formData.poolSize > 50) {
          setError('Pool size must be between 1 and 50.');
          return false;
        }
        return true;
      default:
        return true;
    }
  };

  // Test connection API call
  const testConnection = async () => {
    setIsTesting(true);
    setTestResult(null);
    try {
      const response = await fetch('http://localhost:3000/api/sap/test-connection', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          host: formData.host,
          port: formData.port,
          client: formData.client,
          username: formData.username,
          password: formData.password,
          language: formData.language,
          systemId: formData.systemId,
          instanceNumber: formData.instanceNumber,
        }),
      });
      const data = await response.json();
      if (response.ok && data.success) {
        setTestResult({ success: true, message: data.message || 'Connection successful!' });
      } else {
        setTestResult({ success: false, message: data.error || 'Connection failed.' });
      }
    } catch (err: any) {
      setTestResult({ success: false, message: err.message || 'Network error.' });
    } finally {
      setIsTesting(false);
    }
  };

  // Save the connection
  const handleSave = async () => {
    if (!validateStep(2)) {
      setCurrentStep(2);
      return;
    }
    setIsSaving(true);
    try {
      // First test again (optional, but ensures connection works)
      if (!testResult?.success) {
        const testRes = await fetch('http://localhost:3000/api/sap/test-connection', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(formData),
        });
        const testData = await testRes.json();
        if (!testRes.ok || !testData.success) {
          setError(testData.error || 'Connection test failed. Cannot save.');
          setCurrentStep(3);
          setIsSaving(false);
          return;
        }
      }

      // Save to backend
      const saveRes = await fetch('http://localhost:3000/api/sap/connections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      });
      const saveData = await saveRes.json();
      if (!saveRes.ok || !saveData.success) {
        throw new Error(saveData.error || 'Failed to save connection.');
      }

      // Dispatch custom event to refresh repository tree
      window.dispatchEvent(new CustomEvent('metadata-created', {
        detail: {
          metadata: {
            ...formData,
            id: saveData.id,
            createdAt: new Date().toISOString(),
          },
          type: 'sap',
          folderId: 'sap-connection',
        },
      }));
      toast.success(`SAP connection "${formData.name}" created successfully.`);
      onSave(formData);
      handleClose();
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsSaving(false);
    }
  };

  const handleClose = () => {
    setCurrentStep(1);
    setFormData({
      name: '',
      purpose: '',
      description: '',
      host: '',
      port: 3300,
      client: '100',
      username: '',
      password: '',
      language: 'EN',
      poolSize: 5,
      systemId: '',
      instanceNumber: '',
    });
    setTestResult(null);
    setError(null);
    onClose();
  };

  const handleNext = () => {
    if (validateStep(currentStep)) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleBack = () => {
    setCurrentStep(currentStep - 1);
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              SAP Connection Wizard
            </h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
              Step {currentStep} of {totalSteps}
            </p>
          </div>
          <Button variant="ghost" size="sm" onClick={handleClose} className="h-8 w-8 p-0">
            <X className="h-4 w-4" />
          </Button>
        </div>

        {/* Progress Bar */}
        <div className="px-6 pt-4">
          <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2">
            <div
              className="bg-blue-600 h-2 rounded-full transition-all duration-300"
              style={{ width: `${(currentStep / totalSteps) * 100}%` }}
            />
          </div>
        </div>

        {/* Content */}
        <div className="p-6 max-h-[60vh] overflow-y-auto">
          {currentStep === 1 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                General Properties
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Enter a Name for the SAP connection. Optionally, add a Purpose and Description.
              </p>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => updateFormData({ name: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="e.g., SAP Production"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Purpose
                  </label>
                  <input
                    type="text"
                    value={formData.purpose}
                    onChange={(e) => updateFormData({ purpose: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="e.g., Data extraction for sales"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Description
                  </label>
                  <textarea
                    value={formData.description}
                    onChange={(e) => updateFormData({ description: e.target.value })}
                    rows={3}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Detailed description"
                  />
                </div>
              </div>
            </div>
          )}

          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Connection Details
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Enter the SAP system connection parameters.
              </p>
              <div className="grid grid-cols-2 gap-4">
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Host *
                  </label>
                  <input
                    type="text"
                    value={formData.host}
                    onChange={(e) => updateFormData({ host: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="sap.example.com"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Port *
                  </label>
                  <input
                    type="number"
                    value={formData.port}
                    onChange={(e) => updateFormData({ port: parseInt(e.target.value) })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="3300"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Client *
                  </label>
                  <input
                    type="text"
                    value={formData.client}
                    onChange={(e) => updateFormData({ client: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="100"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Username *
                  </label>
                  <input
                    type="text"
                    value={formData.username}
                    onChange={(e) => updateFormData({ username: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="SAP_USER"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Password *
                  </label>
                  <input
                    type="password"
                    value={formData.password}
                    onChange={(e) => updateFormData({ password: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="********"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Language
                  </label>
                  <input
                    type="text"
                    value={formData.language}
                    onChange={(e) => updateFormData({ language: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="EN"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Pool Size
                  </label>
                  <input
                    type="number"
                    value={formData.poolSize}
                    onChange={(e) => updateFormData({ poolSize: parseInt(e.target.value) })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="5"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    System ID (optional)
                  </label>
                  <input
                    type="text"
                    value={formData.systemId}
                    onChange={(e) => updateFormData({ systemId: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="SID"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Instance Number (optional)
                  </label>
                  <input
                    type="text"
                    value={formData.instanceNumber}
                    onChange={(e) => updateFormData({ instanceNumber: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="00"
                  />
                </div>
              </div>
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Test Connection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Click the button below to test the connection with the provided parameters.
              </p>
              <div className="flex flex-col items-center space-y-4">
                <Button
                  onClick={testConnection}
                  disabled={isTesting}
                  className="w-40"
                >
                  {isTesting ? (
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white" />
                  ) : (
                    <>
                      <Wifi className="h-4 w-4 mr-2" />
                      Test Connection
                    </>
                  )}
                </Button>
                {testResult && (
                  <div
                    className={`p-3 rounded-md ${
                      testResult.success
                        ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800'
                        : 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800'
                    }`}
                  >
                    <div className="flex items-center space-x-2">
                      {testResult.success ? (
                        <CheckCircle className="h-4 w-4 text-green-600" />
                      ) : (
                        <AlertCircle className="h-4 w-4 text-red-600" />
                      )}
                      <span
                        className={`text-sm ${
                          testResult.success ? 'text-green-700' : 'text-red-700'
                        }`}
                      >
                        {testResult.message}
                      </span>
                    </div>
                  </div>
                )}
                {error && (
                  <div className="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 rounded-md w-full">
                    <div className="flex items-center space-x-2">
                      <AlertCircle className="h-4 w-4 text-red-600" />
                      <span className="text-sm text-red-700">{error}</span>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review the SAP connection configuration and click Finish to save.
              </p>
              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your SAP connection will be stored securely and available under the "SAP Connection" folder.
                </p>
              </div>
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="text-gray-500">Name:</span>{' '}
                  <span className="font-medium">{formData.name}</span>
                </div>
                <div>
                  <span className="text-gray-500">Host:</span>{' '}
                  <span>{formData.host}:{formData.port}</span>
                </div>
                <div>
                  <span className="text-gray-500">Client:</span>{' '}
                  <span>{formData.client}</span>
                </div>
                <div>
                  <span className="text-gray-500">User:</span>{' '}
                  <span>{formData.username}</span>
                </div>
                <div>
                  <span className="text-gray-500">Language:</span>{' '}
                  <span>{formData.language}</span>
                </div>
                <div>
                  <span className="text-gray-500">Pool Size:</span>{' '}
                  <span>{formData.poolSize}</span>
                </div>
                {formData.systemId && (
                  <div>
                    <span className="text-gray-500">System ID:</span>{' '}
                    <span>{formData.systemId}</span>
                  </div>
                )}
                {formData.instanceNumber && (
                  <div>
                    <span className="text-gray-500">Instance:</span>{' '}
                    <span>{formData.instanceNumber}</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between p-6 border-t border-gray-200 dark:border-gray-700">
          <Button variant="outline" onClick={currentStep === 1 ? handleClose : handleBack}>
            <ArrowLeft className="h-4 w-4 mr-2" />
            {currentStep === 1 ? 'Cancel' : 'Back'}
          </Button>
          <div className="flex items-center space-x-3">
            <span className="text-sm text-gray-500 dark:text-gray-400">
              Step {currentStep} of {totalSteps}
            </span>
            {currentStep < totalSteps ? (
              <Button onClick={handleNext}>Next <ArrowRight className="h-4 w-4 ml-2" /></Button>
            ) : (
              <Button onClick={handleSave} disabled={isSaving}>
                {isSaving ? 'Saving...' : <><CheckCircle className="h-4 w-4 mr-2" /> Finish & Save</>}
              </Button>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default SAPConnectionWizard;