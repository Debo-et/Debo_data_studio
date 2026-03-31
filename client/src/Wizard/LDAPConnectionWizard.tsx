// src/components/Wizard/LDAPConnectionWizard.tsx

import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  CheckCircle,
  AlertCircle,
  Server,
  Database,
  Lock,
  Shield,
} from 'lucide-react';
import { LDAPConnectionFormData, LDAPConnectionWizardProps } from '../types/types';

// Helper validation functions
const validateHost = (host: string): boolean => {
  return host.trim().length > 0;
};

const validatePort = (port: number): boolean => {
  return !isNaN(port) && port >= 1 && port <= 65535;
};

const validateBaseDN = (baseDN: string): boolean => {
  return baseDN.trim().length > 0;
};

const validateBindDN = (bindDN: string): boolean => {
  return bindDN.trim().length > 0;
};

const LDAPConnectionWizard: React.FC<LDAPConnectionWizardProps> = ({
  isOpen,
  onClose,
  onSave,
}) => {
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<LDAPConnectionFormData>({
    name: '',
    purpose: '',
    description: '',
    host: '',
    port: 389,
    baseDN: '',
    bindDN: '',
    bindPassword: '',
    encryption: 'none', // 'none', 'ssl', 'starttls'
    timeout: 30,
    connectionName: '',
    connectionType: 'LDAP',
  });
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<{ success: boolean; message: string } | null>(null);
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});

  const totalSteps = 4; // General → LDAP Settings → Test Connection → Finish

  // Update form data
  const updateFormData = (updates: Partial<LDAPConnectionFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
    // Clear test result when parameters change
    if (currentStep === 3) {
      setTestResult(null);
    }
  };

  // Validate current step fields
  const validateStep = (step: number): boolean => {
    const errors: Record<string, string> = {};

    if (step === 1) {
      if (!formData.name.trim()) {
        errors.name = 'Name is required';
      }
    }

    if (step === 2) {
      if (!validateHost(formData.host)) {
        errors.host = 'Host is required';
      }
      if (!validatePort(formData.port)) {
        errors.port = 'Port must be between 1 and 65535';
      }
      if (!validateBaseDN(formData.baseDN)) {
        errors.baseDN = 'Base DN is required (e.g., dc=example,dc=com)';
      }
      if (!validateBindDN(formData.bindDN)) {
        errors.bindDN = 'Bind DN is required (e.g., cn=admin,dc=example,dc=com)';
      }
      if (!formData.bindPassword) {
        errors.bindPassword = 'Bind password is required';
      }
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  // Test connection (simulated API call)
  const testConnection = async () => {
    if (!validateStep(2)) {
      setCurrentStep(2); // Go back to settings step if validation fails
      return false;
    }

    setIsLoading(true);
    setTestResult(null);
    setError(null);

    try {
      // In a real app, you would call an API service, e.g.:
      // const response = await apiService.testLDAPConnection(formData);
      // For demonstration, we simulate a successful connection after 1 second
      await new Promise(resolve => setTimeout(resolve, 1000));

      // Simulate success (you can add logic to simulate failure based on parameters)
      const success = true; // Replace with actual API result
      const message = success
        ? 'Connection successful! The LDAP server responded correctly.'
        : 'Connection failed: Incorrect credentials or server unreachable.';

      setTestResult({ success, message });
      return success;
    } catch (err: any) {
      setTestResult({ success: false, message: err.message || 'Connection test failed' });
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  const handleNext = async () => {
    if (currentStep === 2) {
      // Before leaving step 2, optionally test if user wants to?
      // We'll just validate.
      if (!validateStep(2)) return;
    }

    if (currentStep === 3) {
      // Step 3: Test Connection – we run test and then proceed to step 4 only if successful
      const success = await testConnection();
      if (success) {
        setCurrentStep(currentStep + 1);
      }
      return;
    }

    if (currentStep < totalSteps) {
      if (validateStep(currentStep)) {
        setCurrentStep(currentStep + 1);
      }
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
    }
  };

  const handleSave = () => {
    onSave(formData);
    handleClose();
  };

  const handleClose = () => {
    onClose();
    // Reset state
    setCurrentStep(1);
    setFormData({
      name: '',
      purpose: '',
      description: '',
      host: '',
      port: 389,
      baseDN: '',
      bindDN: '',
      bindPassword: '',
      encryption: 'none',
      timeout: 30,
      connectionName: '',
      connectionType: 'LDAP',
    });
    setValidationErrors({});
    setTestResult(null);
    setError(null);
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              LDAP Connection Wizard
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
          {/* Step 1: General Properties */}
          {currentStep === 1 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                General Properties
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Enter a name for this LDAP connection. Optionally, add a purpose and description.
              </p>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Connection Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => updateFormData({ name: e.target.value })}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white ${
                      validationErrors.name ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder="e.g., Corporate LDAP"
                  />
                  {validationErrors.name && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{validationErrors.name}</p>
                  )}
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
                    placeholder="e.g., User authentication"
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
                    placeholder="Describe the purpose of this connection"
                  />
                </div>
              </div>
            </div>
          )}

          {/* Step 2: LDAP Connection Settings */}
          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                LDAP Connection Settings
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Provide the details of your LDAP server.
              </p>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Host *
                  </label>
                  <input
                    type="text"
                    value={formData.host}
                    onChange={(e) => updateFormData({ host: e.target.value })}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white ${
                      validationErrors.host ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder="ldap.example.com"
                  />
                  {validationErrors.host && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{validationErrors.host}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Port *
                  </label>
                  <input
                    type="number"
                    value={formData.port}
                    onChange={(e) => updateFormData({ port: parseInt(e.target.value) || 389 })}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white ${
                      validationErrors.port ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder="389"
                  />
                  {validationErrors.port && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{validationErrors.port}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Base DN *
                  </label>
                  <input
                    type="text"
                    value={formData.baseDN}
                    onChange={(e) => updateFormData({ baseDN: e.target.value })}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white ${
                      validationErrors.baseDN ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder="dc=example,dc=com"
                  />
                  {validationErrors.baseDN && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{validationErrors.baseDN}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Bind DN *
                  </label>
                  <input
                    type="text"
                    value={formData.bindDN}
                    onChange={(e) => updateFormData({ bindDN: e.target.value })}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white ${
                      validationErrors.bindDN ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder="cn=admin,dc=example,dc=com"
                  />
                  {validationErrors.bindDN && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{validationErrors.bindDN}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Bind Password *
                  </label>
                  <input
                    type="password"
                    value={formData.bindPassword}
                    onChange={(e) => updateFormData({ bindPassword: e.target.value })}
                    className={`w-full px-3 py-2 border rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white ${
                      validationErrors.bindPassword ? 'border-red-500' : 'border-gray-300 dark:border-gray-600'
                    }`}
                    placeholder="••••••••"
                  />
                  {validationErrors.bindPassword && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{validationErrors.bindPassword}</p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Encryption
                  </label>
                  <select
                    value={formData.encryption}
                    onChange={(e) => updateFormData({ encryption: e.target.value as any })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                  >
                    <option value="none">None (plain)</option>
                    <option value="ssl">SSL (LDAPS)</option>
                    <option value="starttls">StartTLS</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Timeout (seconds)
                  </label>
                  <input
                    type="number"
                    value={formData.timeout}
                    onChange={(e) => updateFormData({ timeout: parseInt(e.target.value) || 30 })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="30"
                    min="1"
                    max="300"
                  />
                </div>
              </div>

              {/* Info note about connection type */}
              <div className="mt-4 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-md flex items-start space-x-2">
                <Server className="h-4 w-4 text-blue-600 dark:text-blue-400 mt-0.5" />
                <p className="text-xs text-blue-700 dark:text-blue-300">
                  The connection will be stored as a reusable resource in the repository. You can later use it in jobs or data sources.
                </p>
              </div>
            </div>
          )}

          {/* Step 3: Test Connection */}
          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Test Connection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Verify that the provided LDAP settings are correct. Click "Test Connection" to proceed.
              </p>

              <div className="bg-gray-50 dark:bg-gray-700/50 rounded-md p-4">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center space-x-2">
                    <Database className="h-5 w-5 text-gray-600 dark:text-gray-400" />
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                      Connection Details
                    </span>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={testConnection}
                    disabled={isLoading}
                  >
                    {isLoading ? (
                      <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-current mr-2" />
                    ) : (
                      <Shield className="h-4 w-4 mr-2" />
                    )}
                    {isLoading ? 'Testing...' : 'Test Connection'}
                  </Button>
                </div>

                {/* Display current settings summary */}
                <div className="grid grid-cols-2 gap-2 text-sm mb-4">
                  <div className="text-gray-500 dark:text-gray-400">Host:</div>
                  <div className="text-gray-900 dark:text-white font-mono">{formData.host || '—'}</div>
                  <div className="text-gray-500 dark:text-gray-400">Port:</div>
                  <div className="text-gray-900 dark:text-white">{formData.port}</div>
                  <div className="text-gray-500 dark:text-gray-400">Base DN:</div>
                  <div className="text-gray-900 dark:text-white font-mono">{formData.baseDN || '—'}</div>
                  <div className="text-gray-500 dark:text-gray-400">Bind DN:</div>
                  <div className="text-gray-900 dark:text-white font-mono">{formData.bindDN || '—'}</div>
                  <div className="text-gray-500 dark:text-gray-400">Encryption:</div>
                  <div className="text-gray-900 dark:text-white">{formData.encryption}</div>
                </div>

                {/* Test result */}
                {testResult && (
                  <div
                    className={`p-3 rounded-md flex items-start space-x-2 ${
                      testResult.success
                        ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800'
                        : 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800'
                    }`}
                  >
                    {testResult.success ? (
                      <CheckCircle className="h-5 w-5 text-green-600 dark:text-green-400 flex-shrink-0" />
                    ) : (
                      <AlertCircle className="h-5 w-5 text-red-600 dark:text-red-400 flex-shrink-0" />
                    )}
                    <div>
                      <p
                        className={`text-sm ${
                          testResult.success
                            ? 'text-green-800 dark:text-green-300'
                            : 'text-red-800 dark:text-red-300'
                        }`}
                      >
                        {testResult.message}
                      </p>
                    </div>
                  </div>
                )}

                {error && (
                  <div className="mt-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                    <p className="text-sm text-red-800 dark:text-red-300">{error}</p>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Step 4: Finish */}
          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Finish and Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Review your LDAP connection configuration and click Finish to save it to the Repository.
              </p>

              <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                  <CheckCircle className="h-5 w-5" />
                  <span className="font-medium">Ready to save</span>
                </div>
                <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                  Your LDAP connection is configured and ready to be stored in the repository.
                </p>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-4">
                  <h4 className="font-medium text-gray-900 dark:text-white">Configuration Summary</h4>
                  <div className="space-y-3 text-sm">
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Name:</span>
                      <span className="text-gray-900 dark:text-white font-medium">{formData.name}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Purpose:</span>
                      <span className="text-gray-900 dark:text-white">{formData.purpose || '—'}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Host:</span>
                      <span className="text-gray-900 dark:text-white">{formData.host}:{formData.port}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Base DN:</span>
                      <span className="text-gray-900 dark:text-white font-mono">{formData.baseDN}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Bind DN:</span>
                      <span className="text-gray-900 dark:text-white font-mono">{formData.bindDN}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500 dark:text-gray-400">Encryption:</span>
                      <span className="text-gray-900 dark:text-white">{formData.encryption}</span>
                    </div>
                  </div>
                </div>

                <div className="space-y-3">
                  <h4 className="font-medium text-gray-900 dark:text-white">Security</h4>
                  <div className="border border-gray-200 dark:border-gray-600 rounded-md p-3">
                    <div className="flex items-center space-x-2 mb-2">
                      <Lock className="h-4 w-4 text-gray-500" />
                      <span className="text-sm text-gray-700 dark:text-gray-300">Password stored securely</span>
                    </div>
                    <p className="text-xs text-gray-500 dark:text-gray-400">
                      The bind password will be encrypted and stored in the repository.
                    </p>
                  </div>
                </div>
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
              <Button
                onClick={handleNext}
                disabled={
                  (currentStep === 1 && !formData.name.trim()) ||
                  (currentStep === 2 && (Object.keys(validationErrors).length > 0))
                }
              >
                {currentStep === 3 ? 'Test Connection' : 'Next'}
                {currentStep !== 3 && <ArrowRight className="h-4 w-4 ml-2" />}
              </Button>
            ) : (
              <Button onClick={handleSave}>
                <CheckCircle className="h-4 w-4 mr-2" />
                Finish & Save to Repository
              </Button>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default LDAPConnectionWizard;