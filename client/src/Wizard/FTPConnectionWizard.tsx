
import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  CheckCircle,
  Server,
  AlertCircle,
  Loader2,
} from 'lucide-react';
import { FTPConnectionFormData, FTPConnectionWizardProps } from '../types/types';

const FTPConnectionWizard: React.FC<FTPConnectionWizardProps> = ({
  isOpen,
  onClose,
  onSave,
}) => {
  const totalSteps = 5;
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<FTPConnectionFormData>({
    name: '',
    purpose: '',
    description: '',
    protocol: 'sftp',
    host: '',
    port: 22,
    passiveMode: true,
    timeout: 30,
    authType: 'password',
    username: '',
    password: '',
    privateKey: '',
    privateKeyPassphrase: '',
    implicitTLS: false,
  });

  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
  const [testError, setTestError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  // Validation helpers
  const isStepValid = (step: number): boolean => {
    switch (step) {
      case 1:
        return !!formData.name.trim();
      case 2:
        return !!formData.host.trim() && formData.port > 0;
      case 3:
        if (!formData.username.trim()) return false;
        if (formData.authType === 'password') return !!formData.password;
        if (formData.authType === 'privateKey') return !!formData.privateKey;
        return false;
      case 4:
        return true; // advanced settings are optional
      case 5:
        return true; // test & save step
      default:
        return true;
    }
  };

  const updateFormData = (updates: Partial<FTPConnectionFormData>) => {
    setFormData((prev) => ({ ...prev, ...updates }));
  };

  const handleProtocolChange = (protocol: 'ftp' | 'sftp') => {
    // Reset port to defaults
    const newPort = protocol === 'ftp' ? 21 : 22;
    updateFormData({ protocol, port: newPort });
  };

  // Test connection
  const testConnection = async () => {
    setTestStatus('testing');
    setTestError(null);

    try {
      const payload = {
        protocol: formData.protocol,
        host: formData.host,
        port: formData.port,
        username: formData.username,
        authType: formData.authType,
        password: formData.authType === 'password' ? formData.password : undefined,
        privateKey: formData.authType === 'privateKey' ? formData.privateKey : undefined,
        privateKeyPassphrase: formData.privateKeyPassphrase,
        passiveMode: formData.passiveMode,
        timeout: formData.timeout,
        implicitTLS: formData.implicitTLS,
      };

      const response = await fetch('http://localhost:3000/api/ftp/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Connection test failed');
      }

      setTestStatus('success');
    } catch (err: any) {
      setTestStatus('error');
      setTestError(err.message || 'Could not connect to server');
    }
  };

  // Save wizard data
  const handleSave = async () => {
    if (testStatus !== 'success') {
      setTestStatus('error');
      setTestError('Please test the connection before saving.');
      return;
    }

    setIsSaving(true);
    try {
      await onSave(formData);
      onClose();
    } catch (err: any) {
      console.error('Save failed:', err);
      setTestError(err.message || 'Failed to save connection');
      setTestStatus('error');
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
      protocol: 'sftp',
      host: '',
      port: 22,
      passiveMode: true,
      timeout: 30,
      authType: 'password',
      username: '',
      password: '',
      privateKey: '',
      privateKeyPassphrase: '',
      implicitTLS: false,
    });
    setTestStatus('idle');
    setTestError(null);
    onClose();
  };

  const handleNext = () => {
    if (currentStep < totalSteps && isStepValid(currentStep)) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) setCurrentStep(currentStep - 1);
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
              FTP/SFTP Connection Wizard
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
                Enter a unique name and optional description for this connection.
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
                    placeholder="e.g., Production SFTP"
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
                    placeholder="e.g., Incoming customer files"
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
                    placeholder="Optional description"
                  />
                </div>
              </div>
            </div>
          )}

          {/* Step 2: Connection Details */}
          {currentStep === 2 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Connection Details
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Specify the server address, protocol, and port.
              </p>
              <div className="space-y-4">
                <div className="flex space-x-4">
                  <label className="flex items-center space-x-2">
                    <input
                      type="radio"
                      checked={formData.protocol === 'ftp'}
                      onChange={() => handleProtocolChange('ftp')}
                      className="text-blue-600"
                    />
                    <span className="text-sm text-gray-700 dark:text-gray-300">FTP</span>
                  </label>
                  <label className="flex items-center space-x-2">
                    <input
                      type="radio"
                      checked={formData.protocol === 'sftp'}
                      onChange={() => handleProtocolChange('sftp')}
                      className="text-blue-600"
                    />
                    <span className="text-sm text-gray-700 dark:text-gray-300">SFTP</span>
                  </label>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Host *
                  </label>
                  <input
                    type="text"
                    value={formData.host}
                    onChange={(e) => updateFormData({ host: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="e.g., sftp.example.com"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Port *
                  </label>
                  <input
                    type="number"
                    value={formData.port}
                    onChange={(e) => updateFormData({ port: parseInt(e.target.value, 10) })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder={formData.protocol === 'ftp' ? '21' : '22'}
                  />
                </div>
                {formData.protocol === 'ftp' && (
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="passiveMode"
                      checked={formData.passiveMode}
                      onChange={(e) => updateFormData({ passiveMode: e.target.checked })}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <label htmlFor="passiveMode" className="text-sm text-gray-700 dark:text-gray-300">
                      Use Passive Mode
                    </label>
                  </div>
                )}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Timeout (seconds)
                  </label>
                  <input
                    type="number"
                    value={formData.timeout}
                    onChange={(e) => updateFormData({ timeout: parseInt(e.target.value, 10) })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    min={1}
                    max={300}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Step 3: Authentication */}
          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Authentication
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Choose authentication method and enter credentials.
              </p>
              <div className="space-y-4">
                <div className="flex space-x-4">
                  <label className="flex items-center space-x-2">
                    <input
                      type="radio"
                      checked={formData.authType === 'password'}
                      onChange={() => updateFormData({ authType: 'password', privateKey: '', privateKeyPassphrase: '' })}
                      className="text-blue-600"
                    />
                    <span className="text-sm text-gray-700 dark:text-gray-300">Password</span>
                  </label>
                  <label className="flex items-center space-x-2">
                    <input
                      type="radio"
                      checked={formData.authType === 'privateKey'}
                      onChange={() => updateFormData({ authType: 'privateKey', password: '' })}
                      className="text-blue-600"
                    />
                    <span className="text-sm text-gray-700 dark:text-gray-300">Private Key</span>
                  </label>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Username *
                  </label>
                  <input
                    type="text"
                    value={formData.username}
                    onChange={(e) => updateFormData({ username: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="e.g., john.doe"
                  />
                </div>
                {formData.authType === 'password' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                      Password *
                    </label>
                    <input
                      type="password"
                      value={formData.password}
                      onChange={(e) => updateFormData({ password: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    />
                  </div>
                )}
                {formData.authType === 'privateKey' && (
                  <>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Private Key (PEM format) *
                      </label>
                      <textarea
                        value={formData.privateKey}
                        onChange={(e) => updateFormData({ privateKey: e.target.value })}
                        rows={5}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                        placeholder="-----BEGIN RSA PRIVATE KEY-----&#10;...&#10;-----END RSA PRIVATE KEY-----"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Passphrase (if any)
                      </label>
                      <input
                        type="password"
                        value={formData.privateKeyPassphrase}
                        onChange={(e) => updateFormData({ privateKeyPassphrase: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                      />
                    </div>
                  </>
                )}
              </div>
            </div>
          )}

          {/* Step 4: Advanced Options */}
          {currentStep === 4 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Advanced Options
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Optional settings for fine‑tuning the connection.
              </p>
              <div className="space-y-4">
                {formData.protocol === 'ftp' && (
                  <div className="flex items-center space-x-2">
                    <input
                      type="checkbox"
                      id="implicitTLS"
                      checked={formData.implicitTLS}
                      onChange={(e) => updateFormData({ implicitTLS: e.target.checked })}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    <label htmlFor="implicitTLS" className="text-sm text-gray-700 dark:text-gray-300">
                      Use Implicit TLS (FTPS)
                    </label>
                  </div>
                )}
                {/* Add any other advanced options you might need */}
              </div>
            </div>
          )}

          {/* Step 5: Test & Save */}
          {currentStep === 5 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Test Connection & Save
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Verify that the connection works before saving.
              </p>

              <div className="flex items-center space-x-4">
                <Button
                  variant="outline"
                  onClick={testConnection}
                  disabled={testStatus === 'testing'}
                >
                  {testStatus === 'testing' ? (
                    <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <Server className="h-4 w-4 mr-2" />
                  )}
                  Test Connection
                </Button>
                {testStatus === 'success' && (
                  <span className="text-sm text-green-600 dark:text-green-400 flex items-center">
                    <CheckCircle className="h-4 w-4 mr-1" /> Connected successfully
                  </span>
                )}
                {testStatus === 'error' && (
                  <span className="text-sm text-red-600 dark:text-red-400 flex items-center">
                    <AlertCircle className="h-4 w-4 mr-1" /> {testError || 'Connection failed'}
                  </span>
                )}
              </div>

              {testStatus === 'success' && (
                <div className="mt-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-md p-4">
                  <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                    <CheckCircle className="h-5 w-5" />
                    <span className="font-medium">Ready to save</span>
                  </div>
                  <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                    Connection test succeeded. Click Finish to save to the repository.
                  </p>
                </div>
              )}
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
                disabled={!isStepValid(currentStep)}
              >
                Next
                <ArrowRight className="h-4 w-4 ml-2" />
              </Button>
            ) : (
              <Button
                onClick={handleSave}
                disabled={testStatus !== 'success' || isSaving}
              >
                {isSaving ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <CheckCircle className="h-4 w-4 mr-2" />
                )}
                Finish & Save to Repository
              </Button>
            )}
          </div>
        </div>
      </motion.div>
    </div>
  );
};

export default FTPConnectionWizard;