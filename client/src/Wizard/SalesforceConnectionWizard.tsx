import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../components/ui/Button';
import {
  X,
  ArrowLeft,
  ArrowRight,
  CheckCircle,
  AlertCircle,
  Loader2,
  Database,
} from 'lucide-react';
import { toast } from 'react-toastify';

// --- Types ---
export interface SalesforceConnectionFormData {
  name: string;
  purpose: string;
  description: string;
  authType: 'basic' | 'oauth2';
  username?: string;
  password?: string;
  securityToken?: string;
  clientId?: string;
  clientSecret?: string;
  loginUrl: string;
  apiVersion: string;
}

interface SalesforceConnectionWizardProps {
  isOpen: boolean;
  onClose: () => void;
  onSave: (data: SalesforceConnectionFormData) => void;
}

// --- Helper: Test connection via backend ---
const testSalesforceConnection = async (data: SalesforceConnectionFormData): Promise<boolean> => {
  // Simulate API call – replace with real fetch to your backend
  console.log('Testing connection with:', data);
  // For demo, assume success after 1s
  await new Promise(resolve => setTimeout(resolve, 1000));
  return true;
};

// --- Main Component ---
const SalesforceConnectionWizard: React.FC<SalesforceConnectionWizardProps> = ({
  isOpen,
  onClose,
  onSave,
}) => {
  const totalSteps = 3;
  const [currentStep, setCurrentStep] = useState(1);
  const [formData, setFormData] = useState<SalesforceConnectionFormData>({
    name: '',
    purpose: '',
    description: '',
    authType: 'basic',
    username: '',
    password: '',
    securityToken: '',
    clientId: '',
    clientSecret: '',
    loginUrl: 'https://login.salesforce.com',
    apiVersion: '57.0',
  });

  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [testStatus, setTestStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');

  // --- Validation Functions ---
  const validateStep1 = (): boolean => {
    if (!formData.name.trim()) {
      setError('Connection name is required.');
      return false;
    }
    setError(null);
    return true;
  };

  const validateStep2 = (): boolean => {
    if (!formData.loginUrl.trim()) {
      setError('Login URL is required.');
      return false;
    }
    if (formData.authType === 'basic') {
      if (!formData.username?.trim()) {
        setError('Username is required for basic authentication.');
        return false;
      }
      if (!formData.password?.trim()) {
        setError('Password is required for basic authentication.');
        return false;
      }
      // Security token is optional
    } else if (formData.authType === 'oauth2') {
      if (!formData.clientId?.trim()) {
        setError('Client ID is required for OAuth2.');
        return false;
      }
      if (!formData.clientSecret?.trim()) {
        setError('Client Secret is required for OAuth2.');
        return false;
      }
    }
    setError(null);
    return true;
  };

  // --- Step Navigation ---
  const handleNext = () => {
    if (currentStep === 1 && validateStep1()) {
      setCurrentStep(2);
    } else if (currentStep === 2 && validateStep2()) {
      setCurrentStep(3);
    }
  };

  const handleBack = () => {
    if (currentStep > 1) {
      setCurrentStep(currentStep - 1);
      setError(null);
      setTestStatus('idle');
    }
  };

  // --- Test Connection ---
  const handleTestConnection = async () => {
    if (!validateStep2()) return;

    setTestStatus('testing');
    setError(null);
    setIsLoading(true);

    try {
      const success = await testSalesforceConnection(formData);
      if (success) {
        setTestStatus('success');
        toast.success('Connection test succeeded!');
      } else {
        setTestStatus('error');
        setError('Connection test failed. Please check your credentials.');
      }
    } catch (err: any) {
      setTestStatus('error');
      setError(err.message || 'Test connection failed.');
    } finally {
      setIsLoading(false);
    }
  };

  // --- Save ---
  const handleSave = () => {
    if (testStatus !== 'success') {
      toast.warning('Please test the connection before saving.');
      return;
    }

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
      authType: 'basic',
      username: '',
      password: '',
      securityToken: '',
      clientId: '',
      clientSecret: '',
      loginUrl: 'https://login.salesforce.com',
      apiVersion: '57.0',
    });
    setError(null);
    setTestStatus('idle');
  };

  const updateFormData = (updates: Partial<SalesforceConnectionFormData>) => {
    setFormData(prev => ({ ...prev, ...updates }));
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <motion.div
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl w-full max-w-3xl max-h-[90vh] overflow-hidden"
      >
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-gray-200 dark:border-gray-700">
          <div>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Salesforce Connection Wizard
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
                General Information
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Give your Salesforce connection a meaningful name and optional purpose/description.
              </p>

              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Connection Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={e => updateFormData({ name: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="e.g., Production Salesforce"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Purpose
                  </label>
                  <input
                    type="text"
                    value={formData.purpose}
                    onChange={e => updateFormData({ purpose: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter purpose"
                  />
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Description
                  </label>
                  <textarea
                    value={formData.description}
                    onChange={e => updateFormData({ description: e.target.value })}
                    rows={3}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 dark:bg-gray-700 dark:text-white"
                    placeholder="Enter description"
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
                Provide the authentication method and connection parameters for Salesforce.
              </p>

              <div className="space-y-4">
                {/* Authentication Type */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                    Authentication Method
                  </label>
                  <div className="flex space-x-4">
                    <label className="flex items-center space-x-2">
                      <input
                        type="radio"
                        value="basic"
                        checked={formData.authType === 'basic'}
                        onChange={() => updateFormData({ authType: 'basic' })}
                        className="text-blue-600"
                      />
                      <span className="text-sm text-gray-700 dark:text-gray-300">
                        Username/Password + Security Token
                      </span>
                    </label>
                    <label className="flex items-center space-x-2">
                      <input
                        type="radio"
                        value="oauth2"
                        checked={formData.authType === 'oauth2'}
                        onChange={() => updateFormData({ authType: 'oauth2' })}
                        className="text-blue-600"
                      />
                      <span className="text-sm text-gray-700 dark:text-gray-300">
                        OAuth2 (Client Credentials)
                      </span>
                    </label>
                  </div>
                </div>

                {/* Basic Auth Fields */}
                {formData.authType === 'basic' && (
                  <>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Username *
                      </label>
                      <input
                        type="text"
                        value={formData.username}
                        onChange={e => updateFormData({ username: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                        placeholder="yourname@company.com"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Password *
                      </label>
                      <input
                        type="password"
                        value={formData.password}
                        onChange={e => updateFormData({ password: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                        placeholder="••••••••"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Security Token
                      </label>
                      <input
                        type="password"
                        value={formData.securityToken}
                        onChange={e => updateFormData({ securityToken: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                        placeholder="Optional"
                      />
                    </div>
                  </>
                )}

                {/* OAuth2 Fields */}
                {formData.authType === 'oauth2' && (
                  <>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Client ID *
                      </label>
                      <input
                        type="text"
                        value={formData.clientId}
                        onChange={e => updateFormData({ clientId: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Client Secret *
                      </label>
                      <input
                        type="password"
                        value={formData.clientSecret}
                        onChange={e => updateFormData({ clientSecret: e.target.value })}
                        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                      />
                    </div>
                  </>
                )}

                {/* Common Fields */}
                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    Login URL *
                  </label>
                  <input
                    type="text"
                    value={formData.loginUrl}
                    onChange={e => updateFormData({ loginUrl: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="https://login.salesforce.com"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Use <code>https://test.salesforce.com</code> for Sandbox.
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                    API Version
                  </label>
                  <input
                    type="text"
                    value={formData.apiVersion}
                    onChange={e => updateFormData({ apiVersion: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md"
                    placeholder="57.0"
                  />
                </div>
              </div>

              {error && (
                <div className="flex items-center space-x-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-md">
                  <AlertCircle className="h-4 w-4 text-red-600" />
                  <span className="text-sm text-red-600">{error}</span>
                </div>
              )}
            </div>
          )}

          {currentStep === 3 && (
            <div className="space-y-4">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                Test Connection
              </h3>
              <p className="text-sm text-gray-600 dark:text-gray-400">
                Verify that the provided connection details are correct before saving.
              </p>

              <div className="bg-gray-50 dark:bg-gray-700 rounded-md p-4">
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center space-x-2">
                    <Database className="h-5 w-5 text-gray-500" />
                    <span className="font-medium text-gray-700 dark:text-gray-300">
                      Connection Summary
                    </span>
                  </div>
                  {testStatus === 'success' && (
                    <span className="text-green-600 text-sm flex items-center">
                      <CheckCircle className="h-4 w-4 mr-1" /> Connected
                    </span>
                  )}
                  {testStatus === 'error' && (
                    <span className="text-red-600 text-sm">Connection failed</span>
                  )}
                </div>

                <dl className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <dt className="text-gray-500">Name</dt>
                    <dd className="font-medium">{formData.name}</dd>
                  </div>
                  <div className="flex justify-between">
                    <dt className="text-gray-500">Auth Type</dt>
                    <dd className="capitalize">{formData.authType}</dd>
                  </div>
                  <div className="flex justify-between">
                    <dt className="text-gray-500">Login URL</dt>
                    <dd>{formData.loginUrl}</dd>
                  </div>
                  {formData.authType === 'basic' && (
                    <div className="flex justify-between">
                      <dt className="text-gray-500">Username</dt>
                      <dd>{formData.username}</dd>
                    </div>
                  )}
                  {formData.authType === 'oauth2' && (
                    <div className="flex justify-between">
                      <dt className="text-gray-500">Client ID</dt>
                      <dd>{formData.clientId?.slice(0, 8)}…</dd>
                    </div>
                  )}
                </dl>

                <div className="mt-4">
                  <Button
                    onClick={handleTestConnection}
                    disabled={isLoading}
                    className="w-full"
                  >
                    {isLoading ? (
                      <>
                        <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                        Testing...
                      </>
                    ) : (
                      <>
                        <CheckCircle className="h-4 w-4 mr-2" />
                        Test Connection
                      </>
                    )}
                  </Button>
                </div>

                {error && (
                  <div className="mt-3 flex items-center space-x-2 p-2 bg-red-50 dark:bg-red-900/20 border border-red-200 rounded-md">
                    <AlertCircle className="h-4 w-4 text-red-600" />
                    <span className="text-sm text-red-600">{error}</span>
                  </div>
                )}
              </div>

              {testStatus === 'success' && (
                <div className="bg-green-50 dark:bg-green-900/20 border border-green-200 rounded-md p-3">
                  <div className="flex items-center space-x-2 text-green-800 dark:text-green-300">
                    <CheckCircle className="h-5 w-5" />
                    <span className="font-medium">Connection successful!</span>
                  </div>
                  <p className="text-sm text-green-700 dark:text-green-400 mt-1">
                    You can now save this connection to the repository.
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
            <span className="text-sm text-gray-500">
              Step {currentStep} of {totalSteps}
            </span>
            {currentStep < totalSteps ? (
              <Button onClick={handleNext}>
                Next
                <ArrowRight className="h-4 w-4 ml-2" />
              </Button>
            ) : (
              <Button
                onClick={handleSave}
                disabled={testStatus !== 'success'}
                title={testStatus !== 'success' ? 'Test connection first' : ''}
              >
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

export default SalesforceConnectionWizard;