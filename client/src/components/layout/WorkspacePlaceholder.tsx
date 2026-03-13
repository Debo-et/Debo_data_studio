// src/components/layout/WorkspacePlaceholder.tsx
import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { Button } from '../ui/Button';
import { Plus, FileCode, Grid3X3, Database, Settings, Zap, X } from 'lucide-react';

interface WorkspacePlaceholderProps {
  onCreateJob: (jobName: string) => void;
}

// Job Creation Dialog Component (same as in Sidebar)
interface JobCreationDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onCreateJob: (jobName: string) => void;
}

const JobCreationDialog: React.FC<JobCreationDialogProps> = ({
  isOpen,
  onClose,
  onCreateJob
}) => {
  const [jobName, setJobName] = useState('');
  const [template, setTemplate] = useState('blank');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (jobName.trim()) {
      let finalJobName = jobName.trim();
      if (template !== 'blank') {
        finalJobName = `${template} - ${finalJobName}`;
      }
      onCreateJob(finalJobName);
      onClose();
      setJobName('');
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-800 rounded-lg p-6 w-full max-w-md">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold">Create New Job Design</h2>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        
        <form onSubmit={handleSubmit}>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">
                Job Name *
              </label>
              <input
                type="text"
                value={jobName}
                onChange={(e) => setJobName(e.target.value)}
                className="w-full px-3 py-2 border rounded-md dark:bg-gray-700 dark:border-gray-600"
                placeholder="e.g., Customer Data Pipeline"
                autoFocus
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">
                Template (Optional)
              </label>
              <select
                value={template}
                onChange={(e) => setTemplate(e.target.value)}
                className="w-full px-3 py-2 border rounded-md dark:bg-gray-700 dark:border-gray-600"
              >
                <option value="blank">Blank Job</option>
                <option value="ETL Pipeline">ETL Pipeline</option>
                <option value="Data Transformation">Data Transformation</option>
                <option value="Data Migration">Data Migration</option>
                <option value="Real-time Processing">Real-time Processing</option>
              </select>
            </div>

            <div className="bg-gray-50 dark:bg-gray-900 p-3 rounded text-sm">
              <p className="font-medium mb-1">What happens next:</p>
              <ul className="list-disc list-inside text-gray-600 dark:text-gray-400 space-y-1">
                <li>A new draggable canvas will be created</li>
                <li>You can add components from the sidebar or palette</li>
                <li>Job will be automatically saved</li>
                <li>You can close the job anytime and return to this view</li>
              </ul>
            </div>
            
            <div className="flex justify-end space-x-2 pt-4">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-sm border rounded-md hover:bg-gray-50 dark:hover:bg-gray-700"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={!jobName.trim()}
                className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
              >
                Create Job Workspace
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
};

const WorkspacePlaceholder: React.FC<WorkspacePlaceholderProps> = ({ onCreateJob }) => {
  const [jobName, setJobName] = useState('');
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  const handleCreateClick = () => {
    if (jobName.trim()) {
      onCreateJob(jobName.trim());
      setJobName('');
    }
  };

  const quickCreate = (template: string) => {
    onCreateJob(`${template} Job - ${new Date().toLocaleDateString()}`);
  };

  // Handle Enter key press on the input field
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleCreateClick();
    }
  };

  return (
    <>
      <div className="h-full w-full bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-800 flex flex-col items-center justify-center p-8">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl w-full text-center"
        >
          {/* Icon and Title */}
          <div className="mb-8">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-gradient-to-r from-blue-500 to-purple-600 mb-4">
              <Grid3X3 className="h-8 w-8 text-white" />
            </div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white mb-2">
              Welcome to Debo Data Studio
            </h1>
            <p className="text-gray-600 dark:text-gray-300">
              Create a new job design to start building your data workflow
            </p>
          </div>

          {/* Create Job Form - UPDATED */}
          <motion.div
            initial={{ scale: 0.95 }}
            animate={{ scale: 1 }}
            className="bg-white dark:bg-gray-800 rounded-xl shadow-lg p-8 mb-8"
          >
            <div className="flex items-center justify-center space-x-3 mb-6">
              <FileCode className="h-6 w-6 text-blue-600 dark:text-blue-400" />
              <h2 className="text-xl font-semibold text-gray-800 dark:text-white">
                Create New Job Design
              </h2>
            </div>

            <div className="space-y-4">
              {/* Option 1: Quick create with name only (original behavior) */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Quick Create (Direct)
                </label>
                <div className="flex space-x-2">
                  <input
                    type="text"
                    value={jobName}
                    onChange={(e) => setJobName(e.target.value)}
                    placeholder="e.g., Customer Data Pipeline"
                    className="flex-1 px-4 py-3 border border-gray-300 dark:border-gray-600 dark:bg-gray-700 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all"
                    onKeyDown={handleKeyDown}
                  />
                  <Button
                    onClick={handleCreateClick}
                    disabled={!jobName.trim()}
                    className="py-3 text-lg font-medium"
                    size="lg"
                  >
                    <Plus className="h-5 w-5 mr-2" />
                    Create
                  </Button>
                </div>
                <p className="text-xs text-gray-500 dark:text-gray-400 mt-2">
                  Quick create with job name only
                </p>
              </div>

              {/* Divider */}
              <div className="relative flex items-center py-2">
                <div className="flex-grow border-t border-gray-300 dark:border-gray-600"></div>
                <span className="flex-shrink mx-4 text-gray-500 dark:text-gray-400 text-sm">OR</span>
                <div className="flex-grow border-t border-gray-300 dark:border-gray-600"></div>
              </div>

              {/* Option 2: Open the advanced dialog (synchronized with sidebar) */}
              <div>
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Advanced Create (with templates)
                </label>
                <Button
                  onClick={() => setIsDialogOpen(true)}
                  className="w-full py-3 text-lg font-medium bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700"
                  size="lg"
                >
                  <Plus className="h-5 w-5 mr-2" />
                  Create Job Workspace
                </Button>
                <p className="text-xs text-gray-500 dark:text-gray-400 mt-2">
                  Open advanced dialog with template selection (same as sidebar)
                </p>
              </div>
            </div>
          </motion.div>

          {/* Quick Templates */}
          <div className="mb-8">
            <h3 className="text-lg font-medium text-gray-700 dark:text-gray-300 mb-4">
              Quick Start Templates
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <button
                onClick={() => quickCreate('ETL Pipeline')}
                className="group p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 hover:border-blue-300 dark:hover:border-blue-500 hover:shadow-md transition-all"
              >
                <div className="flex items-center space-x-3 mb-3">
                  <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg group-hover:bg-blue-200 dark:group-hover:bg-blue-800 transition-colors">
                    <Database className="h-5 w-5 text-blue-600 dark:text-blue-400" />
                  </div>
                  <h4 className="font-medium text-gray-800 dark:text-white">ETL Pipeline</h4>
                </div>
                <p className="text-sm text-gray-600 dark:text-gray-400 text-left">
                  Extract, transform, and load data from multiple sources
                </p>
              </button>

              <button
                onClick={() => quickCreate('Data Transformation')}
                className="group p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 hover:border-purple-300 dark:hover:border-purple-500 hover:shadow-md transition-all"
              >
                <div className="flex items-center space-x-3 mb-3">
                  <div className="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg group-hover:bg-purple-200 dark:group-hover:bg-purple-800 transition-colors">
                    <Settings className="h-5 w-5 text-purple-600 dark:text-purple-400" />
                  </div>
                  <h4 className="font-medium text-gray-800 dark:text-white">Data Transformation</h4>
                </div>
                <p className="text-sm text-gray-600 dark:text-gray-400 text-left">
                  Clean, normalize, and process data with transformations
                </p>
              </button>

              <button
                onClick={() => quickCreate('Real-time Processing')}
                className="group p-4 bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 hover:border-green-300 dark:hover:border-green-500 hover:shadow-md transition-all"
              >
                <div className="flex items-center space-x-3 mb-3">
                  <div className="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg group-hover:bg-green-200 dark:group-hover:bg-green-800 transition-colors">
                    <Zap className="h-5 w-5 text-green-600 dark:text-green-400" />
                  </div>
                  <h4 className="font-medium text-gray-800 dark:text-white">Real-time Processing</h4>
                </div>
                <p className="text-sm text-gray-600 dark:text-gray-400 text-left">
                  Stream and process data in real-time workflows
                </p>
              </button>
            </div>
          </div>

          {/* Features */}
          <div className="bg-gradient-to-r from-blue-50 to-purple-50 dark:from-blue-900/20 dark:to-purple-900/20 rounded-xl p-6 border border-blue-100 dark:border-blue-800">
            <h3 className="text-lg font-medium text-gray-800 dark:text-white mb-4">
              What you can do with Job Designs
            </h3>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div className="text-center">
                <div className="text-2xl mb-2">📊</div>
                <p className="text-sm font-medium text-gray-700 dark:text-gray-300">Data Integration</p>
              </div>
              <div className="text-center">
                <div className="text-2xl mb-2">🔄</div>
                <p className="text-sm font-medium text-gray-700 dark:text-gray-300">Transformations</p>
              </div>
              <div className="text-center">
                <div className="text-2xl mb-2">🔗</div>
                <p className="text-sm font-medium text-gray-700 dark:text-gray-300">Connections</p>
              </div>
              <div className="text-center">
                <div className="text-2xl mb-2">💾</div>
                <p className="text-sm font-medium text-gray-700 dark:text-gray-300">Save & Export</p>
              </div>
            </div>
          </div>
        </motion.div>
      </div>

      {/* Job Creation Dialog - same as in Sidebar */}
      <JobCreationDialog
        isOpen={isDialogOpen}
        onClose={() => setIsDialogOpen(false)}
        onCreateJob={onCreateJob}
      />
    </>
  );
};

export default WorkspacePlaceholder;