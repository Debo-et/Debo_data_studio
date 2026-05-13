// RuleSetSelector.tsx
import React, { useState, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  ChevronDown,
  ChevronUp,
  Search,
  Filter,
  Plus,
  Edit2,
  Copy,
  Trash2,
  MoreVertical,
  Calendar,
  User,
  Layers,
  Check,
  Hash
} from 'lucide-react';
import { RuleSet } from '../../../types/types';

interface RuleSetSelectorProps {
  ruleSets: RuleSet[];
  selectedRuleSetId: string;
  onSelect: (ruleSetId: string) => void;
  onNewRuleSet: () => void;
  onEditRuleSet?: (ruleSetId: string) => void;
  onDuplicateRuleSet?: (ruleSetId: string) => void;
  onDeleteRuleSet?: (ruleSetId: string) => void;
}

const RuleSetSelector: React.FC<RuleSetSelectorProps> = ({
  ruleSets,
  selectedRuleSetId,
  onSelect,
  onNewRuleSet,
  onEditRuleSet,
  onDuplicateRuleSet,
  onDeleteRuleSet
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [showActions, setShowActions] = useState<string | null>(null);
  const [filter, setFilter] = useState<'all' | 'recent' | 'favorites'>('all');

  const selectedRuleSet = ruleSets.find(rs => rs.id === selectedRuleSetId);

  // Filter and sort rule sets
  const filteredRuleSets = useMemo(() => {
    let filtered = [...ruleSets];

    // Apply search filter
    if (searchQuery) {
      filtered = filtered.filter(rs =>
        rs.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        rs.description.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    // Apply category filter
    if (filter === 'recent') {
      filtered = filtered.sort((a, b) => 
        new Date(b.lastModified).getTime() - new Date(a.lastModified).getTime()
      );
    } else if (filter === 'favorites') {
      // In a real app, this would use favorite metadata
      filtered = filtered.filter(rs => rs.name.includes('Customer')); // Example
    }

    return filtered;
  }, [ruleSets, searchQuery, filter]);

  const getRuleSetStats = (ruleSet: RuleSet) => {
    const activeRules = ruleSet.rules.filter(r => r.enabled).length;
    const totalRules = ruleSet.rules.length;
    const validationErrors = 0; // In real app, would calculate from validation
    const hasSchema = !!ruleSet.inputSchema && ruleSet.inputSchema.length > 0;

    return {
      activeRules,
      totalRules,
      validationErrors,
      hasSchema,
      completion: totalRules > 0 ? Math.round((activeRules / totalRules) * 100) : 0
    };
  };

  const formatDate = (date: Date) => {
    const now = new Date();
    const diffTime = Math.abs(now.getTime() - date.getTime());
    const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));

    if (diffDays === 0) {
      return 'Today';
    } else if (diffDays === 1) {
      return 'Yesterday';
    } else if (diffDays < 7) {
      return `${diffDays} days ago`;
    } else {
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    }
  };

  const handleRuleSetAction = (ruleSetId: string, action: string) => {
    switch (action) {
      case 'edit':
        onEditRuleSet?.(ruleSetId);
        break;
      case 'duplicate':
        onDuplicateRuleSet?.(ruleSetId);
        break;
      case 'delete':
        if (window.confirm('Are you sure you want to delete this rule set?')) {
          onDeleteRuleSet?.(ruleSetId);
        }
        break;
    }
    setShowActions(null);
  };

  return (
    <div className="relative w-80">
      {/* Main Selector Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full px-4 py-3 bg-white dark:bg-gray-800 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 flex items-center justify-between transition-all"
      >
        <div className="flex items-center space-x-3">
          <div className="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
            <Layers className="w-5 h-5 text-blue-600 dark:text-blue-400" />
          </div>
          <div className="text-left">
            <div className="font-medium text-gray-900 dark:text-white">
              {selectedRuleSet?.name || 'Select Rule Set'}
            </div>
            {selectedRuleSet && (
              <div className="text-sm text-gray-500 dark:text-gray-400 flex items-center space-x-2">
                <span>{selectedRuleSet.rules.length} rules</span>
                <span>•</span>
                <span>v{selectedRuleSet.version}</span>
              </div>
            )}
          </div>
        </div>
        {isOpen ? (
          <ChevronUp className="w-5 h-5 text-gray-400" />
        ) : (
          <ChevronDown className="w-5 h-5 text-gray-400" />
        )}
      </button>

      {/* Dropdown Panel */}
      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="absolute top-full left-0 right-0 mt-2 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl shadow-2xl z-50 overflow-hidden"
          >
            {/* Header */}
            <div className="p-4 border-b border-gray-100 dark:border-gray-700">
              <div className="flex items-center justify-between mb-3">
                <h3 className="font-semibold text-gray-900 dark:text-white">
                  Rule Sets
                </h3>
                <button
                  onClick={onNewRuleSet}
                  className="px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center space-x-2 text-sm"
                >
                  <Plus className="w-4 h-4" />
                  <span>New</span>
                </button>
              </div>

              {/* Search */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search rule sets..."
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-gray-50 dark:bg-gray-700 text-gray-900 dark:text-white text-sm"
                />
              </div>

              {/* Filters */}
              <div className="flex space-x-2 mt-3">
                <button
                  onClick={() => setFilter('all')}
                  className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                    filter === 'all'
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600'
                  }`}
                >
                  All
                </button>
                <button
                  onClick={() => setFilter('recent')}
                  className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                    filter === 'recent'
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600'
                  }`}
                >
                  Recent
                </button>
                <button
                  onClick={() => setFilter('favorites')}
                  className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                    filter === 'favorites'
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-600'
                  }`}
                >
                  Favorites
                </button>
              </div>
            </div>

            {/* Rule Sets List */}
            <div className="max-h-96 overflow-y-auto">
              {filteredRuleSets.length === 0 ? (
                <div className="p-8 text-center text-gray-500 dark:text-gray-400">
                  <Filter className="w-12 h-12 mx-auto mb-3 opacity-50" />
                  <p>No rule sets found</p>
                  {searchQuery && (
                    <p className="text-sm mt-2">Try adjusting your search</p>
                  )}
                </div>
              ) : (
                filteredRuleSets.map((ruleSet) => {
                  const stats = getRuleSetStats(ruleSet);
                  const isSelected = ruleSet.id === selectedRuleSetId;

                  return (
                    <div
                      key={ruleSet.id}
                      className={`border-b border-gray-100 dark:border-gray-700 last:border-b-0 ${
                        isSelected ? 'bg-blue-50 dark:bg-blue-900/20' : 'hover:bg-gray-50 dark:hover:bg-gray-700/50'
                      }`}
                    >
                      <div className="p-4">
                        <div className="flex items-start justify-between">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center space-x-2 mb-1">
                              <button
                                onClick={() => onSelect(ruleSet.id)}
                                className={`text-left flex-1 ${
                                  isSelected ? 'text-blue-600 dark:text-blue-400' : 'text-gray-900 dark:text-white'
                                }`}
                              >
                                <div className="flex items-center space-x-2">
                                  <h4 className="font-medium truncate">
                                    {ruleSet.name}
                                    {isSelected && (
                                      <Check className="w-4 h-4 inline-block ml-2" />
                                    )}
                                  </h4>
                                  <span className="text-xs px-2 py-0.5 bg-gray-100 dark:bg-gray-700 rounded">
                                    v{ruleSet.version}
                                  </span>
                                </div>
                              </button>
                              
                              <button
                                onClick={() => setShowActions(showActions === ruleSet.id ? null : ruleSet.id)}
                                className="p-1 hover:bg-gray-200 dark:hover:bg-gray-600 rounded"
                              >
                                <MoreVertical className="w-4 h-4" />
                              </button>
                            </div>

                            <p className="text-sm text-gray-600 dark:text-gray-400 mb-3 line-clamp-2">
                              {ruleSet.description}
                            </p>

                            {/* Stats */}
                            <div className="flex items-center space-x-4 text-xs">
                              <div className="flex items-center space-x-1">
                                <Hash className="w-3 h-3" />
                                <span className="text-gray-500 dark:text-gray-400">
                                  {stats.activeRules}/{stats.totalRules} active
                                </span>
                              </div>
                              <div className="flex items-center space-x-1">
                                <Calendar className="w-3 h-3" />
                                <span className="text-gray-500 dark:text-gray-400">
                                  {formatDate(ruleSet.lastModified)}
                                </span>
                              </div>
                              <div className="flex items-center space-x-1">
                                <User className="w-3 h-3" />
                                <span className="text-gray-500 dark:text-gray-400">
                                  {ruleSet.author}
                                </span>
                              </div>
                            </div>

                            {/* Progress Bar */}
                            {stats.totalRules > 0 && (
                              <div className="mt-2">
                                <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400 mb-1">
                                  <span>Completion</span>
                                  <span>{stats.completion}%</span>
                                </div>
                                <div className="h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                                  <div
                                    className="h-full bg-green-500 rounded-full"
                                    style={{ width: `${stats.completion}%` }}
                                  />
                                </div>
                              </div>
                            )}
                          </div>
                        </div>

                        {/* Actions Menu */}
                        <AnimatePresence>
                          {showActions === ruleSet.id && (
                            <motion.div
                              initial={{ opacity: 0, height: 0 }}
                              animate={{ opacity: 1, height: 'auto' }}
                              exit={{ opacity: 0, height: 0 }}
                              className="mt-3 pt-3 border-t border-gray-100 dark:border-gray-700"
                            >
                              <div className="grid grid-cols-3 gap-1">
                                <button
                                  onClick={() => handleRuleSetAction(ruleSet.id, 'edit')}
                                  className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded flex flex-col items-center text-xs"
                                >
                                  <Edit2 className="w-4 h-4 mb-1" />
                                  Edit
                                </button>
                                <button
                                  onClick={() => handleRuleSetAction(ruleSet.id, 'duplicate')}
                                  className="p-2 hover:bg-gray-100 dark:hover:bg-gray-700 rounded flex flex-col items-center text-xs"
                                >
                                  <Copy className="w-4 h-4 mb-1" />
                                  Duplicate
                                </button>
                                <button
                                  onClick={() => handleRuleSetAction(ruleSet.id, 'delete')}
                                  className="p-2 hover:bg-red-100 dark:hover:bg-red-900/20 text-red-600 rounded flex flex-col items-center text-xs"
                                >
                                  <Trash2 className="w-4 h-4 mb-1" />
                                  Delete
                                </button>
                              </div>
                            </motion.div>
                          )}
                        </AnimatePresence>
                      </div>
                    </div>
                  );
                })
              )}
            </div>

            {/* Footer */}
            <div className="p-4 border-t border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50">
              <div className="flex items-center justify-between text-sm text-gray-500 dark:text-gray-400">
                <div className="flex items-center space-x-2">
                  <Layers className="w-4 h-4" />
                  <span>{ruleSets.length} total rule sets</span>
                </div>
                <button
                  onClick={() => setIsOpen(false)}
                  className="text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300"
                >
                  Close
                </button>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Click outside to close */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setIsOpen(false)}
        />
      )}
    </div>
  );
};

// Export with default props for optional functions
export default RuleSetSelector;