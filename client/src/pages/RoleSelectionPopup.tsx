// src/components/canvas/RoleSelectionPopup.tsx - UPDATED
import React from 'react';

interface RoleSelectionModalProps {
  componentType: string;
  displayName: string;
  position: { x: number; y: number };
  onSelect: (role: 'INPUT' | 'OUTPUT') => void;
  onCancel: () => void;
}

const RoleSelectionModal: React.FC<RoleSelectionModalProps> = ({
  displayName,
  position,
  onSelect,
  onCancel,
}) => {
  // Handle click outside to cancel
  React.useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      const modal = document.querySelector('[data-role-selection-modal]');
      if (modal && !modal.contains(event.target as Node)) {
        onCancel();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [onCancel]);

  return (
    <div
      data-role-selection-modal
      style={{
        position: 'fixed',
        left: `${position.x}px`,
        top: `${position.y}px`,
        zIndex: 10000,
        backgroundColor: 'white',
        borderRadius: 8,
        boxShadow: '0 8px 32px rgba(0, 0, 0, 0.2)',
        border: '1px solid #e5e7eb',
        padding: 16,
        minWidth: 280,
        transform: 'translate(-50%, 0)',
      }}
      onClick={(e) => e.stopPropagation()}
    >
      <div style={{ marginBottom: 12 }}>
        <div style={{ fontSize: 14, fontWeight: 600, color: '#1f2937' }}>
          Select Role for {displayName}
        </div>
        <div style={{ fontSize: 12, color: '#6b7280', marginTop: 4 }}>
          Choose how to use this component
        </div>
      </div>

      <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
        <button
          onClick={() => {
            console.log('✅ Selected INPUT role');
            onSelect('INPUT');
          }}
          style={{
            flex: 1,
            padding: '12px 16px',
            backgroundColor: '#10b981',
            color: 'white',
            border: 'none',
            borderRadius: 6,
            fontSize: 13,
            fontWeight: 600,
            cursor: 'pointer',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            gap: 4,
            transition: 'all 0.2s ease',
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = '#0da271';
            e.currentTarget.style.transform = 'translateY(-1px)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = '#10b981';
            e.currentTarget.style.transform = 'translateY(0)';
          }}
        >
          <div style={{ fontSize: 16 }}>→</div>
          <div>Input Data Source</div>
          <div style={{ fontSize: 11, opacity: 0.9 }}>Reads data from source</div>
        </button>

        <button
          onClick={() => {
            console.log('✅ Selected OUTPUT role');
            onSelect('OUTPUT');
          }}
          style={{
            flex: 1,
            padding: '12px 16px',
            backgroundColor: '#3b82f6',
            color: 'white',
            border: 'none',
            borderRadius: 6,
            fontSize: 13,
            fontWeight: 600,
            cursor: 'pointer',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            gap: 4,
            transition: 'all 0.2s ease',
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = '#2563eb';
            e.currentTarget.style.transform = 'translateY(-1px)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = '#3b82f6';
            e.currentTarget.style.transform = 'translateY(0)';
          }}
        >
          <div style={{ fontSize: 16 }}>←</div>
          <div>Output Data Source</div>
          <div style={{ fontSize: 11, opacity: 0.9 }}>Writes data to destination</div>
        </button>
      </div>

      <button
        onClick={() => {
          console.log('❌ Role selection cancelled by user');
          onCancel();
        }}
        style={{
          width: '100%',
          padding: '8px 16px',
          backgroundColor: '#f3f4f6',
          color: '#6b7280',
          border: '1px solid #d1d5db',
          borderRadius: 6,
          fontSize: 13,
          cursor: 'pointer',
          transition: 'all 0.2s ease',
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.backgroundColor = '#e5e7eb';
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.backgroundColor = '#f3f4f6';
        }}
      >
        Cancel
      </button>
    </div>
  );
};

export default RoleSelectionModal;