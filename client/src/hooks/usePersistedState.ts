// src/hooks/usePersistedState.ts
import { useState, useEffect, useCallback } from 'react';

export function usePersistedState<T>(
  key: string,
  defaultValue: T,
  options?: {
    storage?: 'local' | 'session';
    debounce?: number;
    serialize?: (value: T) => any;
    deserialize?: (value: any) => T;
  }
): [T, (value: T | ((prev: T) => T)) => void] {
  const {
    storage = 'local',
    debounce = 300,
    serialize = JSON.stringify,
    deserialize = JSON.parse
  } = options || {};

  const [state, setState] = useState<T>(() => {
    try {
      const item = storage === 'local' 
        ? localStorage.getItem(key)
        : sessionStorage.getItem(key);
      if (item) {
        return deserialize(item);
      }
    } catch (error) {
      console.error(`Error loading ${key} from ${storage}Storage:`, error);
    }
    return defaultValue;
  });

  // Debounced save effect
  useEffect(() => {
    let timeoutId: NodeJS.Timeout;
    
    const saveState = () => {
      try {
        const serialized = serialize(state);
        if (storage === 'local') {
          localStorage.setItem(key, serialized);
        } else {
          sessionStorage.setItem(key, serialized);
        }
      } catch (error) {
        console.error(`Error saving ${key} to ${storage}Storage:`, error);
      }
    };

    if (debounce > 0) {
      timeoutId = setTimeout(saveState, debounce);
    } else {
      saveState();
    }

    return () => {
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, [state, key, storage, serialize, debounce]);

  // Save on window close
  useEffect(() => {
    const handleBeforeUnload = () => {
      const serialized = serialize(state);
      if (storage === 'local') {
        localStorage.setItem(key, serialized);
      } else {
        sessionStorage.setItem(key, serialized);
      }
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    return () => {
      handleBeforeUnload();
      window.removeEventListener('beforeunload', handleBeforeUnload);
    };
  }, [state, key, storage, serialize]);

  const setPersistedState = useCallback((value: T | ((prev: T) => T)) => {
    setState(prev => {
      const newValue = typeof value === 'function' 
        ? (value as (prev: T) => T)(prev)
        : value;
      return newValue;
    });
  }, []);

  return [state, setPersistedState];
}