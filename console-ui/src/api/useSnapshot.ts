import { useCallback, useEffect, useRef, useState } from 'react';
import type { Snapshot } from './types';

export function useSnapshot() {
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const request = useRef<AbortController | undefined>(undefined);

  const refresh = useCallback(async () => {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    try {
      const response = await fetch('/api/v1/snapshot', {
        signal: controller.signal,
        headers: { Accept: 'application/json' },
      });
      if (!response.ok) {
        const body = await response.json().catch(() => ({}));
        throw new Error(body.message || `HTTP ${response.status}`);
      }
      setSnapshot((await response.json()) as Snapshot);
      setError(undefined);
    } catch (reason) {
      if ((reason as Error).name !== 'AbortError') setError((reason as Error).message);
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const interval = window.setInterval(() => void refresh(), 2000);
    return () => {
      window.clearInterval(interval);
      request.current?.abort();
    };
  }, [refresh]);

  return { snapshot, error, loading, refresh };
}
