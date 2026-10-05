import { useCallback, useEffect, useRef, useState } from 'react';
import type { Snapshot } from './types';

export function useSnapshot() {
  const [snapshot, setSnapshot] = useState<Snapshot>();
  const [error, setError] = useState<string>();
  const [loading, setLoading] = useState(true);
  const request = useRef<AbortController | undefined>(undefined);

  const refresh = useCallback(async () => {
    if (request.current && !request.current.signal.aborted) return;
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
      const next = (await response.json()) as Snapshot;
      if (controller.signal.aborted) return;
      setSnapshot(next);
      setError(undefined);
    } catch (reason) {
      if (!controller.signal.aborted && (reason as Error).name !== 'AbortError') {
        setError((reason as Error).message);
      }
    } finally {
      if (request.current === controller) request.current = undefined;
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
