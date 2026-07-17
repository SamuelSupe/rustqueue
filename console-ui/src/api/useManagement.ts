import { useCallback, useEffect, useState } from 'react';
import type { ActionPreview, ManagementAction, ManagementStatus } from './types';

export function useManagement() {
  const [status, setStatus] = useState<ManagementStatus>();
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);

  const refreshStatus = useCallback(async () => {
    try {
      setStatus(await request<ManagementStatus>('/api/v1/management'));
      setError(undefined);
    } catch (value) {
      setError(message(value));
    }
  }, []);

  useEffect(() => { void refreshStatus(); }, [refreshStatus]);
  useEffect(() => {
    if (!status?.expires_at_ms) return;
    const delay = Math.max(0, status.expires_at_ms - Date.now()) + 50;
    const timer = window.setTimeout(() => void refreshStatus(), delay);
    return () => window.clearTimeout(timer);
  }, [status?.expires_at_ms, refreshStatus]);

  const unlock = useCallback(async (confirmation: string) => {
    setBusy(true);
    try {
      const next = await request<ManagementStatus>('/api/v1/management/unlock', { confirmation });
      setStatus({ ...next, confirmation: status?.confirmation || confirmation });
      setError(undefined);
    } catch (value) {
      setError(message(value));
      throw value;
    } finally {
      setBusy(false);
    }
  }, [status?.confirmation]);

  const lock = useCallback(async () => {
    if (!status?.csrf_token) return;
    setBusy(true);
    try {
      await request('/api/v1/management/lock', {}, status.csrf_token);
      await refreshStatus();
    } finally {
      setBusy(false);
    }
  }, [refreshStatus, status?.csrf_token]);

  const preview = useCallback(async (action: ManagementAction) => {
    if (!status?.csrf_token) throw new Error('Management session is locked');
    setBusy(true);
    try {
      const value = await request<ActionPreview>('/api/v1/management/preview', action, status.csrf_token);
      setError(undefined);
      return value;
    } catch (value) {
      setError(message(value));
      throw value;
    } finally {
      setBusy(false);
    }
  }, [status?.csrf_token]);

  const apply = useCallback(async (action: ManagementAction, actionToken: string, confirmation: string) => {
    if (!status?.csrf_token) throw new Error('Management session is locked');
    setBusy(true);
    try {
      const value = await request<{ status: string; operation_id: string; resource_revision: number }>('/api/v1/management/apply', {
        ...action,
        action_token: actionToken,
        confirmation,
      }, status.csrf_token);
      setError(undefined);
      return value;
    } catch (value) {
      setError(message(value));
      throw value;
    } finally {
      setBusy(false);
    }
  }, [status?.csrf_token]);

  return { status, error, busy, unlock, lock, preview, apply, refreshStatus };
}

async function request<T = unknown>(path: string, body?: unknown, csrf?: string): Promise<T> {
  const response = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    credentials: 'same-origin',
    headers: body === undefined ? undefined : {
      'content-type': 'application/json',
      ...(csrf ? { 'x-rustqueue-csrf': csrf } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    const value = await response.json().catch(() => ({ detail: response.statusText })) as { detail?: string; code?: string };
    throw new Error(value.detail || value.code || `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

function message(value: unknown) {
  return value instanceof Error ? value.message : String(value);
}
