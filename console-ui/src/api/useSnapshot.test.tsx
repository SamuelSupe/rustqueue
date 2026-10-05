// @vitest-environment jsdom

import { StrictMode } from 'react';
import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Snapshot } from './types';
import { useSnapshot } from './useSnapshot';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function response(snapshot: Snapshot): Response {
  return {
    ok: true,
    status: 200,
    json: vi.fn().mockResolvedValue(snapshot),
  } as unknown as Response;
}

function errorResponse(message: string): Response {
  return {
    ok: false,
    status: 503,
    json: vi.fn().mockResolvedValue({ message }),
  } as unknown as Response;
}

function snapshot(schemaVersion: number): Snapshot {
  return { schema_version: schemaVersion } as Snapshot;
}

function Probe() {
  const { snapshot: value, error, loading } = useSnapshot();
  return (
    <div>
      <span data-testid="schema">{value?.schema_version ?? ''}</span>
      <span data-testid="state">{loading ? 'loading' : 'ready'}</span>
      <span data-testid="error">{error ?? ''}</span>
    </div>
  );
}

describe('useSnapshot polling', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('keeps a slow request alive and refreshes after it completes', async () => {
    const requests: Array<{
      signal: AbortSignal;
      deferred: ReturnType<typeof deferred<Response>>;
    }> = [];
    const fetchMock = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      const request = { signal: init?.signal as AbortSignal, deferred: deferred<Response>() };
      requests.push(request);
      return request.deferred.promise;
    });
    vi.stubGlobal('fetch', fetchMock);

    render(<Probe />);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(6000);
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(requests[0].signal.aborted).toBe(false);

    await act(async () => {
      requests[0].deferred.resolve(response(snapshot(1)));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByTestId('schema').textContent).toBe('1');
    expect(screen.getByTestId('state').textContent).toBe('ready');

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      requests[1].deferred.resolve(response(snapshot(2)));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByTestId('schema').textContent).toBe('2');
  });

  it.each(['success', 'failure'] as const)(
    'does not let a StrictMode-aborted %s clear the replacement request',
    async (lateResult) => {
      const requests: Array<{
        signal: AbortSignal;
        deferred: ReturnType<typeof deferred<Response>>;
      }> = [];
      const fetchMock = vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
        const request = { signal: init?.signal as AbortSignal, deferred: deferred<Response>() };
        requests.push(request);
        return request.deferred.promise;
      });
      vi.stubGlobal('fetch', fetchMock);

      render(
        <StrictMode>
          <Probe />
        </StrictMode>,
      );
      expect(fetchMock).toHaveBeenCalledTimes(2);
      expect(requests[0].signal.aborted).toBe(true);
      expect(requests[1].signal.aborted).toBe(false);

      await act(async () => {
        requests[0].deferred.resolve(
          lateResult === 'success'
            ? response(snapshot(1))
            : errorResponse('stale backend failure'),
        );
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(screen.getByTestId('schema').textContent).toBe('');
      expect(screen.getByTestId('error').textContent).toBe('');
      await act(async () => {
        await vi.advanceTimersByTimeAsync(2000);
      });
      expect(fetchMock).toHaveBeenCalledTimes(2);

      await act(async () => {
        requests[1].deferred.resolve(response(snapshot(2)));
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(screen.getByTestId('schema').textContent).toBe('2');
      expect(screen.getByTestId('error').textContent).toBe('');
    },
  );
});
