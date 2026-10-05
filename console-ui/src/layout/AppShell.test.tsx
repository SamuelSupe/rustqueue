// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { I18nProvider } from '../i18n';
import { AppShell } from './AppShell';

describe('compact navigation', () => {
  beforeEach(() => {
    localStorage.setItem('rustqueue-language', 'en');
    vi.stubGlobal('matchMedia', vi.fn((media: string) => ({
      matches: false,
      media,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    })));
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  function renderShell() {
    const onPage = vi.fn();
    const view = render(
      <I18nProvider>
        <AppShell page="overview" onPage={onPage} dark={false} onTheme={vi.fn()} onRefresh={vi.fn()}>
          <h1>Cluster overview</h1>
        </AppShell>
      </I18nProvider>,
    );
    return { ...view, onPage };
  }

  it('opens navigation and closes it after selecting a page', () => {
    const { onPage } = renderShell();
    const menu = screen.getByRole('button', { name: 'Open navigation' });
    expect(menu.getAttribute('aria-expanded')).toBe('false');
    fireEvent.click(menu);
    expect(screen.getByRole('button', { name: 'Close navigation' }).getAttribute('aria-expanded')).toBe('true');

    fireEvent.click(screen.getByRole('link', { name: 'Configuration' }));
    expect(onPage).toHaveBeenCalledWith('configuration');
    expect(screen.getByRole('button', { name: 'Open navigation' }).getAttribute('aria-expanded')).toBe('false');
  });

  it.each(['menu', 'overlay', 'Escape'] as const)('dismisses navigation with %s', (action) => {
    const { container } = renderShell();
    fireEvent.click(screen.getByRole('button', { name: 'Open navigation' }));
    if (action === 'menu') {
      const menu = screen.getByRole('button', { name: 'Close navigation' });
      fireEvent.blur(screen.getByRole('navigation'), { relatedTarget: menu });
      fireEvent.click(menu);
    } else if (action === 'overlay') {
      fireEvent.click(container.querySelector('.cds--side-nav__overlay')!);
    } else {
      fireEvent.keyDown(screen.getByRole('navigation'), { key: 'Escape' });
      expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Open navigation' }));
    }
    expect(screen.getByRole('button', { name: 'Open navigation' }).getAttribute('aria-expanded')).toBe('false');
  });
});
