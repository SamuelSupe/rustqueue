import { InlineNotification, Theme } from '@carbon/react';
import { lazy, Suspense, useEffect, useState } from 'react';
import type { Snapshot } from './api/types';
import { useSnapshot } from './api/useSnapshot';
import { LoadingState } from './components/LoadingState';
import { AppShell, type Page } from './layout/AppShell';
import { useI18n } from './i18n';

const OverviewPage = lazy(() => import('./pages/OverviewPage').then((module) => ({ default: module.OverviewPage })));
const BrokersPage = lazy(() => import('./pages/BrokersPage').then((module) => ({ default: module.BrokersPage })));
const TopicsPage = lazy(() => import('./pages/TopicsPage').then((module) => ({ default: module.TopicsPage })));
const StoragePage = lazy(() => import('./pages/StoragePage').then((module) => ({ default: module.StoragePage })));
const OperationsPage = lazy(() => import('./pages/OperationsPage').then((module) => ({ default: module.OperationsPage })));
const ConfigurationPage = lazy(() => import('./pages/ConfigurationPage').then((module) => ({ default: module.ConfigurationPage })));

const pages: Page[] = ['overview', 'brokers', 'topics', 'storage', 'operations', 'configuration'];

export default function App() {
  const { t } = useI18n();
  const { snapshot, error, loading, refresh } = useSnapshot();
  const [page, setPage] = useState<Page>(() => pageFromHash());
  const [dark, setDark] = useState(() => {
    const saved = localStorage.getItem('rustqueue-theme');
    return saved ? saved === 'dark' : window.matchMedia('(prefers-color-scheme: dark)').matches;
  });

  useEffect(() => {
    const update = () => setPage(pageFromHash());
    window.addEventListener('hashchange', update);
    return () => window.removeEventListener('hashchange', update);
  }, []);

  const navigate = (next: Page) => {
    window.location.hash = next;
    setPage(next);
  };
  const toggleTheme = () => setDark((value) => {
    localStorage.setItem('rustqueue-theme', value ? 'light' : 'dark');
    return !value;
  });

  return (
    <Theme theme={dark ? 'g100' : 'g10'} className="theme-root">
      <AppShell page={page} onPage={navigate} dark={dark} onTheme={toggleTheme} onRefresh={() => void refresh()}>
        {error && <InlineNotification className="global-notification" kind="error" lowContrast hideCloseButton title={t('common.error')} subtitle={error.includes('403') || error.includes('401') ? t('common.permission') : error} />}
        {loading && !snapshot ? <LoadingState /> : snapshot ? <Suspense fallback={<LoadingState />}>{renderPage(page, snapshot, dark, refresh)}</Suspense> : <LoadingState />}
      </AppShell>
    </Theme>
  );
}

function pageFromHash(): Page {
  const value = window.location.hash.slice(1) as Page;
  return pages.includes(value) ? value : 'overview';
}

function renderPage(page: Page, snapshot: Snapshot, dark: boolean, refresh: () => Promise<void>) {
  switch (page) {
    case 'brokers': return <BrokersPage snapshot={snapshot} />;
    case 'topics': return <TopicsPage snapshot={snapshot} refresh={refresh} />;
    case 'storage': return <StoragePage snapshot={snapshot} />;
    case 'operations': return <OperationsPage snapshot={snapshot} />;
    case 'configuration': return <ConfigurationPage snapshot={snapshot} />;
    default: return <OverviewPage snapshot={snapshot} dark={dark} />;
  }
}
