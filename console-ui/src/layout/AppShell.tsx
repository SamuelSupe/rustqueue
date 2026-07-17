import {
  Header,
  HeaderGlobalAction,
  HeaderGlobalBar,
  SideNav,
  SideNavItems,
  SideNavLink,
  SkipToContent,
} from '@carbon/react';
import { Dashboard, DataBase, Language, Moon, Renew, ServerProxy, Settings, StoragePool, Task, Sun } from '@carbon/icons-react';
import type { ReactNode } from 'react';
import { useI18n } from '../i18n';

export type Page = 'overview' | 'brokers' | 'topics' | 'storage' | 'operations' | 'configuration';

const items: Array<{ id: Page; icon: typeof Dashboard }> = [
  { id: 'overview', icon: Dashboard },
  { id: 'brokers', icon: ServerProxy },
  { id: 'topics', icon: DataBase },
  { id: 'storage', icon: StoragePool },
  { id: 'operations', icon: Task },
  { id: 'configuration', icon: Settings },
];

export function AppShell({ page, onPage, dark, onTheme, onRefresh, children }: {
  page: Page;
  onPage: (page: Page) => void;
  dark: boolean;
  onTheme: () => void;
  onRefresh: () => void;
  children: ReactNode;
}) {
  const { language, setLanguage, t } = useI18n();
  return (
    <div className="app-shell">
      <Header aria-label={t('app.name')}>
        <SkipToContent>{t('action.skipToContent')}</SkipToContent>
        <a className="header-brand" href="#overview" aria-label="RustQueue Console">
          <img className="header-brand__icon" src="/rustqueue-icon.svg" alt="" />
          <span className="header-brand__name">RustQueue</span>
          <span className="header-brand__product">Console</span>
        </a>
        <span className="header-mode">{t('app.readOnly')}</span>
        <HeaderGlobalBar>
          <HeaderGlobalAction aria-label={t('action.refresh')} tooltipAlignment="end" onClick={onRefresh}><Renew size={20} /></HeaderGlobalAction>
          <HeaderGlobalAction aria-label={t('action.language')} tooltipAlignment="end" onClick={() => setLanguage(language === 'zh' ? 'en' : 'zh')}><Language size={20} /></HeaderGlobalAction>
          <HeaderGlobalAction aria-label={t('action.theme')} tooltipAlignment="end" onClick={onTheme}>{dark ? <Sun size={20} /> : <Moon size={20} />}</HeaderGlobalAction>
        </HeaderGlobalBar>
      </Header>
      <SideNav aria-label={t('app.name')} expanded isPersistent>
        <SideNavItems>
          {items.map((item) => <SideNavLink
            key={item.id}
            href={`#${item.id}`}
            renderIcon={item.icon}
            isActive={page === item.id}
            onClick={(event) => { event.preventDefault(); onPage(item.id); }}
          >{t(`nav.${item.id}`)}</SideNavLink>)}
        </SideNavItems>
        <div className="sidenav-foot"><span>{t('app.readOnly')}</span><small>v0.7</small></div>
      </SideNav>
      <main id="main-content" className="main-content">{children}</main>
    </div>
  );
}
