import { Tag } from '@carbon/react';
import { useI18n } from '../i18n';

export function StatusTag({ ready, label, tone }: {
  ready: boolean;
  label?: string;
  tone?: 'green' | 'red' | 'warm-gray' | 'cool-gray';
}) {
  const { t } = useI18n();
  return <Tag type={tone || (ready ? 'green' : 'red')} size="sm">{label || t(ready ? 'common.ready' : 'common.notReady')}</Tag>;
}

export function StateTag({ value }: { value: string }) {
  const { t } = useI18n();
  const normalized = value.toLowerCase();
  const positive = ['ready', 'running', 'bound', 'completed', 'true', 'active'].includes(normalized);
  const warning = ['pending', 'progressing', 'preflight', 'maintenance', 'unknown'].includes(normalized);
  const translated = t(`state.${normalized}`);
  const label = translated.startsWith('state.') ? value : translated;
  return <Tag type={positive ? 'green' : warning ? 'cool-gray' : 'red'} size="sm">{label || 'N/A'}</Tag>;
}
