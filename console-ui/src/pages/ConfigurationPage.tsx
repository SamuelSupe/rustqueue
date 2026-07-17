import { CodeSnippet, Column, Grid, Tile } from '@carbon/react';
import type { Snapshot } from '../api/types';
import { PageHeader } from '../components/PageHeader';
import { StateTag } from '../components/StatusTag';
import { useI18n } from '../i18n';

export function ConfigurationPage({ snapshot }: { snapshot: Snapshot }) {
  const { t } = useI18n();
  const entries = flatten(snapshot.cluster.spec);
  return (
    <>
      <PageHeader title={t('configuration.title')} subtitle={t('configuration.subtitle')} meta={<StateTag value={snapshot.cluster.phase} />} />
      <Grid fullWidth narrow className="content-grid">
        <Column sm={4} md={8} lg={6}>
          <Tile className="panel config-identity">
            <div className="panel__header"><h2>{t('configuration.identity')}</h2></div>
            <dl className="detail-list">
              <div><dt>{t('configuration.name')}</dt><dd>{snapshot.cluster.name}</dd></div>
              <div><dt>{t('configuration.namespace')}</dt><dd>{snapshot.cluster.namespace}</dd></div>
              <div><dt>{t('configuration.generation')}</dt><dd>{snapshot.cluster.generation ?? 'N/A'}</dd></div>
              <div><dt>{t('configuration.observedGeneration')}</dt><dd>{snapshot.cluster.observed_generation ?? 'N/A'}</dd></div>
              <div><dt>{t('storage.featureLevel')}</dt><dd>{snapshot.cluster.active_storage_feature_level}</dd></div>
            </dl>
          </Tile>
        </Column>
        <Column sm={4} md={8} lg={10}>
          <Tile className="panel config-values">
            <div className="panel__header"><h2>{t('configuration.raw')}</h2></div>
            <div className="config-table">{entries.map(([key, value]) => <div key={key}><code>{key}</code><span>{value}</span></div>)}</div>
          </Tile>
        </Column>
      </Grid>
      <Tile className="panel raw-config">
        <CodeSnippet
          type="multi"
          aria-label={t('action.copy')}
          copyButtonDescription={t('action.copy')}
          feedback={t('action.copied')}
          showMoreText={t('action.showMore')}
          showLessText={t('action.showLess')}
          wrapText
        >
          {JSON.stringify(snapshot.cluster.spec, null, 2)}
        </CodeSnippet>
      </Tile>
    </>
  );
}

function flatten(value: Record<string, unknown>, prefix = ''): Array<[string, string]> {
  return Object.entries(value).flatMap(([key, item]) => {
    const path = prefix ? `${prefix}.${key}` : key;
    if (item && typeof item === 'object' && !Array.isArray(item)) return flatten(item as Record<string, unknown>, path);
    return [[path, Array.isArray(item) ? item.map(String).join(', ') : String(item)]] as Array<[string, string]>;
  });
}
