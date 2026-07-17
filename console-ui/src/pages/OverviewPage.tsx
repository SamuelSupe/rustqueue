import { Column, Grid, InlineNotification, ProgressBar, Tile } from '@carbon/react';
import { Activity, DataBase, Events, IbmCloudHyperProtectDbaas, Network_3, WarningAlt } from '@carbon/icons-react';
import type { Snapshot } from '../api/types';
import { EmptyState } from '../components/EmptyState';
import { MetricTile } from '../components/MetricTile';
import { PageHeader } from '../components/PageHeader';
import { StateTag, StatusTag } from '../components/StatusTag';
import { TrendChart } from '../components/TrendChart';
import { useI18n } from '../i18n';
import { bytes, duration, number } from '../utils/format';

export function OverviewPage({ snapshot, dark }: { snapshot: Snapshot; dark: boolean }) {
  const { t } = useI18n();
  const { summary, cluster, storage } = snapshot;
  return (
    <>
      <PageHeader
        title={t('overview.title')}
        subtitle={t('overview.subtitle')}
        meta={<><StateTag value={cluster.phase} /><span>{cluster.namespace} / {cluster.name}</span><span>{t('common.updated')} {duration(snapshot.collected_at_ms)}</span></>}
      />
      {!snapshot.complete && (
        <InlineNotification
          kind="warning"
          lowContrast
          hideCloseButton
          title={t('common.partial')}
          subtitle={snapshot.errors.join('; ')}
        />
      )}
      <Grid fullWidth narrow className="metric-grid">
        <Column sm={4} md={4} lg={4}><MetricTile label={t('overview.publishRate')} value={number(summary.publish_per_second)} meta={t('overview.rateUnit')} icon={<Activity size={20} />} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('overview.deliverRate')} value={number(summary.deliver_per_second)} meta={t('overview.rateUnit')} icon={<Events size={20} />} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('overview.backlog')} value={number(summary.depth)} meta={`${number(summary.stored_messages)} ${t('topics.messages')}`} icon={<DataBase size={20} />} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('overview.inFlight')} value={number(summary.in_flight)} meta={`${number(summary.deferred)} ${t('topics.deferred')}`} icon={<Network_3 size={20} />} /></Column>
        <Column sm={4} md={4} lg={4}>
          <Tile className="metric-tile">
            <div className="metric-tile__head"><span>{t('overview.disk')}</span><IbmCloudHyperProtectDbaas size={20} /></div>
            <div className="metric-tile__value">{storage.used_percent.toFixed(1)}%</div>
            <ProgressBar hideLabel label={t('overview.disk')} value={storage.used_percent} max={100} status={storage.pressure_brokers.length ? 'error' : 'active'} />
            <div className="metric-tile__meta">{bytes(storage.available_bytes)} {t('storage.available')}</div>
          </Tile>
        </Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('overview.brokers')} value={`${cluster.ready_brokers} / ${cluster.desired_brokers}`} meta={`${number(summary.connections)} ${t('overview.connections')}`} icon={<StatusTag ready={cluster.ready_brokers === cluster.desired_brokers} />} /></Column>
      </Grid>
      <Grid fullWidth narrow className="content-grid">
        <Column sm={4} md={8} lg={10}>
          <Tile className="panel chart-panel">
            <div className="panel__header"><div><h2>{t('overview.throughput')}</h2><p>{t('overview.throughputHint')}</p></div></div>
            {snapshot.history.length > 1 ? <TrendChart samples={snapshot.history} dark={dark} /> : <EmptyState title={t('common.loading')} detail={t('overview.throughputHint')} />}
          </Tile>
        </Column>
        <Column sm={4} md={8} lg={6}>
          <Tile className="panel">
            <div className="panel__header"><h2>{t('overview.anomalies')}</h2><WarningAlt size={20} /></div>
            {snapshot.anomalies.length === 0 ? <EmptyState title={t('overview.noAnomalies')} /> : (
              <div className="signal-list">
                {snapshot.anomalies.map((item) => (
                  <div className="signal" key={`${item.code}-${item.subject}`}>
                    <StatusTag
                      ready={item.severity !== 'critical'}
                      label={t(`severity.${item.severity}`)}
                      tone={item.severity === 'critical' ? 'red' : 'warm-gray'}
                    />
                    <div><strong>{t(`anomaly.${item.code}`)}</strong><span>{item.subject}</span><p>{item.detail}</p></div>
                  </div>
                ))}
              </div>
            )}
          </Tile>
          <Tile className="panel operation-card">
            <div className="panel__header"><h2>{t('overview.currentOperation')}</h2></div>
            {snapshot.current_operation ? (
              <div className="operation-summary"><StateTag value={snapshot.current_operation.phase} /><strong>{snapshot.current_operation.kind}</strong><span>{snapshot.current_operation.target}</span><p>{snapshot.current_operation.message}</p></div>
            ) : <EmptyState title={t('overview.noOperation')} />}
          </Tile>
        </Column>
      </Grid>
    </>
  );
}
