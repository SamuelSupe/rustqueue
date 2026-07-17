import { Column, Grid, ProgressBar, Table, TableBody, TableCell, TableContainer, TableHead, TableHeader, TableRow, Tile } from '@carbon/react';
import { useState } from 'react';
import type { Broker, Snapshot } from '../api/types';
import { EmptyState } from '../components/EmptyState';
import { MetricTile } from '../components/MetricTile';
import { PageHeader } from '../components/PageHeader';
import { StatusTag } from '../components/StatusTag';
import { useI18n } from '../i18n';
import { bytes, duration, number, shortImage } from '../utils/format';

export function BrokersPage({ snapshot }: { snapshot: Snapshot }) {
  const { t } = useI18n();
  const [selected, setSelected] = useState(snapshot.brokers[0]?.name);
  const broker = snapshot.brokers.find((item) => item.name === selected) || snapshot.brokers[0];
  return (
    <>
      <PageHeader title={t('brokers.title')} subtitle={t('brokers.subtitle')} meta={<span>{snapshot.brokers.length} Broker</span>} />
      {snapshot.brokers.length === 0 ? <EmptyState title={t('common.empty')} /> : (
        <Grid fullWidth narrow className="split-grid">
          <Column sm={4} md={8} lg={11}>
            <TableContainer className="panel" title={t('brokers.title')}>
              <Table size="md" useZebraStyles>
                <TableHead><TableRow>
                  {[t('brokers.name'), t('common.status'), t('brokers.node'), t('brokers.version'), t('brokers.connections'), t('brokers.disk'), t('brokers.pvc'), t('brokers.restarts')].map((label) => <TableHeader key={label}>{label}</TableHeader>)}
                </TableRow></TableHead>
                <TableBody>
                  {snapshot.brokers.map((item) => <BrokerRow key={item.name} broker={item} selected={item.name === broker?.name} onSelect={() => setSelected(item.name)} />)}
                </TableBody>
              </Table>
            </TableContainer>
          </Column>
          <Column sm={4} md={8} lg={5}>{broker && <BrokerDetail broker={broker} />}</Column>
        </Grid>
      )}
    </>
  );
}

function BrokerRow({ broker, selected, onSelect }: { broker: Broker; selected: boolean; onSelect: () => void }) {
  const { t } = useI18n();
  const observation = broker.observation;
  return (
    <TableRow className={selected ? 'selected-row' : ''} onClick={onSelect}>
      <TableCell><button className="table-link" onClick={onSelect}>{broker.name}</button></TableCell>
      <TableCell><StatusTag ready={Boolean(observation?.readiness.publish_ready && broker.ready)} /></TableCell>
      <TableCell>{broker.node_name || 'N/A'}</TableCell>
      <TableCell>{observation?.node.version || shortImage(broker.image)}</TableCell>
      <TableCell>{number(observation?.runtime.tcp_connections || 0)}</TableCell>
      <TableCell>{observation ? `${observation.disk.used_percent}%` : 'N/A'}</TableCell>
      <TableCell>{broker.pvc?.capacity || broker.pvc?.requested || 'N/A'}</TableCell>
      <TableCell>{broker.restarts}</TableCell>
    </TableRow>
  );
}

function BrokerDetail({ broker }: { broker: Broker }) {
  const { t } = useI18n();
  const value = broker.observation;
  return (
    <div className="detail-stack">
      <Tile className="panel broker-detail">
        <div className="panel__header"><div><h2>{broker.name}</h2><p>{broker.pod_ip}</p></div><StatusTag ready={Boolean(value?.readiness.publish_ready)} /></div>
        {broker.error && <p className="error-copy">{broker.error}</p>}
        <dl className="detail-list">
          <div><dt>{t('brokers.image')}</dt><dd>{broker.image}</dd></div>
          <div><dt>{t('brokers.started')}</dt><dd>{duration(broker.started_at)}</dd></div>
          <div><dt>{t('brokers.readiness')}</dt><dd>{value?.readiness.process_ready ? t('common.ready') : t('common.notReady')}</dd></div>
          <div><dt>{t('brokers.storage')}</dt><dd>{value?.readiness.storage_healthy ? t('common.healthy') : t('common.unhealthy')}</dd></div>
          <div><dt>{t('brokers.publish')}</dt><dd>{value?.readiness.publish_ready ? t('common.yes') : t('common.no')}</dd></div>
          <div><dt>{t('brokers.consume')}</dt><dd>{value?.readiness.consume_ready ? t('common.yes') : t('common.no')}</dd></div>
          <div><dt>{t('brokers.capability')}</dt><dd>format {value?.node.data_format || 'N/A'}</dd></div>
        </dl>
        {value && <ProgressBar label={t('brokers.disk')} value={value.disk.used_percent} max={100} status={value.disk.pressure ? 'error' : 'active'} />}
      </Tile>
      {value && <Grid fullWidth narrow className="mini-metrics">
        <Column sm={2} md={4} lg={8}><MetricTile label={t('topics.messages')} value={number(value.queue.topics.reduce((sum, topic) => sum + topic.message_count, 0))} /></Column>
        <Column sm={2} md={4} lg={8}><MetricTile label={t('storage.segmentBytes')} value={bytes(value.storage.segment_bytes)} /></Column>
        <Column sm={2} md={4} lg={8}><MetricTile label={t('topics.retryTotal')} value={number(value.runtime.requeued_messages)} /></Column>
        <Column sm={2} md={4} lg={8}><MetricTile label={t('overview.connections')} value={number(value.runtime.tcp_connections)} /></Column>
      </Grid>}
    </div>
  );
}
