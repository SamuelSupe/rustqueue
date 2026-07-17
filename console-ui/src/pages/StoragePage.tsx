import { Column, Grid, ProgressBar, Table, TableBody, TableCell, TableContainer, TableHead, TableHeader, TableRow, Tag, Tile } from '@carbon/react';
import type { Snapshot } from '../api/types';
import { EmptyState } from '../components/EmptyState';
import { MetricTile } from '../components/MetricTile';
import { PageHeader } from '../components/PageHeader';
import { StatusTag } from '../components/StatusTag';
import { useI18n } from '../i18n';
import { bytes, micros, percentile } from '../utils/format';

export function StoragePage({ snapshot }: { snapshot: Snapshot }) {
  const { t } = useI18n();
  const storage = snapshot.storage;
  const first = snapshot.brokers.find((broker) => broker.observation)?.observation;
  return (
    <>
      <PageHeader title={t('storage.title')} subtitle={t('storage.subtitle')} meta={<StatusTag ready={storage.pressure_brokers.length === 0} />} />
      <Grid fullWidth narrow className="metric-grid">
        <Column sm={4} md={4} lg={4}><MetricTile label={t('storage.capacity')} value={bytes(storage.total_bytes)} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('storage.available')} value={bytes(storage.available_bytes)} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('storage.segmentBytes')} value={bytes(storage.segment_bytes)} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('storage.segmentCount')} value={String(storage.segment_count)} /></Column>
      </Grid>
      <Grid fullWidth narrow className="content-grid">
        <Column sm={4} md={8} lg={10}>
          <TableContainer className="panel" title={t('storage.brokerTable')}>
            <Table size="md" useZebraStyles>
              <TableHead><TableRow>{[t('brokers.name'), t('common.status'), t('brokers.pvc'), t('storage.capacity'), t('storage.available'), t('overview.disk'), t('storage.segmentCount')].map((value) => <TableHeader key={value}>{value}</TableHeader>)}</TableRow></TableHead>
              <TableBody>{snapshot.brokers.map((broker) => {
                const value = broker.observation;
                return <TableRow key={broker.name}>
                  <TableCell>{broker.name}</TableCell>
                  <TableCell><StatusTag ready={Boolean(value?.readiness.storage_healthy && !value.disk.pressure)} /></TableCell>
                  <TableCell>{broker.pvc?.name || 'N/A'}<br /><span className="table-secondary">{broker.pvc?.capacity || broker.pvc?.requested}</span></TableCell>
                  <TableCell>{bytes(value?.disk.total_bytes || 0)}</TableCell>
                  <TableCell>{bytes(value?.disk.available_bytes || 0)}</TableCell>
                  <TableCell>{value ? <div className="table-progress"><span>{value.disk.used_percent}%</span><ProgressBar hideLabel label={t('overview.disk')} value={value.disk.used_percent} max={100} status={value.disk.pressure ? 'error' : 'active'} /></div> : 'N/A'}</TableCell>
                  <TableCell>{value?.storage.segment_count || 0}</TableCell>
                </TableRow>;
              })}</TableBody>
            </Table>
          </TableContainer>
        </Column>
        <Column sm={4} md={8} lg={6}>
          <Tile className="panel">
            <div className="panel__header"><h2>{t('storage.watermarks')}</h2></div>
            {first ? <dl className="detail-list">
              <div><dt>{t('storage.high')}</dt><dd>{first.disk.high_watermark_percent}%</dd></div>
              <div><dt>{t('storage.low')}</dt><dd>{first.disk.low_watermark_percent}%</dd></div>
              <div><dt>{t('storage.minFree')}</dt><dd>{bytes(first.disk.min_free_bytes)}</dd></div>
            </dl> : <EmptyState title={t('common.empty')} />}
            {storage.pressure_brokers.length ? <div className="tag-row">{storage.pressure_brokers.map((name) => <Tag key={name} type="red">{name}</Tag>)}</div> : <p className="healthy-copy">{t('storage.noPressure')}</p>}
          </Tile>
          <Tile className="panel">
            <div className="panel__header"><h2>{t('storage.compatibility')}</h2></div>
            <dl className="detail-list">
              <div><dt>{t('storage.dataFormat')}</dt><dd>{first?.node.data_format || 'N/A'}</dd></div>
              <div><dt>{t('storage.featureLevel')}</dt><dd>{snapshot.cluster.active_storage_feature_level}</dd></div>
            </dl>
          </Tile>
        </Column>
      </Grid>
      <Tile className="panel latency-panel">
        <div className="panel__header"><h2>{t('storage.latency')}</h2></div>
        <Grid fullWidth narrow className="latency-grid">
          {[
            [t('storage.fsync'), storage.fsync],
            [t('storage.groupCommit'), storage.group_commit_wait],
            [t('storage.payloadRead'), storage.payload_read],
            [t('storage.scrub'), storage.scrub],
            [t('storage.gc'), storage.gc],
          ].map(([label, histogram]) => <Column key={label as string} sm={2} md={4} lg={3}><MetricTile label={label as string} value={micros(percentile(histogram as typeof storage.fsync))} meta={`${(histogram as typeof storage.fsync).count} samples`} /></Column>)}
        </Grid>
      </Tile>
    </>
  );
}
