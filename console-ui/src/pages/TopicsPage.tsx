import { Add } from '@carbon/icons-react';
import { Button, Column, Grid, InlineLoading, InlineNotification, Search, Table, TableBody, TableCell, TableContainer, TableHead, TableHeader, TableRow, Tag, Tile } from '@carbon/react';
import { useMemo, useState } from 'react';
import { useManagement } from '../api/useManagement';
import type { ManagementAction, Snapshot, Topic } from '../api/types';
import { CreateResourceDialog } from '../components/CreateResourceDialog';
import { EmptyState } from '../components/EmptyState';
import { ManagementAccess } from '../components/ManagementAccess';
import { ManagementActionDialog } from '../components/ManagementActionDialog';
import { MetricTile } from '../components/MetricTile';
import { PageHeader } from '../components/PageHeader';
import { ResourceActionMenu } from '../components/ResourceActionMenu';
import { StatusTag } from '../components/StatusTag';
import { useI18n } from '../i18n';
import { bytes, number } from '../utils/format';

export function TopicsPage({ snapshot, refresh }: { snapshot: Snapshot; refresh: () => Promise<void> }) {
  const { t } = useI18n();
  const management = useManagement();
  const [query, setQuery] = useState('');
  const [createKind, setCreateKind] = useState<'topic' | 'channel'>();
  const [pending, setPending] = useState<ManagementAction>();
  const [notice, setNotice] = useState<string>();
  const [settling, setSettling] = useState(false);
  const filtered = useMemo(() => snapshot.topics.filter((topic) => topic.name.toLowerCase().includes(query.toLowerCase())), [query, snapshot.topics]);
  const [selected, setSelected] = useState(snapshot.topics[0]?.name);
  const topic = filtered.find((item) => item.name === selected) || filtered[0];
  const enabled = Boolean(management.status?.enabled);
  const unlocked = Boolean(management.status?.unlocked);
  const complete = (_action: ManagementAction) => {
    setPending(undefined);
    setSettling(true);
    setNotice(t('management.accepted'));
    void (async () => {
      try {
        await refresh();
        await new Promise((resolve) => window.setTimeout(resolve, 2200));
        await refresh();
      } finally {
        setSettling(false);
      }
    })();
  };
  return (
    <>
      <PageHeader
        title={t('topics.title')}
        subtitle={t('topics.subtitle')}
        meta={<>
          <Tag type="cool-gray">{snapshot.topics.length} Topic</Tag>
          <Tag type="blue">{snapshot.topics.reduce((sum, item) => sum + item.channels.length, 0)} Channel</Tag>
          {enabled && <Button size="sm" renderIcon={Add} disabled={!unlocked || settling} onClick={() => setCreateKind('topic')}>{t('management.createTopic')}</Button>}
        </>}
      />
      <ManagementAccess status={management.status} busy={management.busy} error={management.error} onUnlock={management.unlock} onLock={management.lock} />
      {notice && <InlineNotification className="management-notice" kind="success" lowContrast title={t('management.acceptedTitle')} subtitle={notice} onClose={() => setNotice(undefined)} />}
      {settling && <InlineLoading className="management-settling" description={t('management.refreshing')} />}
      <Grid fullWidth narrow className="metric-grid metric-grid--compact">
        <Column sm={4} md={4} lg={4}><MetricTile label={t('topics.messages')} value={number(snapshot.summary.stored_messages)} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('topics.depth')} value={number(snapshot.summary.depth)} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('topics.retryTotal')} value={number(snapshot.summary.retry_total)} /></Column>
        <Column sm={4} md={4} lg={4}><MetricTile label={t('topics.deadLetterTotal')} value={number(snapshot.summary.dead_letter_total)} /></Column>
      </Grid>
      <Search size="lg" labelText={t('topics.search')} placeholder={t('topics.search')} value={query} onChange={(event) => setQuery(event.target.value)} className="topic-search" />
      {filtered.length === 0 ? <EmptyState title={t(query ? 'common.noMatch' : 'common.empty')} /> : (
        <Grid fullWidth narrow className="split-grid">
          <Column sm={4} md={8} lg={9}>
            <TableContainer className="panel" title={t('topics.title')}>
              <Table size="md" useZebraStyles>
                <TableHead><TableRow>{[t('topics.name'), t('common.status'), t('topics.messages'), t('topics.channels'), t('topics.segments'), t('common.owner'), ...(enabled ? [t('management.actions')] : [])].map((value) => <TableHeader key={value}>{value}</TableHeader>)}</TableRow></TableHead>
                <TableBody>{filtered.map((item) => <TopicRow key={item.name} topic={item} selected={item.name === topic?.name} managementEnabled={enabled} managementUnlocked={unlocked && !settling} onAction={setPending} onSelect={() => setSelected(item.name)} />)}</TableBody>
              </Table>
            </TableContainer>
          </Column>
          <Column sm={4} md={8} lg={7}>{topic && <ChannelPanel topic={topic} managementEnabled={enabled} managementUnlocked={unlocked && !settling} onCreate={() => setCreateKind('channel')} onAction={setPending} />}</Column>
        </Grid>
      )}
      <CreateResourceDialog open={Boolean(createKind)} kind={createKind || 'topic'} topic={topic?.name} onClose={() => setCreateKind(undefined)} onContinue={(action) => { setCreateKind(undefined); setPending(action); }} />
      <ManagementActionDialog action={pending} busy={management.busy || settling} onPreview={management.preview} onApply={management.apply} onClose={() => setPending(undefined)} onComplete={complete} />
    </>
  );
}

function TopicRow({ topic, selected, managementEnabled, managementUnlocked, onAction, onSelect }: { topic: Topic; selected: boolean; managementEnabled: boolean; managementUnlocked: boolean; onAction: (action: ManagementAction) => void; onSelect: () => void }) {
  const { t } = useI18n();
  return (
    <TableRow className={selected ? 'selected-row' : ''} onClick={onSelect}>
      <TableCell><button className="table-link" onClick={onSelect}>{topic.name}</button></TableCell>
      <TableCell><ResourceStatus phase={topic.managed_phase} paused={topic.paused} tombstoneUntil={topic.tombstone_until_ms} /></TableCell>
      <TableCell>{number(topic.stored_messages)}</TableCell>
      <TableCell>{topic.channels.length}</TableCell>
      <TableCell>{topic.segment_count} / {bytes(topic.segment_bytes)}</TableCell>
      <TableCell><div className="tag-row">{topic.owners.map((owner) => <Tag key={owner} size="sm" type="outline">{owner}</Tag>)}</div></TableCell>
      {managementEnabled && <TableCell><div onClick={(event) => event.stopPropagation()}><ResourceActionMenu kind="topic" topic={topic.name} paused={topic.paused} phase={topic.managed_phase} disabled={!managementUnlocked} onAction={onAction} /></div></TableCell>}
    </TableRow>
  );
}

function ChannelPanel({ topic, managementEnabled, managementUnlocked, onCreate, onAction }: { topic: Topic; managementEnabled: boolean; managementUnlocked: boolean; onCreate: () => void; onAction: (action: ManagementAction) => void }) {
  const { t } = useI18n();
  return (
    <Tile className="panel channel-panel">
      <div className="panel__header">
        <div><h2>{topic.name}</h2><p>{t('topics.channelDetail')}</p></div>
        <div className="panel__actions"><ResourceStatus phase={topic.managed_phase} paused={topic.paused} tombstoneUntil={topic.tombstone_until_ms} />{managementEnabled && <Button size="sm" kind="ghost" renderIcon={Add} disabled={!managementUnlocked || topic.managed_phase !== 'ACTIVE'} onClick={onCreate}>{t('management.createChannel')}</Button>}</div>
      </div>
      {topic.management_error && <InlineNotification kind="error" lowContrast hideCloseButton title={t('management.failed')} subtitle={topic.management_error} />}
      {topic.channels.length === 0 ? <EmptyState title={t('common.empty')} /> : (
        <div className="channel-list">
          {topic.channels.map((channel) => (
            <section key={channel.name} className="channel-card">
              <header>
                <div><strong>{channel.name}</strong><div className="tag-row">{channel.ephemeral && <Tag type="purple" size="sm">{t('topics.ephemeral')}</Tag>}{channel.paused && <Tag type="warm-gray" size="sm">{t('common.paused')}</Tag>}{channel.managed_phase && channel.managed_phase !== 'ACTIVE' && <ManagedPhaseTag phase={channel.managed_phase} />}</div></div>
                {managementEnabled && <ResourceActionMenu kind="channel" topic={topic.name} channel={channel.name} paused={channel.paused} phase={channel.managed_phase} ephemeral={channel.ephemeral} disabled={!managementUnlocked} onAction={onAction} />}
              </header>
              {channel.management_error && <p className="error-copy">{channel.management_error}</p>}
              {channel.tombstone_until_ms && <p className="resource-note">{t('management.tombstoneUntil')} {new Date(channel.tombstone_until_ms).toLocaleString()}</p>}
              <dl>
                <div><dt>{t('topics.depth')}</dt><dd>{number(channel.depth)}</dd></div>
                <div><dt>{t('topics.inFlight')}</dt><dd>{number(channel.in_flight)}</dd></div>
                <div><dt>{t('topics.deferred')}</dt><dd>{number(channel.deferred)}</dd></div>
                <div><dt>{t('topics.ackGap')}</dt><dd>{number(channel.ack_gap)}</dd></div>
              </dl>
              <div className="tag-row">{channel.owners.map((owner) => <Tag key={owner} size="sm" type="outline">{owner}</Tag>)}</div>
            </section>
          ))}
        </div>
      )}
    </Tile>
  );
}

function ResourceStatus({ phase, paused, tombstoneUntil }: { phase: string; paused: boolean; tombstoneUntil?: number }) {
  const { t } = useI18n();
  if (phase && phase !== 'ACTIVE') return <div className="resource-status"><ManagedPhaseTag phase={phase} />{tombstoneUntil && <span>{t('management.until')} {new Date(tombstoneUntil).toLocaleTimeString()}</span>}</div>;
  return <StatusTag ready={!paused} />;
}

function ManagedPhaseTag({ phase }: { phase: string }) {
  const { t } = useI18n();
  return <Tag type={phase === 'FAILED' ? 'red' : phase === 'TOMBSTONED' ? 'warm-gray' : 'blue'} size="sm">{t(`management.phase.${phase.toLowerCase()}`)}</Tag>;
}
