import { Column, Grid, Table, TableBody, TableCell, TableContainer, TableHead, TableHeader, TableRow, Tile } from '@carbon/react';
import type { Operation, Snapshot } from '../api/types';
import { EmptyState } from '../components/EmptyState';
import { PageHeader } from '../components/PageHeader';
import { StateTag } from '../components/StatusTag';
import { useI18n } from '../i18n';
import { duration } from '../utils/format';

export function OperationsPage({ snapshot }: { snapshot: Snapshot }) {
  const { t } = useI18n();
  return (
    <>
      <PageHeader title={t('operations.title')} subtitle={t('operations.subtitle')} meta={<span>{snapshot.operation_history.length} {t('operations.history')}</span>} />
      <Grid fullWidth narrow className="content-grid">
        <Column sm={4} md={8} lg={10}>
          <TableContainer className="panel" title={t('operations.conditions')}>
            <Table size="md" useZebraStyles>
              <TableHead><TableRow>{[t('operations.type'), t('common.status'), t('operations.reason'), t('common.message'), t('common.updated')].map((value) => <TableHeader key={value}>{value}</TableHeader>)}</TableRow></TableHead>
              <TableBody>{snapshot.conditions.map((condition) => <TableRow key={condition.type}>
                <TableCell>{condition.type}</TableCell><TableCell><StateTag value={condition.status} /></TableCell><TableCell>{condition.reason}</TableCell><TableCell className="wide-cell">{condition.message}</TableCell><TableCell>{duration(condition.lastTransitionTime)}</TableCell>
              </TableRow>)}</TableBody>
            </Table>
          </TableContainer>
        </Column>
        <Column sm={4} md={8} lg={6}>
          <Tile className="panel operation-detail">
            <div className="panel__header"><h2>{t('operations.current')}</h2></div>
            {snapshot.current_operation ? <OperationDetail operation={snapshot.current_operation} /> : <EmptyState title={t('overview.noOperation')} />}
          </Tile>
        </Column>
      </Grid>
      <TableContainer className="panel" title={t('operations.history')}>
        {snapshot.operation_history.length ? <Table size="md" useZebraStyles>
          <TableHead><TableRow>{[t('operations.type'), t('operations.phase'), t('operations.target'), t('common.message'), t('operations.started'), t('operations.completed')].map((value) => <TableHeader key={value}>{value}</TableHeader>)}</TableRow></TableHead>
          <TableBody>{snapshot.operation_history.map((operation) => <TableRow key={operation.id}>
            <TableCell>{operation.kind}</TableCell><TableCell><StateTag value={operation.phase} /></TableCell><TableCell>{operation.target}</TableCell><TableCell className="wide-cell">{operation.message}</TableCell><TableCell>{duration(operation.startedAt)}</TableCell><TableCell>{duration(operation.completedAt)}</TableCell>
          </TableRow>)}</TableBody>
        </Table> : <EmptyState title={t('common.empty')} />}
      </TableContainer>
      <TableContainer className="panel" title={t('operations.events')}>
        {snapshot.events.length ? <Table size="md" useZebraStyles>
          <TableHead><TableRow>{[t('common.updated'), t('common.status'), t('operations.reason'), t('operations.object'), t('common.message')].map((value) => <TableHeader key={value}>{value}</TableHeader>)}</TableRow></TableHead>
          <TableBody>{snapshot.events.map((event, index) => <TableRow key={`${event.at}-${event.reason}-${index}`}>
            <TableCell>{duration(event.at)}</TableCell><TableCell><StateTag value={event.type_} /></TableCell><TableCell>{event.reason}</TableCell><TableCell>{event.object}</TableCell><TableCell className="wide-cell">{event.message}{event.count > 1 ? ` (${event.count})` : ''}</TableCell>
          </TableRow>)}</TableBody>
        </Table> : <EmptyState title={t('common.empty')} />}
      </TableContainer>
    </>
  );
}

function OperationDetail({ operation }: { operation: Operation }) {
  const { t } = useI18n();
  return <div className="operation-summary"><StateTag value={operation.phase} /><strong>{operation.kind}</strong><span>{operation.target}</span><p>{operation.message}</p><dl className="detail-list"><div><dt>{t('operations.started')}</dt><dd>{duration(operation.startedAt)}</dd></div><div><dt>Revision</dt><dd>{operation.revision}</dd></div></dl></div>;
}
