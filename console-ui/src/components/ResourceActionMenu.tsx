import { OverflowMenu, OverflowMenuItem } from '@carbon/react';
import type { ManagementAction } from '../api/types';
import { useI18n } from '../i18n';

interface Props {
  kind: 'topic' | 'channel';
  topic: string;
  channel?: string;
  paused: boolean;
  phase: string;
  disabled: boolean;
  ephemeral?: boolean;
  onAction: (action: ManagementAction) => void;
}

export function ResourceActionMenu({ kind, topic, channel, paused, phase, disabled, ephemeral, onAction }: Props) {
  const { t } = useI18n();
  const action = (name: ManagementAction['action']) => () => onAction({ kind, action: name, topic, channel });
  const blocked = disabled || Boolean(ephemeral) || !['ACTIVE', 'FAILED'].includes(phase);
  return (
    <OverflowMenu size="sm" flipped ariaLabel={t('management.actions')} disabled={blocked}>
      {phase === 'FAILED' ? (
        <OverflowMenuItem itemText={t('management.retry')} onClick={action('retry')} />
      ) : (
        <>
          <OverflowMenuItem itemText={t(paused ? 'management.unpause' : 'management.pause')} onClick={action(paused ? 'unpause' : 'pause')} />
          <OverflowMenuItem hasDivider itemText={t('management.empty')} onClick={action('empty')} isDelete />
          <OverflowMenuItem itemText={t('management.delete')} onClick={action('delete')} isDelete />
        </>
      )}
    </OverflowMenu>
  );
}
