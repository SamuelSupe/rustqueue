import { InlineLoading, InlineNotification, Modal, Tag, TextInput } from '@carbon/react';
import { useEffect, useState } from 'react';
import type { ActionPreview, ManagementAction } from '../api/types';
import { useI18n } from '../i18n';
import { number } from '../utils/format';

interface Props {
  action?: ManagementAction;
  busy: boolean;
  onPreview: (action: ManagementAction) => Promise<ActionPreview>;
  onApply: (action: ManagementAction, token: string, confirmation: string) => Promise<unknown>;
  onClose: () => void;
  onComplete: (action: ManagementAction) => void;
}

export function ManagementActionDialog({ action, busy, onPreview, onApply, onClose, onComplete }: Props) {
  const { t } = useI18n();
  const [preview, setPreview] = useState<ActionPreview>();
  const [confirmation, setConfirmation] = useState('');
  const [error, setError] = useState<string>();
  useEffect(() => {
    setPreview(undefined);
    setConfirmation('');
    setError(undefined);
    if (!action) return;
    let active = true;
    void onPreview(action)
      .then((value) => { if (active) setPreview(value); })
      .catch((value) => { if (active) setError(value instanceof Error ? value.message : String(value)); });
    return () => { active = false; };
  }, [action, onPreview]);
  if (!action) return null;
  const destructive = action.action === 'empty' || action.action === 'delete' || action.action === 'retry';
  const expected = preview?.confirmation_required || '';
  const submit = async () => {
    if (!preview) return;
    try {
      await onApply(action, preview.action_token, confirmation);
      onComplete(action);
    } catch (value) {
      setError(value instanceof Error ? value.message : String(value));
    }
  };
  return (
    <Modal
      open
      danger={destructive}
      modalHeading={t(`management.action.${action.action}`)}
      primaryButtonText={t(`management.action.${action.action}`)}
      secondaryButtonText={t('action.cancel')}
      primaryButtonDisabled={busy || !preview || Boolean(expected && confirmation !== expected)}
      onRequestClose={onClose}
      onRequestSubmit={() => void submit()}
    >
      <p className="modal-copy">{action.kind === 'topic' ? action.topic : `${action.topic} / ${action.channel}`}</p>
      {!preview && !error && <InlineLoading description={t('management.previewing')} />}
      {error && <InlineNotification kind="error" lowContrast hideCloseButton title={t('management.failed')} subtitle={error} />}
      {preview && (
        <>
          <dl className="impact-grid">
            <div><dt>{t('common.owner')}</dt><dd className="tag-row">{preview.impact.owners.map((owner) => <Tag key={owner} type="outline" size="sm">{owner}</Tag>)}</dd></div>
            <div><dt>{t('topics.messages')}</dt><dd>{number(preview.impact.stored_messages)}</dd></div>
            <div><dt>{t('topics.depth')}</dt><dd>{number(preview.impact.depth)}</dd></div>
            <div><dt>{t('topics.inFlight')}</dt><dd>{number(preview.impact.in_flight)}</dd></div>
          </dl>
          {preview.impact.warnings.map((warning) => <InlineNotification key={warning} kind={destructive ? 'warning' : 'info'} lowContrast hideCloseButton title={t('management.impact')} subtitle={t(`management.warning.${warning}`)} />)}
          {expected && (
            <TextInput
              id="management-action-confirmation"
              labelText={t('management.typeName')}
              helperText={expected}
              value={confirmation}
              autoComplete="off"
              onChange={(event) => setConfirmation(event.target.value)}
            />
          )}
        </>
      )}
    </Modal>
  );
}
