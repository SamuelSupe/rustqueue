import { Button, InlineNotification, Modal, TextInput } from '@carbon/react';
import { Locked, Unlocked } from '@carbon/icons-react';
import { useState } from 'react';
import type { ManagementStatus } from '../api/types';
import { useI18n } from '../i18n';

interface Props {
  status?: ManagementStatus;
  busy: boolean;
  error?: string;
  onUnlock: (confirmation: string) => Promise<void>;
  onLock: () => Promise<void>;
}

export function ManagementAccess({ status, busy, error, onUnlock, onLock }: Props) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [confirmation, setConfirmation] = useState('');
  const expected = status?.confirmation || '';
  const submit = async () => {
    await onUnlock(confirmation);
    setConfirmation('');
    setOpen(false);
  };

  if (!status?.enabled) {
    return <InlineNotification className="management-notice" kind="info" lowContrast hideCloseButton title={t('management.readOnly')} subtitle={t('management.disabledHint')} />;
  }
  return (
    <>
      <div className={`management-access ${status.unlocked ? 'management-access--unlocked' : ''}`}>
        <div className="management-access__copy">
          {status.unlocked ? <Unlocked size={20} /> : <Locked size={20} />}
          <div>
            <strong>{t(status.unlocked ? 'management.unlocked' : 'management.locked')}</strong>
            <span>{status.unlocked && status.expires_at_ms ? `${t('management.expires')} ${new Date(status.expires_at_ms).toLocaleTimeString()}` : t('management.lockedHint')}</span>
          </div>
        </div>
        {status.unlocked ? (
          <Button size="sm" kind="ghost" disabled={busy} onClick={() => void onLock()}>{t('management.lock')}</Button>
        ) : (
          <Button size="sm" kind="primary" disabled={busy} onClick={() => setOpen(true)}>{t('management.unlock')}</Button>
        )}
      </div>
      {error && <InlineNotification className="management-notice" kind="error" lowContrast hideCloseButton title={t('management.failed')} subtitle={error} />}
      <Modal
        open={open}
        modalHeading={t('management.unlockTitle')}
        primaryButtonText={t('management.unlock')}
        secondaryButtonText={t('action.cancel')}
        primaryButtonDisabled={busy || confirmation !== expected}
        onRequestClose={() => setOpen(false)}
        onRequestSubmit={() => void submit()}
      >
        <p className="modal-copy">{t('management.unlockHint')}</p>
        <TextInput
          id="management-unlock-confirmation"
          labelText={t('management.typeScope')}
          helperText={expected}
          value={confirmation}
          autoComplete="off"
          onChange={(event) => setConfirmation(event.target.value)}
        />
      </Modal>
    </>
  );
}
