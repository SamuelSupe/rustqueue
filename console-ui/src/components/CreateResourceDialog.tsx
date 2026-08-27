import { Modal, Select, SelectItem, TextInput } from '@carbon/react';
import { useEffect, useState } from 'react';
import type { ManagementAction } from '../api/types';
import { useI18n } from '../i18n';

interface Props {
  open: boolean;
  kind: 'topic' | 'channel';
  topic?: string;
  onClose: () => void;
  onContinue: (action: ManagementAction) => void;
}

export function CreateResourceDialog({ open, kind, topic, onClose, onContinue }: Props) {
  const { t } = useI18n();
  const [name, setName] = useState('');
  const [deliveryMode, setDeliveryMode] = useState<'RELIABLE' | 'TTL_DISCARD'>('RELIABLE');
  const [ttlSeconds, setTtlSeconds] = useState('');
  useEffect(() => {
    if (!open) {
      setName('');
      setDeliveryMode('RELIABLE');
      setTtlSeconds('');
    }
  }, [open]);
  const ttl = Number(ttlSeconds);
  const policyValid = kind !== 'topic' || deliveryMode === 'RELIABLE' || (Number.isInteger(ttl) && ttl > 0);
  const valid = /^[.A-Za-z0-9_-]{1,64}$/.test(name) && !name.endsWith('#ephemeral') && policyValid;
  const submit = () => {
    onContinue(kind === 'topic'
      ? {
          kind,
          action: 'create',
          topic: name,
          delivery_mode: deliveryMode,
          message_ttl_seconds: deliveryMode === 'TTL_DISCARD' ? ttl : undefined,
        }
      : { kind, action: 'create', topic: topic || '', channel: name });
  };
  return (
    <Modal
      open={open}
      modalHeading={t(kind === 'topic' ? 'management.createTopic' : 'management.createChannel')}
      primaryButtonText={t('action.continue')}
      secondaryButtonText={t('action.cancel')}
      primaryButtonDisabled={!valid}
      onRequestClose={onClose}
      onRequestSubmit={submit}
    >
      <TextInput
        id={`create-${kind}-name`}
        labelText={t(kind === 'topic' ? 'topics.name' : 'management.channelName')}
        helperText={t('management.nameHint')}
        value={name}
        autoComplete="off"
        onChange={(event) => setName(event.target.value)}
      />
      {kind === 'topic' && (
        <div className="topic-policy-fields">
          <Select id="create-topic-delivery-mode" labelText={t('topics.deliveryMode')} value={deliveryMode} onChange={(event) => setDeliveryMode(event.target.value as 'RELIABLE' | 'TTL_DISCARD')}>
            <SelectItem value="RELIABLE" text={t('topics.mode.reliable')} />
            <SelectItem value="TTL_DISCARD" text={t('topics.mode.ttlDiscard')} />
          </Select>
          {deliveryMode === 'TTL_DISCARD' && (
            <TextInput id="create-topic-ttl" type="number" min={1} labelText={t('topics.messageTtlSeconds')} helperText={t('topics.ttlHint')} value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} />
          )}
        </div>
      )}
    </Modal>
  );
}
