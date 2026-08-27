import { Modal, Select, SelectItem, TextInput } from '@carbon/react';
import { useEffect, useState } from 'react';
import type { ManagementAction, Topic } from '../api/types';
import { useI18n } from '../i18n';

interface Props {
  topic?: Topic;
  onClose: () => void;
  onContinue: (action: ManagementAction) => void;
}

export function ConfigureTopicDialog({ topic, onClose, onContinue }: Props) {
  const { t } = useI18n();
  const [deliveryMode, setDeliveryMode] = useState<'RELIABLE' | 'TTL_DISCARD'>('RELIABLE');
  const [ttlSeconds, setTtlSeconds] = useState('');

  useEffect(() => {
    setDeliveryMode(topic?.delivery_mode || 'RELIABLE');
    setTtlSeconds(topic?.message_ttl_seconds?.toString() || '');
  }, [topic]);

  if (!topic) return null;
  const ttl = Number(ttlSeconds);
  const valid = deliveryMode === 'RELIABLE' || (Number.isInteger(ttl) && ttl > 0);
  const submit = () => onContinue({
    kind: 'topic',
    action: 'configure',
    topic: topic.name,
    delivery_mode: deliveryMode,
    message_ttl_seconds: deliveryMode === 'TTL_DISCARD' ? ttl : undefined,
  });

  return (
    <Modal
      open
      modalHeading={t('management.configureTtl')}
      primaryButtonText={t('action.continue')}
      secondaryButtonText={t('action.cancel')}
      primaryButtonDisabled={!valid}
      onRequestClose={onClose}
      onRequestSubmit={submit}
    >
      <p className="modal-copy">{topic.name}</p>
      <div className="topic-policy-fields">
        <Select id="configure-topic-delivery-mode" labelText={t('topics.deliveryMode')} value={deliveryMode} onChange={(event) => setDeliveryMode(event.target.value as 'RELIABLE' | 'TTL_DISCARD')}>
          <SelectItem value="RELIABLE" text={t('topics.mode.reliable')} />
          <SelectItem value="TTL_DISCARD" text={t('topics.mode.ttlDiscard')} />
        </Select>
        {deliveryMode === 'TTL_DISCARD' && (
          <TextInput id="configure-topic-ttl" type="number" min={1} labelText={t('topics.messageTtlSeconds')} helperText={t('topics.ttlHint')} value={ttlSeconds} onChange={(event) => setTtlSeconds(event.target.value)} />
        )}
      </div>
    </Modal>
  );
}
