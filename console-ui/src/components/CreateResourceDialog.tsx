import { Modal, TextInput } from '@carbon/react';
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
  useEffect(() => { if (!open) setName(''); }, [open]);
  const valid = /^[.A-Za-z0-9_-]{1,64}$/.test(name) && !name.endsWith('#ephemeral');
  const submit = () => {
    onContinue(kind === 'topic'
      ? { kind, action: 'create', topic: name }
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
    </Modal>
  );
}
