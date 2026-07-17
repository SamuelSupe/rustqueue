import { Information } from '@carbon/icons-react';

export function EmptyState({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="empty-state">
      <Information size={24} />
      <strong>{title}</strong>
      {detail && <span>{detail}</span>}
    </div>
  );
}
