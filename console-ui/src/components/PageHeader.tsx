import type { ReactNode } from 'react';

export function PageHeader({ title, subtitle, meta }: { title: string; subtitle: string; meta?: ReactNode }) {
  return (
    <header className="page-header">
      <div>
        <h1>{title}</h1>
        <p>{subtitle}</p>
      </div>
      {meta && <div className="page-header__meta">{meta}</div>}
    </header>
  );
}
