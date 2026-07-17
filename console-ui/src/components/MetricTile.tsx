import { Tile } from '@carbon/react';
import type { ReactNode } from 'react';

export function MetricTile({ label, value, meta, icon }: { label: string; value: string; meta?: string; icon?: ReactNode }) {
  return (
    <Tile className="metric-tile">
      <div className="metric-tile__head"><span>{label}</span>{icon}</div>
      <div className="metric-tile__value">{value}</div>
      {meta && <div className="metric-tile__meta">{meta}</div>}
    </Tile>
  );
}
