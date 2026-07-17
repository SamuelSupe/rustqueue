import type { Histogram } from '../api/types';

export function number(value: number, maximumFractionDigits = 1) {
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits }).format(value);
}

export function bytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / 1024 ** index).toFixed(index > 1 ? 1 : 0)} ${units[index]}`;
}

export function duration(value?: string | number) {
  if (!value) return 'N/A';
  return new Intl.DateTimeFormat(undefined, {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value));
}

const buckets = [100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_500_000, 5_000_000, 10_000_000];

export function percentile(histogram: Histogram | undefined, quantile = 0.95) {
  if (!histogram?.count) return 0;
  const target = histogram.count * quantile;
  let cumulative = 0;
  for (let index = 0; index < histogram.buckets.length; index += 1) {
    cumulative += histogram.buckets[index] || 0;
    if (cumulative >= target) return buckets[index] || buckets[buckets.length - 1];
  }
  return buckets[buckets.length - 1];
}

export function micros(value: number) {
  if (!value) return 'N/A';
  if (value < 1000) return `${Math.round(value)} µs`;
  if (value < 1_000_000) return `${(value / 1000).toFixed(1)} ms`;
  return `${(value / 1_000_000).toFixed(2)} s`;
}

export function shortImage(image: string) {
  const digest = image.split('@')[0];
  return digest.split('/').pop() || image;
}
