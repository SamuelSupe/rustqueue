import { describe, expect, it } from 'vitest';
import { bytes, micros, percentile } from './format';

describe('format helpers', () => {
  it('formats binary storage units', () => {
    expect(bytes(1024)).toBe('1 KiB');
    expect(bytes(1024 * 1024)).toBe('1.0 MiB');
  });

  it('derives a percentile from non-cumulative histogram buckets', () => {
    expect(percentile({ buckets: [1, 0, 9], count: 10, sum_us: 9000 }, 0.95)).toBe(500);
    expect(micros(500)).toBe('500 µs');
  });
});
