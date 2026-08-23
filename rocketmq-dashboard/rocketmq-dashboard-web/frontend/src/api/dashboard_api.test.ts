import { describe, expect, it, vi } from 'vitest';
import { readHistoryPages } from './dashboard_api';
import type { DashboardHistoryQuery, DashboardHistorySeries } from '../types/dashboard';

const query: DashboardHistoryQuery = { date: '2026-08-24' };

function page(timestamp: number, nextCursor: string | null): DashboardHistorySeries {
  return {
    date: query.date,
    metric: 'broker',
    collected: true,
    points: [{ timestamp, value: timestamp }],
    nextCursor
  };
}

describe('readHistoryPages', () => {
  it('merges every cursor page, including the latest point from the continuation', async () => {
    const request = vi
      .fn<(request: DashboardHistoryQuery) => Promise<DashboardHistorySeries>>()
      .mockResolvedValueOnce(page(1, 'cursor-1'))
      .mockResolvedValueOnce(page(2, null));

    await expect(readHistoryPages(request, query)).resolves.toMatchObject({
      points: [{ timestamp: 1 }, { timestamp: 2 }],
      nextCursor: null
    });
    expect(request).toHaveBeenNthCalledWith(1, { date: query.date, cursor: undefined });
    expect(request).toHaveBeenNthCalledWith(2, { date: query.date, cursor: 'cursor-1' });
  });

  it('stops after the safe 64-page maximum', async () => {
    const request = vi.fn<(request: DashboardHistoryQuery) => Promise<DashboardHistorySeries>>((request) =>
      Promise.resolve(page(1, `cursor-${request.cursor ?? 'first'}`))
    );

    await expect(readHistoryPages(request, query)).rejects.toThrow('safe page limit');
    expect(request).toHaveBeenCalledTimes(64);
  });

  it('propagates a continuation-page failure', async () => {
    const request = vi
      .fn<(request: DashboardHistoryQuery) => Promise<DashboardHistorySeries>>()
      .mockResolvedValueOnce(page(1, 'cursor-1'))
      .mockRejectedValueOnce(new Error('second page unavailable'));

    await expect(readHistoryPages(request, query)).rejects.toThrow('second page unavailable');
    expect(request).toHaveBeenCalledTimes(2);
  });
});
