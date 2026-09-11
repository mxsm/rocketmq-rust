import { expect, it } from 'vitest';
import type { HistorySample } from '../../services/history.service';
import { historyPoints, mergeHistoryPages } from './historySeries';

const sample = (timestampMs: number, value: number): HistorySample => ({ metric: 'topic-count', dimension: '', timestampMs, value });

it('keeps zero observations but inserts a null across missing collection intervals', () => {
    expect(historyPoints([sample(180_000, 2), sample(0, 0), sample(60_000, 0)], 'topic-count', '', 60)).toEqual([
        { timestampMs: 0, value: 0 }, { timestampMs: 60_000, value: 0 },
        { timestampMs: 120_000, value: null }, { timestampMs: 180_000, value: 2 },
    ]);
});

it('filters mismatched identities and invalid values and de-duplicates timestamp overlap', () => {
    const points = historyPoints([
        sample(0, 1), sample(0, 2), { ...sample(100, 4), metric: 'broker-count' },
        { ...sample(200, 5), dimension: 'orders' }, sample(300, Number.NaN), sample(400, -1),
    ], 'topic-count', '', null);
    expect(points).toEqual([{ timestampMs: 0, value: 2 }]);
});

it('does not invent a sampling interval when collector status is unavailable', () => {
    expect(historyPoints([sample(0, 1), sample(3_600_000, 4)], 'topic-count', '', null)).toHaveLength(2);
});

it('merges overlapping history pages while retaining the latest observation and next cursor', () => {
    const page = mergeHistoryPages({ samples: [sample(3, 10), sample(2, 9)], nextBeforeMs: 2 },
        { samples: [sample(2, 8), sample(1, 7)], nextBeforeMs: null });
    expect(page.samples).toHaveLength(3);
    expect(page.samples.find(item => item.timestampMs === 2)?.value).toBe(9);
    expect(page.nextBeforeMs).toBeNull();
});
