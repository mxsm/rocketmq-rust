import type { HistoryPage, HistorySample } from '../../services/history.service';

export interface HistoryPoint { timestampMs: number; value: number | null }

/** Pagination can overlap during collection. Never render duplicate timestamps. */
export function mergeHistoryPages(previous: HistoryPage, next: HistoryPage): HistoryPage {
    const samples = new Map<string, HistorySample>();
    for (const sample of [...next.samples, ...previous.samples]) {
        samples.set(`${sample.metric}:${sample.dimension}:${sample.timestampMs}`, sample);
    }
    return { samples: [...samples.values()], nextBeforeMs: next.nextBeforeMs };
}

export function historyPoints(samples: HistorySample[], metric: HistorySample['metric'], dimension: string,
    intervalSeconds: number | null): HistoryPoint[] {
    const unique = new Map<number, number>();
    for (const sample of samples) {
        if (sample.metric === metric && sample.dimension === dimension &&
            Number.isFinite(sample.timestampMs) && Number.isFinite(sample.value) && sample.value >= 0) {
            unique.set(sample.timestampMs, sample.value);
        }
    }
    const points: HistoryPoint[] = [];
    const sorted = [...unique].sort(([a], [b]) => a - b);
    const interval = intervalSeconds && intervalSeconds > 0 ? intervalSeconds * 1000 : null;
    for (const [timestampMs, value] of sorted) {
        const previous = points.at(-1);
        // Allow timer jitter, but do not connect an omitted collection interval.
        if (previous && interval && timestampMs - previous.timestampMs > interval * 1.5) {
            points.push({ timestampMs: previous.timestampMs + interval, value: null });
        }
        points.push({ timestampMs, value });
    }
    return points;
}
