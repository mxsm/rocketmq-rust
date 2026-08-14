import { describe, expect, it } from 'vitest';
import type { DashboardOverview, TopicCurrentMetric } from '../../types/dashboard';
import { buildDashboardAdvisories, formatDashboardMetric, sortTopTopics } from './dashboard-model';

const healthyOverview: DashboardOverview = {
  currentNamesrv: '127.0.0.1:9876',
  brokerCount: 3,
  topicCount: 8,
  consumerGroupCount: 4,
  producerCount: 5,
  messageBacklog: 0,
  systemStatus: 'UP'
};

describe('dashboard model', () => {
  it('builds actionable advisories only from overview evidence', () => {
    expect(buildDashboardAdvisories(healthyOverview)).toEqual([]);

    const advisories = buildDashboardAdvisories({
      ...healthyOverview,
      currentNamesrv: null,
      brokerCount: 0,
      messageBacklog: 14_200,
      systemStatus: 'DEGRADED'
    });

    expect(advisories.map((advisory) => [advisory.id, advisory.target])).toEqual([
      ['system-status', '/brokers'],
      ['nameserver', '/config'],
      ['brokers', '/brokers'],
      ['backlog', '/consumers']
    ]);
    expect(advisories[3].detail).toContain('14,200');
  });

  it('formats compact operational metrics without hiding zero', () => {
    expect(formatDashboardMetric(0)).toBe('0');
    expect(formatDashboardMetric(999)).toBe('999');
    expect(formatDashboardMetric(1_250)).toBe('1.25K');
    expect(formatDashboardMetric(2_500_000)).toBe('2.5M');
  });

  it('sorts top topics stably without mutating API arrays', () => {
    const topics: TopicCurrentMetric[] = [
      { topic: 'orders', totalMsg: 20, inTps: 2, outTps: 1 },
      { topic: 'payments', totalMsg: 40, inTps: 4, outTps: 3 },
      { topic: 'inventory', totalMsg: 40, inTps: 1, outTps: 1 }
    ];
    const snapshot = structuredClone(topics);

    expect(sortTopTopics(topics).map((topic) => topic.topic)).toEqual(['payments', 'inventory', 'orders']);
    expect(topics).toEqual(snapshot);
  });
});
