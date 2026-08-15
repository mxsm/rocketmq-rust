import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useState } from 'react';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import TopicFilterToolbar from './TopicFilterToolbar';
import type { TopicFilters } from './topic-model';

const initialFilters: TopicFilters = {
  query: '',
  brokerName: 'all',
  clusterName: 'all',
  messageTypes: [],
  categories: []
};

beforeAll(() => {
  Object.defineProperty(Element.prototype, 'scrollIntoView', { configurable: true, value: vi.fn() });
});

afterAll(() => {
  Reflect.deleteProperty(Element.prototype, 'scrollIntoView');
});

function ToolbarHarness({ initial = initialFilters }: { initial?: TopicFilters }) {
  const [filters, setFilters] = useState(initial);
  return (
    <TopicFilterToolbar
      filters={filters}
      clusterOptions={['ArchiveCluster', 'DefaultCluster']}
      brokerOptions={['broker-a', 'broker-b']}
      onFiltersChange={setFilters}
    />
  );
}

describe('TopicFilterToolbar', () => {
  it('supports keyboard-accessible multi-select type and category filters', async () => {
    const user = userEvent.setup();
    render(<ToolbarHarness />);

    const typeTrigger = screen.getByRole('button', { name: 'Message types: All types' });
    typeTrigger.focus();
    await user.keyboard('{Enter}');
    const fifo = screen.getByRole('menuitemcheckbox', { name: 'FIFO' });
    expect(fifo).toHaveAttribute('aria-checked', 'false');
    await user.click(fifo);
    expect(screen.getByRole('button', { name: 'Message types: FIFO' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Message types: FIFO' }));
    await user.click(screen.getByRole('menuitemcheckbox', { name: 'Delay' }));
    expect(screen.getByRole('button', { name: 'Message types: 2 types' })).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Categories: All categories' }));
    await user.click(screen.getByRole('menuitemcheckbox', { name: 'Retry' }));
    expect(screen.getByRole('button', { name: 'Categories: Retry' })).toBeInTheDocument();
  });

  it('changes cluster and broker filters and resets the complete toolbar', async () => {
    const user = userEvent.setup();
    render(<ToolbarHarness initial={{
      query: 'orders',
      brokerName: 'broker-a',
      clusterName: 'DefaultCluster',
      messageTypes: ['NORMAL'],
      categories: ['APPLICATION']
    }} />);

    screen.getByRole('combobox', { name: 'Cluster filter' }).focus();
    await user.keyboard('{Enter}');
    await user.click(screen.getByRole('option', { name: 'ArchiveCluster' }));
    expect(screen.getByRole('combobox', { name: 'Cluster filter' })).toHaveTextContent('ArchiveCluster');

    screen.getByRole('combobox', { name: 'Broker filter' }).focus();
    await user.keyboard('{Enter}');
    await user.click(screen.getByRole('option', { name: 'broker-b' }));
    expect(screen.getByRole('combobox', { name: 'Broker filter' })).toHaveTextContent('broker-b');

    await user.click(screen.getByRole('button', { name: 'Reset filters' }));
    expect(screen.getByRole('searchbox', { name: 'Filter topics' })).toHaveValue('');
    expect(screen.getByRole('button', { name: 'Message types: All types' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Categories: All categories' })).toBeInTheDocument();
    expect(screen.getByRole('combobox', { name: 'Cluster filter' })).toHaveTextContent('All clusters');
    expect(screen.getByRole('combobox', { name: 'Broker filter' })).toHaveTextContent('All brokers');
  });
});
