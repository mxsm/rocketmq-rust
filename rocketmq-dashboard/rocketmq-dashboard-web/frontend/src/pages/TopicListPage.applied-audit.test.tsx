import type { ReactNode } from 'react';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { topicApi } from '../api/topic_api';
import { renderAtRoute } from '../test/render';
import TopicListPage from './TopicListPage';

type TerminalDialogProps = {
  open: boolean;
  mode?: string;
  onOpenChange?: (open: boolean) => void;
  onAppliedAuditFailure?: () => Promise<void> | void;
};

async function settleApplied(props: TerminalDialogProps) {
  props.onOpenChange?.(false);
  await props.onAppliedAuditFailure?.();
}

vi.mock('../api/topic_api', () => ({ topicApi: { list: vi.fn() } }));
vi.mock('../components/TopicMutationDialog', () => ({
  default: (props: TerminalDialogProps) => props.open ? (
    <button type="button" onClick={() => void settleApplied(props)}>Applied {props.mode}</button>
  ) : null
}));
vi.mock('../components/TopicResetOffsetDialog', () => ({
  default: (props: TerminalDialogProps) => props.open ? (
    <button type="button" onClick={() => void settleApplied(props)}>Applied reset</button>
  ) : null
}));
vi.mock('../components/TopicSkipBacklogDialog', () => ({
  default: (props: TerminalDialogProps) => props.open ? (
    <button type="button" onClick={() => void settleApplied(props)}>Applied skip</button>
  ) : null
}));
vi.mock('../components/TopicDeleteDialog', () => ({
  default: (props: TerminalDialogProps) => props.open ? (
    <button type="button" onClick={() => void settleApplied(props)}>Applied delete</button>
  ) : null
}));
vi.mock('../components/TopicSendMessageDialog', () => ({
  default: (props: TerminalDialogProps) => props.open ? (
    <button type="button" onClick={() => void settleApplied(props)}>Applied send</button>
  ) : null
}));
vi.mock('./topics/TopicConsumerActionDialog', () => ({
  default: ({ open, onSelect }: {
    open: boolean;
    onSelect: (consumerGroup: string) => void;
  }) => open ? (
    <button type="button" onClick={() => onSelect('orders-consumer')}>Choose consumer</button>
  ) : null
}));
vi.mock('../components/EntitySheet', () => ({
  default: ({ open, actions, children }: { open: boolean; actions?: ReactNode; children?: ReactNode }) => open ? (
    <section aria-label="Topic detail">{actions}{children}</section>
  ) : null
}));
vi.mock('./topics/TopicDetailContent', () => ({
  default: ({ resourceRevisions }: { resourceRevisions: { route: number; stats: number; consumers: number; config: number } }) => (
    <output data-testid="topic-revisions">{JSON.stringify(resourceRevisions)}</output>
  )
}));

const catalog = {
  items: [{
    topic: 'orders',
    brokerName: 'broker-a',
    brokers: ['broker-a'],
    clusters: ['DefaultCluster'],
    readQueueCount: 4,
    writeQueueCount: 4,
    perm: 6,
    category: 'NORMAL',
    messageType: 'NORMAL',
    order: false,
    systemTopic: false
  }],
  total: 1,
  targets: [{ clusterName: 'DefaultCluster', brokerNames: ['broker-a'] }]
};

async function openAction(user: ReturnType<typeof userEvent.setup>, label: string) {
  await user.click(screen.getByRole('button', { name: 'Actions for orders' }));
  await user.click(screen.getByRole('menuitem', { name: label }));
}

async function expectCatalogRefresh() {
  await waitFor(() => expect(topicApi.list).toHaveBeenCalledTimes(1));
}

describe('TopicListPage applied audit failure refreshes', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(topicApi.list).mockResolvedValue(catalog);
  });

  it('closes each terminal dialog and refreshes the exact catalog/detail resources once', async () => {
    const user = userEvent.setup();
    renderAtRoute(<TopicListPage />, '/topics');
    await screen.findByRole('heading', { name: 'Topics' });
    await user.click(screen.getByText('orders'));
    await screen.findByTestId('topic-revisions');

    vi.clearAllMocks();
    await openAction(user, 'Edit configuration');
    await user.click(screen.getByRole('button', { name: 'Applied edit' }));
    await expectCatalogRefresh();
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"config":1');
    expect(screen.queryByRole('button', { name: 'Applied edit' })).not.toBeInTheDocument();

    vi.clearAllMocks();
    await openAction(user, 'Reset consumer offset');
    await user.click(screen.getByRole('button', { name: 'Choose consumer' }));
    await user.click(screen.getByRole('button', { name: 'Applied reset' }));
    await expectCatalogRefresh();
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"stats":1');
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"consumers":1');
    expect(screen.queryByRole('button', { name: 'Applied reset' })).not.toBeInTheDocument();

    vi.clearAllMocks();
    await openAction(user, 'Skip accumulated messages');
    await user.click(screen.getByRole('button', { name: 'Choose consumer' }));
    await user.click(screen.getByRole('button', { name: 'Applied skip' }));
    await expectCatalogRefresh();
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"stats":2');
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"consumers":2');
    expect(screen.queryByRole('button', { name: 'Applied skip' })).not.toBeInTheDocument();

    vi.clearAllMocks();
    await openAction(user, 'Delete from broker');
    await user.click(screen.getByRole('button', { name: 'Applied delete' }));
    await expectCatalogRefresh();
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"route":1');
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"stats":3');
    expect(screen.getByTestId('topic-revisions')).toHaveTextContent('"config":2');
    expect(screen.queryByRole('button', { name: 'Applied delete' })).not.toBeInTheDocument();
  });
});
