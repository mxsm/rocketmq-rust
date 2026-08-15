import { screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderAtRoute } from '../test/render';
import TopicMaintenanceDialog from './TopicMaintenanceDialog';

describe('TopicMaintenanceDialog', () => {
  it('renders the API-backed consumer offset reset controls', () => {
    renderAtRoute(
      <TopicMaintenanceDialog
        topic="orders"
        open
        onOpenChange={vi.fn()}
        consumerGroups={[{ group: 'order-service', clientCount: 1, diffTotal: 0, consumeType: 'CONSUME_PASSIVELY', messageModel: 'CLUSTERING' }]}
        onMutationFinished={vi.fn()}
      />
    );

    expect(screen.getByRole('heading', { name: 'Reset Consumer Offset' })).toBeInTheDocument();
    expect(screen.getByLabelText('Consumer groups')).toBeInTheDocument();
    expect(screen.getByLabelText('Reset time')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reset' })).toBeEnabled();
  });
});
