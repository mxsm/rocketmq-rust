import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import type { ProducerConnectionInfo } from '../../types/producer';
import ProducerDetailContent from './ProducerDetailContent';

const connection: ProducerConnectionInfo = {
  clientId: 'payment-producer-1-7f9c8b',
  clientAddr: '10.0.0.12:10911',
  language: 'JAVA',
  version: '5.2.0'
};

describe('ProducerDetailContent', () => {
  it('shows only API-backed client identity and confirms successful copies', async () => {
    const user = userEvent.setup();
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
    render(<ProducerDetailContent connection={connection} topic="payment-events" producerGroup="payment-producer" />);

    expect(screen.getByText('payment-producer-1-7f9c8b')).toBeInTheDocument();
    expect(screen.getByText('10.0.0.12:10911')).toBeInTheDocument();
    expect(screen.getByText('JAVA')).toBeInTheDocument();
    expect(screen.getByText('5.2.0')).toBeInTheDocument();
    expect(screen.getByText('payment-events')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Copy client ID' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Copy client address' })).toBeInTheDocument();
    expect(screen.queryByText(/TPS|success rate|latency/i)).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Copy client ID' }));
    expect(writeText).toHaveBeenCalledWith('payment-producer-1-7f9c8b');
    expect(await screen.findByRole('status')).toHaveTextContent('Client ID copied.');
  });

  it('reports clipboard failures instead of silently swallowing them', async () => {
    const user = userEvent.setup();
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText: vi.fn().mockRejectedValue(new Error('clipboard denied')) }
    });
    render(<ProducerDetailContent connection={connection} topic="payment-events" producerGroup="payment-producer" />);

    await user.click(screen.getByRole('button', { name: 'Copy client address' }));
    expect(await screen.findByRole('status')).toHaveTextContent('Unable to copy client address.');
  });
});
