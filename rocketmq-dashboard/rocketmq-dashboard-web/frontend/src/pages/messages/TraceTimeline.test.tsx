import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { MessageTraceNode } from '../../types/message';
import TraceTimeline from './TraceTimeline';

describe('TraceTimeline', () => {
  it('renders only returned node fields in timestamp order without inferred edges or latency', () => {
    const nodes: MessageTraceNode[] = [
      { nodeType: 'CONSUMER', name: 'order-service', status: 'SUCCESS', timestamp: 30 },
      { nodeType: 'PRODUCER', name: 'order-producer', status: 'SENT', timestamp: 10 }
    ];
    render(<TraceTimeline nodes={nodes} />);

    expect(screen.getAllByRole('listitem').map((item) => item.textContent)).toEqual([
      expect.stringContaining('order-producer'),
      expect.stringContaining('order-service')
    ]);
    expect(screen.queryByText(/latency|duration|edge/i)).not.toBeInTheDocument();
    expect(nodes[0].timestamp).toBe(30);
  });
});
