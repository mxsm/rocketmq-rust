import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import KeyValueTable from './KeyValueTable';

describe('KeyValueTable', () => {
  it('renders JSON values as a compact summary and opens a formatted detail view', async () => {
    const user = userEvent.setup();
    render(
      <KeyValueTable
        emptyTitle="No values"
        rows={[
          { key: 'timerPipelineMetrics', value: '{"retryCount":0,"activeWorkers":2}' },
          { key: 'timerPrecisionMillis', value: '1000' }
        ]}
      />
    );

    expect(screen.getByRole('button', { name: 'View JSON for timerPipelineMetrics' })).toHaveTextContent('JSON object · 2 fields');
    expect(screen.getByText('1000')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'View JSON for timerPipelineMetrics' }));

    expect(await screen.findByRole('dialog')).toHaveTextContent('"retryCount": 0');
    expect(screen.getByRole('dialog')).toHaveTextContent('"activeWorkers": 2');
  });
});
