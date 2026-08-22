import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { useRef, useState } from 'react';
import { vi } from 'vitest';
import AppDataTable, { type AppDataTableColumn } from './AppDataTable';
import EntitySheet from './EntitySheet';
import QueryToolbar from './QueryToolbar';
import RefreshButton from './RefreshButton';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/Tabs';

interface BrokerRow {
  id: string;
  name: string;
}

const columns: AppDataTableColumn<BrokerRow>[] = [
  { id: 'broker', header: 'Broker', cell: (row) => row.name }
];

function SheetHarness() {
  const [open, setOpen] = useState(true);
  const triggerRef = useRef<HTMLButtonElement>(null);
  return (
    <>
      <button ref={triggerRef} type="button">Inspect broker-a</button>
      <EntitySheet open={open} title="broker-a" description="Broker details" onOpenChange={setOpen} restoreFocusRef={triggerRef}>
        Broker content
      </EntitySheet>
    </>
  );
}

function QueryHarness() {
  const [query, setQuery] = useState('');
  return (
    <QueryToolbar
      searchValue={query}
      searchPlaceholder="Search broker or address"
      onSearchChange={setQuery}
      onReset={() => setQuery('')}
    >
      <span>Role filter</span>
    </QueryToolbar>
  );
}

describe('operational workspace components', () => {
  it('controls search input and resets query state', async () => {
    const user = userEvent.setup();
    render(<QueryHarness />);

    const search = screen.getByRole('searchbox', { name: 'Search broker or address' });
    await user.type(search, 'broker-a');
    expect(search).toHaveValue('broker-a');
    expect(screen.getByText('Role filter')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Reset filters' }));
    expect(search).toHaveValue('');
  });

  it('announces refresh progress and prevents duplicate refreshes', () => {
    const onRefresh = vi.fn();
    const { rerender } = render(<RefreshButton onRefresh={onRefresh} />);
    expect(screen.getByRole('button', { name: 'Refresh' })).toBeEnabled();

    rerender(<RefreshButton refreshing onRefresh={onRefresh} />);
    expect(screen.getByRole('button', { name: 'Refreshing' })).toBeDisabled();
  });

  it('frames contextual details with an accessible title and keyboard tabs', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    render(
      <EntitySheet
        open
        title="broker-a"
        description="Broker runtime and configuration"
        actions={<button type="button">Restart broker</button>}
        onOpenChange={onOpenChange}
      >
        <Tabs defaultValue="overview">
          <TabsList aria-label="Broker detail sections">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="runtime">Runtime</TabsTrigger>
          </TabsList>
          <TabsContent value="overview">Overview content</TabsContent>
          <TabsContent value="runtime">Runtime content</TabsContent>
        </Tabs>
      </EntitySheet>
    );

    expect(screen.getByRole('dialog', { name: 'broker-a' })).toHaveAttribute('data-surface', 'frosted');
    expect(screen.getByText('Broker runtime and configuration')).toBeInTheDocument();
    expect(screen.getByRole('toolbar', { name: 'Detail actions' })).toContainElement(screen.getByRole('button', { name: 'Restart broker' }));
    const overview = screen.getByRole('tab', { name: 'Overview' });
    overview.focus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('tab', { name: 'Runtime' })).toHaveFocus();
    expect(screen.getByText('Runtime content')).toBeVisible();
  });

  it('restores focus to the external trigger after closing a contextual sheet', async () => {
    const user = userEvent.setup();
    render(<SheetHarness />);

    await user.click(screen.getByRole('button', { name: 'Close details' }));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Inspect broker-a' })).toHaveFocus());
  });

  it('renders table states, controlled pagination, and keyboard row activation', async () => {
    const user = userEvent.setup();
    const onRetry = vi.fn();
    const onPageChange = vi.fn();
    const onRowActivate = vi.fn();
    const { rerender } = render(
      <AppDataTable
        ariaLabel="Broker inventory"
        rows={[]}
        columns={columns}
        getRowId={(row) => row.id}
        page={1}
        pageSize={1}
        total={0}
        onPageChange={onPageChange}
        loading
      />
    );

    expect(screen.getByRole('status', { name: 'Loading broker inventory' })).toBeInTheDocument();

    rerender(
      <AppDataTable
        ariaLabel="Broker inventory"
        rows={[]}
        columns={columns}
        getRowId={(row) => row.id}
        page={1}
        pageSize={1}
        total={0}
        onPageChange={onPageChange}
        error="Broker request failed"
        onRetry={onRetry}
      />
    );
    await user.click(screen.getByRole('button', { name: 'Retry' }));
    expect(onRetry).toHaveBeenCalledTimes(1);

    rerender(
      <AppDataTable
        ariaLabel="Broker inventory"
        rows={[]}
        columns={columns}
        getRowId={(row) => row.id}
        page={1}
        pageSize={1}
        total={0}
        onPageChange={onPageChange}
        emptyTitle="No brokers match"
      />
    );
    expect(screen.getByText('No brokers match')).toBeInTheDocument();

    rerender(
      <AppDataTable
        ariaLabel="Broker inventory"
        rows={[{ id: 'b', name: 'broker-b' }]}
        columns={columns}
        getRowId={(row) => row.id}
        page={2}
        pageSize={1}
        total={2}
        onPageChange={onPageChange}
        onRowActivate={onRowActivate}
      />
    );
    expect(screen.getByRole('region', { name: 'Broker inventory' })).toBeInTheDocument();
    const row = screen.getByRole('row', { name: 'broker-b' });
    row.focus();
    await user.keyboard('{Enter}');
    expect(onRowActivate).toHaveBeenCalledWith({ id: 'b', name: 'broker-b' }, row);

    await user.click(screen.getByRole('button', { name: 'Previous page' }));
    expect(onPageChange).toHaveBeenCalledWith(1);
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled();

    rerender(
      <AppDataTable
        ariaLabel="Broker inventory"
        rows={[{ id: 'a', name: 'broker-a' }]}
        columns={columns}
        getRowId={(row) => row.id}
        page={1}
        pageSize={1}
        total={1}
        hasNextPage
        onPageChange={onPageChange}
      />
    );
    await user.click(screen.getByRole('button', { name: 'Next page' }));
    expect(onPageChange).toHaveBeenLastCalledWith(2);
  });

  it('clamps exact-total callers when the requested page exceeds the new page count', () => {
    const columns: AppDataTableColumn<BrokerRow>[] = [
      { id: 'broker', header: 'Broker', cell: (row) => row.name }
    ];
    render(
      <AppDataTable
        ariaLabel="Exact total rows"
        rows={[{ id: 'a', name: 'broker-a' }]}
        columns={columns}
        getRowId={(row) => row.id}
        page={5}
        pageSize={10}
        total={1}
        onPageChange={vi.fn()}
      />
    );

    expect(screen.getByLabelText('Page 1 of 1')).toBeInTheDocument();
  });

  it('does not activate a row when keyboard events originate from a nested control', async () => {
    const user = userEvent.setup();
    const onRowActivate = vi.fn();
    const nestedAction = vi.fn();
    const interactiveColumns: AppDataTableColumn<BrokerRow>[] = [
      { id: 'broker', header: 'Broker', cell: (row) => row.name },
      { id: 'action', header: 'Action', cell: () => <button type="button" onClick={nestedAction}>Open direct</button> }
    ];
    render(
      <AppDataTable
        ariaLabel="Broker inventory"
        rows={[{ id: 'a', name: 'broker-a' }]}
        columns={interactiveColumns}
        getRowId={(row) => row.id}
        page={1}
        pageSize={10}
        total={1}
        onPageChange={vi.fn()}
        onRowActivate={onRowActivate}
      />
    );

    const nestedButton = screen.getByRole('button', { name: 'Open direct' });
    nestedButton.focus();
    await user.keyboard('{Enter}');
    expect(nestedAction).toHaveBeenCalledTimes(1);
    expect(onRowActivate).not.toHaveBeenCalled();

    await user.click(screen.getByText('broker-a'));
    const row = screen.getByRole('row', { name: /broker-a/ });
    expect(onRowActivate).toHaveBeenCalledWith({ id: 'a', name: 'broker-a' }, row);
  });
});
