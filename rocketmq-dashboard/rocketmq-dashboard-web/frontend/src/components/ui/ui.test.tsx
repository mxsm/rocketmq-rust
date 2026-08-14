import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi } from 'vitest';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogTitle, AlertDialogTrigger } from './AlertDialog';
import { Badge } from './Badge';
import { Button } from './Button';
import { Card, CardContent, CardHeader, CardTitle } from './Card';
import { Dialog, DialogContent, DialogDescription, DialogTitle, DialogTrigger } from './Dialog';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from './DropdownMenu';
import { Input } from './Input';
import { Label } from './Label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './Select';
import { Separator } from './Separator';
import { Sheet, SheetContent, SheetDescription, SheetTitle, SheetTrigger } from './Sheet';
import { Skeleton } from './Skeleton';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './Tabs';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from './Tooltip';

describe('UI primitives', () => {
  it('disables a loading button and keeps its accessible name', () => {
    render(<Button loading variant="outline" size="sm">Refresh</Button>);

    const button = screen.getByRole('button', { name: 'Refresh' });
    expect(button).toBeDisabled();
    expect(button).toHaveClass('ui-button-outline', 'ui-button-size-sm');
  });

  it('renders semantic status and card content', () => {
    render(
      <Card>
        <CardHeader><CardTitle>Cluster health</CardTitle></CardHeader>
        <CardContent><Badge tone="success">Healthy</Badge></CardContent>
      </Card>
    );

    expect(screen.getByRole('heading', { name: 'Cluster health' })).toBeInTheDocument();
    expect(screen.getByText('Healthy')).toHaveAttribute('data-tone', 'success');
  });

  it('associates labels with inputs and renders supporting primitives', () => {
    render(
      <>
        <Label htmlFor="topic">Topic</Label>
        <Input id="topic" />
        <Separator decorative={false} />
        <Skeleton aria-label="Loading topics" />
      </>
    );

    expect(screen.getByRole('textbox', { name: 'Topic' })).toBeInTheDocument();
    expect(screen.getByRole('separator')).toBeInTheDocument();
    expect(screen.getByLabelText('Loading topics')).toBeInTheDocument();
  });

  it('switches tab content with keyboard navigation', async () => {
    const user = userEvent.setup();
    render(
      <Tabs defaultValue="overview">
        <TabsList aria-label="Broker details">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="runtime">Runtime</TabsTrigger>
        </TabsList>
        <TabsContent value="overview">Overview panel</TabsContent>
        <TabsContent value="runtime">Runtime panel</TabsContent>
      </Tabs>
    );

    await user.tab();
    expect(screen.getByRole('tab', { name: 'Overview' })).toHaveFocus();
    await user.keyboard('{ArrowRight}');
    expect(screen.getByRole('tab', { name: 'Runtime' })).toHaveFocus();
    expect(screen.getByText('Runtime panel')).toBeVisible();
  });

  it('opens and closes a sheet while returning focus to its trigger', async () => {
    const user = userEvent.setup();
    render(
      <Sheet>
        <SheetTrigger asChild><Button>Inspect broker</Button></SheetTrigger>
        <SheetContent>
          <SheetTitle>Broker details</SheetTitle>
          <SheetDescription>Runtime and configuration</SheetDescription>
        </SheetContent>
      </Sheet>
    );

    const trigger = screen.getByRole('button', { name: 'Inspect broker' });
    await user.click(trigger);
    expect(screen.getByRole('dialog', { name: 'Broker details' })).toBeInTheDocument();
    await user.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('dialog', { name: 'Broker details' })).not.toBeInTheDocument());
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it('does not run a destructive action until confirmation', async () => {
    const user = userEvent.setup();
    const onConfirm = vi.fn();
    render(
      <AlertDialog>
        <AlertDialogTrigger asChild><Button>Delete topic</Button></AlertDialogTrigger>
        <AlertDialogContent>
          <AlertDialogTitle>Delete orders</AlertDialogTitle>
          <AlertDialogDescription>This cannot be undone.</AlertDialogDescription>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction onClick={onConfirm}>Delete</AlertDialogAction>
        </AlertDialogContent>
      </AlertDialog>
    );

    await user.click(screen.getByRole('button', { name: 'Delete topic' }));
    expect(onConfirm).not.toHaveBeenCalled();
    const cancel = screen.getByRole('button', { name: 'Cancel' });
    expect(cancel).toHaveClass('ui-button-size-default');
    expect(screen.getByRole('button', { name: 'Delete' })).toHaveClass('ui-button-size-default');
    await user.click(cancel);
    expect(onConfirm).not.toHaveBeenCalled();
  });

  it('exposes dialog, menu, select and tooltip content accessibly', async () => {
    const user = userEvent.setup();
    render(
      <TooltipProvider delayDuration={0}>
        <Dialog>
          <DialogTrigger asChild><Button>Edit</Button></DialogTrigger>
          <DialogContent><DialogTitle>Edit broker</DialogTitle><DialogDescription>Update configuration</DialogDescription></DialogContent>
        </Dialog>
        <DropdownMenu>
          <DropdownMenuTrigger asChild><Button>Actions</Button></DropdownMenuTrigger>
          <DropdownMenuContent><DropdownMenuItem>Refresh</DropdownMenuItem></DropdownMenuContent>
        </DropdownMenu>
        <Select defaultValue="alpha">
          <SelectTrigger aria-label="Cluster"><SelectValue /></SelectTrigger>
          <SelectContent><SelectItem value="alpha">Alpha</SelectItem></SelectContent>
        </Select>
        <Tooltip><TooltipTrigger asChild><Button aria-label="Help">?</Button></TooltipTrigger><TooltipContent>Connection help</TooltipContent></Tooltip>
      </TooltipProvider>
    );

    await user.click(screen.getByRole('button', { name: 'Edit' }));
    expect(screen.getByRole('dialog', { name: 'Edit broker' })).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Close dialog' }));
    expect(screen.getByRole('combobox', { name: 'Cluster' })).toHaveTextContent('Alpha');
    await user.click(screen.getByRole('button', { name: 'Actions' }));
    expect(screen.getByRole('menuitem', { name: 'Refresh' })).toBeInTheDocument();
    await user.keyboard('{Escape}');
    await user.hover(screen.getByRole('button', { name: 'Help' }));
    expect(await screen.findByRole('tooltip', { name: 'Connection help' })).toBeInTheDocument();
  });
});
