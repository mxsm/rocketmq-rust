import { useSyncExternalStore } from 'react';

export type ConsumerMutationKind = 'create' | 'update' | 'delete';

export interface ConsumerMutationLockTicket {
  key: string;
  sequence: number;
  kind: ConsumerMutationKind;
  targetGroup: string;
  scopeKey: string;
}

interface ConsumerMutationLock {
  ticket: ConsumerMutationLockTicket;
  appliedHandled: boolean;
  terminalHandled: boolean;
}

const locks = new Map<string, ConsumerMutationLock>();
const listeners = new Map<string, Set<() => void>>();
const allListeners = new Set<() => void>();
const terminalRevisions = new Map<string, number>();
const terminalListeners = new Map<string, Set<() => void>>();
let nextSequence = 0;

export function consumerMutationKey(kind: ConsumerMutationKind, targetGroup: string, scopeKey: string) {
  return `${kind}\u0000${targetGroup}\u0000${scopeKey}`;
}

export function isConsumerMutationLocked(key: string) {
  return locks.has(key);
}

export function beginConsumerMutation(
  kind: ConsumerMutationKind,
  targetGroup: string,
  scopeKey: string
): ConsumerMutationLockTicket | null {
  const key = consumerMutationKey(kind, targetGroup, scopeKey);
  if (locks.has(key)) return null;
  const ticket = { key, sequence: ++nextSequence, kind, targetGroup, scopeKey };
  locks.set(key, { ticket, appliedHandled: false, terminalHandled: false });
  notify(key);
  return ticket;
}

export function markConsumerMutationApplied(ticket: ConsumerMutationLockTicket) {
  const lock = locks.get(ticket.key);
  if (!lock || lock.ticket.sequence !== ticket.sequence || lock.appliedHandled) return false;
  lock.appliedHandled = true;
  return true;
}

export function finishConsumerMutation(ticket: ConsumerMutationLockTicket) {
  const lock = locks.get(ticket.key);
  if (!lock || lock.ticket.sequence !== ticket.sequence) return;
  locks.delete(ticket.key);
  notify(ticket.key);
}

/**
 * Records the one terminal, authoritative invalidation for a submitted
 * consumer operation. The ticket, not a mounted dialog, owns this state so
 * a completion that arrives after route or scope changes cannot refresh the
 * newly visible scope.
 */
export function completeConsumerMutation(ticket: ConsumerMutationLockTicket) {
  const lock = locks.get(ticket.key);
  if (!lock || lock.ticket.sequence !== ticket.sequence || lock.terminalHandled) return false;
  lock.terminalHandled = true;
  terminalRevisions.set(ticket.scopeKey, (terminalRevisions.get(ticket.scopeKey) ?? 0) + 1);
  terminalListeners.get(ticket.scopeKey)?.forEach((listener) => listener());
  return true;
}

export function useConsumerMutationLocked(key: string) {
  return useSyncExternalStore(
    (listener) => subscribe(key, listener),
    () => isConsumerMutationLocked(key),
    () => false
  );
}

/**
 * Returns the immutable operation ticket currently in flight for a create
 * scope. A remounted create dialog has no local draft identity yet, so it
 * must discover the original ticket by scope rather than by its mutable form.
 */
export function useConsumerMutationInFlight(kind: ConsumerMutationKind, scopeKey: string) {
  return useSyncExternalStore(
    subscribeAll,
    () => findConsumerMutationInFlight(kind, scopeKey),
    () => null
  );
}

/**
 * A scope-local revision advances once for every terminal mutation owned by
 * that scope. A mounted list consumes it immediately; an inactive scope sees
 * it on remount and refreshes its own data without touching another scope.
 */
export function useConsumerMutationScopeRevision(scopeKey: string) {
  return useSyncExternalStore(
    (listener) => subscribeTerminal(scopeKey, listener),
    () => terminalRevisions.get(scopeKey) ?? 0,
    () => 0
  );
}

function findConsumerMutationInFlight(kind: ConsumerMutationKind, scopeKey: string) {
  for (const lock of locks.values()) {
    if (lock.ticket.kind === kind && lock.ticket.scopeKey === scopeKey) return lock.ticket;
  }
  return null;
}

function subscribe(key: string, listener: () => void) {
  const keyListeners = listeners.get(key) ?? new Set<() => void>();
  keyListeners.add(listener);
  listeners.set(key, keyListeners);
  return () => {
    keyListeners.delete(listener);
    if (keyListeners.size === 0) listeners.delete(key);
  };
}

function notify(key: string) {
  listeners.get(key)?.forEach((listener) => listener());
  allListeners.forEach((listener) => listener());
}

function subscribeAll(listener: () => void) {
  allListeners.add(listener);
  return () => {
    allListeners.delete(listener);
  };
}

function subscribeTerminal(scopeKey: string, listener: () => void) {
  const scopeListeners = terminalListeners.get(scopeKey) ?? new Set<() => void>();
  scopeListeners.add(listener);
  terminalListeners.set(scopeKey, scopeListeners);
  return () => {
    scopeListeners.delete(listener);
    if (scopeListeners.size === 0) terminalListeners.delete(scopeKey);
  };
}

export function resetConsumerMutationLocksForTests() {
  locks.clear();
  listeners.clear();
  allListeners.clear();
  terminalRevisions.clear();
  terminalListeners.clear();
  nextSequence = 0;
}
