import { RefreshCw } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { Button } from './ui/Button';

interface RefreshButtonProps {
  refreshing?: boolean;
  onRefresh: () => void;
  compact?: boolean;
}

/** Longest wait for the turn in progress to finish once a refresh settles. */
const TURN_TIMEOUT_MS = 900;

/**
 * Keeps the icon turning while `active`, then lets the current turn finish so the icon
 * comes to rest upright instead of snapping back mid-rotation. Wire `finishTurn` to the
 * icon's `onAnimationIteration` and `onAnimationEnd`.
 */
export function useSpinUntilTurnEnds(active: boolean) {
  const [spinning, setSpinning] = useState(active);
  const spinningRef = useRef(active);

  useEffect(() => {
    if (active) {
      spinningRef.current = true;
      setSpinning(true);
      return undefined;
    }
    if (!spinningRef.current) return undefined;
    // Covers reduced motion and anything else that never reports the end of a turn.
    const timer = window.setTimeout(() => {
      spinningRef.current = false;
      setSpinning(false);
    }, TURN_TIMEOUT_MS);
    return () => window.clearTimeout(timer);
  }, [active]);

  const finishTurn = () => {
    if (active) return;
    spinningRef.current = false;
    setSpinning(false);
  };

  return { spinning, finishTurn };
}

export default function RefreshButton({ refreshing = false, onRefresh, compact = false }: RefreshButtonProps) {
  const { spinning, finishTurn } = useSpinUntilTurnEnds(refreshing);

  return (
    <Button
      type="button"
      variant="outline"
      size={compact ? 'icon' : 'sm'}
      disabled={refreshing}
      aria-label={refreshing ? 'Refreshing' : 'Refresh'}
      onClick={onRefresh}
    >
      <RefreshCw
        className={spinning ? 'refresh-icon is-spinning' : 'refresh-icon'}
        size={15}
        aria-hidden="true"
        onAnimationIteration={finishTurn}
        onAnimationEnd={finishTurn}
      />
      {!compact ? (refreshing ? 'Refreshing' : 'Refresh') : null}
    </Button>
  );
}
