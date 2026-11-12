import { type FormEvent, useEffect, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

import type { Investigation } from "@/api/types";
import { Button } from "@/components/ui/button";
import { useSreData } from "@/data/SreDataContext";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";

export function InvestigationPromotion({
  investigation,
}: {
  investigation: Investigation;
}) {
  const { api } = useSreData();
  const navigate = useNavigate();
  const m = useWorkspaceMessages();
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(false);
  const pending = useRef<AbortController | undefined>(undefined);
  useEffect(() => () => pending.current?.abort(), []);
  const promote = async (event: FormEvent) => {
    event.preventDefault();
    if (!reason.trim() || busy) return;
    const controller = new AbortController();
    pending.current = controller;
    setBusy(true);
    setError(false);
    try {
      const result = await api.promoteInvestigation(
        investigation.id,
        { reason: reason.trim() },
        controller.signal,
      );
      if (!controller.signal.aborted)
        navigate(`/incidents/${result.incident.id}`);
    } catch {
      if (!controller.signal.aborted) setError(true);
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  };
  return (
    <form
      className="investigation-promotion"
      onSubmit={(event) => void promote(event)}
    >
      <h3>{m.promote}</h3>
      <label>
        <span>{m.promoteReason}</span>
        <textarea
          maxLength={2048}
          onChange={(event) => setReason(event.target.value)}
          required
          rows={3}
          value={reason}
        />
      </label>
      {error && (
        <p className="inline-alert warning" role="alert">
          {m.promotionFailure}
        </p>
      )}
      <Button
        disabled={busy || !reason.trim()}
        size="sm"
        type="submit"
        variant="outline"
      >
        {busy ? m.promoting : m.promoteSubmit}
      </Button>
    </form>
  );
}
