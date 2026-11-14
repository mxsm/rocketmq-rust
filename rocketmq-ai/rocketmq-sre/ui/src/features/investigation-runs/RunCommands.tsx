import { useEffect, useRef, useState } from "react";

import { ApiError } from "@/api/client";
import type {
  InvestigationCommandRequest,
  InvestigationRun,
  InvestigationRunControls,
} from "@/api/types";
import { Button } from "@/components/ui/button";
import { absoluteTimeKey, parseAbsoluteWindow } from "@/context/workspaceScope";
import { useRunMessages } from "@/i18n/runMessages";
import { commandAvailable } from "./runPresentation";
import { useRunLabel } from "./runLabels";
import { RunDialog } from "./RunDialog";

type Command = InvestigationCommandRequest["command"];
interface Props {
  run: InvestigationRun;
  controls?: InvestigationRunControls;
  submit: (request: InvestigationCommandRequest) => Promise<unknown>;
  refresh: () => Promise<void>;
}

export function RunCommands(props: Props) {
  const { run, controls } = props;
  const replayWithheld =
    controls?.receipt_replay_allowed === false &&
    controls.run.id === run.id &&
    controls.run.revision === run.revision &&
    controls.run.control_epoch === run.control_epoch;
  // Authoritative revocation discards the old intent and its in-flight callbacks.
  // A temporary missing projection keeps the same session across network recovery.
  return (
    <RunCommandSession
      key={replayWithheld ? "withheld" : "current"}
      {...props}
    />
  );
}

function RunCommandSession({ run, controls, submit, refresh }: Props) {
  const m = useRunMessages();
  const label = useRunLabel();
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [retry, setRetry] = useState<InvestigationCommandRequest>();
  const [steer, setSteer] = useState(false);
  const active = useRef(true);
  const busyRef = useRef(false);
  const replayAllowed =
    controls?.receipt_replay_allowed === true &&
    controls.run.id === run.id &&
    controls.run.revision === run.revision &&
    controls.run.control_epoch === run.control_epoch;
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  // Commands belong to this keyed run/scope session; an unmounted session cannot submit again.
  const send = async (
    command: Command,
    previous?: InvestigationCommandRequest,
  ) => {
    if (
      !active.current ||
      busyRef.current ||
      (previous
        ? !replayAllowed || previous.run_id !== run.id
        : !commandAvailable(controls, run, command.kind) || !reason.trim())
    )
      return;
    busyRef.current = true;
    setBusy(true);
    setMessage("");
    setRetry(undefined);
    const request = previous ?? {
      schema_version: "rocketmq-sre.investigation-run.v1",
      command_id: crypto.randomUUID(),
      run_id: run.id,
      expected_revision: run.revision,
      idempotency_key: crypto.randomUUID(),
      reason: reason.trim(),
      command,
    };
    try {
      await submit(request);
      if (active.current) {
        setMessage(m.commandSuccess);
        setSteer(false);
      }
    } catch (error) {
      if (!active.current) return;
      if (error instanceof ApiError && [401, 403, 404].includes(error.status)) {
        setMessage(m.forbidden);
        await refresh();
      } else if (error instanceof ApiError && error.status === 409) {
        setMessage(m.conflict);
        await refresh();
      } else if (
        error instanceof ApiError &&
        error.status >= 400 &&
        error.status < 500
      ) {
        setMessage(m.commandRejected);
        await refresh();
      } else {
        setMessage(m.commandFailure);
        setRetry(request);
        await refresh();
      }
    } finally {
      busyRef.current = false;
      if (active.current) setBusy(false);
    }
  };
  const commands = (["pause", "resume", "cancel", "steer"] as const).filter(
    (kind) => commandAvailable(controls, run, kind),
  );
  return (
    <div className="run-command-panel">
      <p className="muted">{m.commandAuthority}</p>
      <label>
        {m.reason}
        <input
          value={reason}
          maxLength={512}
          placeholder={m.reasonHint}
          disabled={busy}
          onChange={(event) => setReason(event.target.value)}
        />
      </label>
      <div className="inline-actions">
        {commands.map((kind) => (
          <Button
            key={kind}
            disabled={busy || !reason.trim()}
            variant={kind === "cancel" ? "outline" : "default"}
            onClick={() =>
              kind === "steer" ? setSteer(true) : void send({ kind })
            }
          >
            {m[kind]}
          </Button>
        ))}
        {retry && replayAllowed && retry.run_id === run.id && (
          <Button
            disabled={busy}
            onClick={() => void send(retry.command, retry)}
          >
            {m.retryRequest}
          </Button>
        )}
      </div>
      {retry && replayAllowed && <p className="muted">{m.retryHint}</p>}
      {!commands.length && <p>{m.noCommands}</p>}
      {controls?.commands
        .filter((command) => !command.allowed && command.blocked_reason)
        .map((command) => (
          <p className="muted" key={command.kind}>
            {m[command.kind]} · {label(command.blocked_reason!)}
          </p>
        ))}
      {commands.includes("cancel") && <p className="muted">{m.cancelHint}</p>}
      <p role="status" aria-live="polite">
        {busy ? m.pending : message}
      </p>
      {steer && (
        <SteerForm
          run={run}
          busy={busy}
          allowed={commandAvailable(controls, run, "steer")}
          onClose={() => setSteer(false)}
          onSubmit={(command) => void send(command)}
        />
      )}
    </div>
  );
}

function SteerForm({
  run,
  busy,
  allowed,
  onClose,
  onSubmit,
}: {
  run: InvestigationRun;
  busy: boolean;
  allowed: boolean;
  onClose: () => void;
  onSubmit: (command: Command) => void;
}) {
  const m = useRunMessages();
  const [start, setStart] = useState(run.effective_scope.time_range.start);
  const [end, setEnd] = useState(run.effective_scope.time_range.end);
  const [packs, setPacks] = useState(run.effective_packs);
  const [evidence, setEvidence] = useState(run.evidence_ids);
  const [error, setError] = useState(false);
  const submit = () => {
    const parsed = parseAbsoluteWindow(
      `?window_start=${encodeURIComponent(start)}&window_end=${encodeURIComponent(end)}`,
    );
    if (
      !allowed ||
      !parsed ||
      parsed === "invalid" ||
      absoluteTimeKey(start)! <
        absoluteTimeKey(run.effective_scope.time_range.start)! ||
      absoluteTimeKey(end)! >
        absoluteTimeKey(run.effective_scope.time_range.end)! ||
      !packs.length ||
      packs.some(
        (pack) =>
          !run.effective_packs.some(
            (current) =>
              current.pack_id === pack.pack_id &&
              current.pack_version === pack.pack_version,
          ),
      ) ||
      evidence.some((id) => !run.evidence_ids.includes(id))
    ) {
      setError(true);
      return;
    }
    onSubmit({
      kind: "steer",
      refinement: {
        scope: { ...run.effective_scope, time_range: parsed },
        packs,
        evidence_ids: evidence,
      },
    });
  };
  return (
    <RunDialog title={m.steer} onClose={onClose}>
      <form
        className="run-form"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        <p>{m.steerHint}</p>
        <fieldset disabled={busy || !allowed}>
          <label>
            {m.start}
            <input
              required
              value={start}
              onChange={(event) => setStart(event.target.value)}
            />
          </label>
          <label>
            {m.end}
            <input
              required
              value={end}
              onChange={(event) => setEnd(event.target.value)}
            />
          </label>
          <fieldset>
            <legend>{m.packs}</legend>
            {run.effective_packs.map((pack) => (
              <label
                className="run-checkbox"
                key={`${pack.pack_id}@${pack.pack_version}`}
              >
                <input
                  type="checkbox"
                  checked={packs.some(
                    (item) =>
                      item.pack_id === pack.pack_id &&
                      item.pack_version === pack.pack_version,
                  )}
                  onChange={(event) =>
                    setPacks(
                      event.target.checked
                        ? [...packs, pack]
                        : packs.filter(
                            (item) =>
                              item.pack_id !== pack.pack_id ||
                              item.pack_version !== pack.pack_version,
                          ),
                    )
                  }
                />
                {pack.pack_id} · {pack.pack_version}
              </label>
            ))}
          </fieldset>
          <fieldset>
            <legend>{m.keepEvidence}</legend>
            {run.evidence_ids.map((id) => (
              <label className="run-checkbox" key={id}>
                <input
                  type="checkbox"
                  checked={evidence.includes(id)}
                  onChange={(event) =>
                    setEvidence(
                      event.target.checked
                        ? [...evidence, id]
                        : evidence.filter((item) => item !== id),
                    )
                  }
                />
                <span className="run-id">{id}</span>
              </label>
            ))}
          </fieldset>
        </fieldset>
        {error && <p role="alert">{m.narrowOnly}</p>}
        <Button type="submit" disabled={busy || !allowed}>
          {m.submitSteer}
        </Button>
      </form>
    </RunDialog>
  );
}
