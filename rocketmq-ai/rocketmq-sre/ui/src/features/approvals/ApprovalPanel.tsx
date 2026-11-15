import { Check, Clock3, Play, ShieldCheck, X } from "lucide-react";
import { useState } from "react";

import type {
  ActionPlanView,
  ApprovalDecisionRequest,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useDeadlineClock } from "@/hooks/useDeadlineClock";
import { useActionFormMessages } from "@/i18n/actionFormMessages";

import { shortDigest } from "../plans/planPresentation";

export function ApprovalPanel({
  view,
  canApprove,
  approvalReady = true,
  canReject = canApprove,
  canExecute,
  canCriticReview = false,
  busy,
  readOnly = false,
  message,
  dispatchBlockedMessage,
  approvalBlockedMessage,
  onDecision,
  onExecute,
  onCriticReview,
}: {
  view: ActionPlanView;
  canApprove: boolean;
  approvalReady?: boolean;
  canReject?: boolean;
  canExecute: boolean;
  canCriticReview?: boolean;
  busy: boolean;
  readOnly?: boolean;
  message?: string;
  dispatchBlockedMessage?: string;
  approvalBlockedMessage?: string;
  onDecision: (
    decision: "approve" | "reject",
    request: ApprovalDecisionRequest,
  ) => Promise<void>;
  onExecute: (preconditionHash: string) => Promise<void>;
  onCriticReview: () => Promise<void>;
}) {
  const { m, formatDateTime } = useActionFormMessages();
  const preconditionHash = view.precondition_hash;
  const [reason, setReason] = useState("");
  const [validitySeconds, setValiditySeconds] = useState("900");

  const approval = view.latest_approval;
  const now = useDeadlineClock([view.plan.expires_at, approval?.expires_at]);
  const planDeadline = Date.parse(view.plan.expires_at);
  const timeKnown = Number.isFinite(planDeadline);
  const expired = !timeKnown || planDeadline <= now;
  const criticReady =
    view.risk !== "r2" || view.critic_state === "accepted";
  const approvalBound = approval?.plan_id === view.plan.id && approval?.plan_hash === view.plan.plan_hash &&
    approval?.tenant_id === view.plan.tenant_id && approval?.cluster_id === view.plan.cluster_id &&
    approval?.requester_subject === view.plan.created_by && approval?.approver_subject !== view.plan.created_by;
  const approved =
    !expired && view.plan.status === "approved" && approvalBound &&
    approval?.decision === "approved" &&
    Date.parse(approval.decided_at) <= now && Date.parse(approval.expires_at) > now;
  const seconds = Number(validitySeconds);
  const validDecision =
    !readOnly && !expired && Number.isSafeInteger(seconds) && seconds >= 60 && seconds <= 3600 &&
    reason.trim().length >= 8 &&
    /^sha256:[a-f0-9]{64}$/u.test(preconditionHash);
  const canDecide = canApprove && validDecision && criticReady && approvalReady;
  const canDecline = canReject && validDecision &&
    ["needs_critic", "ready_for_approval", "in_review"].includes(view.plan.status);

  const submit = async (decision: "approve" | "reject") => {
    if (busy || !validDecision || Date.parse(view.plan.expires_at) <= Date.now() ||
      (decision === "approve" ? !canDecide : !canDecline)) return;
    await onDecision(decision, {
      plan_hash: view.plan.plan_hash,
      precondition_hash: preconditionHash,
      reason: reason.trim(),
      validity_seconds: Number(validitySeconds),
    });
  };

  return (
    <aside className="data-surface approval-panel">
      <header className="surface-heading">
        <div>
          <h2>{m.title}</h2>
          <p>{m.intro}</p>
        </div>
        <Badge variant={approved ? "success" : expired ? "destructive" : "warning"}>
          {!timeKnown ? m.invalidTime : approved ? m.currentApproval : approval?.decision === "approved"
            ? m.historicalApproval : approval?.decision === "rejected" ? m.rejectedApproval : expired ? m.expired : m.waiting}
        </Badge>
      </header>

      <div className="approval-body">
        <dl className="approval-facts">
          <div>
            <dt>Plan hash</dt>
            <dd>{shortDigest(view.plan.plan_hash)}</dd>
          </div>
          <div>
            <dt>{m.requester}</dt>
            <dd>{view.plan.created_by}</dd>
          </div>
          <div>
            <dt>{m.role}</dt>
            <dd>{canApprove ? "approver" : m.noRole}</dd>
          </div>
          <div>
            <dt>{m.expiry}</dt>
            <dd>{timeKnown ? formatDateTime(view.plan.expires_at) : m.invalidTime}</dd>
          </div>
          {approval && <div><dt>{m.approvalUntil}</dt><dd>{Number.isFinite(Date.parse(approval.expires_at)) ? formatDateTime(approval.expires_at) : m.invalidTime}</dd></div>}
        </dl>

        {view.risk === "r2" && (
          <div className="critic-gate">
            <ShieldCheck size={16} />
            <div>
              <strong>R2 异构 Critic</strong>
              <span>{view.critic_state}</span>
            </div>
            <Button
              disabled={readOnly || busy || expired || !canCriticReview || view.plan.status !== "needs_critic" || view.critic_state === "accepted"}
              onClick={() => {
                if (!readOnly && !busy && !expired && canCriticReview && view.plan.status === "needs_critic") void onCriticReview();
              }}
              size="sm"
              variant="outline"
            >
              {m.critic}
            </Button>
          </div>
        )}

        <label className="form-field">
          <span>{m.boundHash}</span>
          <input
            className="text-input"
            readOnly
            value={preconditionHash}
          />
        </label>
        <label className="form-field">
          <span>{m.validity}</span>
          <input
            className="text-input"
            max="3600"
            min="60"
            readOnly={readOnly}
            onChange={(event) => setValiditySeconds(event.target.value)}
            type="number"
            value={validitySeconds}
          />
        </label>
        <label className="form-field">
          <span>{m.reason}</span>
          <textarea
            onChange={(event) => setReason(event.target.value)}
            placeholder={m.reasonHint}
            maxLength={512}
            readOnly={readOnly}
            value={reason}
          />
        </label>

        {message && <p className="approval-message">{message}</p>}
        {!canApprove && (
          <p className="permission-copy">{m.roleHint}</p>
        )}
        {!criticReady && (
              <p className="permission-copy">{m.criticHint}</p>
        )}
        {readOnly && <p className="permission-copy">{m.historical}</p>}
        {dispatchBlockedMessage && <p className="permission-copy" role="status">{dispatchBlockedMessage}</p>}
        {approvalBlockedMessage && <p className="permission-copy" role="status">{approvalBlockedMessage}</p>}

        <div className="approval-actions">
          <Button
            disabled={busy || !canDecline}
            onClick={() => void submit("reject")}
            variant="destructive"
          >
            <X size={15} />
            {m.reject}
          </Button>
          <Button
            disabled={busy || !canDecide}
            onClick={() => void submit("approve")}
            variant="outline"
          >
            <Check size={15} />
            {m.approve}
          </Button>
          <Button
            disabled={readOnly || busy || !approved || !canExecute}
            onClick={() => {
              if (!readOnly && !busy && approved && canExecute && Date.parse(view.plan.expires_at) > Date.now() &&
                approval && Date.parse(approval.expires_at) > Date.now()) void onExecute(preconditionHash);
            }}
          >
            <Play size={15} />
            {m.submit}
          </Button>
        </div>
        {approval && (
          <footer className="approval-record">
            <Clock3 size={14} />
            <span>
              {approval.approver_subject} · {approval.decision} ·{" "}
              {formatDateTime(approval.decided_at)}
            </span>
          </footer>
        )}
      </div>
    </aside>
  );
}
