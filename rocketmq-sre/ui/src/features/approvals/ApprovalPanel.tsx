import { Check, Clock3, Play, ShieldCheck, X } from "lucide-react";
import { useEffect, useState } from "react";

import type {
  ActionPlanView,
  ApprovalDecisionRequest,
} from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

import { formatTimestamp, shortDigest } from "../plans/planPresentation";

export function ApprovalPanel({
  view,
  canApprove,
  canExecute,
  busy,
  message,
  onDecision,
  onExecute,
  onCriticReview,
}: {
  view: ActionPlanView;
  canApprove: boolean;
  canExecute: boolean;
  busy: boolean;
  message?: string;
  onDecision: (
    decision: "approve" | "reject",
    request: ApprovalDecisionRequest,
  ) => Promise<void>;
  onExecute: (preconditionHash: string) => Promise<void>;
  onCriticReview: () => Promise<void>;
}) {
  const aggregatePrecondition = view.precondition_hash;
  const [reason, setReason] = useState("");
  const [preconditionHash, setPreconditionHash] = useState(
    aggregatePrecondition,
  );
  const [validitySeconds, setValiditySeconds] = useState("900");

  useEffect(
    () => setPreconditionHash(aggregatePrecondition),
    [aggregatePrecondition],
  );

  const expired = new Date(view.plan.expires_at).getTime() <= Date.now();
  const criticReady =
    view.risk !== "r2" || view.critic_state === "accepted";
  const approval = view.latest_approval;
  const approved =
    approval?.decision === "approved" &&
    new Date(approval.expires_at).getTime() > Date.now();
  const canDecide =
    canApprove &&
    criticReady &&
    !expired &&
    reason.trim().length >= 8 &&
    preconditionHash.startsWith("sha256:");

  const submit = async (decision: "approve" | "reject") => {
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
          <h2>审批与执行授权</h2>
          <p>审批人必须与发起人不同，并拥有当前集群 scope。</p>
        </div>
        <Badge variant={approved ? "success" : expired ? "destructive" : "warning"}>
          {approved ? "授权有效" : expired ? "计划已过期" : "等待审批"}
        </Badge>
      </header>

      <div className="approval-body">
        <dl className="approval-facts">
          <div>
            <dt>Plan hash</dt>
            <dd>{shortDigest(view.plan.plan_hash)}</dd>
          </div>
          <div>
            <dt>请求人</dt>
            <dd>{view.plan.created_by}</dd>
          </div>
          <div>
            <dt>审批角色</dt>
            <dd>{canApprove ? "approver" : "无审批权限"}</dd>
          </div>
          <div>
            <dt>过期时间</dt>
            <dd>{formatTimestamp(view.plan.expires_at)}</dd>
          </div>
        </dl>

        {view.risk === "r2" && (
          <div className="critic-gate">
            <ShieldCheck size={16} />
            <div>
              <strong>R2 异构 Critic</strong>
              <span>{view.critic_state}</span>
            </div>
            <Button
              disabled={busy || view.critic_state === "accepted"}
              onClick={() => void onCriticReview()}
              size="sm"
              variant="outline"
            >
              运行 Critic
            </Button>
          </div>
        )}

        <label className="form-field">
          <span>Precondition hash</span>
          <input
            className="text-input"
            onChange={(event) => setPreconditionHash(event.target.value)}
            value={preconditionHash}
          />
        </label>
        <label className="form-field">
          <span>审批有效期（秒）</span>
          <input
            className="text-input"
            max="3600"
            min="60"
            onChange={(event) => setValiditySeconds(event.target.value)}
            type="number"
            value={validitySeconds}
          />
        </label>
        <label className="form-field">
          <span>审批或拒绝原因（至少 8 个字符）</span>
          <textarea
            onChange={(event) => setReason(event.target.value)}
            placeholder="说明风险判断、观察窗口和回滚依据"
            value={reason}
          />
        </label>

        {message && <p className="approval-message">{message}</p>}
        {!canApprove && (
          <p className="permission-copy">当前身份没有 approver role，审批按钮保持禁用。</p>
        )}
        {!criticReady && (
          <p className="permission-copy">R2 计划必须先取得有效异构 Critic 结论。</p>
        )}

        <div className="approval-actions">
          <Button
            disabled={busy || !canDecide}
            onClick={() => void submit("reject")}
            variant="destructive"
          >
            <X size={15} />
            Reject
          </Button>
          <Button
            disabled={busy || !canDecide}
            onClick={() => void submit("approve")}
            variant="outline"
          >
            <Check size={15} />
            Approve
          </Button>
          <Button
            disabled={busy || !approved || !canExecute}
            onClick={() => void onExecute(preconditionHash)}
          >
            <Play size={15} />
            提交 Executor
          </Button>
        </div>
        {approval && (
          <footer className="approval-record">
            <Clock3 size={14} />
            <span>
              {approval.approver_subject} · {approval.decision} ·{" "}
              {formatTimestamp(approval.decided_at)}
            </span>
          </footer>
        )}
      </div>
    </aside>
  );
}
