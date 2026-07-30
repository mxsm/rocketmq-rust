import {
  ArrowRight,
  CheckCircle2,
  FileCheck2,
  LockKeyhole,
  Radar,
} from "lucide-react";
import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import { createSupervisedSreApi } from "@/api/supervisedClient";
import type {
  CreatePlanResponse,
  DiagnosisExecutionConfirmation,
  DiagnosisRevision,
  ExecutionPreconditionEvidenceView,
  Incident,
  PrepareExecutionPreconditionRequest,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

import { diagnosisAttribution } from "./incidentPresentation";

const ACTION_ID = "observability.logger_level_ttl.v1";
const DESCRIPTOR_VERSION = "1.0.0";

interface SupervisedDiagnosisPanelProps {
  incident: Incident;
  revisions: DiagnosisRevision[];
  onChanged: () => void;
}

interface ActionParameters {
  component: "broker";
  logger: string;
  level: "DEBUG" | "INFO";
  ttl_seconds: number;
}

export function SupervisedDiagnosisPanel({
  incident,
  revisions,
  onChanged,
}: SupervisedDiagnosisPanelProps) {
  const auth = useAuth();
  const navigate = useNavigate();
  const api = useMemo(
    () =>
      auth.requestContext
        ? createSupervisedSreApi(auth.requestContext)
        : undefined,
    [auth.requestContext],
  );
  const latestRevision = revisions.at(-1);
  const attribution = latestRevision
    ? diagnosisAttribution(latestRevision)
    : undefined;
  const [confirmation, setConfirmation] =
    useState<DiagnosisExecutionConfirmation>();
  const [precondition, setPrecondition] =
    useState<ExecutionPreconditionEvidenceView>();
  const [plan, setPlan] = useState<CreatePlanResponse>();
  const [reason, setReason] = useState(
    "已人工核对模型诊断、Evidence 引用与影响范围",
  );
  const [resource, setResource] = useState(incident.resource ?? "");
  const [logger, setLogger] = useState("rocketmq_broker::processor");
  const [level, setLevel] = useState<ActionParameters["level"]>("DEBUG");
  const [ttlSeconds, setTtlSeconds] = useState(60);
  const [busy, setBusy] = useState<
    "confirm" | "precondition" | "plan"
  >();
  const [message, setMessage] = useState<string>();

  const confirmedRevisionId =
    confirmation?.confirmed_revision_id ??
    (latestRevision?.execution_eligible ? latestRevision.id : undefined);
  const diagnosisEvidenceIds =
    confirmation?.evidence_ids ??
    (latestRevision?.execution_eligible
      ? latestRevision.evidence_ids
      : []);
  const modelAssisted =
    attribution?.mode === "model_assisted" &&
    Boolean(latestRevision?.primary_model_invocation_id);
  const canOperate = Boolean(
    auth.session?.roles.some(
      (role) => role === "operator" || role === "rocketmq:operate",
    ),
  );
  const parameters: ActionParameters = {
    component: "broker",
    logger,
    level,
    ttl_seconds: ttlSeconds,
  };

  const run = async (
    operation: NonNullable<typeof busy>,
    task: () => Promise<void>,
  ) => {
    setBusy(operation);
    setMessage(undefined);
    try {
      await task();
    } catch (error) {
      setMessage(
        error instanceof Error ? error.message : "受监督操作请求失败",
      );
    } finally {
      setBusy(undefined);
    }
  };

  const confirmDiagnosis = () =>
    run("confirm", async () => {
      if (!api || !latestRevision) return;
      const result = await api.confirmDiagnosisExecution(
        incident.id,
        latestRevision.id,
        {
          human_confirmed: true,
          reason: reason.trim(),
        },
      );
      setConfirmation(result);
      setPrecondition(undefined);
      setPlan(undefined);
      setMessage("已生成不可变的人工作业确认 revision。");
      onChanged();
    });

  const preparePrecondition = () =>
    run("precondition", async () => {
      if (!api || !confirmedRevisionId) return;
      const request: PrepareExecutionPreconditionRequest = {
        cluster_id: incident.cluster_id,
        diagnosis_revision_id: confirmedRevisionId,
        action_id: ACTION_ID,
        descriptor_version: DESCRIPTOR_VERSION,
        resource: resource.trim(),
        parameters:
          parameters as unknown as PrepareExecutionPreconditionRequest["parameters"],
      };
      const result = await api.prepareExecutionPrecondition(
        incident.id,
        request,
      );
      setPrecondition(result);
      setPlan(undefined);
      setMessage("Execution Agent 只读前置检查已封装为 Evidence。");
    });

  const createPlan = () =>
    run("plan", async () => {
      if (!api || !confirmedRevisionId || !precondition) return;
      const evidenceIds = [
        ...new Set([
          ...diagnosisEvidenceIds,
          precondition.evidence.evidence_id,
        ]),
      ];
      const result = await api.createPlan({
        cluster_id: incident.cluster_id,
        incident_id: incident.id,
        diagnosis_revision_id: confirmedRevisionId,
        steps: [
          {
            action_id: ACTION_ID,
            descriptor_version: DESCRIPTOR_VERSION,
            resource: resource.trim(),
            parameters:
              parameters as unknown as PrepareExecutionPreconditionRequest["parameters"],
            evidence_ids: evidenceIds,
          },
        ],
      });
      setPlan(result);
      if (result.kind === "action_plan") {
        setMessage("受监督计划已创建，下一步进入 Critic 与人工审批。");
      } else {
        setMessage("策略仅允许生成手工 Runbook，未创建可执行计划。");
      }
    });

  return (
    <section className="data-surface supervised-diagnosis-panel">
      <header className="surface-heading">
        <div>
          <h2>AI 诊断到受监督执行</h2>
          <p>
            人工确认模型诊断后，由隔离的 Execution Agent
            读取当前状态；这里只暴露固定、带 TTL 的日志级别动作。
          </p>
        </div>
        <Badge variant={confirmedRevisionId ? "success" : "secondary"}>
          {confirmedRevisionId ? "已确认" : "等待人工确认"}
        </Badge>
      </header>

      <div className="supervised-diagnosis-steps">
        <article>
          <div className="supervised-step-heading">
            <span>01</span>
            <div>
              <strong>确认模型诊断</strong>
              <small>
                {modelAssisted
                  ? `${attribution?.provider} · ${attribution?.model}`
                  : "需要完整的 model_assisted revision"}
              </small>
            </div>
            <LockKeyhole size={17} />
          </div>
          <label className="form-field">
            <span>人工确认说明</span>
            <Input
              onChange={(event) => setReason(event.target.value)}
              value={reason}
            />
          </label>
          <Button
            disabled={
              !api ||
              !canOperate ||
              !modelAssisted ||
              !reason.trim() ||
              Boolean(confirmedRevisionId) ||
              Boolean(busy)
            }
            onClick={() => void confirmDiagnosis()}
            variant="outline"
          >
            <CheckCircle2 size={15} />
            {busy === "confirm" ? "确认中…" : "确认执行资格"}
          </Button>
        </article>

        <article>
          <div className="supervised-step-heading">
            <span>02</span>
            <div>
              <strong>采集执行前置 Evidence</strong>
              <small>{ACTION_ID}</small>
            </div>
            <Radar size={17} />
          </div>
          <label className="form-field">
            <span>目标 Broker 资源</span>
            <Input
              onChange={(event) => setResource(event.target.value)}
              placeholder="broker/name-or-address"
              value={resource}
            />
          </label>
          <div className="supervised-action-fields">
            <label className="form-field">
              <span>Logger</span>
              <Input
                onChange={(event) => setLogger(event.target.value)}
                value={logger}
              />
            </label>
            <label className="form-field">
              <span>级别</span>
              <Select
                onValueChange={(value: ActionParameters["level"]) =>
                  setLevel(value)
                }
                value={level}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="DEBUG">DEBUG</SelectItem>
                  <SelectItem value="INFO">INFO</SelectItem>
                </SelectContent>
              </Select>
            </label>
            <label className="form-field">
              <span>TTL（秒）</span>
              <Input
                max={300}
                min={30}
                onChange={(event) =>
                  setTtlSeconds(Number(event.target.value))
                }
                type="number"
                value={ttlSeconds}
              />
            </label>
          </div>
          <Button
            disabled={
              !api ||
              !canOperate ||
              !confirmedRevisionId ||
              !resource.trim() ||
              !logger.trim() ||
              ttlSeconds < 30 ||
              ttlSeconds > 300 ||
              Boolean(busy)
            }
            onClick={() => void preparePrecondition()}
            variant="outline"
          >
            <Radar size={15} />
            {busy === "precondition" ? "采集中…" : "运行只读前置检查"}
          </Button>
          {precondition && (
            <dl className="supervised-evidence-summary">
              <div>
                <dt>Evidence</dt>
                <dd>{shortId(precondition.evidence.evidence_id)}</dd>
              </div>
              <div>
                <dt>Precondition hash</dt>
                <dd>{shortId(precondition.precondition_hash)}</dd>
              </div>
            </dl>
          )}
        </article>

        <article>
          <div className="supervised-step-heading">
            <span>03</span>
            <div>
              <strong>创建不可变计划</strong>
              <small>诊断 Evidence + 实时前置 Evidence</small>
            </div>
            <FileCheck2 size={17} />
          </div>
          <div className="supervised-plan-readiness">
            <Badge variant={precondition ? "success" : "secondary"}>
              {precondition ? "前置证据就绪" : "等待前置证据"}
            </Badge>
            <span>
              {diagnosisEvidenceIds.length} 条诊断 Evidence ·{" "}
              {precondition ? "1 条执行前置 Evidence" : "0 条执行前置 Evidence"}
            </span>
          </div>
          <Button
            disabled={!api || !precondition || Boolean(busy)}
            onClick={() => void createPlan()}
          >
            <FileCheck2 size={15} />
            {busy === "plan" ? "创建中…" : "创建受监督计划"}
          </Button>
          {plan?.kind === "action_plan" && (
            <Button
              onClick={() =>
                navigate(`/changes/plans/${plan.plan.id}`)
              }
              variant="outline"
            >
              打开计划与审批
              <ArrowRight size={15} />
            </Button>
          )}
        </article>
      </div>

      {!canOperate && (
        <div className="inline-alert warning">
          当前身份缺少 operator / rocketmq:operate 角色，只能查看诊断。
        </div>
      )}
      {message && <div className="inline-alert">{message}</div>}
    </section>
  );
}

function shortId(value: string) {
  return value.length > 24
    ? `${value.slice(0, 12)}…${value.slice(-8)}`
    : value;
}
