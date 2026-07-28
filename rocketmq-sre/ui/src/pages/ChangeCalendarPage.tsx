import {
  AlertTriangle,
  CalendarClock,
  CalendarPlus,
  LockKeyhole,
  Snowflake,
  Wrench,
} from "lucide-react";
import type { FormEvent } from "react";
import { useEffect, useMemo, useState } from "react";

import { createChangeManagementApi } from "@/api/changeManagementClient";
import type {
  ChangeWindow,
  ChangeWindowKind,
  CreateChangeWindowRequest,
} from "@/api/types";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useSreData } from "@/data/SreDataContext";
import {
  ChangeClusterSelect,
  ChangeStatePanel,
  ChangeWorkspaceNav,
} from "@/features/change-management/ChangeWorkspace";
import {
  changeWindowKindLabel,
  dateTimeLocalToIso,
  formatChangeTimestamp,
  toDateTimeLocal,
} from "@/features/change-management/changePresentation";
import { createMockChangeManagementApi } from "@/data/phase3ChangeDemo";

export function ChangeCalendarPage() {
  const auth = useAuth();
  const { clusters, demoMode } = useSreData();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockChangeManagementApi(auth.requestContext)
          : createChangeManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const defaults = useMemo(() => calendarDefaults(), []);
  const [clusterId, setClusterId] = useState("");
  const [from, setFrom] = useState(defaults.from);
  const [to, setTo] = useState(defaults.to);
  const [windows, setWindows] = useState<ChangeWindow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    if (!clusterId && clusters[0]) {
      setClusterId(clusters[0].id);
    }
  }, [clusterId, clusters]);

  useEffect(() => {
    if (!api || !clusterId) {
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void api
      .listChangeWindows(
        clusterId,
        dateTimeLocalToIso(from),
        dateTimeLocalToIso(to),
        256,
        controller.signal,
      )
      .then((page) => setWindows(page.items))
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "变更窗口暂不可用",
          );
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [api, clusterId, from, refresh, to]);

  const canCreate = auth.session?.roles.includes("operator") ?? false;

  return (
    <div className="page change-page">
      <PageHeader
        eyebrow="P3-11 · CHANGE CALENDAR"
        title="维护窗口与变更日历"
        description="维护窗口允许受控排程；freeze 与 blackout 始终阻断。时间范围使用明确 IANA 时区，冲突由服务端重新校验。"
        actions={
          <ChangeClusterSelect
            clusters={clusters}
            value={clusterId}
            onValueChange={setClusterId}
          />
        }
      />
      <ChangeWorkspaceNav />

      <section className="calendar-toolbar">
        <label>
          <span>查看起点</span>
          <input
            type="datetime-local"
            value={from}
            onChange={(event) => setFrom(event.target.value)}
          />
        </label>
        <label>
          <span>查看终点</span>
          <input
            type="datetime-local"
            value={to}
            onChange={(event) => setTo(event.target.value)}
          />
        </label>
        <div className="calendar-legend">
          <Badge variant="success">
            <Wrench size={13} /> 维护
          </Badge>
          <Badge variant="warning">
            <Snowflake size={13} /> 冻结
          </Badge>
          <Badge variant="destructive">
            <LockKeyhole size={13} /> 禁止
          </Badge>
        </div>
      </section>

      <div className="calendar-workspace">
        <section className="calendar-timeline-panel">
          <header>
            <div>
              <span className="section-kicker">ABSOLUTE WINDOWS</span>
              <h2>当前时间范围</h2>
            </div>
            <Badge variant="outline">{windows.length} 个窗口</Badge>
          </header>
          {loading && windows.length === 0 ? (
            <ChangeStatePanel state="loading" title="正在加载变更日历" />
          ) : error && windows.length === 0 ? (
            <ChangeStatePanel
              state="error"
              title="变更日历加载失败"
              detail={error}
            />
          ) : windows.length === 0 ? (
            <ChangeStatePanel
              state="empty"
              title="当前范围没有变更窗口"
              detail="没有维护窗口时，任何新排程都会被服务端判定为冲突。"
            />
          ) : (
            <div className="calendar-window-list">
              {windows.map((window) => (
                <CalendarWindowCard key={window.id} window={window} />
              ))}
            </div>
          )}
        </section>

        <CreateWindowPanel
          canCreate={canCreate}
          clusterId={clusterId}
          demoMode={demoMode}
          onCreated={() => setRefresh((value) => value + 1)}
        />
      </div>
    </div>
  );
}

function CalendarWindowCard({ window }: { window: ChangeWindow }) {
  const Icon =
    window.kind === "maintenance"
      ? Wrench
      : window.kind === "freeze"
        ? Snowflake
        : LockKeyhole;
  const variant: "success" | "warning" | "destructive" =
    window.kind === "maintenance"
      ? "success"
      : window.kind === "freeze"
        ? "warning"
        : "destructive";
  return (
    <article className={`calendar-window-card ${window.kind}`}>
      <span className="calendar-window-icon">
        <Icon aria-hidden="true" size={17} />
      </span>
      <div className="calendar-window-copy">
        <header>
          <div>
            <strong>{window.name}</strong>
            <span>{window.reason}</span>
          </div>
          <Badge variant={variant}>
            {changeWindowKindLabel(window.kind)}
          </Badge>
        </header>
        <div className="calendar-window-time">
          <CalendarClock aria-hidden="true" size={14} />
          <strong>{formatChangeTimestamp(window.starts_at)}</strong>
          <span>→</span>
          <strong>{formatChangeTimestamp(window.ends_at)}</strong>
          <code>{window.timezone}</code>
        </div>
        <footer>
          <span>并发上限 {window.max_parallelism}</span>
          <span>
            {window.resource_keys.length === 0
              ? "全部资源"
              : window.resource_keys.join(" · ")}
          </span>
        </footer>
      </div>
    </article>
  );
}

function CreateWindowPanel({
  clusterId,
  canCreate,
  demoMode,
  onCreated,
}: {
  clusterId: string;
  canCreate: boolean;
  demoMode: boolean;
  onCreated: () => void;
}) {
  const auth = useAuth();
  const api = useMemo(
    () =>
      auth.requestContext
        ? demoMode
          ? createMockChangeManagementApi(auth.requestContext)
          : createChangeManagementApi(auth.requestContext)
        : undefined,
    [auth.requestContext, demoMode],
  );
  const defaults = useMemo(() => createWindowDefaults(), []);
  const [name, setName] = useState("");
  const [kind, setKind] = useState<ChangeWindowKind>("maintenance");
  const [timezone, setTimezone] = useState("Asia/Shanghai");
  const [startsAt, setStartsAt] = useState(defaults.startsAt);
  const [endsAt, setEndsAt] = useState(defaults.endsAt);
  const [resources, setResources] = useState("");
  const [parallelism, setParallelism] = useState("1");
  const [reason, setReason] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string>();

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!api || !clusterId) {
      return;
    }
    setBusy(true);
    setMessage(undefined);
    const request: CreateChangeWindowRequest = {
      cluster_id: clusterId,
      name: name.trim(),
      kind,
      timezone: timezone.trim(),
      starts_at: dateTimeLocalToIso(startsAt),
      ends_at: dateTimeLocalToIso(endsAt),
      resource_keys: splitResources(resources),
      max_parallelism: Number(parallelism),
      reason: reason.trim(),
    };
    try {
      await api.createChangeWindow(request);
      setName("");
      setResources("");
      setReason("");
      setMessage("窗口已写入不可变变更日历。");
      onCreated();
    } catch (cause) {
      setMessage(
        cause instanceof Error ? cause.message : "窗口创建失败",
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <aside className="change-composer-panel">
      <header>
        <span className="composer-icon">
          <CalendarPlus aria-hidden="true" size={18} />
        </span>
        <div>
          <span className="section-kicker">NEW WINDOW</span>
          <h2>创建绝对窗口</h2>
          <p>创建后不可原地修改；调整时间需要新建窗口。</p>
        </div>
      </header>
      {!canCreate && (
        <div className="inline-alert">
          <AlertTriangle size={15} />
          当前身份缺少 operator 角色，仅可查看。
        </div>
      )}
      <form className="change-form" onSubmit={(event) => void submit(event)}>
        <label className="span-2">
          <span>窗口名称</span>
          <input
            disabled={!canCreate}
            maxLength={128}
            onChange={(event) => setName(event.target.value)}
            placeholder="例如：周三 Broker 维护"
            required
            value={name}
          />
        </label>
        <label>
          <span>窗口类型</span>
          <Select
            disabled={!canCreate}
            value={kind}
            onValueChange={(value) => setKind(value as ChangeWindowKind)}
          >
            <SelectTrigger aria-label="窗口类型">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="maintenance">维护窗口</SelectItem>
              <SelectItem value="freeze">冻结期</SelectItem>
              <SelectItem value="blackout">禁止变更</SelectItem>
            </SelectContent>
          </Select>
        </label>
        <label>
          <span>IANA 时区</span>
          <input
            disabled={!canCreate}
            onChange={(event) => setTimezone(event.target.value)}
            required
            value={timezone}
          />
        </label>
        <label>
          <span>开始时间</span>
          <input
            disabled={!canCreate}
            type="datetime-local"
            value={startsAt}
            onChange={(event) => setStartsAt(event.target.value)}
          />
        </label>
        <label>
          <span>结束时间</span>
          <input
            disabled={!canCreate}
            type="datetime-local"
            value={endsAt}
            onChange={(event) => setEndsAt(event.target.value)}
          />
        </label>
        <label>
          <span>最大并发（1–16）</span>
          <input
            disabled={!canCreate}
            min={1}
            max={16}
            onChange={(event) => setParallelism(event.target.value)}
            type="number"
            value={parallelism}
          />
        </label>
        <label className="span-2">
          <span>资源范围（每行一个；留空表示全部）</span>
          <textarea
            disabled={!canCreate}
            onChange={(event) => setResources(event.target.value)}
            placeholder={"broker/broker-a\nproxy/rocketmq/proxy"}
            value={resources}
          />
        </label>
        <label className="span-2">
          <span>创建原因</span>
          <textarea
            disabled={!canCreate}
            maxLength={2048}
            onChange={(event) => setReason(event.target.value)}
            required
            value={reason}
          />
        </label>
        {message && <p className="form-message span-2">{message}</p>}
        <div className="form-actions span-2">
          <Button
            disabled={!canCreate || busy || !clusterId}
            type="submit"
          >
            {busy ? "正在创建…" : "创建窗口"}
          </Button>
        </div>
      </form>
    </aside>
  );
}

function calendarDefaults() {
  const now = new Date();
  const from = new Date(now);
  from.setHours(0, 0, 0, 0);
  const to = new Date(from);
  to.setDate(to.getDate() + 14);
  return { from: toDateTimeLocal(from), to: toDateTimeLocal(to) };
}

function createWindowDefaults() {
  const startsAt = new Date(Date.now() + 60 * 60 * 1000);
  const endsAt = new Date(startsAt.getTime() + 2 * 60 * 60 * 1000);
  return {
    startsAt: toDateTimeLocal(startsAt),
    endsAt: toDateTimeLocal(endsAt),
  };
}

function splitResources(value: string): string[] {
  return [
    ...new Set(
      value
        .split(/[\n,]/)
        .map((item) => item.trim())
        .filter(Boolean),
    ),
  ];
}
