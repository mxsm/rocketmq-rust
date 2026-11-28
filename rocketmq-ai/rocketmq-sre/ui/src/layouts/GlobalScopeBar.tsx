import { ChevronDown, Clock3, LockKeyhole, RefreshCw } from "lucide-react";
import { type FormEvent, useCallback, useState } from "react";

import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useWorkspaceMessages } from "@/i18n/workspaceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import { ShellDisclosure } from "./ShellDisclosure";

export function GlobalScopeBar() {
  const scope = useWorkspaceScope();
  const { clusters, api, demoMode } = useSreData();
  const m = useWorkspaceMessages();
  const english = useOperatorPreferences().locale === "en-US";
  const [start, setStart] = useState(scope.window.start.slice(0, 16));
  const [end, setEnd] = useState(scope.window.end.slice(0, 16));
  const [error, setError] = useState(false);
  const load = useCallback(
    (signal: AbortSignal) =>
      scope.clusterId
        ? api.listAssets(scope.clusterId, signal)
        : Promise.resolve({
            items: [],
            partial: false,
            warnings: [],
            observed_at: "",
          }),
    [api, scope.clusterId],
  );
  const assets = useAsyncResource(load);
  const options = assets.data?.items.filter((asset) => asset.cluster_id === scope.clusterId) ?? [];
  const selected = options.find(
    (asset) => asset.kind === scope.resourceKind && asset.external_key === scope.resourceKey,
  );
  const preset = (hours: number) => {
    const now = new Date();
    scope.setWindow({
      start: new Date(now.getTime() - hours * 60 * 60_000).toISOString(),
      end: now.toISOString(),
    });
  };
  const apply = (event: FormEvent) => {
    event.preventDefault();
    const from = new Date(`${start}:00Z`);
    const to = new Date(`${end}:00Z`);
    if (!Number.isFinite(from.getTime()) || !Number.isFinite(to.getTime()) || from >= to) {
      setError(true);
      return;
    }
    setError(false);
    scope.setWindow({ start: from.toISOString(), end: to.toISOString() });
  };

  return (
    <section aria-label={m.globalScope} className="shell-scope">
      <label className="shell-cluster">
        <span>{english ? "Cluster" : "集群"}</span>
        <select
          aria-label={m.cluster}
          disabled={!clusters.length}
          onChange={(event) => scope.setClusterId(event.target.value)}
          value={scope.clusterId}
        >
          {!scope.clusterId && <option value="">{m.noClusters}</option>}
          {clusters.map((cluster) => (
            <option key={cluster.id} value={cluster.id}>
              {cluster.external_cluster_key} · {cluster.environment}
            </option>
          ))}
        </select>
      </label>
      {demoMode && (
        <span className="shell-demo" title={m.demoHint}>
          {english ? "Demo" : "演示"}
        </span>
      )}
      <ShellDisclosure
        className="shell-filters"
        label={m.globalScope}
        summary={
          <>
            <Clock3 aria-hidden="true" size={15} />
            <span>{scope.resourceKey ?? (english ? "Resource & time" : "资源与时间")}</span>
            <ChevronDown aria-hidden="true" size={14} />
          </>
        }
      >
        <div className="shell-filter-fields">
          <label>
            <span>{m.resource}</span>
            <select
              aria-label={m.resource}
              disabled={!scope.clusterId || assets.loading}
              onChange={(event) => {
                const asset = options.find((item) => item.id === event.target.value);
                scope.setScope({
                  clusterId: scope.clusterId,
                  window: scope.window,
                  resourceKind: asset?.kind,
                  resourceKey: asset?.external_key,
                });
              }}
              value={selected?.id ?? (scope.resourceKey ? "current" : "")}
            >
              <option value="">{m.allResources}</option>
              {scope.resourceKey && !selected && (
                <option value="current">
                  {scope.resourceKind}/{scope.resourceKey}
                </option>
              )}
              {options.map((asset) => (
                <option key={asset.id} value={asset.id}>
                  {asset.kind} · {asset.display_name}
                </option>
              ))}
            </select>
          </label>
          <form className="shell-window" onSubmit={apply}>
            <label>
              <span>{m.start}</span>
              <input
                aria-label={m.start}
                onChange={(event) => setStart(event.target.value)}
                required
                type="datetime-local"
                value={start}
              />
            </label>
            <label>
              <span>{m.end}</span>
              <input
                aria-label={m.end}
                onChange={(event) => setEnd(event.target.value)}
                required
                type="datetime-local"
                value={end}
              />
            </label>
            <Button disabled={!scope.clusterId} size="sm" type="submit" variant="outline">
              {m.apply}
            </Button>
            <Button
              disabled={!scope.clusterId}
              onClick={() => preset(1)}
              size="sm"
              type="button"
              variant="ghost"
            >
              {m.recentHour}
            </Button>
            <Button
              disabled={!scope.clusterId}
              onClick={() => preset(24)}
              size="sm"
              type="button"
              variant="ghost"
            >
              {m.recentDay}
            </Button>
          </form>
          <div className="shell-scope-hint">
            <span>{m.scopeHint}</span>
            {demoMode && <span>{m.demoHint}</span>}
          </div>
          {error && (
            <div className="scope-error" role="alert">
              {m.invalidWindow}
            </div>
          )}
        </div>
      </ShellDisclosure>
      <span
        className="shell-boundary"
        title={
          english
            ? "Diagnosis is read-only. Changes require approval."
            : "诊断保持只读，变更需经审批。"
        }
      >
        <LockKeyhole size={15} aria-hidden="true" />
        {english ? "Read-only diagnosis" : "诊断只读"}
      </span>
      {scope.invalid && (
        <div className="scope-error shell-invalid-scope" role="alert">
          <span>{m.invalidScope}</span>
          <Button onClick={scope.reset} size="sm" variant="outline">
            <RefreshCw size={14} />
            {m.resetScope}
          </Button>
        </div>
      )}
    </section>
  );
}
