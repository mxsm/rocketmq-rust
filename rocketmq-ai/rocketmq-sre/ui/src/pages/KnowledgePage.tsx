import { useCallback, useRef, useState, type FormEvent } from "react";
import { useLocation } from "react-router-dom";
import type { SreApi } from "@/api/client";
import type { KnowledgeSearchInput } from "@/api/knowledgeClient";
import { useAuth } from "@/auth/AuthContext";
import { PageHeader } from "@/components/PageHeader";
import { ClusterScopeSelect } from "@/components/Phase1Primitives";
import { Button } from "@/components/ui/button";
import { useOptionalWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { useSreData } from "@/data/SreDataContext";
import { KnowledgeDetail } from "@/features/knowledge/KnowledgeDetail";
import { KnowledgeMetadata, KnowledgeResults, KnowledgeStatus } from "@/features/knowledge/KnowledgeResults";
import { itemId, itemMatches, searchMatches, validSearch } from "@/features/knowledge/presentation";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useClusterScope } from "@/hooks/useClusterScope";
import { useKnowledgeMessages } from "@/i18n/knowledgeMessages";

export function KnowledgePage() {
  const { api } = useSreData();
  const { session } = useAuth();
  const scope = useClusterScope();
  const workspace = useOptionalWorkspaceScope();
  const location = useLocation();
  const { t } = useKnowledgeMessages();
  const identity = JSON.stringify([session?.tenantId, session?.subject, [...(session?.clusterIds ?? [])].sort(), [...(session?.roles ?? [])].sort()]);
  const owner = JSON.stringify([identity, scope.clusterId, workspace?.scopeKey]);
  const values = new URLSearchParams(location.search).getAll("focus");
  const rawFocus = workspace ? workspace.knowledgeFocus : values.length === 1 ? values[0] : values.length ? "invalid" : "";
  // A focus belongs to the identity/scope in which that URL was selected. Keeping
  // the same query across a scope switch must not reopen the previous item.
  const focusOwner = useRef({ value: rawFocus, owner, active: true });
  const switchIntent = useRef<{ focus: string; cluster: string; identity: string } | undefined>(undefined);
  if (focusOwner.current.value !== rawFocus) focusOwner.current = { value: rawFocus, owner, active: true };
  else if (focusOwner.current.owner !== owner) focusOwner.current = { value: rawFocus, owner, active: false };
  if (switchIntent.current?.identity === identity && switchIntent.current.cluster === scope.clusterId && switchIntent.current.focus === rawFocus) {
    focusOwner.current = { value: rawFocus, owner, active: true };
    switchIntent.current = undefined;
  }
  const focus = focusOwner.current.active ? rawFocus : "";
  const authorized = !!session && session.clusterIds.includes(scope.clusterId);
  return <div className="page knowledge-page">
    <PageHeader title={t("title")} description={t("description")} actions={
      <ClusterScopeSelect clusters={scope.clusters} value={scope.clusterId} onChange={scope.setClusterId} />
    } />
    {!authorized ? <p role="status">{t("noScope")}</p> : <KnowledgeWorkspace key={`${owner}:${focus}`} api={api} cluster={scope.clusterId} tenant={session.tenantId} roles={session.roles} allowedClusters={session.clusterIds} focus={focus} onSwitchCluster={(cluster) => {
      if (!session.clusterIds.includes(cluster)) return;
      if (workspace) {
        workspace.setScope({ clusterId: cluster, window: workspace.window }, { knowledgeFocus: rawFocus });
        return;
      }
      switchIntent.current = { focus: rawFocus, cluster, identity };
      scope.setClusterId(cluster);
    }} />}
  </div>;
}

function KnowledgeWorkspace({ api, tenant, cluster, roles, allowedClusters, focus, onSwitchCluster }: { api: SreApi; tenant: string; cluster: string; roles: string[]; allowedClusters: string[]; focus: string; onSwitchCluster: (cluster: string) => void }) {
  const { t, time } = useKnowledgeMessages();
  const [filter, setFilter] = useState("");
  const [q, setQ] = useState("");
  const [version, setVersion] = useState("");
  const [component, setComponent] = useState("");
  const [include, setInclude] = useState(false);
  const [submitted, setSubmitted] = useState<KnowledgeSearchInput>();
  const [selected, setSelected] = useState(itemId(focus) ? focus : "");
  const [invalid, setInvalid] = useState(false);
  const loadList = useCallback((signal: AbortSignal) => api.listKnowledgePage(cluster, signal), [api, cluster]);
  const list = useAsyncResource(loadList);
  const loadSearch = useCallback((signal: AbortSignal) => submitted ? api.searchKnowledge(submitted, signal) : Promise.resolve(undefined), [api, submitted]);
  const search = useAsyncResource(loadSearch);
  const reset = () => { setSubmitted(undefined); setSelected(""); setInvalid(false); };
  const submit = (event: FormEvent) => {
    event.preventDefault();
    setSelected("");
    if (!validSearch(q, version, component)) { setSubmitted(undefined); setInvalid(true); return; }
    setInvalid(false);
    setSubmitted({ cluster_id: cluster, q: q.trim(), component: component.trim() || null, rocketmq_version: version.trim(), limit: 20, include_unvalidated: include });
  };
  const listMatches = Array.isArray(list.data?.items) && list.data.items.length <= 100 && list.data.items.every((item) => itemMatches(item, tenant, cluster));
  const rows = listMatches ? (list.data?.items ?? []).filter((item) => [item.title, item.component].some((value) => value.toLocaleLowerCase().includes(filter.trim().toLocaleLowerCase()))) : [];
  const matches = submitted && search.data && searchMatches(search.data, submitted, tenant);
  return <>
    {focus && !itemId(focus) && <p role="alert">{t("focusInvalid")}</p>}
    <section className="knowledge-section" aria-label={t("searchTitle")}>
      <h2>{t("searchTitle")}</h2><p>{t("versionHint")}</p>
      <form className="knowledge-search" onSubmit={submit}>
        <label>{t("query")}<input type="search" value={q} onChange={(e) => { reset(); setQ(e.target.value); }} /></label>
        <label>{t("component")}<input value={component} onChange={(e) => { reset(); setComponent(e.target.value); }} /></label>
        <label>{t("version")}<input value={version} placeholder="5.3.0" onChange={(e) => { reset(); setVersion(e.target.value); }} /></label>
        <label className="knowledge-checkbox"><input type="checkbox" checked={include} onChange={(e) => { reset(); setInclude(e.target.checked); }} />{t("include")}</label>
        <Button type="submit">{t("search")}</Button>
      </form>
      {invalid && <p role="alert">{t("invalidSearch")}</p>}
      {submitted && search.loading && <p role="status">{t("loading")}</p>}
      {submitted && !!search.error && <KnowledgeReadError onRetry={search.reload} />}
      {!search.loading && !search.error && search.data && !matches && <p role="alert">{t("mismatch")}</p>}
    </section>
    {!search.loading && !search.error && search.data && matches && <KnowledgeResults page={search.data} onSelect={setSelected} />}
    {selected && <KnowledgeDetail key={selected} id={selected} api={api} tenant={tenant} cluster={cluster} roles={roles} allowedClusters={allowedClusters} onSwitchCluster={onSwitchCluster} onClose={() => setSelected("")} />}
    <section className="knowledge-section" aria-label={t("metadata")}>
      <h2>{t("metadata")}</h2>
      <label className="knowledge-filter">{t("filter")}<input type="search" placeholder={t("filterHint")} value={filter} onChange={(e) => { reset(); setFilter(e.target.value); }} /></label>
      {list.loading && <p role="status">{t("loading")}</p>}
      {!!list.error && <KnowledgeReadError onRetry={list.reload} />}
      {!list.loading && !list.error && list.data && !listMatches && <p role="alert">{t("mismatch")}</p>}
      {!list.loading && !list.error && listMatches && <>
        <p>{t("asOf")}: {time(list.data?.observed_at)}</p>
        {(list.data?.next_cursor || list.data?.partial) && <p className="knowledge-notice">{t("listBound")}</p>}
        {list.data?.next_cursor === undefined && <p className="knowledge-notice">{t("listUnknown")}</p>}
        {rows.length === 0 && <p>{t("empty")}</p>}
        <div className="knowledge-records">{rows.map((item) => <article key={item.id} className="knowledge-record">
          <div className="knowledge-record-heading">
            <KnowledgeStatus item={item} />
            <h3>{item.title}</h3>
            <Button variant="outline" onClick={() => setSelected(item.id)}>{t("detail")}</Button>
          </div>
          <KnowledgeMetadata item={item} />
        </article>)}</div>
      </>}
    </section>
  </>;
}

export function KnowledgeReadError({ onRetry }: { onRetry: () => void }) {
  const { t } = useKnowledgeMessages();
  return <div role="alert"><p>{t("unavailable")}</p><Button variant="outline" onClick={onRetry}>{t("retry")}</Button></div>;
}
