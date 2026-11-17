import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import type { SreApi } from "@/api/client";
import type { KnowledgeFeedbackRequest, KnowledgeHistoryPage, KnowledgeItem, KnowledgeReviewRequest } from "@/api/types";
import { Button } from "@/components/ui/button";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useKnowledgeMessages } from "@/i18n/knowledgeMessages";
import { KnowledgeMetadata, KnowledgeStatus } from "./KnowledgeResults";
import { bytes, itemMatches, reviewTransitions, safeSource } from "./presentation";
import { isKnowledgeHistory, isKnowledgeItem } from "./responseGuards";

export function KnowledgeDetail({ id, api, tenant, cluster, roles, allowedClusters, onSwitchCluster, onClose }: {
  id: string; api: SreApi; tenant: string; cluster: string; roles: string[]; allowedClusters: string[]; onSwitchCluster: (cluster: string) => void; onClose: () => void;
}) {
  const { t, time } = useKnowledgeMessages();
  const loadItem = useCallback((signal: AbortSignal) => api.getKnowledge(id, signal), [api, id]);
  const loadHistory = useCallback((signal: AbortSignal) => api.getKnowledgeHistory(id, signal), [api, id]);
  const item = useAsyncResource(loadItem);
  const history = useAsyncResource(loadHistory);
  const current = !item.loading && !item.error && item.data?.id === id && itemMatches(item.data, tenant, cluster) ? item.data : undefined;
  const other = !item.loading && !item.error && isKnowledgeItem(item.data) && item.data.id === id && item.data.tenant_id === tenant && item.data.cluster_id &&
    item.data.cluster_id !== cluster && allowedClusters.includes(item.data.cluster_id) ? item.data.cluster_id : undefined;
  const record = !history.loading && !history.error && current && isKnowledgeHistory(history.data) && history.data.knowledge_item_id === id &&
    history.data.tenant_id === tenant && (history.data.cluster_id ?? null) === (current.cluster_id ?? null) &&
    history.data.item_content_hash === current.content_hash && history.data.feedback.every((entry) => entry.cluster_id == null || allowedClusters.includes(entry.cluster_id)) ? history.data : undefined;
  const refresh = async () => { await Promise.all([item.reload(), history.reload()]); };
  const canReview = roles.includes("operator") && (current?.cluster_id != null || roles.includes("sre-admin"));
  return <section className="knowledge-section knowledge-detail" aria-label={t("detailTitle")}>
    <header><h2>{t("detailTitle")}</h2><Button variant="outline" onClick={onClose}>{t("close")}</Button></header>
    {item.loading && <p role="status">{t("loading")}</p>}
    {!!item.error && <p role="alert">{t("unavailable")}</p>}
    {!item.loading && !item.error && !current && !other && <p role="alert">{t("mismatch")}</p>}
    {other && <div role="status"><p>{t("otherScope")}</p><code>{other}</code><Button variant="outline" onClick={() => onSwitchCluster(other)}>{t("switchScope")}</Button></div>}
    {current && <>
      <h3>{current.title}</h3><KnowledgeStatus item={current} /><KnowledgeMetadata item={current} />
      <p>{t("source")}: {safeSource(record?.source_uri_display) ?? t("omittedSource")}</p>
      <h3>{t("history")}</h3>
      {history.loading && <p role="status">{t("loading")}</p>}
      {!!history.error && <p role="alert">{t("unavailable")}</p>}
      {!history.loading && !history.error && !record && <p role="alert">{t("mismatch")}</p>}
      {record && <><p>{t("asOf")}: {time(record.observed_at)}</p><KnowledgeHistory history={record} /></>}
      {!canReview && <p>{t("noCuration")}</p>}
      <KnowledgeActions key={`${current.id}:${current.review_status}:${current.updated_at}`} api={api} item={current} canReview={canReview} onRead={refresh} />
    </>}
    <Button variant="outline" onClick={() => void refresh()}>{t("retry")}</Button>
  </section>;
}

function KnowledgeHistory({ history }: { history: KnowledgeHistoryPage }) {
  const { t, label, time } = useKnowledgeMessages();
  const flag = (value: boolean | null | undefined) => t(value === true ? "yes" : value === false ? "no" : "unknown");
  return <div className="knowledge-history">
    {(history.reviews_truncated || history.feedback_truncated) && <p className="knowledge-notice">{t("historyBound")}</p>}
    {!history.reviews.length && <p>{t("noReviews")}</p>}
    <ol>{history.reviews.map((review) => <li key={review.id}>
      <strong>{label(review.event_kind)}</strong> · <time>{time(review.created_at)}</time>
      <p>{t("actor")}: {review.actor_subject ?? t("unknown")}</p>
      {!review.event_kind && <p>{t("legacy")}</p>}
      <p>{label(review.from_status)} → {label(review.to_status)}</p>
      <p>{t("declaredHuman")}: {flag(review.human_confirmed)} · {t("declaredAi")}: {flag(review.ai_generated)}</p>
      <p>{t("reason")}: {review.reason}</p>
      <p>{t("hash")}: <code>{review.item_content_hash ?? t("unknown")}</code></p>
      {review.details_truncated && <p>{t("shortened")}</p>}
    </li>)}</ol>
    {!history.feedback.length && <p>{t("noFeedback")}</p>}
    {!!history.feedback.length && <p>{t("historyScope")}</p>}
    <ol>{history.feedback.map((feedback) => <li key={feedback.id}>
      <strong>{label(feedback.kind)}</strong> · <time>{time(feedback.created_at)}</time>
      <p>{t("actor")}: {feedback.actor_subject ?? t("unknown")}</p>
      <p>{t("scope")}: {feedback.cluster_id ?? t("tenantWide")}</p>
      {feedback.comment && <p>{feedback.comment}</p>}
      {feedback.details_truncated && <p>{t("shortened")}</p>}
    </li>)}</ol>
    {history.postmortem && <div className="knowledge-notice">
      <strong>{t("publication")}</strong>
      <p>{t("revision")}: {history.postmortem.revision} · <code>{history.postmortem.postmortem_id}</code></p>
      <p>{t("actor")}: {history.postmortem.confirmed_by ?? t("unknown")} · {time(history.postmortem.confirmed_at)}</p>
    </div>}
  </div>;
}

function KnowledgeActions({ api, item, canReview, onRead }: { api: SreApi; item: KnowledgeItem; canReview: boolean; onRead: () => Promise<void> }) {
  const { t, label } = useKnowledgeMessages();
  const transitions = reviewTransitions(item.review_status);
  const [status, setStatus] = useState<KnowledgeReviewRequest["status"]>(transitions[0] ?? item.review_status);
  const [reason, setReason] = useState("");
  const [confirmed, setConfirmed] = useState(false);
  const [kind, setKind] = useState<KnowledgeFeedbackRequest["kind"]>("useful");
  const [comment, setComment] = useState("");
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<"saved" | "uncertain">();
  const active = useRef(true);
  const pending = useRef<AbortController | undefined>(undefined);
  useEffect(() => { active.current = true; return () => { active.current = false; pending.current?.abort(); }; }, []);
  const mutate = async (type: "review" | "feedback") => {
    if (pending.current || outcome) return;
    const controller = new AbortController();
    pending.current = controller;
    setBusy(true);
    try {
      const result = type === "review"
        ? await api.reviewKnowledge(item.id, { status, reason: reason.trim(), human_confirmed: confirmed }, controller.signal)
        : await api.feedbackKnowledge(item.id, { kind, comment: comment.trim() || null }, controller.signal);
      if (active.current && !controller.signal.aborted) setOutcome(result.id === item.id && result.tenant_id === item.tenant_id && result.cluster_id === item.cluster_id && result.content_hash === item.content_hash ? "saved" : "uncertain");
    } catch {
      if (active.current && !controller.signal.aborted) setOutcome("uncertain");
    } finally {
      if (pending.current === controller) pending.current = undefined;
      if (active.current && !controller.signal.aborted) setBusy(false);
    }
  };
  const submit = (type: "review" | "feedback") => (event: FormEvent) => { event.preventDefault(); void mutate(type); };
  return <div className="knowledge-actions">
    {outcome && <p role="status" className="knowledge-notice">{t(outcome)}</p>}
    {outcome && <Button variant="outline" onClick={() => { void onRead(); }}>{t("retry")}</Button>}
    <form onSubmit={submit("feedback")}>
      <h3>{t("feedbackTitle")}</h3><p>{t("feedbackHint")}</p>
      <label>{t("feedbackTitle")}<select value={kind} onChange={(e) => {
        const next = e.target.value;
        if (next === "useful" || next === "incorrect" || next === "outdated") setKind(next);
      }}>{(["useful", "incorrect", "outdated"] as const).map((value) => <option key={value} value={value}>{label(value)}</option>)}</select></label>
      <label>{t("comment")}<textarea value={comment} onChange={(e) => setComment(e.target.value)} /></label>
      <Button type="submit" disabled={busy || !!outcome || bytes(comment) > 2000}>{busy ? t("submitting") : t("sendFeedback")}</Button>
    </form>
    {canReview && transitions.length > 0 && <form onSubmit={submit("review")}>
      <h3>{t("reviewTitle")}</h3><p>{t("reviewHint")}</p>
      <label>{t("nextStatus")}<select value={status} onChange={(e) => {
        const next = transitions.find((value) => value === e.target.value);
        if (next) setStatus(next);
      }}>{transitions.map((value) => <option key={value} value={value}>{label(value)}</option>)}</select></label>
      <label>{t("reason")}<textarea value={reason} onChange={(e) => setReason(e.target.value)} required /></label>
      {status === "validated" && <label className="knowledge-checkbox"><input type="checkbox" checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)} />{t("confirm")}</label>}
      <Button type="submit" disabled={busy || !!outcome || !reason.trim() || bytes(reason) > 2000 || (status === "validated" && !confirmed)}>{busy ? t("submitting") : t("submitReview")}</Button>
    </form>}
  </div>;
}
