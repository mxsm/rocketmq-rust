import type { KnowledgeChunkView, KnowledgeItem, KnowledgeSearchPage } from "@/api/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useKnowledgeMessages } from "@/i18n/knowledgeMessages";
import { safeSource } from "./presentation";

export function KnowledgeMetadata({ item }: { item: KnowledgeItem }) {
  const { t, time } = useKnowledgeMessages();
  return <dl className="knowledge-definitions">
    <dt>{t("itemScope")}</dt><dd>{item.cluster_id ?? t("tenantWide")}</dd>
    <dt>{t("component")}</dt><dd>{item.component}</dd>
    <dt>{t("range")}</dt><dd>{item.rocketmq_version_range}</dd>
    <dt>{t("sourceVersion")}</dt><dd>{item.source_version}</dd>
    <dt>{t("owner")}</dt><dd>{item.owner}</dd>
    <dt>{t("validFrom")}</dt><dd>{time(item.valid_from)}</dd>
    <dt>{t("validUntil")}</dt><dd>{time(item.valid_until)}</dd>
    <dt>{t("due")}</dt><dd>{time(item.review_due_at)}</dd>
    <dt>{t("sensitivity")}</dt><dd>{item.sensitivity}</dd>
    <dt>{t("hash")}</dt><dd><code>{item.content_hash}</code></dd>
  </dl>;
}

export function KnowledgeStatus({ item }: { item: Pick<KnowledgeItem, "review_status" | "conflict"> }) {
  const { t, label } = useKnowledgeMessages();
  return <div className="knowledge-badges"><Badge variant={item.review_status === "validated" ? "info" : "secondary"}>{label(item.review_status)}</Badge>
    {item.conflict && <Badge variant="warning">{t("conflict")}</Badge>}</div>;
}

export function KnowledgeResults({ page, onSelect }: { page: KnowledgeSearchPage; onSelect: (id: string) => void }) {
  const { t, time } = useKnowledgeMessages();
  return <section className="knowledge-section" aria-label={t("results")}>
    <h2>{t("results")}</h2>
    <p>{t("asOf")}: {time(page.observed_at)} · {page.binding.rocketmq_version}</p>
    <p>{t("candidates")}: {page.bounds.scanned_candidates} / {page.bounds.candidate_limit}</p>
    {page.bounds.candidate_limit_reached && <p role="status" className="knowledge-notice">{t("bounded")}</p>}
    {page.bounds.results_truncated && <p role="status" className="knowledge-notice">{t("truncated")}</p>}
    {page.partial && <p className="knowledge-notice">{t("partial")}</p>}
    {page.items.length === 0 && <p>{t("noResults")}</p>}
    <div className="knowledge-cards">{page.items.map((chunk) => <KnowledgeChunk key={chunk.id} chunk={chunk} onSelect={onSelect} />)}</div>
  </section>;
}

function KnowledgeChunk({ chunk, onSelect }: { chunk: KnowledgeChunkView; onSelect: (id: string) => void }) {
  const { t, label, time } = useKnowledgeMessages();
  const source = safeSource(chunk.source_uri_display);
  return <article className="knowledge-card">
    <KnowledgeStatus item={chunk} />
    <h3>{chunk.title}</h3>{chunk.heading && <h4>{chunk.heading}</h4>}
    <Badge variant={chunk.eligible_for_diagnosis ? "info" : "warning"}>{t(chunk.eligible_for_diagnosis ? "eligible" : "ineligible")}</Badge>
    <ul>{chunk.validity.map((reason) => <li key={reason}>{label(reason)}</li>)}
      {chunk.exclusion_reasons.map((reason) => <li key={reason}>{label(reason)}</li>)}</ul>
    <p className="knowledge-content">{chunk.content}</p>
    {(chunk.content_truncated || chunk.metadata_truncated) && <p>{t("shortened")}</p>}
    <dl className="knowledge-definitions">
      <dt>{t("component")}</dt><dd>{chunk.component}</dd>
      <dt>{t("range")}</dt><dd>{chunk.rocketmq_version_range}</dd>
      <dt>{t("source")}</dt><dd>{source ?? t("omittedSource")}</dd>
      <dt>{t("sourceVersion")}</dt><dd>{chunk.source_version}</dd>
      <dt>{t("due")}</dt><dd>{time(chunk.review_due_at)}</dd>
      <dt>{t("validFrom")}</dt><dd>{time(chunk.valid_from)}</dd>
      <dt>{t("validUntil")}</dt><dd>{time(chunk.valid_until)}</dd>
      <dt>{t("hash")}</dt><dd><code>{chunk.item_hash}</code></dd>
      <dt>{t("chunkHash")}</dt><dd><code>{chunk.chunk_hash}</code></dd>
    </dl>
    <Button variant="outline" onClick={() => onSelect(chunk.knowledge_item_id)}>{t("detail")}</Button>
  </article>;
}
