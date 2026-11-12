import { FileJson, RefreshCw, X } from "lucide-react";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";

import { DefinitionGrid } from "@/components/Phase1Primitives";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useWorkspaceScope } from "@/context/WorkspaceScopeContext";
import { resourcePath } from "@/context/workspaceScope";
import { useSreData } from "@/data/SreDataContext";
import { useAsyncResource } from "@/hooks/useAsyncResource";
import { useEvidenceMessages } from "@/i18n/evidenceMessages";
import { useOperatorPreferences } from "@/preferences/OperatorPreferences";
import {
  evidenceAvailability,
  evidenceFreshness,
  evidenceOutsideWindow,
  safeEvidenceJson,
  type EvidenceAccess,
  type EvidenceFreshnessPolicy,
} from "./evidencePresentation";
import {
  evidenceErrorKind,
  SELECTED_RESOURCE,
  useEvidenceMetadata,
} from "./useEvidenceMetadata";

interface EvidenceDrawerProps {
  evidenceId: string;
  onClose: () => void;
  access?: EvidenceAccess;
  freshnessPolicy?: EvidenceFreshnessPolicy;
}

/** A new scope or revision never reuses an earlier drawer's metadata or content. */
export function EvidenceDrawer(props: EvidenceDrawerProps) {
  const scope = useWorkspaceScope();
  const access = props.access ?? SELECTED_RESOURCE;
  return (
    <EvidenceDrawerSession
      {...props}
      access={access}
      key={`${scope.scopeKey}:${JSON.stringify(access)}`}
    />
  );
}

function EvidenceDrawerSession({
  evidenceId,
  onClose,
  access = SELECTED_RESOURCE,
  freshnessPolicy,
}: EvidenceDrawerProps) {
  const m = useEvidenceMessages();
  const scope = useWorkspaceScope();
  const { api } = useSreData();
  const { formatDateTime } = useOperatorPreferences();
  const metadata = useEvidenceMetadata(evidenceId, access);
  const evidence = metadata.error ? undefined : metadata.data;
  const [requested, setRequested] = useState("");
  const contentKey = evidence
    ? `${scope.scopeKey}:${evidence.evidence_id}:${evidence.content_hash}`
    : "";
  const canRead = !!evidence && evidenceAvailability(evidence) !== "missing";
  const load = useCallback(
    async (signal: AbortSignal) => {
      if (!canRead || !contentKey || requested !== contentKey) return undefined;
      const content = await api.getEvidenceContent(evidenceId, signal);
      return safeEvidenceJson(content);
    },
    [api, canRead, contentKey, evidenceId, requested],
  );
  const content = useAsyncResource(load);
  const panel = useRef<HTMLElement | null>(null);
  const close = useRef(onClose);
  useLayoutEffect(() => {
    close.current = onClose;
  }, [onClose]);
  useEffect(() => {
    const previous =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : undefined;
    const overflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const focusable = () =>
      [
        ...(panel.current?.querySelectorAll<HTMLElement>(
          'button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), summary, [tabindex]:not([tabindex="-1"])',
        ) ?? []),
      ].filter((element) => {
        if (element.closest("[hidden]")) return false;
        const details = element.closest("details:not([open])");
        return !details || element.tagName === "SUMMARY";
      });
    const focusFirst = () => (focusable()[0] ?? panel.current)?.focus();
    focusFirst();
    const keydown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close.current();
      }
      if (event.key !== "Tab") return;
      const elements = focusable();
      const first = elements[0];
      const last = elements.at(-1);
      if (!first) {
        event.preventDefault();
        panel.current?.focus();
        return;
      }
      const current = document.activeElement;
      if (
        event.shiftKey &&
        (current === first || !panel.current?.contains(current))
      ) {
        event.preventDefault();
        last?.focus();
      } else if (
        !event.shiftKey &&
        (current === last || !panel.current?.contains(current))
      ) {
        event.preventDefault();
        first.focus();
      }
    };
    const focusin = (event: FocusEvent) => {
      if (
        event.target instanceof Node &&
        !panel.current?.contains(event.target)
      )
        focusFirst();
    };
    document.addEventListener("keydown", keydown, true);
    document.addEventListener("focusin", focusin);
    return () => {
      document.removeEventListener("keydown", keydown, true);
      document.removeEventListener("focusin", focusin);
      document.body.style.overflow = overflow;
      if (previous?.isConnected) previous.focus();
      else document.getElementById("workspace-content")?.focus();
    };
  }, []);
  const related =
    evidence &&
    access.kind === "revision_evidence" &&
    resourcePath(scope) !== undefined &&
    evidence.resource !== resourcePath(scope);
  const freshness = evidence
    ? evidenceFreshness(evidence, freshnessPolicy)
    : "unknown";

  return createPortal(
    <div
      className="evidence-drawer-overlay"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <section
        aria-labelledby="evidence-drawer-title"
        aria-modal="true"
        className="evidence-drawer"
        ref={panel}
        role="dialog"
        tabIndex={-1}
      >
        <header className="evidence-drawer-header">
          <div>
            <p className="eyebrow">EVIDENCE</p>
            <h2 id="evidence-drawer-title">{m.drawer}</h2>
          </div>
          <Button
            aria-label={m.close}
            onClick={onClose}
            size="icon"
            variant="ghost"
          >
            <X size={18} />
          </Button>
        </header>
        <div className="evidence-drawer-body">
          {metadata.loading && !evidence && <p role="status">{m.loading}</p>}
          {Boolean(metadata.error) && (
            <div className="evidence-drawer-state" role="alert">
              <strong>{m[evidenceErrorKind(metadata.error)]}</strong>
              <Button onClick={metadata.reload} size="sm" variant="outline">
                <RefreshCw size={14} />
                {m.retry}
              </Button>
            </div>
          )}
          {evidence && (
            <>
              <div className="investigation-badges">
                <Badge
                  variant={
                    evidenceAvailability(evidence) === "available"
                      ? "outline"
                      : "warning"
                  }
                >
                  {m[evidenceAvailability(evidence)]}
                </Badge>
              </div>
              {related && (
                <p className="inline-alert" role="status">
                  {m.related}
                </p>
              )}
              {evidenceOutsideWindow(evidence, scope) && (
                <p className="inline-alert warning" role="status">
                  {m.outside}
                </p>
              )}
              <DefinitionGrid
                items={[
                  { label: m.source, value: evidence.source },
                  { label: m.resource, value: evidence.resource },
                  {
                    label: m.observed,
                    value: formatDateTime(evidence.observed_at),
                  },
                  {
                    label: m.freshness,
                    value: `${evidence.freshness_seconds}s`,
                  },
                  {
                    label: m.window,
                    value: `${formatDateTime(evidence.time_range.start)} → ${formatDateTime(evidence.time_range.end)}`,
                  },
                  {
                    label: m.sensitivity,
                    value: `${evidence.sensitivity} / ${evidence.exposure}`,
                  },
                ]}
              />
              <p
                className={`evidence-validity-note ${freshness === "stale" ? "warning" : ""}`}
              >
                {freshness === "unknown"
                  ? m.unknownExpiry
                  : `${m[freshness]} · ${freshnessPolicy?.maxAgeSeconds}s · ${freshnessPolicy?.asOf}`}
              </p>
              {evidence.warnings?.length ? (
                <ul className="evidence-warning-list">
                  {evidence.warnings.map((warning, index) => (
                    <li key={index}>{warning}</li>
                  ))}
                </ul>
              ) : null}
              <details className="investigation-audit-details">
                <summary>{m.audit}</summary>
                <DefinitionGrid
                  items={[
                    {
                      label: "Evidence ID",
                      value: evidence.evidence_id,
                      mono: true,
                    },
                    {
                      label: "Content hash",
                      value: evidence.content_hash,
                      mono: true,
                    },
                    {
                      label: "Correlation",
                      value: evidence.correlation_id,
                      mono: true,
                    },
                    { label: "Query ID", value: evidence.query_id, mono: true },
                    {
                      label: "Schema",
                      value: `${evidence.schema.family}.v${evidence.schema.major}.${evidence.schema.minor}`,
                      mono: true,
                    },
                    { label: "Storage", value: evidence.storage },
                    ...(access.kind === "revision_evidence"
                      ? [
                          {
                            label: "Revision",
                            value: access.revisionId,
                            mono: true,
                          },
                        ]
                      : []),
                    ...(freshnessPolicy
                      ? [{ label: m.expiry, value: freshnessPolicy.sourceRef }]
                      : []),
                  ]}
                />
              </details>
              <section
                aria-label={m.content}
                className="evidence-content-section"
              >
                <h3>
                  <FileJson aria-hidden="true" size={16} />
                  {m.content}
                </h3>
                <p>{m.redaction}</p>
                {!canRead ? (
                  <p>{m.noContent}</p>
                ) : (
                  <Button
                    disabled={requested === contentKey && content.loading}
                    onClick={() => {
                      if (requested === contentKey) content.reload();
                      else setRequested(contentKey);
                    }}
                    size="sm"
                    variant="outline"
                  >
                    {requested === contentKey && content.loading
                      ? m.reading
                      : m.read}
                  </Button>
                )}
                {requested === contentKey && Boolean(content.error) && (
                  <p role="alert">{m[evidenceErrorKind(content.error)]}</p>
                )}
                {requested === contentKey && !content.error && content.data && (
                  <pre>{content.data}</pre>
                )}
              </section>
            </>
          )}
        </div>
      </section>
    </div>,
    document.body,
  );
}
