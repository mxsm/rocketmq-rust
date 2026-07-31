import type { DiagnosisRevision } from "@/api/types";
import { Badge } from "@/components/ui/badge";

import { diagnosisAttribution } from "./incidentPresentation";

export function DiagnosisRevisionList({
  revisions,
}: {
  revisions: DiagnosisRevision[];
}) {
  if (revisions.length === 0) {
    return <div className="state-message">尚无诊断 revision。</div>;
  }

  return (
    <div className="diagnosis-list">
      {revisions.map((revision) => {
        const attribution = diagnosisAttribution(revision);
        return (
          <article key={revision.id}>
            <header>
              <strong>Revision {revision.revision}</strong>
              <Badge
                variant={revision.partial ? "warning" : "success"}
              >
                {revision.partial ? "partial" : "complete"}
              </Badge>
            </header>
            <dl className="diagnosis-attribution-grid">
              <div>
                <dt>DiagnosticPack</dt>
                <dd>{attribution.pack}</dd>
              </div>
              <div>
                <dt>Pack version</dt>
                <dd>{attribution.version}</dd>
              </div>
              <div>
                <dt>诊断模式</dt>
                <dd>{attribution.mode}</dd>
              </div>
              <div>
                <dt>Provider</dt>
                <dd>{attribution.provider}</dd>
              </div>
              <div>
                <dt>Model invocation</dt>
                <dd>{attribution.model}</dd>
              </div>
            </dl>
            <div className="hypothesis-list">
              {revision.hypotheses.map((hypothesis) => (
                <div key={hypothesis.title}>
                  <span>{hypothesis.title}</span>
                  <strong>
                    {Math.round(hypothesis.confidence * 100)}%
                  </strong>
                  <Badge
                    variant={
                      hypothesis.status === "supported"
                        ? "success"
                        : hypothesis.status === "contradicted"
                          ? "secondary"
                          : "outline"
                    }
                  >
                    {hypothesis.status}
                  </Badge>
                </div>
              ))}
            </div>
            <div className="diagnosis-evidence-summary">
              <div>
                <span>Evidence 引用</span>
                <p>
                  {revision.evidence_ids.join(" · ") || "没有可引用证据"}
                </p>
              </div>
              <div>
                <span>反证</span>
                <p>
                  {revision.hypotheses
                    .filter(
                      (hypothesis) =>
                        hypothesis.status === "contradicted",
                    )
                    .map((hypothesis) => hypothesis.title)
                    .join(" · ") || "未记录反证"}
                </p>
              </div>
              <div>
                <span>缺失证据</span>
                <p>
                  {attribution.missingEvidence.join(" · ") || "无"}
                </p>
              </div>
            </div>
            <footer>
              <span>{revision.evidence_ids.length} Evidence 引用</span>
              <code>execution_eligible=false</code>
            </footer>
          </article>
        );
      })}
    </div>
  );
}
