import { ShieldCheck } from "lucide-react";

export function ReadOnlyBoundary({ compact = false }: { compact?: boolean }) {
  return (
    <section className={compact ? "readonly-boundary compact" : "readonly-boundary"}>
      <ShieldCheck aria-hidden="true" size={17} />
      <div>
        <strong>安全声明（只读）</strong>
        {!compact && (
          <p>
            仅提供只读分析和可观测性视图；无 Apply、审批、自动变更或集群
            mutation 通道。
          </p>
        )}
      </div>
      <code>mutation_supported=false</code>
    </section>
  );
}
