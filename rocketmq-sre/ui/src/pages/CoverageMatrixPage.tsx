import {
  CircleAlert,
  Info,
  RefreshCw,
  ShieldCheck,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";

import type {
  CoverageCellStatus,
  CoverageMatrix,
  CoverageRequirement,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import {
  CoverageBadge,
  coverageLabel,
} from "@/components/StatusBadge";
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

const statuses: CoverageCellStatus[] = [
  "queryable",
  "implemented_local",
  "in_process_only",
  "missing_instrumentation",
  "not_production_verified",
];

export function CoverageMatrixPage() {
  const { coverage } = useSreData();
  const [matrix, setMatrix] = useState<CoverageMatrix>();
  const [componentFilter, setComponentFilter] = useState("all");
  const [selected, setSelected] = useState<{
    component: string;
    pack: string;
    status: CoverageCellStatus;
  }>();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string>();

  const load = useCallback(async () => {
    setLoading(true);
    setError(undefined);
    try {
      const value = await coverage();
      setMatrix(value);
      setSelected(value.selected);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "覆盖度清单暂不可用",
      );
    } finally {
      setLoading(false);
    }
  }, [coverage]);

  useEffect(() => {
    void load();
  }, [load]);

  const rows = useMemo(
    () =>
      matrix?.rows.filter(
        (row) =>
          componentFilter === "all" || row.component === componentFilter,
      ) ?? [],
    [componentFilter, matrix],
  );
  const selectedRequirements =
    matrix &&
    selected?.component === matrix.selected.component &&
    selected.pack === matrix.selected.pack
      ? matrix.selected.requirements
      : [];

  return (
    <div className="page coverage-page">
      <PageHeader
        eyebrow="REQUIRED SIGNALS"
        title="证据覆盖"
        description="基于语义注册表评估组件与诊断包的可查询性；未收集数据不视为 0。"
        actions={
          <>
            <Select value={componentFilter} onValueChange={setComponentFilter}>
              <SelectTrigger aria-label="组件筛选">
                <SelectValue placeholder="全部组件" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部组件</SelectItem>
                {matrix?.rows.map((row) => (
                  <SelectItem key={row.component} value={row.component}>
                    {row.component}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button disabled={loading} onClick={() => void load()}>
              <RefreshCw
                aria-hidden="true"
                className={loading ? "spin" : undefined}
                size={15}
              />
              重新检查覆盖
            </Button>
          </>
        }
      />

      <div className="coverage-truth">
        <Info aria-hidden="true" size={15} />
        <span>
          语义来源：
          <strong>{matrix?.semanticSignalCount ?? "未加载"} 个信号</strong> /{" "}
          <strong>{matrix?.semanticOwnerCount ?? "未加载"} 位 owner</strong>
        </span>
        <span className="coverage-truth-boundary">
          Phase 00 · execution_supported=false
        </span>
      </div>

      {error && (
        <div className="inline-alert warning">
          <CircleAlert aria-hidden="true" size={16} />
          {error}
        </div>
      )}

      <section className="coverage-matrix-surface">
        <div className="table-scroll">
          <table className="coverage-matrix">
            <thead>
              <tr>
                <th>组件 / 诊断包</th>
                {matrix?.packs.map((pack) => (
                  <th key={pack.id}>{pack.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.component}>
                  <th scope="row">{row.component}</th>
                  {matrix?.packs.map((pack) => {
                    const status =
                      row.cells[pack.id] ?? "not_production_verified";
                    const isSelected =
                      selected?.component === row.component &&
                      selected.pack === pack.id;
                    return (
                      <td key={pack.id}>
                        <button
                          aria-pressed={isSelected}
                          className={`coverage-cell ${
                            isSelected ? "selected" : ""
                          }`}
                          onClick={() =>
                            setSelected({
                              component: row.component,
                              pack: pack.id,
                              status,
                            })
                          }
                          type="button"
                        >
                          <CoverageBadge status={status} />
                        </button>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {rows.length === 0 && (
          <div className="state-message">没有匹配的组件覆盖记录。</div>
        )}
        <div className="coverage-legend">
          {statuses.map((status) => (
            <span key={status}>
              <CoverageBadge status={status} />
              <small>{legendDescription(status)}</small>
            </span>
          ))}
        </div>
      </section>

      {selected && (
        <section className="coverage-detail">
          <div className="coverage-detail-heading">
            <div>
              <h2>
                证据项详情 · {selected.component} ×{" "}
                {matrix?.packs.find((pack) => pack.id === selected.pack)
                  ?.label ?? selected.pack}
              </h2>
              <CoverageBadge status={selected.status} />
            </div>
            <span>
              检查时间：
              {matrix
                ? new Date(matrix.generatedAt).toLocaleString("zh-CN", {
                    hour12: false,
                    timeZone: "Asia/Shanghai",
                  })
                : "未加载"}
            </span>
          </div>
          {selectedRequirements.length === 0 ? (
            <div className="coverage-no-detail">
              <CircleAlert aria-hidden="true" size={17} />
              当前矩阵单元没有提交 requirement 明细；状态保持为
              “{coverageLabel(selected.status)}”，不推断缺失字段。
            </div>
          ) : (
            <div className="coverage-detail-grid">
              <div className="requirement-list">
                {selectedRequirements.map((requirement) => (
                  <RequirementSummary
                    key={requirement.id}
                    requirement={requirement}
                  />
                ))}
              </div>
              <div className="field-map">
                <h3>Evidence 字段映射</h3>
                <table>
                  <thead>
                    <tr>
                      <th>Requirement</th>
                      <th>Evidence 字段</th>
                      <th>类型</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedRequirements.map((requirement) => (
                      <tr key={requirement.id}>
                        <td>{requirement.id}</td>
                        <td>{requirement.evidenceField}</td>
                        <td>{requirement.signalType}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <div className="field-map-note">
                  <ShieldCheck aria-hidden="true" size={15} />
                  字段与语义注册表保持一致，路径为逻辑标识而非存储地址。
                </div>
              </div>
            </div>
          )}
        </section>
      )}
    </div>
  );
}

function RequirementSummary({
  requirement,
}: {
  requirement: CoverageRequirement;
}) {
  return (
    <article className="requirement-summary">
      <div className="requirement-title">
        <strong>{requirement.id}</strong>
        <Badge variant="outline">{requirement.signalType}</Badge>
      </div>
      <p>{requirement.purpose}</p>
      <dl>
        <div>
          <dt>语义注册表引用</dt>
          <dd>{requirement.registryReference}</dd>
        </div>
        <div>
          <dt>新鲜度</dt>
          <dd>{requirement.freshness}</dd>
        </div>
        <div>
          <dt>期望属性</dt>
          <dd>{requirement.expectedAttributes.join(", ") || "无"}</dd>
        </div>
        <div>
          <dt>敏感性</dt>
          <dd>{requirement.sensitivity}</dd>
        </div>
        <div>
          <dt>缺失行为</dt>
          <dd>{requirement.missingBehavior}</dd>
        </div>
        <div>
          <dt>Owner</dt>
          <dd>{requirement.owner}</dd>
        </div>
      </dl>
    </article>
  );
}

function legendDescription(status: CoverageCellStatus) {
  switch (status) {
    case "queryable":
      return "可通过受保护查询用于诊断";
    case "implemented_local":
      return "组件内已实现，尚未统一暴露";
    case "in_process_only":
      return "仅进程内可获取";
    case "missing_instrumentation":
      return "语义信号尚未采集或落地";
    case "not_production_verified":
      return "实现存在但未完成生产验证";
  }
}
