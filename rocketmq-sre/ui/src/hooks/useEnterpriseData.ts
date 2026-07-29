import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import {
  loadEnterpriseSnapshot,
  type EnterpriseQuery,
} from "@/api/enterprise";
import type { EnterpriseSnapshot } from "@/api/enterpriseTypes";
import { useAuth } from "@/auth/AuthContext";
import { useSreData } from "@/data/SreDataContext";

const QUERY_KEYS = [
  "region",
  "environment",
  "owner",
  "health",
  "q",
  "page",
] as const;

export interface EnterpriseFilters {
  region: string;
  environment: string;
  owner: string;
  health: string;
  search: string;
  page: number;
}

export function useEnterpriseData() {
  const auth = useAuth();
  const { demoMode } = useSreData();
  const [searchParams, setSearchParams] = useSearchParams();
  const [data, setData] = useState<EnterpriseSnapshot>();
  const [error, setError] = useState<unknown>();
  const [loading, setLoading] = useState(true);
  const [reloadRevision, setReloadRevision] = useState(0);
  const filters = useMemo(
    () => filtersFromSearch(searchParams),
    [searchParams],
  );
  const query = useMemo<EnterpriseQuery>(
    () => ({
      regionId: emptyToUndefined(filters.region),
      environment: emptyToUndefined(filters.environment),
      owner: emptyToUndefined(filters.owner),
      health: emptyToUndefined(filters.health),
    }),
    [filters.environment, filters.health, filters.owner, filters.region],
  );

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void loadEnterpriseSnapshot(
      auth.requestContext,
      query,
      demoMode,
      controller.signal,
    )
      .then((snapshot) => {
        setData(snapshot);
        setLoading(false);
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          setError(reason);
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [auth.requestContext, demoMode, query, reloadRevision]);

  const setFilter = useCallback(
    (key: keyof EnterpriseFilters, value: string | number) => {
      const next = new URLSearchParams(searchParams);
      const urlKey = key === "search" ? "q" : key;
      const normalized = String(value);
      if (
        normalized.length === 0 ||
        (urlKey === "page" && normalized === "1")
      ) {
        next.delete(urlKey);
      } else {
        next.set(urlKey, normalized);
      }
      if (urlKey !== "page") {
        next.delete("page");
      }
      setSearchParams(next, { replace: true });
    },
    [searchParams, setSearchParams],
  );

  const resetFilters = useCallback(() => {
    const next = new URLSearchParams(searchParams);
    QUERY_KEYS.forEach((key) => next.delete(key));
    setSearchParams(next, { replace: true });
  }, [searchParams, setSearchParams]);

  return {
    data,
    error,
    loading,
    filters,
    setFilter,
    resetFilters,
    reload: () => setReloadRevision((revision) => revision + 1),
  };
}

export function useVirtualRows(
  itemCount: number,
  rowHeight: number,
  viewportHeight: number,
  overscan = 5,
) {
  const [scrollTop, setScrollTop] = useState(0);
  const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const visibleCount = Math.ceil(viewportHeight / rowHeight) + overscan * 2;
  const end = Math.min(itemCount, start + visibleCount);

  return {
    start,
    end,
    totalHeight: itemCount * rowHeight,
    offsetTop: start * rowHeight,
    onScroll: (event: React.UIEvent<HTMLElement>) =>
      setScrollTop(event.currentTarget.scrollTop),
  };
}

export function exportCsv(
  filename: string,
  headers: string[],
  rows: Array<Array<string | number | boolean | undefined>>,
) {
  const content = [headers, ...rows]
    .map((row) => row.map(csvCell).join(","))
    .join("\r\n");
  const blob = new Blob([`\uFEFF${content}`], {
    type: "text/csv;charset=utf-8",
  });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function filtersFromSearch(searchParams: URLSearchParams): EnterpriseFilters {
  const requestedPage = Number(searchParams.get("page") ?? "1");
  return {
    region: searchParams.get("region") ?? "",
    environment: searchParams.get("environment") ?? "",
    owner: searchParams.get("owner") ?? "",
    health: searchParams.get("health") ?? "",
    search: searchParams.get("q") ?? "",
    page:
      Number.isSafeInteger(requestedPage) && requestedPage > 0
        ? requestedPage
        : 1,
  };
}

function emptyToUndefined(value: string) {
  return value.length === 0 ? undefined : value;
}

function csvCell(value: string | number | boolean | undefined) {
  const text = value === undefined ? "" : String(value);
  return `"${text.replaceAll('"', '""')}"`;
}
