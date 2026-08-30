const SENSITIVE_KEY = /(token|secret|password|credential|private|access.?key|tls|acl)/i;

export function sanitizePlanParameters(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.slice(0, 32).map(sanitizePlanParameters);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .slice(0, 64)
        .map(([key, item]) => [
          key,
          SENSITIVE_KEY.test(key) ? "[REDACTED]" : sanitizePlanParameters(item),
        ]),
    );
  }
  return value;
}

export function shortDigest(value: string) {
  return value.length > 20 ? `${value.slice(0, 12)}…${value.slice(-7)}` : value;
}

export function formatTimestamp(value: string | null | undefined) {
  return value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
}
