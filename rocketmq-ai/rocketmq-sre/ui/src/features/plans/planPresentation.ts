export function shortDigest(value: string) {
  return value.length > 20 ? `${value.slice(0, 12)}…${value.slice(-7)}` : value;
}

export function formatTimestamp(value: string | null | undefined) {
  return value ? new Date(value).toLocaleString("zh-CN", { hour12: false }) : "—";
}
