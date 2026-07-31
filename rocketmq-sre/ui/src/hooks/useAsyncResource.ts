import { useCallback, useEffect, useState } from "react";

export interface AsyncResource<T> {
  data?: T;
  loading: boolean;
  error?: unknown;
  reload: () => void;
}

export function useAsyncResource<T>(
  load: (signal: AbortSignal) => Promise<T>,
): AsyncResource<T> {
  const [version, setVersion] = useState(0);
  const [data, setData] = useState<T>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>();

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void load(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) {
          setData(value);
        }
      })
      .catch((reason: unknown) => {
        if (
          !controller.signal.aborted &&
          !(reason instanceof DOMException && reason.name === "AbortError")
        ) {
          setError(reason);
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      });
    return () => controller.abort();
  }, [load, version]);

  const reload = useCallback(() => {
    setVersion((current) => current + 1);
  }, []);

  return { data, loading, error, reload };
}
