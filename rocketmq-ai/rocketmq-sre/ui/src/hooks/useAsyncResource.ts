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
  const [result, setResult] = useState<{
    owner: typeof load;
    version: number;
    data?: T;
    error?: unknown;
    loading: boolean;
  }>();

  useEffect(() => {
    const controller = new AbortController();
    setResult((previous) => ({
      owner: load,
      version,
      loading: true,
      data: previous?.owner === load ? previous.data : undefined,
    }));
    void load(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) {
          setResult({ owner: load, version, data: value, loading: false });
        }
      })
      .catch((reason: unknown) => {
        if (
          !controller.signal.aborted &&
          !(reason instanceof DOMException && reason.name === "AbortError")
        ) {
          setResult((previous) => ({
            owner: load,
            version,
            error: reason,
            loading: false,
            data: previous?.owner === load ? previous.data : undefined,
          }));
        }
      });
    return () => controller.abort();
  }, [load, version]);

  const reload = useCallback(() => {
    setVersion((current) => current + 1);
  }, []);

  // A changed request must hide the previous result during render, before effects run.
  const current = result?.owner === load ? result : undefined;
  return {
    data: current?.data,
    loading: current?.version === version ? current.loading : true,
    error: current?.error,
    reload,
  };
}
