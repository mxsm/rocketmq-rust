import { useCallback, useEffect, useRef, useState } from "react";

export interface AsyncResource<T> {
  data?: T;
  loading: boolean;
  error?: unknown;
  reload: () => Promise<void>;
}

export function useAsyncResource<T>(
  load: (signal: AbortSignal) => Promise<T>,
): AsyncResource<T> {
  const [version, setVersion] = useState(0);
  const requestedVersion = useRef(0);
  const completions = useRef(new Map<number, () => void>());
  const settle = useCallback((through = Infinity) => {
    for (const [requested, complete] of completions.current) {
      if (requested <= through) {
        completions.current.delete(requested);
        complete();
      }
    }
  }, []);
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
      })
      .finally(() => settle(version));
    return () => {
      controller.abort();
      settle(version);
    };
  }, [load, settle, version]);

  useEffect(() => () => settle(), [settle]);

  const reload = useCallback(() => {
    const requested = ++requestedVersion.current;
    const completion = new Promise<void>((resolve) => {
      completions.current.set(requested, resolve);
    });
    setVersion(requested);
    return completion;
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
