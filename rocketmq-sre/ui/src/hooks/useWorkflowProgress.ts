import { useEffect, useState } from "react";

import type { WorkflowStreamEvent } from "@/api/types";
import { useSreData } from "@/data/SreDataContext";

export type ProgressTransport = "connecting" | "sse" | "polling";

export function useWorkflowProgress(
  clusterId: string,
  poll?: () => void | Promise<void>,
) {
  const { api } = useSreData();
  const [transport, setTransport] =
    useState<ProgressTransport>("connecting");
  const [events, setEvents] = useState<WorkflowStreamEvent[]>([]);

  useEffect(() => {
    const controller = new AbortController();
    let timer: number | undefined;

    const fallback = () => {
      if (controller.signal.aborted) {
        return;
      }
      setTransport("polling");
      const run = () => {
        if (poll) {
          void Promise.resolve(poll()).catch(() => undefined);
        }
      };
      run();
      timer = window.setInterval(run, 10_000);
    };

    void api
      .subscribeWorkflowEvents((event) => {
        if (!clusterId || event.cluster_id === clusterId) {
          setTransport("sse");
          setEvents((current) => [event, ...current].slice(0, 8));
        }
      }, controller.signal)
      .then(() => {
        if (!controller.signal.aborted) {
          fallback();
        }
      })
      .catch(() => fallback());

    return () => {
      controller.abort();
      if (timer !== undefined) {
        window.clearInterval(timer);
      }
    };
  }, [api, clusterId, poll]);

  return { transport, events };
}
