import { useCallback, useEffect, useRef, useState } from "react";
import { Poller, type PollState } from "./poll";
import { useAuth } from "../auth/AuthContext";
import type { ApiClient } from "./client";

const INITIAL = { data: null, error: null, updatedAt: null, loading: true, failures: 0, paused: false };

/** Poll while mounted; key change (e.g. navigation) restarts and aborts. */
export function usePoll<T>(load: (c: ApiClient, s: AbortSignal) => Promise<T>, key: string, intervalMs = 5000) {
  const { client, expire } = useAuth();
  const [state, setState] = useState<PollState<T>>(INITIAL);
  const poller = useRef<Poller<T> | null>(null);
  const loadRef = useRef(load);
  loadRef.current = load;

  useEffect(() => {
    if (!client) return;
    setState(INITIAL);
    const p = new Poller<T>({
      load: (s) => loadRef.current(client, s),
      intervalMs,
      isHidden: () => document.visibilityState === "hidden",
      onChange: (s) => {
        setState(s);
        if (s.error?.kind === "unauthorized") expire();
      },
    });
    poller.current = p;
    const onVis = () => p.visibilityChanged();
    document.addEventListener("visibilitychange", onVis);
    p.start();
    return () => {
      document.removeEventListener("visibilitychange", onVis);
      p.stop();
      poller.current = null;
    };
  }, [client, key, intervalMs, expire]);

  const refresh = useCallback(() => poller.current?.refresh(), []);
  return [state, refresh] as const;
}
