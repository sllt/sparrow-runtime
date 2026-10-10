import { useCallback, useEffect, useRef, useState } from "react";
import { useAuth } from "../auth/AuthContext";
import { ApiError, type ApiClient } from "./client";

export interface LoadState<T> { data: T | null; error: ApiError | null; loading: boolean }

/** One-shot load (editors/forms are not polled). Aborts on key change/unmount. */
export function useLoad<T>(load: (c: ApiClient, s: AbortSignal) => Promise<T>, key: string) {
  const { client, expire } = useAuth();
  const [state, setState] = useState<LoadState<T>>({ data: null, error: null, loading: true });
  const ref = useRef(load);
  ref.current = load;
  const [n, setN] = useState(0);
  useEffect(() => {
    if (!client) return;
    const ac = new AbortController();
    setState((s) => ({ ...s, loading: true }));
    ref.current(client, ac.signal).then(
      (data) => setState({ data, error: null, loading: false }),
      (e) => {
        if (ac.signal.aborted) return;
        const err = e instanceof ApiError ? e : new ApiError("bad_response", null, null, String(e), false);
        if (err.kind === "unauthorized") expire();
        setState((s) => ({ data: s.data, error: err, loading: false }));
      },
    );
    return () => ac.abort();
  }, [client, key, n, expire]);
  const reload = useCallback(() => setN((x) => x + 1), []);
  return [state, reload, setState] as const;
}

export function asApiError(e: unknown): ApiError {
  return e instanceof ApiError ? e : new ApiError("bad_response", null, null, e instanceof Error ? e.message : String(e), false);
}

export function opId(): string {
  const r = crypto.getRandomValues(new Uint8Array(12));
  return "op-" + Array.from(r, (b) => b.toString(16).padStart(2, "0")).join("");
}
