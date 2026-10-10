import { ApiError, enc, type ApiClient } from "./client";
import { obj, type Json } from "./json";

export const MAX_OVERVIEW = 50;
const CONCURRENCY = 4;

export interface Overview {
  names: string[];
  truncated: boolean;
  status: Record<string, { body: Json } | { error: ApiError }>;
  at: number;
}

/** One bounded sweep: list, then at most 50 status reads with concurrency 4. */
export async function loadOverview(c: ApiClient, signal: AbortSignal, now = Date.now): Promise<Overview> {
  const list = obj(await c.get("/v1/pipelines", signal));
  const all = Array.isArray(list?.pipelines) ? list!.pipelines.filter((n): n is string => typeof n === "string") : [];
  const names = all.slice(0, MAX_OVERVIEW);
  const status: Overview["status"] = {};
  let i = 0;
  async function worker() {
    while (i < names.length) {
      const name = names[i++]!;
      try {
        status[name] = { body: await c.get(`/v1/pipelines/${enc(name)}/status`, signal) };
      } catch (e) {
        if (e instanceof ApiError && (e.kind === "aborted" || e.kind === "unauthorized" || e.kind === "offline")) throw e;
        status[name] = { error: e instanceof ApiError ? e : new ApiError("bad_response", null, null, String(e), false) };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, names.length) }, worker));
  return { names, truncated: all.length > names.length, status, at: now() };
}
