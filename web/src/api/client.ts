// API client. The bearer token lives only in this closure (memory). It is
// never written to URLs, Web Storage, logs or error messages, and requests
// only go to same-origin relative `/v1` paths (no configurable base URL).
import { parseJson, stringifyJson, obj, str, type Json } from "./json";

export type ApiErrorKind =
  | "unauthorized" // 401: token missing/invalid
  | "forbidden" // 403: authenticated but role not permitted
  | "not_found"
  | "rate_limited" // 429
  | "unavailable" // 503 draining / 504 deadline
  | "offline" // network failure, no HTTP response
  | "aborted"
  | "bad_response"
  | "server";

export class ApiError extends Error {
  constructor(
    readonly kind: ApiErrorKind,
    readonly status: number | null,
    readonly code: string | null,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export function kindForStatus(status: number): ApiErrorKind {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 429) return "rate_limited";
  if (status === 503 || status === 504) return "unavailable";
  return "server";
}

export type Fetcher = (input: string, init: RequestInit) => Promise<Response>;

export interface ApiClient {
  get(path: string, signal?: AbortSignal): Promise<Json>;
  getText(path: string, signal?: AbortSignal): Promise<string>;
  send(method: "POST" | "PUT", path: string, body: unknown, signal?: AbortSignal): Promise<Json>;
}

export function createClient(token: string, fetcher: Fetcher = (i, n) => fetch(i, n)): ApiClient {
  async function raw(method: string, path: string, body: unknown, signal?: AbortSignal): Promise<string> {
    if (!path.startsWith("/v1/") || path.includes("//") || /[\s]/.test(path)) {
      throw new ApiError("bad_response", null, null, "非法的 API 路径", false);
    }
    const headers: Record<string, string> = { Authorization: `Bearer ${token}`, Accept: "application/json" };
    let payload: string | undefined;
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      payload = stringifyJson(body);
    }
    let res: Response;
    try {
      res = await fetcher(path, {
        method,
        headers,
        body: payload,
        signal,
        credentials: "omit",
        cache: "no-store",
        redirect: "error",
        referrerPolicy: "no-referrer",
      });
    } catch (e) {
      if (signal?.aborted || (e instanceof DOMException && e.name === "AbortError")) {
        throw new ApiError("aborted", null, null, "请求已取消", false);
      }
      throw new ApiError("offline", null, null, "无法连接到 Sparrow 服务", true);
    }
    const text = await res.text();
    if (!res.ok) {
      let code: string | null = null;
      let message = `HTTP ${res.status}`;
      let retryable = res.status === 429 || res.status >= 500;
      try {
        const err = obj(obj(parseJson(text))?.error);
        code = str(err?.code);
        message = str(err?.message) ?? message;
        if (typeof err?.retryable === "boolean") retryable = err.retryable;
      } catch {
        /* non-JSON error body: keep status only, never echo raw text */
      }
      throw new ApiError(kindForStatus(res.status), res.status, code, message, retryable);
    }
    return text;
  }
  async function json(method: string, path: string, body: unknown, signal?: AbortSignal): Promise<Json> {
    const text = await raw(method, path, body, signal);
    if (text === "") return null;
    try {
      return parseJson(text);
    } catch {
      throw new ApiError("bad_response", null, null, "服务返回了无法解析的 JSON", false);
    }
  }
  return {
    get: (p, s) => json("GET", p, undefined, s),
    getText: (p, s) => raw("GET", p, undefined, s),
    send: (m, p, b, s) => json(m, p, b, s),
  };
}

export const enc = encodeURIComponent;
