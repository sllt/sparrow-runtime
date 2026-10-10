// API client. The bearer token lives only in this closure (memory). It is
// never written to URLs, Web Storage, logs or error messages, and requests
// only go to same-origin relative `/v1` paths (no configurable base URL).
import { parseJson, stringifyJson, obj, str, type Json } from "./json";

export type ApiErrorKind =
  | "unauthorized" // 401: token missing/invalid
  | "forbidden" // 403: authenticated but role not permitted
  | "not_found"
  | "conflict" // 409/412/428: ETag/CAS precondition failed — reload, keep local edits
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
    readonly context: Record<string, string> = {},
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export function kindForStatus(status: number): ApiErrorKind {
  if (status === 401) return "unauthorized";
  if (status === 403) return "forbidden";
  if (status === 404) return "not_found";
  if (status === 409 || status === 412 || status === 428) return "conflict";
  if (status === 429) return "rate_limited";
  if (status === 503 || status === 504) return "unavailable";
  return "server";
}

export type Fetcher = (input: string, init: RequestInit) => Promise<Response>;

export interface ApiClient {
  get(path: string, signal?: AbortSignal): Promise<Json>;
  getText(path: string, signal?: AbortSignal): Promise<string>;
  send(method: "POST" | "PUT", path: string, body: unknown, signal?: AbortSignal): Promise<Json>;
  /** Full request with conditional headers; returns the response ETag. */
  request(method: "GET" | "POST" | "PUT" | "DELETE", path: string, opts?: { body?: unknown; ifMatch?: string; signal?: AbortSignal }): Promise<{ status: number; etag: string | null; data: Json }>;
}

export function createClient(token: string, fetcher: Fetcher = (i, n) => fetch(i, n)): ApiClient {
  async function raw(method: string, path: string, body: unknown, signal?: AbortSignal, ifMatch?: string, meta?: { status: number; etag: string | null }): Promise<string> {
    if (!path.startsWith("/v1/") || path.includes("//") || /[\s]/.test(path)) {
      throw new ApiError("bad_response", null, null, "非法的 API 路径", false);
    }
    const headers: Record<string, string> = { Authorization: `Bearer ${token}`, Accept: "application/json" };
    if (ifMatch !== undefined) headers["If-Match"] = `"${ifMatch.replace(/"/g, "")}"`;
    let payload: string | Uint8Array | undefined;
    if (body instanceof Uint8Array) {
      // Opaque package bytes (plugin install); the server enforces size/signature.
      headers["Content-Type"] = "application/octet-stream";
      payload = body;
    } else if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      payload = stringifyJson(body);
    }
    let res: Response;
    try {
      res = await fetcher(path, {
        method,
        headers,
        body: payload as BodyInit | undefined,
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
    if (meta) { meta.status = res.status; meta.etag = res.headers.get("ETag")?.replace(/"/g, "") ?? null; }
    if (!res.ok) {
      let code: string | null = null;
      const context: Record<string, string> = {};
      let message = `HTTP ${res.status}`;
      let retryable = res.status === 429 || res.status >= 500;
      try {
        const err = obj(obj(parseJson(text))?.error);
        code = str(err?.code);
        message = str(err?.message) ?? message;
        if (typeof err?.retryable === "boolean") retryable = err.retryable;
        const ctx = err?.context;
        if (Array.isArray(ctx)) for (const c of ctx) { const o = obj(c); const k = str(o?.key); const v = str(o?.value); if (k && v !== null) context[k] = v; }
      } catch {
        /* non-JSON error body: keep status only, never echo raw text */
      }
      throw new ApiError(kindForStatus(res.status), res.status, code, message, retryable, context);
    }
    return text;
  }
  async function json(method: string, path: string, body: unknown, signal?: AbortSignal, ifMatch?: string, meta?: { status: number; etag: string | null }): Promise<Json> {
    const text = await raw(method, path, body, signal, ifMatch, meta);
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
    request: async (m, p, o = {}) => {
      const meta = { status: 0, etag: null as string | null };
      const data = await json(m, p, o.body, o.signal, o.ifMatch, meta);
      return { status: meta.status, etag: meta.etag, data };
    },
  };
}

export const enc = encodeURIComponent;
