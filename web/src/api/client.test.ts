import { describe, expect, it, vi } from "vitest";
import { ApiError, createClient } from "./client";
import { numText, obj } from "./json";

function res(status: number, body: string) {
  return new Response(body, { status, headers: { "content-type": "application/json" } });
}

describe("ApiClient", () => {
  it("sends bearer only to same-origin /v1 paths with no-store and omit credentials", async () => {
    const f = vi.fn(async () => res(200, '{"revision":12345678901234567890}'));
    const c = createClient("tok-123", f);
    const v = await c.get("/v1/pipelines/a/status");
    expect(numText(obj(v)!.revision)).toBe("12345678901234567890");
    const [url, init] = f.mock.calls[0]! as unknown as [string, RequestInit];
    expect(url).toBe("/v1/pipelines/a/status");
    expect((init.headers as Record<string, string>).Authorization).toBe("Bearer tok-123");
    expect(init.credentials).toBe("omit");
    expect(init.cache).toBe("no-store");
    expect(init.redirect).toBe("error");
  });
  it("refuses absolute or foreign URLs (token never leaves the origin)", async () => {
    const f = vi.fn(async () => res(200, "{}"));
    const c = createClient("tok", f);
    for (const p of ["https://evil.example/v1/x", "//evil/v1", "/other", "/v1//x"]) {
      await expect(c.get(p)).rejects.toBeInstanceOf(ApiError);
    }
    expect(f).not.toHaveBeenCalled();
  });
  it.each([
    [401, "unauthorized"], [403, "forbidden"], [404, "not_found"], [429, "rate_limited"], [503, "unavailable"], [500, "server"],
  ])("maps HTTP %i to %s and parses error envelope", async (status, kind) => {
    const c = createClient("tok", async () => res(status, '{"error":{"code":"policy_denied","message":"forbidden: role viewer","retryable":false}}'));
    const e = (await c.get("/v1/x").catch((x) => x)) as ApiError;
    expect(e.kind).toBe(kind);
    expect(e.status).toBe(status);
    expect(e.code).toBe("policy_denied");
  });
  it("network failure is 'offline', never a success", async () => {
    const c = createClient("tok", async () => { throw new TypeError("Failed to fetch"); });
    await expect(c.get("/v1/x")).rejects.toMatchObject({ kind: "offline" });
  });
  it("error messages never contain the token", async () => {
    const c = createClient("SECRET-TOKEN-XYZ", async () => res(500, "not json SECRET-TOKEN-XYZ"));
    const e = (await c.get("/v1/x").catch((x) => x)) as ApiError;
    expect(e.message).not.toContain("SECRET-TOKEN-XYZ");
  });
  it("serialises request bodies losslessly", async () => {
    const f = vi.fn(async (_u: string, _i: RequestInit) => res(200, "{}"));
    const c = createClient("t", f);
    const { parseJson } = await import("./json");
    await c.send("PUT", "/v1/x", parseJson('{"expected_revision":18446744073709551615}'));
    expect(f.mock.calls[0]![1].body).toBe('{"expected_revision":18446744073709551615}');
  });
});
