import { createHash } from "node:crypto";

export const PORT = Number(process.env.K5_PORT ?? "43991");
export const BASE = `http://127.0.0.1:${PORT}`;
// Test-only tokens for an ephemeral in-memory catalog.
export const TOKENS = {
  viewer: "e2e-viewer-token-0000000001",
  operator: "e2e-operator-token-00000002",
  admin: "e2e-admin-token-000000000003",
};
export const SECRET_VALUE = "e2e-secret-value-DO-NOT-LEAK-77";
export const SQL_SENTINEL = "e2e_sql_sentinel_4412";

export const sha = (t: string) => createHash("sha256").update(t).digest("hex");

export async function api(method: string, path: string, token: string, body?: unknown) {
  const res = await fetch(BASE + path, {
    method,
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, text: await res.text(), headers: res.headers };
}
