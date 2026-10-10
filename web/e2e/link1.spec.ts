import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { api, SECRET_VALUE, SQL_SENTINEL, TOKENS } from "./fixture";

const SHOTS = process.env.K5_SCREENS ?? "/workspace/notes/k5/screens";
mkdirSync(SHOTS, { recursive: true });

async function login(page: Page, token: string, theme: "light" | "dark" = "light") {
  await page.goto(`/ui/?theme=${theme}`);
  await page.getByLabel("访问令牌").fill(token);
  await page.getByRole("button", { name: "登录" }).click();
  await expect(page.getByRole("heading", { name: "总览" })).toBeVisible();
}
const shot = (page: Page, name: string) => page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true });

test("link1: login page, wrong token is rejected", async ({ page }) => {
  for (const theme of ["light", "dark"] as const) {
    await page.goto(`/ui/?theme=${theme}`);
    await expect(page.getByRole("heading", { name: "登录" })).toBeVisible();
    await shot(page, `01-login-${theme}`);
  }
  await page.getByLabel("访问令牌").fill("not-a-valid-token");
  await page.getByRole("button", { name: "登录" }).click();
  await expect(page.getByText("令牌无效")).toBeVisible();
  await shot(page, "02-login-401-dark");
  // token not persisted anywhere in browser storage or URL
  const storage = await page.evaluate(() => JSON.stringify({ ...localStorage }) + JSON.stringify({ ...sessionStorage }) + location.href);
  expect(storage).not.toContain("not-a-valid-token");
});

for (const theme of ["light", "dark"] as const) {
  test(`link1: viewer read-only pages (${theme})`, async ({ page }) => {
    const responses: string[] = [];
    page.on("response", async (r) => { if (r.url().includes("/v1/")) responses.push(await r.text().catch(() => "")); });
    await login(page, TOKENS.viewer, theme);
    await expect(page.getByText("只读", { exact: true })).toBeVisible();
    await expect(page.getByRole("row", { name: /sensor-alerts/ })).toBeVisible();
    await page.waitForTimeout(11_500); // second overview sample -> rates
    await shot(page, `10-dashboard-viewer-${theme}`);
    await page.keyboard.press("2");
    await expect(page.getByRole("heading", { name: "流水线" })).toBeVisible();
    await expect(page.getByRole("row", { name: /archive-draft/ })).toContainText("已停止");
    await expect(page.getByRole("row", { name: /humidity-feed/ })).toContainText("已停止");
    await shot(page, `11-pipelines-viewer-${theme}`);
    await page.getByRole("row", { name: /sensor-alerts/ }).click();
    await expect(page.getByText("安全投影")).toBeVisible();
    await page.waitForTimeout(6_000);
    await shot(page, `12-detail-overview-viewer-${theme}`);
    for (const [tab, file] of [["流量与背压", "13-detail-traffic"], ["错误", "14-detail-errors"], ["Checkpoint 风险", "15-detail-checkpoint"], ["诊断", "16-detail-diagnostics"]] as const) {
      await page.getByRole("tab", { name: tab }).click();
      await shot(page, `${file}-viewer-${theme}`);
    }
    const download = page.waitForEvent("download");
    await page.getByRole("button", { name: "下载诊断包" }).click();
    const d = await download;
    const body = await (await d.createReadStream()).toArray().then((c) => Buffer.concat(c).toString());
    expect(body).toContain("sparrow-diagnostic-v1");
    await page.getByRole("link", { name: "流水线" }).first().click();
    await page.getByRole("row", { name: /archive-draft/ }).click();
    await expect(page.getByRole("heading", { name: "archive-draft" })).toBeVisible();
    await page.getByRole("tab", { name: "流量与背压" }).click();
    await expect(page.getByText("流量观测不可用")).toBeVisible();
    await shot(page, `17-detail-no-attempt-viewer-${theme}`);
    await page.keyboard.press("3");
    await expect(page.getByRole("heading", { name: "审计摘要" })).toBeVisible();
    await shot(page, `18-audit-viewer-${theme}`);
    await page.keyboard.press("4");
    await expect(page.getByRole("heading", { name: "实例与权限" })).toBeVisible();
    await shot(page, `19-instance-viewer-${theme}`);
    await page.keyboard.press("Control+k");
    await page.getByLabel("搜索").fill("sens");
    await shot(page, `20-palette-${theme}`);
    await page.keyboard.press("Escape");
    // no secrets in anything the viewer received
    const all = responses.join("\n") + body;
    for (const s of [SECRET_VALUE, SQL_SENTINEL, TOKENS.viewer, TOKENS.admin, TOKENS.operator, "token_sha256"]) expect(all).not.toContain(s);
    const stored = await page.evaluate(() => JSON.stringify({ ...localStorage }) + JSON.stringify({ ...sessionStorage }) + location.href + document.cookie);
    expect(stored).not.toContain(TOKENS.viewer);
  });
}

test("link1: operator sees full detail; admin audit", async ({ page }) => {
  await login(page, TOKENS.operator, "light");
  await page.getByRole("row", { name: /sensor-alerts/ }).click();
  await expect(page.getByRole("tab", { name: "概览" })).toBeVisible();
  await page.waitForTimeout(6_000);
  await shot(page, "21-detail-operator-light");
  await page.getByRole("button", { name: "注销" }).click();
  await login(page, TOKENS.admin, "dark");
  await page.keyboard.press("3");
  await expect(page.getByRole("cell", { name: "put_secret" })).toBeVisible();
  await expect(page.getByText(SECRET_VALUE)).toHaveCount(0);
  await shot(page, "22-audit-admin-dark");
});

test("link1: viewer write APIs are rejected server-side", async () => {
  for (const [m, p, b] of [
    ["POST", "/v1/pipelines/sensor-alerts/stop", {}], ["POST", "/v1/pipelines/sensor-alerts/start", {}],
    ["PUT", "/v1/pipelines/sensor-alerts", {}], ["PUT", "/v1/secrets/x", { value: "y" }], ["POST", "/v1/pipelines/sensor-alerts/kill", {}],
    ["GET", "/v1/pipelines/sensor-alerts", undefined], ["POST", "/v1/validate", {}],
  ] as const) {
    const r = await api(m, p, TOKENS.viewer, b);
    expect(r.status, `${m} ${p}`).toBe(403);
    expect(r.headers.get("cache-control")).toBe("no-store");
  }
  const op = await api("POST", "/v1/pipelines/sensor-alerts/kill", TOKENS.operator, {});
  expect(op.status).toBe(403);
  const st = await api("GET", "/v1/pipelines/sensor-alerts/status", TOKENS.viewer);
  expect(st.status).toBe(200);
  expect(st.text).not.toContain(SQL_SENTINEL);
  const ui = await fetch(`${process.env.K5_BASE ?? "http://127.0.0.1:" + (process.env.K5_PORT ?? "43991")}/ui/`);
  expect(ui.headers.get("content-security-policy")).not.toContain("unsafe-eval");
  expect(ui.headers.get("x-content-type-options")).toBe("nosniff");
});

test("link1: 401 and offline are never shown as healthy", async ({ page }) => {
  test.setTimeout(180_000);
  await login(page, TOKENS.viewer, "light");
  await page.getByRole("row", { name: /sensor-alerts/ }).click();
  await expect(page.getByText("安全投影")).toBeVisible();
  // Simulate the server going away: abort all API calls.
  await page.route("**/v1/**", (r) => r.abort("connectionrefused"));
  await page.getByRole("button", { name: "立即刷新" }).click();
  await expect(page.getByText("以下数据已过期")).toBeVisible();
  await expect(page.getByText("数据过期").first()).toBeVisible();
  await expect(page.locator(".page-head .pill").first()).not.toHaveText("运行中");
  await shot(page, "30-offline-stale-light");
  // Token revoked/invalid: 401 drops the session; no auto-retry of old commands.
  await page.unroute("**/v1/**");
  await page.route("**/v1/**", (r) => r.fulfill({ status: 401, contentType: "application/json", body: '{"error":{"code":"policy_denied","message":"unauthorized: bearer token required","retryable":false}}' }));
  await page.waitForTimeout(2500);
  const posts: string[] = [];
  page.on("request", (r) => { if (r.method() !== "GET" && r.url().includes("/v1/")) posts.push(r.url()); });
  await page.waitForTimeout(65_000);
  await expect(page.getByText("登录已失效")).toBeVisible();
  await expect(page.getByRole("heading", { name: "登录" })).toBeVisible();
  await shot(page, "31-401-expired-light");
  expect(posts).toEqual([]);
});
