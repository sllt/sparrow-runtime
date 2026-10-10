import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { api, TOKENS } from "./fixture";

const SHOTS = process.env.K5_SCREENS ?? "/workspace/notes/k5/screens";
mkdirSync(SHOTS, { recursive: true });
const shot = async (page: Page, name: string) => { await page.evaluate(() => window.scrollTo(0, 0)); await page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true }); };
async function login(page: Page, token: string, path: string, theme: "light" | "dark" = "light") {
  await page.goto(`${path}?theme=${theme}`);
  await page.getByLabel("访问令牌").fill(token);
  await page.getByRole("button", { name: "登录" }).click();
}
const spec = (t: number) => ({
  version: 1, stream: "sensors", sql: `SELECT device_id, temperature, ts FROM sensors WHERE temperature > ${t}`,
  source: { kind: "mqtt", use_demo_io: true, topic: "sensors/json", client_id: `k55-${t}` },
  sink: { kind: "http", use_demo_io: true }, delivery: "live_best_effort", recovery: "restart_fresh",
});
async function publish(name: string, t: number, etag?: string) {
  const r = await api("PUT", `/v1/pipelines/${name}`, TOKENS.operator, spec(t), etag ? { "if-match": `"${etag}"` } : {});
  expect(r.status, r.text).toBeLessThan(300);
}

for (const theme of ["light", "dark"] as const) {
  test(`link5: rollback as a new revision and bound start (${theme})`, async ({ page }) => {
    const name = `rb-${theme}`;
    await publish(name, 10);
    await publish(name, 20, "rev-1");
    await login(page, TOKENS.operator, `/ui/pipelines/${name}`, theme);
    await page.getByRole("tab", { name: "版本历史" }).click();
    await page.getByRole("button", { name: "回退到 rev 1" }).click();
    const dlg = page.getByRole("dialog");
    await expect(dlg.getByText("rev 2 · rev-2")).toBeVisible();
    await expect(dlg.getByRole("button", { name: "发布为新版本" })).toBeDisabled();
    await dlg.getByLabel("回退原因").fill("阈值 20 漏报，回退到 10");
    await shot(page, `50-rollback-dialog-${theme}`);
    await dlg.getByRole("button", { name: "发布为新版本" }).click();
    await expect(dlg.getByText(/已发布 rev 3（内容来自 rev 1，来源记录为 rollback:r1）/)).toBeVisible();
    await dlg.getByRole("button", { name: "关闭" }).last().click();
    await expect(page.locator("tr", { hasText: "rev 3" }).getByText("latest")).toBeVisible();
    // Start rev 3, bound to rev-3 + desired stopped.
    await page.getByRole("button", { name: "启动 rev 3" }).click();
    await expect(page.getByRole("dialog").getByText("不会暂停")).toBeVisible();
    await page.getByRole("dialog").getByRole("button", { name: "确认启动" }).click();
    await expect(page.getByRole("dialog").getByText(/已提交：期望状态为 running @ rev 3/)).toBeVisible();
    await page.getByRole("dialog").getByRole("button", { name: "关闭" }).last().click();
    // Review rev 1, then someone publishes rev 4: the confirm is refused, nothing is stopped.
    await page.getByRole("button", { name: "启动 rev 1" }).click();
    const d2 = page.getByRole("dialog");
    await publish(name, 30, "rev-3");
    await d2.getByRole("button", { name: "确认启动" }).click();
    await expect(d2.getByRole("alert")).toContainText("状态已变化");
    await shot(page, `51-start-conflict-${theme}`);
    const st = JSON.parse((await api("GET", `/v1/pipelines/${name}/status`, TOKENS.operator)).text);
    expect(st.desired.revision).toBe(3);
    await api("POST", `/v1/pipelines/${name}/stop`, TOKENS.operator, {});
  });
}

test("link5: ops tab, checkpoint, recovery refusal and resources (admin)", async ({ page }) => {
  for (const theme of ["light", "dark"] as const) {
    await login(page, TOKENS.admin, "/ui/pipelines/rb-light", theme);
    await page.getByRole("tab", { name: "运维处置" }).click();
    await expect(page.getByText("未配置 durable outbox")).toBeVisible();
    await expect(page.getByText("未启用输入隔离")).toBeVisible();
    await page.getByRole("button", { name: "新建" }).click();
    const w = page.getByRole("dialog");
    await expect(w.getByLabel("目标配置")).toHaveValue(/temperature > 30/);
    await w.getByLabel("checkpoint_id").fill("1");
    await w.getByLabel("恢复原因").fill("演练");
    await w.getByRole("button", { name: "预览" }).click();
    // MQTT/live pipelines have no recovery contract: refused, and the UI offers no "fresh" fallback.
    await expect(w.getByRole("alert")).toBeVisible();
    await expect(w.getByRole("button", { name: "按审批摘要执行" })).toHaveCount(0);
    await shot(page, `52-recovery-refused-${theme}`);
    await w.getByRole("button", { name: "关闭" }).last().click();
    await page.getByRole("tab", { name: "Checkpoint 风险" }).click();
    await page.getByRole("button", { name: "请求 checkpoint" }).click();
    await expect(page.getByRole("alert").first()).toBeVisible();
    await page.getByRole("link", { name: /参考表与插件/ }).click();
    await expect(page.getByRole("heading", { name: "参考表与插件" })).toBeVisible();
    await page.getByRole("tab", { name: /插件/ }).click();
    await page.getByRole("button", { name: "安装插件包" }).click();
    await expect(page.getByRole("dialog").getByText("页面不会执行或解析其中的代码")).toBeVisible();
    await shot(page, `53-plugin-install-${theme}`);
    await page.getByRole("dialog").getByRole("button", { name: "取消" }).click();
    await page.getByRole("button", { name: "注销" }).click();
  }
});

test("link5: viewer and operator boundaries", async ({ page }) => {
  await login(page, TOKENS.viewer, "/ui/pipelines/rb-light");
  await expect(page.getByRole("tab", { name: "概览" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "运维处置" })).toHaveCount(0);
  expect((await api("POST", "/v1/pipelines/rb-light/rollback", TOKENS.viewer, { operation_id: "viewer-op-01", from_revision: 1, expected_etag: "rev-4", reason: "x" })).status).toBe(403);
  expect((await api("GET", "/v1/pipelines/rb-light/outbox/entries", TOKENS.operator)).status).toBe(403);
  expect((await api("POST", "/v1/pipelines/rb-light/recovery/preview", TOKENS.operator, {})).status).toBe(403);
});
