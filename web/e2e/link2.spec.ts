import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { api, SECRET_VALUE, TOKENS } from "./fixture";

const SHOTS = process.env.K5_SCREENS ?? "/workspace/notes/k5/screens";
mkdirSync(SHOTS, { recursive: true });
const shot = (page: Page, name: string) => page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true });

async function login(page: Page, token: string, theme: "light" | "dark" = "light") {
  await page.goto(`/ui/?theme=${theme}`);
  await page.getByLabel("访问令牌").fill(token);
  await page.getByRole("button", { name: "登录" }).click();
  await expect(page.getByRole("heading", { name: "总览" })).toBeVisible();
}

async function typeInEditor(page: Page, text: string) {
  const ed = page.locator(".cm-content").first();
  await ed.click();
  await page.keyboard.press("Control+A");
  await page.keyboard.type(text);
}

test("link2: SQL config loop — draft → check → sample → publish → start, CAS conflicts", async ({ page }) => {
  const seen: string[] = [];
  page.on("response", async (r) => { if (r.url().includes("/v1/")) seen.push(await r.text().catch(() => "")); });
  await login(page, TOKENS.operator);
  await page.keyboard.press("5");
  await expect(page.getByRole("heading", { name: "草稿与发布" })).toBeVisible();
  await page.getByRole("button", { name: "新建草稿" }).first().click();
  await page.getByLabel("目标流水线名称").fill("temp-hot");
  await page.getByLabel("草稿名称").fill("temp-hot-v1");
  await shot(page, "21-new-draft-light");
  await page.getByRole("button", { name: "创建并编辑" }).click();
  await expect(page.getByRole("heading", { name: "temp-hot-v1" })).toBeVisible();

  // Source/Sink forms: demo I/O for both sides.
  await page.getByRole("tab", { name: /Source \/ Sink/ }).click();
  await page.getByLabel("使用内置演示 I/O").first().check();
  await page.getByLabel("sink kind").selectOption("http");
  await page.getByLabel("使用内置演示 I/O").nth(1).check();
  await page.getByRole("tab", { name: "SQL" }).click();
  await typeInEditor(page, "SELECT device_id, temperature, ts FROM sensors WHERE temperature > 30");
  await expect(page.getByText("未保存")).toBeVisible();
  await page.keyboard.press("Control+s");
  await expect(page.getByText(/已保存为 draft-\d/)).toBeVisible();

  await page.getByRole("button", { name: /^检查/ }).click();
  await expect(page.getByText("校验（validate）")).toBeVisible();
  await expect(page.locator(".check-row", { hasText: "校验（validate）" })).toContainText("通过");
  await page.getByRole("button", { name: "运行" }).click();
  await expect(page.getByText(/输入 2 行 → 输出 1 行/)).toBeVisible();
  await shot(page, "22-draft-editor-checked-light");

  // Another editor saves first; our next save must conflict and keep our text.
  const cur = await api("GET", "/v1/drafts/temp-hot-v1", TOKENS.admin);
  const etag = JSON.parse(cur.text).etag as string;
  const other = JSON.parse(cur.text);
  const r = await fetch(`${new URL(page.url()).origin}/v1/drafts/temp-hot-v1`, { method: "PUT", headers: { authorization: `Bearer ${TOKENS.admin}`, "content-type": "application/json", "if-match": etag }, body: JSON.stringify({ pipeline: other.pipeline, mode: "sql", text: other.text.replace("> 30", "> 31"), metadata: {} }) });
  expect(r.status).toBe(200);
  await typeInEditor(page, "SELECT device_id, temperature, ts FROM sensors WHERE temperature > 35");
  await page.keyboard.press("Control+s");
  await expect(page.getByText("草稿已被其他人修改")).toBeVisible();
  await expect(page.locator(".cm-content").first()).toContainText("> 35");
  await shot(page, "23-draft-conflict-light");
  await page.getByRole("button", { name: "放弃我的修改，加载最新" }).click();
  await expect(page.locator(".cm-content").first()).toContainText("> 31");

  // Publish with diff review; not started.
  await page.getByRole("button", { name: "发布…" }).click();
  const dlg = page.getByRole("dialog");
  await expect(dlg.getByText("完整校验")).toBeVisible();
  await expect(dlg.getByText("配置差异").or(dlg.locator(".diff"))).toBeVisible();
  await expect(dlg.getByRole("button", { name: "确认发布（不启动）" })).toBeDisabled();
  await dlg.getByLabel(/我已核对/).check();
  await shot(page, "24-publish-review-light");
  await dlg.getByRole("button", { name: "确认发布（不启动）" }).click();
  await expect(dlg.getByText(/已发布 rev 1/)).toBeVisible();
  const st = JSON.parse((await api("GET", "/v1/pipelines/temp-hot/status", TOKENS.admin)).text);
  expect(st.desired?.status ?? "stopped").toBe("stopped");
  await shot(page, "25-published-light");
  await dlg.getByRole("button", { name: /启动 rev 1/ }).click();
  await expect(page.getByRole("heading", { name: "temp-hot", exact: true })).toBeVisible();
  await expect(page.getByText("运行中").first()).toBeVisible({ timeout: 20_000 });

  // Revision history.
  await page.getByRole("tab", { name: "版本历史" }).click();
  await expect(page.getByText("发布历史")).toBeVisible();
  await expect(page.locator("tr", { hasText: "rev 1" })).toContainText("actual");
  await shot(page, "26-revisions-light");
  await api("POST", "/v1/pipelines/temp-hot/stop", TOKENS.admin, {});

  // Stale base: someone publishes rev 2 directly; the dialog reports it and publish conflicts.
  const pipe = JSON.parse((await api("GET", "/v1/pipelines/temp-hot", TOKENS.admin)).text);
  const r2 = await fetch(`${new URL(page.url()).origin}/v1/pipelines/temp-hot`, { method: "PUT", headers: { authorization: `Bearer ${TOKENS.admin}`, "content-type": "application/json", "if-match": pipe.etag }, body: JSON.stringify({ ...pipe.spec, sql: pipe.spec.sql.replace("> 31", "> 40") }) });
  expect(r2.status).toBe(200);
  await page.goto("/ui/drafts/temp-hot-v1");
  await page.getByLabel("访问令牌").fill(TOKENS.operator);
  await page.getByRole("button", { name: "登录" }).click();
  // Deep link survives the in-memory re-login.
  await expect(page.getByRole("heading", { name: "temp-hot-v1" })).toBeVisible();
  await page.getByRole("button", { name: "发布…" }).click();
  await expect(page.getByRole("dialog").getByText(/已过期（当前 rev-2）/)).toBeVisible();
  await page.getByRole("dialog").getByLabel(/我已核对/).check();
  await page.getByRole("dialog").getByRole("button", { name: "确认发布（不启动）" }).click();
  await expect(page.getByRole("dialog").getByText("发布冲突：")).toBeVisible();
  await shot(page, "27-publish-conflict-light");
  expect((JSON.parse((await api("GET", "/v1/pipelines/temp-hot", TOKENS.admin)).text)).revision).toBe(2);
  // Secrets never appear in any response the UI received.
  expect(seen.join("\n")).not.toContain(SECRET_VALUE);
});

for (const theme of ["light", "dark"] as const) {
  test(`link2: resource pages (${theme})`, async ({ page }) => {
    await login(page, TOKENS.operator, theme);
    await page.keyboard.press("6");
    await expect(page.getByRole("heading", { name: "Stream 与 Schema" })).toBeVisible();
    await expect(page.getByLabel("字段 1 名称")).toHaveValue("device_id");
    await shot(page, `31-streams-${theme}`);
    await page.keyboard.press("7");
    await expect(page.getByRole("heading", { name: "连接模板" })).toBeVisible();
    if (theme === "light") {
      await page.getByRole("button", { name: "新建模板" }).click();
      await page.getByLabel("名称", { exact: true }).fill("plant-mqtt");
      await page.getByLabel("主题").fill("plant/+/json");
      await page.getByLabel("Client ID").fill("plant");
      await page.getByRole("button", { name: "保存" }).click();
      await expect(page.getByRole("button", { name: /plant-mqtt/ })).toBeVisible();
      await page.getByRole("button", { name: "1 · 配置校验" }).click();
      await expect(page.getByText("配置校验：通过")).toBeVisible();
      await expect(page.getByRole("button", { name: /网络握手（仅管理员）/ })).toBeDisabled();
    }
    await shot(page, `32-connections-${theme}`);
    await page.keyboard.press("8");
    await expect(page.getByRole("heading", { name: "参考表与插件" })).toBeVisible();
    await shot(page, `33-resources-${theme}`);
    await page.keyboard.press("5");
    await page.getByRole("row", { name: /temp-hot-v1/ }).click();
    await expect(page.getByRole("heading", { name: "temp-hot-v1" })).toBeVisible();
    await page.getByRole("tab", { name: "完整配置 JSON" }).click();
    await shot(page, `34-draft-json-${theme}`);
    await page.getByRole("tab", { name: /Source \/ Sink/ }).click();
    await expect(page.getByText(/来源：连接模板|Source 类型/).first()).toBeVisible();
    await shot(page, `35-draft-io-${theme}`);
    await page.keyboard.press("5");
    await shot(page, `20-drafts-${theme}`);
  });
}

test("link2: viewer has no authoring surface and direct API writes are refused", async ({ page }) => {
  await login(page, TOKENS.viewer);
  await expect(page.getByRole("link", { name: "草稿与发布" })).toHaveCount(0);
  await page.goto("/ui/drafts");
  await page.getByLabel("访问令牌").fill(TOKENS.viewer);
  await page.getByRole("button", { name: "登录" }).click();
  await page.goto("/ui/");
  for (const [m, p] of [["GET", "/v1/drafts"], ["PUT", "/v1/drafts/x"], ["POST", "/v1/drafts/temp-hot-v1/publish"], ["GET", "/v1/connections"], ["GET", "/v1/secrets"], ["GET", "/v1/pipelines/temp-hot/revisions"]] as const) {
    expect((await api(m, p, TOKENS.viewer, m === "GET" ? undefined : {})).status, `${m} ${p}`).toBe(403);
  }
});
