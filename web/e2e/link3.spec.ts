import { expect, test, type Page } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { api, TOKENS } from "./fixture";

const SHOTS = process.env.K5_SCREENS ?? "/workspace/notes/k5/screens";
mkdirSync(SHOTS, { recursive: true });
const shot = (page: Page, name: string) => page.screenshot({ path: `${SHOTS}/${name}.png`, fullPage: true });

async function login(page: Page, token: string, theme: "light" | "dark" = "light") {
  await page.goto(`/ui/?theme=${theme}`);
  await page.getByLabel("访问令牌").fill(token);
  await page.getByRole("button", { name: "登录" }).click();
  await expect(page.getByRole("heading", { name: "总览" })).toBeVisible();
}
const node = (page: Page, id: number) => page.locator(`.react-flow__node[data-id="${id}"]`);
async function drag(page: Page, from: number, to: number) {
  const a = await node(page, from).locator(".react-flow__handle.source").boundingBox();
  const b = await node(page, to).locator(".react-flow__handle.target").boundingBox();
  await page.mouse.move(a!.x + a!.width / 2, a!.y + a!.height / 2);
  await page.mouse.down();
  await page.mouse.move(b!.x + 2, b!.y + 2, { steps: 12 });
  await page.mouse.move(b!.x + b!.width / 2, b!.y + b!.height / 2, { steps: 4 });
  await page.mouse.up();
  await page.waitForTimeout(250);
}
const specOf = async (id: string) => JSON.parse(JSON.parse((await api("GET", `/v1/drafts/${id}`, TOKENS.admin)).text).text);

test("link3: graph designer — build, rewire, undo, server-bound schema, publish, start", async ({ page }) => {
  await login(page, TOKENS.operator);
  await page.keyboard.press("5");
  await page.getByRole("button", { name: "新建草稿" }).first().click();
  await page.getByRole("radio", { name: "Graph 设计器" }).click();
  await page.getByLabel("目标流水线名称").fill("graph-hot");
  await page.getByLabel("草稿名称").fill("graph-hot-v1");
  await page.getByRole("button", { name: "创建并编辑" }).click();
  await expect(page.getByRole("heading", { name: "graph-hot-v1" })).toBeVisible();
  await expect(page.getByRole("tab", { name: /Graph 设计器/ })).toHaveAttribute("aria-selected", "true");
  await expect(node(page, 1)).toBeVisible();

  await page.getByRole("tab", { name: /Source \/ Sink/ }).click();
  await page.getByLabel("使用内置演示 I/O").first().check();
  await page.getByLabel("sink kind").selectOption("http");
  await page.getByLabel("使用内置演示 I/O").nth(1).check();
  await page.getByRole("tab", { name: /Graph 设计器/ }).click();

  // Insert a Filter between source and sink: delete edge, add node, wire twice.
  await page.locator(".react-flow__edge").first().click({ force: true });
  await expect(page.getByRole("heading", { name: "连线" })).toBeVisible();
  await page.keyboard.press("Delete");
  await expect(page.locator(".react-flow__edge")).toHaveCount(0);
  await expect(page.getByText(/个结构提示/)).toBeVisible();
  // Undo/redo round trip.
  await page.locator(".gd-canvas").focus();
  await page.keyboard.press("Control+z");
  await expect(page.locator(".react-flow__edge")).toHaveCount(1);
  await page.keyboard.press("Control+Shift+z");
  await expect(page.locator(".react-flow__edge")).toHaveCount(0);

  await page.getByRole("button", { name: "添加 Filter 节点" }).click();
  await expect(node(page, 3)).toBeVisible();
  await page.getByRole("button", { name: "自动布局" }).click();
  await page.waitForTimeout(500); // fitView animation settles
  await drag(page, 1, 3);
  await drag(page, 3, 2);
  await expect(page.locator(".react-flow__edge")).toHaveCount(2);
  // A sink has no output handle and cycles are refused.
  await expect(node(page, 2).locator(".react-flow__handle.source")).toHaveCount(0);
  await drag(page, 3, 3);
  await expect(page.locator(".react-flow__edge")).toHaveCount(2);

  // Edit filter parameters (lossless JSON) and apply.
  await node(page, 3).click();
  await expect(page.getByRole("heading", { name: "filter" })).toBeVisible();
  const ed = page.locator(".gd-inspector .cm-content");
  await ed.click();
  await page.keyboard.press("Control+A");
  await page.keyboard.type(`{"kind":"filter","predicate":{"k":"bin","op":">","left":{"k":"col","name":"temperature"},"right":{"k":"lit","value":{"t":"float64","v":30.0}}}}`);
  await page.getByRole("button", { name: "应用参数" }).click();
  await expect(page.getByText("结构完整")).toBeVisible();
  await page.keyboard.press("Control+s");
  await expect(page.getByText(/已保存为 draft-\d/)).toBeVisible();

  const spec = await specOf("graph-hot-v1");
  expect(spec.graph.nodes.map((n: { id: number; out?: number[] }) => [n.id, n.out ?? null])).toEqual([[1, [3]], [2, null], [3, [2]]]);
  const d = JSON.parse((await api("GET", "/v1/drafts/graph-hot-v1", TOKENS.admin)).text);
  expect(Object.keys(d.metadata.layout).sort()).toEqual(["1", "2", "3"]);
  expect((await api("GET", "/v1/drafts/graph-hot-v1", TOKENS.admin)).text).toContain("30.0");

  // Server check binds the graph; inspector shows the bound output schema.
  await page.getByRole("button", { name: /^检查/ }).click();
  await expect(page.locator(".check-row", { hasText: "校验（validate）" })).toContainText("通过");
  await node(page, 3).click();
  await expect(page.locator(".gd-inspector").getByRole("cell", { name: "temperature", exact: true })).toBeVisible();
  await expect(node(page, 3).locator(".gnode")).toHaveClass(/tone-ok/);
  await page.getByRole("button", { name: "自动布局" }).click();
  await page.waitForTimeout(400);
  await page.locator(".gd-canvas").focus();
  await page.keyboard.press("Control+s");
  await expect(page.getByText("已保存", { exact: true })).toBeVisible();
  await shot(page, "41-graph-designer-light");

  // Publish (not started), then start; the runtime topology is what we drew.
  await page.getByRole("button", { name: "发布…" }).click();
  const dlg = page.getByRole("dialog");
  await dlg.getByLabel(/我已核对/).check();
  await dlg.getByRole("button", { name: "确认发布（不启动）" }).click();
  await expect(dlg.getByText(/已发布 rev 1/)).toBeVisible();
  await dlg.getByRole("button", { name: /启动 rev 1/ }).click();
  await expect(page.getByRole("heading", { name: "graph-hot", exact: true })).toBeVisible();
  await expect(page.getByText("运行中").first()).toBeVisible({ timeout: 20_000 });
  const pub = JSON.parse((await api("GET", "/v1/pipelines/graph-hot", TOKENS.admin)).text);
  expect(pub.spec.graph.nodes.length).toBe(3);
  await api("POST", "/v1/pipelines/graph-hot/stop", TOKENS.admin, {});
});

test("link3: server error maps to the node; invalid JSON keeps the canvas honest", async ({ page }) => {
  await login(page, TOKENS.operator, "dark");
  await page.goto("/ui/drafts/graph-hot-v1?theme=dark");
  await page.getByLabel("访问令牌").fill(TOKENS.operator);
  await page.getByRole("button", { name: "登录" }).click();
  await expect(node(page, 3)).toBeVisible();
  // Add a dangling project node: server arity check names it.
  await page.getByRole("button", { name: "添加 Project 节点" }).click();
  await expect(node(page, 4)).toBeVisible();
  await node(page, 4).click();
  await page.keyboard.press("Control+d");
  await expect(node(page, 5)).toBeVisible();
  await page.keyboard.press("Control+s");
  await expect(page.getByText(/已保存为 draft-\d/)).toBeVisible();
  await page.getByRole("button", { name: /^检查/ }).click();
  await expect(page.locator(".check-row", { hasText: "校验（validate）" })).toContainText("拒绝");
  await expect(page.locator(".gnode.tone-bad")).toHaveCount(1);
  await expect(page.locator(".gd-inspector").getByText("服务端检查：")).toBeVisible();
  await shot(page, "42-graph-error-dark");
  await page.getByRole("button", { name: "删除节点" }).click();
  await page.getByRole("tab", { name: "完整配置 JSON" }).click();
  await page.locator(".cm-content").first().click();
  await page.keyboard.press("Control+End");
  await page.keyboard.type("}}");
  await page.getByRole("tab", { name: /Graph 设计器/ }).click();
  await expect(page.getByText(/无法解析/)).toBeVisible();
  await shot(page, "43-graph-invalid-dark");
});

test("link3: viewer cannot read drafts or bind graphs", async ({ page }) => {
  await login(page, TOKENS.viewer);
  expect((await api("GET", "/v1/drafts/graph-hot-v1", TOKENS.viewer)).status).toBe(403);
  const ex = await api("POST", "/v1/graphs/explain", TOKENS.viewer, (await specOf("graph-hot-v1")).graph);
  expect(ex.status).toBe(403);
});
