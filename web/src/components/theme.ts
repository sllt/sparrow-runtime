// Theme preference is the only thing persisted (not sensitive).
const KEY = "sparrow.theme";
export type Theme = "light" | "dark";

export function currentTheme(): Theme {
  return document.documentElement.dataset.theme === "dark" ? "dark" : "light";
}
export function setTheme(t: Theme) {
  document.documentElement.dataset.theme = t;
  try { localStorage.setItem(KEY, t); } catch { /* storage disabled */ }
}
export function initTheme() {
  let t: string | null = null;
  try { t = localStorage.getItem(KEY); } catch { /* ignore */ }
  const forced = new URLSearchParams(location.search).get("theme");
  if (forced === "dark" || forced === "light") t = forced;
  document.documentElement.dataset.theme =
    t === "dark" || t === "light" ? t : matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}
