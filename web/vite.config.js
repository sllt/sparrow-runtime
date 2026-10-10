/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
// Static build only (no SSR/BFF). Served by `sparrow-server --ui-dir dist` at /ui/.
// The dev proxy is for local development only; production is same-origin.
export default defineConfig({
    base: "/ui/",
    plugins: [react()],
    build: {
        outDir: "dist",
        sourcemap: false,
        assetsInlineLimit: 0, // keep CSP simple: no data: scripts/styles
        chunkSizeWarningLimit: 600,
    },
    server: {
        proxy: { "/v1": globalThis.process?.env.SPARROW_DEV_API ?? "http://127.0.0.1:43180" },
    },
    test: {
        environment: "jsdom",
        include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    },
});
