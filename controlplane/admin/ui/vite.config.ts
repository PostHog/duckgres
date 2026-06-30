/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// The CP serves index.html as an SPA fallback at arbitrary nested routes, so
// asset URLs MUST be absolute (`base: '/'`). A relative `./assets` base would
// resolve against the current nested route and 404. `npm run dev` proxies
// API/auth/health routes to a port-forwarded control plane (or the Go
// devserver) so the SPA can hit a real backend during dev.
const proxyTarget = process.env.VITE_PROXY_TARGET ?? "http://127.0.0.1:8080";

export default defineConfig({
  base: "/",
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      output: {
        // Split the heavy charting lib and core vendor into separate chunks so
        // the initial app shell loads without dragging Recharts along.
        manualChunks: {
          recharts: ["recharts"],
          vendor: ["react", "react-dom", "react-router-dom", "@tanstack/react-query", "@tanstack/react-table"],
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": { target: proxyTarget, changeOrigin: true },
      "/login": { target: proxyTarget, changeOrigin: true },
      "/health": { target: proxyTarget, changeOrigin: true },
    },
  },
  test: {
    // jsdom + jest-dom so the same setup supports both pure-function tests and
    // future component tests. globals lets tests use describe/it/expect without
    // imports. Only test files under src/ are collected (node_modules excluded).
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.{test,spec}.{ts,tsx}"],
    css: false,
  },
});
