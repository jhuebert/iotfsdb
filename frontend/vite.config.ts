import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // The SPA is same-origin in production; in development the Vite dev
      // server proxies API calls to the locally running Spring Boot app.
      "/v2": "http://localhost:8080",
      "/swagger-ui": "http://localhost:8080",
      "/v3": "http://localhost:8080",
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
    rollupOptions: {
      output: {
        manualChunks: {
          react: ["react", "react-dom", "react-router"],
          chart: ["chart.js", "react-chartjs-2", "chartjs-plugin-zoom"],
        },
      },
    },
  },
});
