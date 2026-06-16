import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "path";

// Dev-only: builds the screenshot harness (shots.html) as a static bundle so
// it can be served without the dev server's HMR/fast-refresh machinery.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "_shots/dist",
    emptyOutDir: true,
    rollupOptions: {
      input: resolve(__dirname, "shots.html"),
    },
  },
});
