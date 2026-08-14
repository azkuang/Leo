import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The build lands in internal/ui/dist so the Go binary embeds it: one
// artifact, no static file server to configure, no API/UI version skew.
export default defineConfig({
  plugins: [react()],
  build: { outDir: '../internal/ui/dist', emptyOutDir: true },
  server: {
    proxy: {
      '/orch.v1.OrchService': 'http://localhost:9443',
      '/events': 'http://localhost:9443',
    },
  },
})
