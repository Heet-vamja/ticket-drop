import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// Proxy /api/* to the FastAPI backend so the browser stays same-origin (SSE included).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8765',
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
