import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

const env = loadEnv(process.env.NODE_ENV as string, process.cwd(), 'VITE_')

// https://vitejs.dev/config/
export default defineConfig({
  root: path.resolve(__dirname, '.'),
  plugins: [react()],
  server: {
    proxy: {
      '/api': {
        target: env.VITE_PROXY_TARGET,
        changeOrigin: true,
        secure: false,
        rewrite: (path) => path.replace(/^\/api/, '')
      }
    }
  }
})
