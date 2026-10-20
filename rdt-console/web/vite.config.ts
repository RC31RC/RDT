import { defineConfig, loadEnv } from 'vite'
import vue from '@vitejs/plugin-vue'
import UnoCSS from 'unocss/vite'
import { createSvgIconsPlugin } from 'vite-plugin-svg-icons'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd())
  const port = Number(env.VITE_PORT || 3010)
  const proxyTarget = env.VITE_RDT_API_PROXY_TARGET || 'http://127.0.0.1:9898'

  return {
    plugins: [vue(), UnoCSS(), createSvgIconsPlugin({
      iconDirs: [new URL('./src/assets/icons', import.meta.url).pathname],
      symbolId: 'icon-[name]',
      svgoOptions: { plugins: [{ name: 'convertColors', params: { currentColor: true } }] }
    })],
    css: { preprocessorOptions: { scss: { additionalData: '@use "@/styles/koi/variable.scss" as *;\n' } } },
    // Generated resources survive an IDE rebuild of target/classes.
    build: { outDir: '../target/generated-resources/console/static', emptyOutDir: true },
    server: {
      host: '127.0.0.1',
      port,
      strictPort: true,
      proxy: {
        '^/rdt/': {
          target: proxyTarget,
          changeOrigin: true
        }
      }
    },
    resolve: {
      alias: {
        '@': new URL('./src', import.meta.url).pathname
      }
    }
  }
})
