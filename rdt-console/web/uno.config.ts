import { defineConfig, presetWind3, transformerDirectives } from 'unocss'

export default defineConfig({ presets: [presetWind3()], transformers: [transformerDirectives()] })
