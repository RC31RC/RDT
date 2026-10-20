import js from '@eslint/js'
import ts from 'typescript-eslint'
import vue from 'eslint-plugin-vue'

export default ts.config(
  { ignores: ['node_modules/**', 'dist/**', 'vendor/**', 'test-results/**', 'playwright-report/**'] },
  js.configs.recommended,
  { files: ['playwright.config.ts'], languageOptions: { globals: { process: 'readonly' } } },
  ...ts.configs.recommended,
  ...vue.configs['flat/essential'],
  {
    files: ['src/**/*.{ts,vue}', 'tests/**/*.ts'],
    languageOptions: {
      parserOptions: { parser: ts.parser },
      globals: { localStorage: 'readonly', window: 'readonly', document: 'readonly', console: 'readonly', HTMLElement: 'readonly', ResizeObserver: 'readonly', WheelEvent: 'readonly', Event: 'readonly', MouseEvent: 'readonly' }
    },
    rules: { 'vue/multi-word-component-names': 'off', '@typescript-eslint/no-explicit-any': 'off' }
  }
)
