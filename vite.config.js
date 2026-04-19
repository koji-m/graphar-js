import { resolve } from 'node:path';
import { defineConfig } from 'vite';

export default defineConfig({
  build: {
    lib: {
      entry: resolve(__dirname, 'src/index.js'),
      name: 'GraphAr',
      fileName: (format) => `graphar.${format}.js`,
      formats: ['es'],
    },
    rollupOptions: {
      external: ['apache-arrow', 'js-yaml', 'parquet-wasm'],
    },
  },
});
