import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Treat .js files as JSX so we don't have to rename every component file.
export default defineConfig({
  plugins: [react({ include: /\.(jsx?|tsx?)$/ })],
  esbuild: {
    loader: 'jsx',
    include: /src\/.*\.jsx?$/,
    exclude: [],
  },
  optimizeDeps: {
    esbuildOptions: { loader: { '.js': 'jsx' } },
  },
  server: { port: 3000, open: true },
  // Match CRA's output dir so deploy.sh keeps working.
  build: { outDir: 'build' },
});
