import { defineConfig } from 'vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import react from '@vitejs/plugin-react';
import svgr from '@svgr/rollup';
import checker from 'vite-plugin-checker';
import { splitVendorChunkPlugin } from 'vite';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    tsconfigPaths(),
    react(),
    svgr(),
    checker({ typescript: true }),
    splitVendorChunkPlugin(),
  ],
  server: {
    open: true,
    host: '0.0.0.0',
    port: 4444,
  },
  preview: {
    port: 4444,
  },
});
