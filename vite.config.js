import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig({
  plugins: [react()],
  base: './',
  build: { rollupOptions: { output: { manualChunks: { diagram: ['@xyflow/react'], state: ['xstate', '@xstate/react'] } } } },
});
