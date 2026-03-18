import {defineConfig} from 'vite';
import {resolve} from 'path';

const entry = resolve(__dirname, 'src/index.js');

// Two named configs, selected via BUILD_TARGET env var:
//   BUILD_TARGET=es  → ES module, dayjs external   (Vue / bundled projects)
//   BUILD_TARGET=umd → UMD, dayjs bundled in        (Django templates / script tags)

const target = process.env.BUILD_TARGET || 'es';

export default defineConfig({
    build: {
        // Each target writes to its own subdirectory
        outDir: target === 'umd' ? 'dist/umd' : 'dist/es',
        cssCodeSplit: false,
        lib: {
            entry,
            name: 'DateTimeSelector',
            formats: target === 'umd' ? ['umd'] : ['es'],
            fileName: () => 'datetime-selector.js',
        },
        rollupOptions: target === 'umd'
            ? {
                // UMD: bundle dayjs in — zero external dependencies
            }
            : {
                // ES: keep dayjs external — consuming bundler resolves it
                external: ['dayjs', 'dayjs/plugin/utc'],
            },
    },
});
