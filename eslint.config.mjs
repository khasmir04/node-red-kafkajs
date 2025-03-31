import { defineConfig } from 'eslint/config';
import globals from 'globals';
import js from '@eslint/js';

export default defineConfig([
    { files: ['src/**/*.{js,mjs,cjs}'] },
    { files: ['src/**/*.js'], languageOptions: { sourceType: 'commonjs' } },
    { files: ['src/**/*.{js,mjs,cjs}'], languageOptions: { globals: globals.browser } },
    { files: ['src/**/*.{js,mjs,cjs}'], plugins: { js }, extends: ['js/recommended'] }
]);
