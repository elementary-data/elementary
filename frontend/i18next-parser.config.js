module.exports = {
  input: ['src/**/*.{ts,tsx,js,jsx}'],
  output: 'src/shared/locale/$LOCALE/$NAMESPACE.json',
  createOldCatalogs: false,
  lexers: {
    js: ['JavascriptLexer'],
    ts: ['JavascriptLexer'],
    jsx: ['JsxLexer'],
    tsx: ['JsxLexer'],
    default: ['JsxLexer'],
  },
  locales: ['en'],
  keySeparator: false,
  namespaceSeparator: false,
  sort: true,
};
