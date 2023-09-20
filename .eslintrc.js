// eslint-disable-next-line no-undef
module.exports = {
  extends: ["prettier", "plugin:@typescript-eslint/recommended", "plugin:import/typescript"],
  parser: "@typescript-eslint/parser",
  plugins: ["@typescript-eslint", "import", "jest", "sort-imports-es6-autofix"],
  rules: {
    "@typescript-eslint/no-explicit-any": "off",
    "@typescript-eslint/no-this-alias": "off",
    "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }],
    curly: ["error"],
    "import/no-extraneous-dependencies": ["error"],
    "import/no-internal-modules": ["error", { forbid: ["@dynacron/**/*"] }],
    "sort-imports-es6-autofix/sort-imports-es6": ["error"],
  },
  overrides: [
    {
      files: ["*.js"],
      rules: {
        "@typescript-eslint/no-var-requires": ["off"],
      },
    },
  ],
  parserOptions: {
    sourceType: "module",
    project: false,
  },
  root: true,
};
