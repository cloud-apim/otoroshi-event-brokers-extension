# Documentation

The [Docusaurus](https://docusaurus.io/) site of the Event Brokers extension for Otoroshi.

```bash
npm install
npm start          # dev server on http://localhost:3000/otoroshi-event-brokers-extension/
npm run build      # static build into ./build (a broken link fails it)
npm run serve      # serve the built site
```

The build output is **not committed**. Pushing to `main` with changes under `documentation/`
triggers `.github/workflows/documentation.yaml`, which builds the site and publishes it straight to
GitHub Pages as an artifact.

The pages are plain markdown (`.md`, read as CommonMark): `<id>` or `{"json": true}` in the text stay
text. Write `.mdx` only for a page that needs a React component.
