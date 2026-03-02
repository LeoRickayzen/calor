# Houses frontend

React UI for UK House Price Performance: filter dropdowns and heatmap, integrated with the backend API.

## Setup

```bash
npm install
```

## Development

Start the backend (e.g. `uv run uvicorn app.main:app --reload` from the repo root) on port 8000, then:

```bash
npm run dev
```

The Vite dev server proxies `/api` to `http://localhost:8000` by default. To use a different backend URL, set `VITE_API_URL` (e.g. in `.env`):

```bash
# .env
VITE_API_URL=http://localhost:8000
```

See `.env.example` for the variable name.

## Build

```bash
npm run build
```

Output is in `dist/`. Serve with `npm run preview` or any static host.
