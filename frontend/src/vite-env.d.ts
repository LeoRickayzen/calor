/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_URL: string
  readonly VITE_LOCK_LOCATION_TYPE_TO_BOROUGH?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
