import type {
  DimensionListResponse,
  HeatmapResponse,
  HeatmapParams,
} from './types'

const baseUrl =
  (import.meta.env.VITE_API_URL as string | undefined)?.replace(/\/$/, '') ||
  'http://localhost:8000'

async function fetchApi<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(path, baseUrl)
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v))
  }
  const res = await fetch(url.toString())
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export function getDimensionValues(
  dimensionName: string
): Promise<DimensionListResponse> {
  return fetchApi<DimensionListResponse>(`/api/dimensions/${encodeURIComponent(dimensionName)}`)
}

export function getHeatmap(params: HeatmapParams): Promise<HeatmapResponse> {
  return fetchApi<HeatmapResponse>('/api/performance/heatmap', {
    location_type: params.location_type,
    location_value: params.location_value,
    house_type: params.house_type,
    tenure: params.tenure,
    size_band: params.size_band,
    year_built_band: params.year_built_band,
  })
}
