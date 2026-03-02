export interface DimensionIndexItem {
  dimension_name: string
  value: string
  label?: string | null
  sale_count?: number | null
}

export interface DimensionListResponse {
  dimension_name: string
  values: DimensionIndexItem[]
}

export interface HeatmapCell {
  year_bought: string
  year_sold: string
  avg_appreciation_pounds: number
  median_appreciation_pounds: number
  sale_count: number
  avg_appreciation_pct?: number | null
  median_appreciation_pct?: number | null
  pct_sales_appreciated: number
}

export interface HeatmapResponse {
  cells: HeatmapCell[]
}

export interface HeatmapParams {
  location_type: string
  location_value: string
  house_type: string
  tenure: string
  size_band: string
  year_built_band: string
}
