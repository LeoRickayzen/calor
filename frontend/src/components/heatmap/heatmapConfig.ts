/**
 * Heatmap config: year range, cell size, metric type/options, cell styles.
 * Single source for heatmap and subcomponents.
 */

export type HeatmapMetricKey =
  | 'avg_appreciation_pounds'
  | 'median_appreciation_pounds'
  | 'sale_count'
  | 'avg_appreciation_pct'
  | 'median_appreciation_pct'
  | 'pct_sales_appreciated'

export const HEATMAP_METRIC_OPTIONS: { value: HeatmapMetricKey; label: string }[] = [
  { value: 'avg_appreciation_pounds', label: 'Avg appreciation (£)' },
  { value: 'median_appreciation_pounds', label: 'Median appreciation (£)' },
  { value: 'sale_count', label: 'Sale count' },
  { value: 'avg_appreciation_pct', label: 'Avg appreciation (%)' },
  { value: 'median_appreciation_pct', label: 'Median appreciation (%)' },
  { value: 'pct_sales_appreciated', label: '% that appreciated' },
]

export const YEAR_MIN = 2005
export const YEAR_MAX = 2025
export const HEATMAP_YEARS = Array.from(
  { length: YEAR_MAX - YEAR_MIN + 1 },
  (_, i) => String(YEAR_MIN + i)
)

export const CELL_SIZE_PX = 44

export const squareCellSx = {
  width: CELL_SIZE_PX,
  minWidth: CELL_SIZE_PX,
  height: CELL_SIZE_PX,
  minHeight: CELL_SIZE_PX,
  boxSizing: 'border-box' as const,
  padding: '2px',
}

export const stickyFirstColumnSx = {
  position: 'sticky' as const,
  left: 0,
  zIndex: 2,
  backgroundColor: 'background.paper',
  boxShadow: '2px 0 4px -2px rgba(0,0,0,0.1)',
}
