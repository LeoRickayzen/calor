/**
 * Pure helpers for heatmap: cell value, color scale, format value/tooltip.
 */

import type { HeatmapCell } from '../../types'
import type { HeatmapMetricKey } from './heatmapConfig'

export function getCellValue(cell: HeatmapCell, metric: HeatmapMetricKey): number | null {
  switch (metric) {
    case 'avg_appreciation_pounds':
      return cell.avg_appreciation_pounds
    case 'median_appreciation_pounds':
      return cell.median_appreciation_pounds
    case 'sale_count':
      return cell.sale_count
    case 'avg_appreciation_pct':
      return cell.avg_appreciation_pct ?? null
    case 'median_appreciation_pct':
      return cell.median_appreciation_pct ?? null
    case 'pct_sales_appreciated':
      return cell.pct_sales_appreciated
    default:
      return cell.median_appreciation_pounds
  }
}

export function interpolateColor(min: number, max: number, value: number): string {
  if (max <= min) return 'rgba(33, 150, 243, 0.5)'
  const t = (value - min) / (max - min)
  const r = Math.round(33 + (1 - t) * 200)
  const g = Math.round(150 + (1 - t) * 105)
  const b = 243
  return `rgba(${r},${g},${b},${0.3 + 0.7 * t})`
}

export function formatValue(value: number, metric: HeatmapMetricKey): string {
  if (metric === 'sale_count') {
    if (value >= 1000) {
      const rounded = Math.round(value / 10) * 10
      return `${(rounded / 1000).toFixed(1)}k`
    }
    return value.toLocaleString()
  }
  if (metric === 'avg_appreciation_pct' || metric === 'median_appreciation_pct' || metric === 'pct_sales_appreciated') {
    return `${Math.round(value)}%`
  }
  const abs = Math.abs(value)
  const sign = value < 0 ? '-' : ''
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(1)}M`
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(0)}k`
  return String(Math.round(value))
}

export function formatTooltip(value: number, metric: HeatmapMetricKey): string {
  if (metric === 'sale_count') return `${value.toLocaleString()} sales`
  if (metric === 'avg_appreciation_pct' || metric === 'median_appreciation_pct') return `${Math.round(value)}%`
  if (metric === 'pct_sales_appreciated') return `${Math.round(value)}% appreciated`
  return `£${value.toLocaleString()}`
}
