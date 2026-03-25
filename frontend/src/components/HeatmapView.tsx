import { useEffect, useRef } from 'react'
import { Box, Paper, Typography } from '@mui/material'
import type { HeatmapCell as HeatmapCellType } from '../types'
import {
  HEATMAP_METRIC_OPTIONS,
  HEATMAP_YEARS,
  type HeatmapMetricKey,
} from './heatmap/heatmapConfig'
import { getCellValue } from './heatmap/heatmapUtils'
import { HeatmapGrid } from './heatmap/HeatmapGrid'
import { HeatmapMetricSelector } from './heatmap/HeatmapMetricSelector'

export type { HeatmapMetricKey }
export { HEATMAP_METRIC_OPTIONS }

export interface HeatmapViewProps {
  cells: HeatmapCellType[]
  metric: HeatmapMetricKey
  onMetricChange?: (metric: HeatmapMetricKey) => void
  loading?: boolean
}

export function HeatmapView({ cells, metric, onMetricChange, loading }: HeatmapViewProps) {
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    const snapToMax = () => {
      el.scrollLeft = el.scrollWidth - el.clientWidth
      el.scrollTop = el.scrollHeight - el.clientHeight
    }
    snapToMax()
    const t = requestAnimationFrame(snapToMax)
    const t2 = setTimeout(snapToMax, 100)
    return () => {
      cancelAnimationFrame(t)
      clearTimeout(t2)
    }
  }, [loading, cells.length])

  if (loading) {
    return (
      <Paper sx={{ p: 3 }}>
        <Typography color="text.secondary">Loading heatmap…</Typography>
      </Paper>
    )
  }

  const yearList = HEATMAP_YEARS
  const valueByKey = new Map<string, number>()
  let minVal = Infinity
  let maxVal = -Infinity
  for (const c of cells) {
    const key = `${c.year_bought}\t${c.year_sold}`
    const v = getCellValue(c, metric)
    if (v !== null) {
      valueByKey.set(key, v)
      if (v < minVal) minVal = v
      if (v > maxVal) maxVal = v
    }
  }
  if (minVal === Infinity) minVal = 0
  if (maxVal <= minVal) maxVal = minVal + 1

  return (
    <Paper sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      <Box sx={{ flexShrink: 0, px: 2, pt: 2, pb: 1, display: 'flex', flexWrap: 'wrap', gap: 2, alignItems: 'center' }}>
        <HeatmapMetricSelector metric={metric} onMetricChange={onMetricChange} />
      </Box>
      <Box
        sx={{
          flex: 1,
          minHeight: 0,
          display: 'flex',
          overflow: 'hidden',
          maxWidth: '100%',
        }}
      >
        <Box
          sx={{
            width: 28,
            flexShrink: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            py: 2,
          }}
        >
          <Typography
            component="span"
            color="text.secondary"
            sx={{
              transform: 'rotate(-90deg)',
              whiteSpace: 'nowrap',
              fontStyle: 'italic',
              fontSize: '0.875rem',
            }}
          >
            Year bought
          </Typography>
        </Box>
        <Box sx={{ flex: 1, minWidth: 0, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          <Typography
            component="div"
            color="text.secondary"
            sx={{
              textAlign: 'center',
              py: 0.5,
              fontStyle: 'italic',
              fontSize: '0.875rem',
              flexShrink: 0,
            }}
          >
            Year sold
          </Typography>
          <HeatmapGrid
            ref={scrollRef}
            valueByKey={valueByKey}
            minVal={minVal}
            maxVal={maxVal}
            metric={metric}
            yearList={yearList}
          />
        </Box>
      </Box>
    </Paper>
  )
}
