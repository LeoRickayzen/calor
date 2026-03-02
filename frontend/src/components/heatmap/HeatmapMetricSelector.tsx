import { Box, Typography } from '@mui/material'
import { HEATMAP_METRIC_OPTIONS, type HeatmapMetricKey } from './heatmapConfig'

export interface HeatmapMetricSelectorProps {
  metric: HeatmapMetricKey
  onMetricChange?: (metric: HeatmapMetricKey) => void
}

export function HeatmapMetricSelector({ metric, onMetricChange }: HeatmapMetricSelectorProps) {
  if (!onMetricChange) return null
  return (
    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
      {HEATMAP_METRIC_OPTIONS.map((opt) => (
        <Typography
          key={opt.value}
          component="button"
          type="button"
          variant="caption"
          onClick={() => onMetricChange(opt.value)}
          sx={{
            border: '1px solid',
            borderColor: metric === opt.value ? 'primary.main' : 'divider',
            borderRadius: 1,
            px: 1,
            py: 0.5,
            bgcolor: metric === opt.value ? 'action.selected' : 'transparent',
            color: metric === opt.value ? 'primary.main' : 'text.secondary',
            cursor: 'pointer',
            '&:hover': { bgcolor: 'action.hover' },
          }}
        >
          {opt.label}
        </Typography>
      ))}
    </Box>
  )
}
