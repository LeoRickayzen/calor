import { forwardRef, useState } from 'react'
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import {
  CELL_SIZE_PX,
  HEATMAP_YEARS,
  squareCellSx,
  stickyFirstColumnSx,
  type HeatmapMetricKey,
} from './heatmapConfig'
import { formatTooltip, formatValue, interpolateColor } from './heatmapUtils'

/** Opaque blend of hex color with white so nothing shows through. */
function opaqueBlendWithWhite(hex: string, amount: number): string {
  const n = (x: string) => parseInt(x, 16)
  const r = n(hex.slice(1, 3))
  const g = n(hex.slice(3, 5))
  const b = n(hex.slice(5, 7))
  const r2 = Math.round((1 - amount) * 255 + amount * r)
  const g2 = Math.round((1 - amount) * 255 + amount * g)
  const b2 = Math.round((1 - amount) * 255 + amount * b)
  return `#${[r2, g2, b2].map((c) => c.toString(16).padStart(2, '0')).join('')}`
}

const yearHighlightSx = (theme: { palette: { primary: { main: string } } }) => ({
  backgroundColor: opaqueBlendWithWhite(theme.palette.primary.main, 0.14),
})

export interface HeatmapGridProps {
  valueByKey: Map<string, number>
  minVal: number
  maxVal: number
  metric: HeatmapMetricKey
  yearList?: string[]
}

export const HeatmapGrid = forwardRef<HTMLDivElement, HeatmapGridProps>(
  function HeatmapGrid(
    { valueByKey, minVal, maxVal, metric, yearList = HEATMAP_YEARS },
    ref
  ) {
    const [hoveredCell, setHoveredCell] = useState<{
      year_bought: string
      year_sold: string
    } | null>(null)
    const yearSoldList = yearList
    const yearBoughtList = yearList
    const tableWidth = CELL_SIZE_PX * (1 + yearSoldList.length)

    return (
      <Box ref={ref} sx={{ flex: 1, minHeight: 0, overflow: 'auto' }}>
        <Box sx={{ minWidth: tableWidth }}>
          <Box
            component="div"
            sx={{
              position: 'sticky',
              top: 0,
              zIndex: 4,
              backgroundColor: 'background.paper',
              flexShrink: 0,
              borderBottom: 1,
              borderColor: 'divider',
            }}
          >
            <Table size="small" sx={{ tableLayout: 'fixed', minWidth: tableWidth }}>
              <colgroup>
                <col style={{ width: CELL_SIZE_PX }} />
                {yearSoldList.map((_, i) => (
                  <col key={i} style={{ width: CELL_SIZE_PX }} />
                ))}
              </colgroup>
              <TableHead>
                <TableRow>
                  <TableCell
                    sx={{
                      ...squareCellSx,
                      ...stickyFirstColumnSx,
                      zIndex: 3,
                      backgroundColor: 'background.default',
                    }}
                  >
                  </TableCell>
                  {yearSoldList.map((y) => (
                    <TableCell
                      key={y}
                      align="center"
                      sx={(theme) => ({
                        ...squareCellSx,
                        writingMode: 'vertical-rl',
                        textOrientation: 'mixed',
                        ...(hoveredCell?.year_sold === y ? yearHighlightSx(theme) : {}),
                      })}
                    >
                      <Typography component="span" fontWeight="bold">
                        {y}
                      </Typography>
                    </TableCell>
                  ))}
                </TableRow>
              </TableHead>
            </Table>
          </Box>
          <Box sx={{ minWidth: tableWidth }}>
            <Table
              size="small"
              sx={{ tableLayout: 'fixed', minWidth: tableWidth }}
              onMouseLeave={() => setHoveredCell(null)}
            >
              <colgroup>
                <col style={{ width: CELL_SIZE_PX }} />
                {yearSoldList.map((_, i) => (
                  <col key={i} style={{ width: CELL_SIZE_PX }} />
                ))}
              </colgroup>
              <TableBody>
                {yearBoughtList.map((yb) => (
                  <TableRow key={yb}>
                    <TableCell
                      align="center"
                      sx={(theme) => ({
                        ...squareCellSx,
                        ...stickyFirstColumnSx,
                        ...(hoveredCell?.year_bought === yb
                          ? {
                              ...yearHighlightSx(theme),
                              boxShadow: 'none',
                              border: 'none',
                              outline: 'none',
                            }
                          : {}),
                      })}
                    >
                      <Typography component="span" fontWeight="bold">
                        {yb}
                      </Typography>
                    </TableCell>
                    {yearSoldList.map((ys) => {
                      const key = `${yb}\t${ys}`
                      const value = valueByKey.get(key)
                      const bg =
                        value !== undefined
                          ? interpolateColor(minVal, maxVal, value)
                          : undefined
                      return (
                        <TableCell
                          key={ys}
                          align="center"
                          sx={{
                            backgroundColor: bg,
                            ...squareCellSx,
                          }}
                          title={value !== undefined ? formatTooltip(value, metric) : undefined}
                          onMouseEnter={() => setHoveredCell({ year_bought: yb, year_sold: ys })}
                        >
                          {value !== undefined ? formatValue(value, metric) : '—'}
                        </TableCell>
                      )
                    })}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>
        </Box>
      </Box>
    )
  }
)
