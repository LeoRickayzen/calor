import { useCallback, useEffect, useRef, useState } from 'react'
import { Box, Container, Paper, Typography } from '@mui/material'
import { getDimensionValues, getHeatmap } from './api'
import { lockLocationTypeToBorough } from './config'
import type { DimensionIndexItem, HeatmapCell } from './types'
import type { FilterState } from './components/FilterBar'
import { FilterBar } from './components/FilterBar'
import { HeatmapView, type HeatmapMetricKey } from './components/HeatmapView'

const defaultFilters: FilterState = {
  location_type: lockLocationTypeToBorough ? 'borough' : 'county',
  location_value: 'all',
  house_type: 'all',
  tenure: 'all',
  size_band: 'all',
  year_built_band: 'all',
}

function allOption(): DimensionIndexItem[] {
  return [{ dimension_name: '', value: 'all', label: 'All' }]
}

export default function App() {
  const [filters, setFilters] = useState<FilterState>(defaultFilters)
  const [locationValueOptions, setLocationValueOptions] = useState<DimensionIndexItem[]>(allOption())
  const [houseTypeOptions, setHouseTypeOptions] = useState<DimensionIndexItem[]>(allOption())
  const [tenureOptions, setTenureOptions] = useState<DimensionIndexItem[]>(allOption())
  const [sizeBandOptions, setSizeBandOptions] = useState<DimensionIndexItem[]>(allOption())
  const [yearBuiltBandOptions, setYearBuiltBandOptions] = useState<DimensionIndexItem[]>(allOption())
  const [heatmapCells, setHeatmapCells] = useState<HeatmapCell[]>([])
  const [heatmapMetric, setHeatmapMetric] = useState<HeatmapMetricKey>('avg_appreciation_pounds')
  const [heatmapLoading, setHeatmapLoading] = useState(false)
  const [dimensionsLoading, setDimensionsLoading] = useState(true)

  const loadDimension = useCallback(async (name: string) => {
    try {
      const res = await getDimensionValues(name)
      return res.values
    } catch {
      return allOption()
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    setDimensionsLoading(true)
    Promise.all([
      loadDimension(filters.location_type),
      loadDimension('house_type'),
      loadDimension('tenure'),
      loadDimension('size_band'),
      loadDimension('year_built_band'),
    ]).then(([loc, house, tenure, size, year]) => {
      if (cancelled) return
      setLocationValueOptions(loc)
      setHouseTypeOptions(house)
      setTenureOptions(tenure)
      setSizeBandOptions(size)
      setYearBuiltBandOptions(year)
      setDimensionsLoading(false)
    })
    return () => {
      cancelled = true
    }
  }, [filters.location_type, loadDimension])

  const prevLocationTypeRef = useRef(filters.location_type)
  useEffect(() => {
    if (prevLocationTypeRef.current !== filters.location_type) {
      prevLocationTypeRef.current = filters.location_type
      setFilters((prev) => ({ ...prev, location_value: 'all' }))
    }
  }, [filters.location_type])

  useEffect(() => {
    let cancelled = false
    setHeatmapLoading(true)
    getHeatmap(filters)
      .then((res) => {
        if (!cancelled) {
          setHeatmapCells(res.cells)
        }
      })
      .catch(() => {
        if (!cancelled) setHeatmapCells([])
      })
      .finally(() => {
        if (!cancelled) setHeatmapLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [filters.location_type, filters.location_value, filters.house_type, filters.tenure, filters.size_band, filters.year_built_band])

  const handleFiltersChange = useCallback((f: FilterState) => {
    setFilters((prev) => {
      const next = { ...f }
      if (lockLocationTypeToBorough) next.location_type = 'borough'
      return next
    })
  }, [])

  const disclaimer =
    'This heatmap is for illustrative purposes only. The data and figures may be inaccurate and should not be relied upon for financial decisions. This is not financial advice or a price recommendation. You should consult a qualified financial advisor before making any financial decisions. The author accepts no liability for any loss or damage resulting from the use of this tool. The code and methodology used to compile this data can be found at https://github.com/LeoRickayzen.'

  return (
    <Container
      maxWidth="xl"
      sx={{
        height: '100vh',
        display: 'flex',
        flexDirection: 'column',
        py: 2,
        boxSizing: 'border-box',
        overflow: 'hidden',
      }}
    >
      <Box sx={{ flexShrink: 0, mb: 1.5 }}>
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontStyle: 'italic' }}>
          {disclaimer}
        </Typography>
      </Box>
      <Box sx={{ display: 'flex', flexDirection: 'row', gap: 3, flex: 1, minHeight: 0 }}>
        <Paper
          variant="outlined"
          sx={{
            p: 2,
            flexShrink: 0,
            minWidth: 200,
          }}
        >
          <Typography variant="subtitle2" color="text.secondary" sx={{ mb: 2 }}>
            Filters
          </Typography>
          <FilterBar
            filters={filters}
            onFiltersChange={handleFiltersChange}
            locationValueOptions={locationValueOptions}
            houseTypeOptions={houseTypeOptions}
            tenureOptions={tenureOptions}
            sizeBandOptions={sizeBandOptions}
            yearBuiltBandOptions={yearBuiltBandOptions}
            loading={dimensionsLoading}
            lockLocationTypeToBorough={lockLocationTypeToBorough}
          />
        </Paper>
        <Box sx={{ flex: 1, minWidth: 0, minHeight: 0 }}>
          <HeatmapView
            cells={heatmapCells}
            metric={heatmapMetric}
            onMetricChange={setHeatmapMetric}
            loading={heatmapLoading}
          />
        </Box>
      </Box>
    </Container>
  )
}
