import {
  Box,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  type SelectChangeEvent,
} from '@mui/material'
import type { DimensionIndexItem } from '../types'

function LockIcon({ sx }: { sx?: object }) {
  return (
    <Box
      component="svg"
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 24 24"
      fill="currentColor"
      sx={{ fontSize: 16, opacity: 0.8, ...sx }}
    >
      <path d="M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zm-6 9c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9 2-2 2zm3.1-9H8.9V6c0-1.71 1.39-3.1 3.1-3.1 1.71 0 3.1 1.39 3.1 3.1v2z" />
    </Box>
  )
}

export const LOCATION_TYPES = ['county', 'postcode', 'borough'] as const

export interface FilterState {
  location_type: string
  location_value: string
  house_type: string
  tenure: string
  size_band: string
  year_built_band: string
}

export interface FilterBarProps {
  filters: FilterState
  onFiltersChange: (f: FilterState) => void
  locationValueOptions: DimensionIndexItem[]
  houseTypeOptions: DimensionIndexItem[]
  tenureOptions: DimensionIndexItem[]
  sizeBandOptions: DimensionIndexItem[]
  yearBuiltBandOptions: DimensionIndexItem[]
  loading?: boolean
  /** When true, location type is locked to borough and greyed out (from VITE_LOCK_LOCATION_TYPE_TO_BOROUGH). */
  lockLocationTypeToBorough?: boolean
}

export function FilterBar({
  filters,
  onFiltersChange,
  locationValueOptions,
  houseTypeOptions,
  tenureOptions,
  sizeBandOptions,
  yearBuiltBandOptions,
  loading = false,
  lockLocationTypeToBorough = false,
}: FilterBarProps) {
  const update = (key: keyof FilterState, value: string) => {
    onFiltersChange({ ...filters, [key]: value })
  }

  const select = (
    label: string,
    value: string,
    options: DimensionIndexItem[],
    key: keyof FilterState,
    comingSoon?: boolean
  ) => (
    <FormControl
      size="small"
      sx={{ minWidth: 180 }}
      disabled={loading || comingSoon}
    >
      <InputLabel>
        {comingSoon ? (
          <Box component="span" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}>
            {label}
            <LockIcon sx={{ fontSize: 16, opacity: 0.8 }} />
          </Box>
        ) : (
          label
        )}
      </InputLabel>
      <Select
        label={label}
        value={value}
        onChange={(e: SelectChangeEvent<string>) => update(key, e.target.value)}
      >
        {options.map((opt) => (
          <MenuItem key={opt.value} value={opt.value}>
            {opt.label ?? opt.value}
          </MenuItem>
        ))}
      </Select>
    </FormControl>
  )

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
      <FormControl
        size="small"
        sx={{ minWidth: 180 }}
        disabled={loading || lockLocationTypeToBorough}
      >
        <InputLabel>
          {lockLocationTypeToBorough ? (
            <Box component="span" sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}>
              Location type
              <LockIcon sx={{ fontSize: 16, opacity: 0.8 }} />
            </Box>
          ) : (
            'Location type'
          )}
        </InputLabel>
        <Select
          label="Location type"
          value={lockLocationTypeToBorough ? 'borough' : filters.location_type}
          onChange={(e: SelectChangeEvent<string>) =>
            update('location_type', e.target.value)
          }
        >
          {LOCATION_TYPES.map((t) => (
            <MenuItem key={t} value={t}>
              {t}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      {select('Location', filters.location_value, locationValueOptions, 'location_value')}
      {select('House type', filters.house_type, houseTypeOptions, 'house_type')}
      {select('Tenure', filters.tenure, tenureOptions, 'tenure')}
      {select('Size band', filters.size_band, sizeBandOptions, 'size_band', true)}
      {select('Year built', filters.year_built_band, yearBuiltBandOptions, 'year_built_band', true)}
    </Box>
  )
}
