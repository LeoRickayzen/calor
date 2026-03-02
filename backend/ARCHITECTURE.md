# Backend architecture: DynamoDB data model

This document describes the DynamoDB tables and key design used by the UK house price performance API. Data is **pre-aggregated** by an offline Spark job; the API only reads.

---

## Overview

- **house_price_performance**: Pre-aggregated metrics per location and property dimensions (county, postcode, borough × house type, tenure, size band, year built). One item per cell; each item stores pre-computed **line_graph** (time series by year of sale) and **heatmap_graph** (year_bought × year_sold grid) as JSON.
- **dimension_index**: Filter metadata for the UI (list of counties, house types, etc.).

Planned but not yet implemented in this backend: **street_performance** (sparse street-level aggregates) and **property_sales** (transaction-level for on-demand aggregation).

---

## Key conventions

- **Composite key delimiter**: `#`. Segment values must not contain `#`.
- **Normalisation**: Keys use **lowercase** for segment values (location_value, house_type, tenure, etc.). Street names single-spaced. In dimension_index, postcode labels are displayed uppercase (e.g. `N5`); the stored key and sk are lowercase (`n5`). The API normalises location_value to lowercase for county/postcode/borough so requests match stored keys.
- **Filter “all” option**: For every dimension filter (house_type, tenure, size_band, year_built_band), the API accepts the literal value `all` to mean “no filter” (aggregate across that dimension). The dimensions endpoint includes an `all` entry in each dimension’s value list so UIs can show “All” in dropdowns and send `all` when the user has not selected a specific value.

---

## Table: house_price_performance

**Purpose**: Store pre-aggregated price metrics per (location, property dimensions). Supports county, postcode prefix, and London borough. Filled by the Spark ETL; the API scans by location prefix and filters by dimensions in memory. Each item holds pre-computed **line_graph** and **heatmap_graph** JSON for that cell.

### Key structure

One item per (location, dimensions). No sort key; no GSI. Year-of-sale and year-bought axes are stored inside the item as JSON, not in keys.

- **Partition key (pk)**: `location_type#location_value#house_type#tenure#size_band#year_built_band`
  - All six segments are required. Segment order and rules:
  - `location_type`: `county` | `postcode` | `borough` (code also reserves `street` for future use)
  - `location_value`: Lowercase; county (e.g. `greater london`), postcode prefix (e.g. `n5`), or borough (e.g. `islington`). API accepts any case and normalises for lookup.
  - `house_type`: `flat` | `terraced` | `detached` | `semi_detached` | `bungalow` | `other`
  - `tenure`: `freehold` | `leasehold`
  - `size_band`: Floor area band, e.g. `50_75`, `75_100` (use underscores)
  - `year_built_band`: e.g. `pre_1900`, `1990_1999`

### Attributes (value schema)

Dimension fields are in the partition key only; they are **not** stored in the item value. The stored value contains only the two pre-computed series plus an optional summary field.

| Attribute | Type | Description |
|-----------|------|-------------|
| line_graph | List (JSON) | Time series by year of sale; see structure below. |
| heatmap_graph | List (JSON) | Grid of year_bought × year_sold cells; see structure below. |
| sale_count | Number | Optional; total sales in this cell (for display/ordering). |

#### line_graph (JSON)

One entry per year of sale. Simpler than the heatmap because it only groups by year sold.

```json
[
  { "year_sold": "2019", "avg_price": 280000, "median_price": 265000, "mode_price": 250000, "sale_count": 120 },
  { "year_sold": "2020", "avg_price": 295000, "median_price": 280000, "mode_price": 270000, "sale_count": 145 },
  { "year_sold": "2021", "avg_price": 320000, "median_price": 305000, "mode_price": 290000, "sale_count": 138 }
]
```

#### heatmap_graph (JSON)

One entry per (year_bought, year_sold) cell. Used for heatmap views (e.g. purchase year vs sale year).

```json
[
  { "year_bought": "2018", "year_sold": "2020", "avg_price": 310000, "median_price": 295000, "sale_count": 42, "pct_sales_at_loss": 5.2 },
  { "year_bought": "2018", "year_sold": "2021", "avg_price": 335000, "median_price": 320000, "sale_count": 38, "pct_sales_at_loss": 2.1 },
  { "year_bought": "2019", "year_sold": "2021", "avg_price": 328000, "median_price": 312000, "sale_count": 55, "pct_sales_at_loss": 3.0 }
]
```

Additional optional fields per heatmap cell (e.g. `mode_price`, `min_price`, `max_price`, `mean_pct_appreciation`, `pct_sales_at_loss` — % of sales in that cell sold at a loss) can be included if the ETL writes them.

*Note*: In DynamoDB, store these as List (JSON); numeric values as Number (use `Decimal` in Python with boto3 resource `put_item`).

### Access patterns

- **By location (API)**: Scan with `FilterExpression: begins_with(pk, location_type#location_value#)`; filter in memory by house_type, tenure, size_band, year_built_band. Return items (with line_graph / heatmap_graph) or merge series when multiple items match (e.g. aggregate across dimensions).
- **By full key (ETL / point lookup)**: GetItem with `pk = location_type#location_value#house_type#tenure#size_band#year_built_band`.

---

## Table: dimension_index

**Purpose**: Populate filter dropdowns (counties, house types, tenure, etc.). Written by the Spark job or a one-off loader.

### Key structure

- **Partition key (pk)**: `meta#dimension_name`
  - `dimension_name`: e.g. `county`, `borough`, `postcode`, `house_type`, `tenure`, `size_band`, `year_built_band`, `year_bought_band`

- **Sort key (sk)**: The dimension value as stored elsewhere (e.g. `Greater London`, `flat`, `2020_2021`)

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| label | String | Optional display label |
| sale_count | Number | Optional; for ordering or display |

### Access pattern

- **Query**: `pk = meta#dimension_name` to list all values for that dimension (e.g. all counties).

---

## Dimensionality and pre-aggregation

- **Property dimensions**: house_type × tenure × size_band × year_built_band → on the order of thousands of combinations per location. Time axes (year bought, year sold) live inside each item’s line_graph and heatmap_graph.
- **Location**: One partition per (location_type, location_value). County has ~90 values; postcode prefix ~2.6k; borough ~33. Only (location, dimension) cells with at least one sale are stored (sparse).
- **Street-level** is not pre-aggregated in this table (combinatorial explosion). The design reserves optional tables: **street_performance** (sparse aggregates with fewer dimensions) or **property_sales** (transaction-level) for on-demand aggregation by the API.

---

## API usage

- **GET /api/performance**: Scan with `begins_with(pk, location_type#location_value#)`; filter by request params in memory; return list of items (each with line_graph and heatmap_graph).
- **GET /api/performance/line**: Same query; return the stored **line_graph** from the matching item(s). If multiple items match (e.g. no dimension filters), merge their series by year_sold; otherwise return the single item’s line_graph (already grouped by year of sale).
- **GET /api/performance/heatmap**: Same query; return the stored **heatmap_graph** from the matching item(s). Structure is an array of `{ year_bought, year_sold, avg_price, median_price, ... }`; front end can render the 2D grid from this.
- **GET /api/dimensions/{dimension_name}**: Query `dimension_index` with `pk = meta#{dimension_name}`; return list of values, with an `all` entry first for filter dropdowns.

---

## Plan: filter and location values from DynamoDB

**Goal**: Store only the **values** (counties, house types, tenure options, etc.) in DynamoDB and expose them via the API. **Types** (location_type and dimension names) stay hardcoded for now.

- **Types (hardcoded in code)**  
  - **Location type**: `county` | `postcode` | `borough` (and optionally `street`). The API knows this fixed list.  
  - **Dimension names**: `house_type`, `tenure`, `size_band`, `year_built_band`. The API knows this fixed list for filter dropdowns and performance query params.

- **Values (from DynamoDB)**  
  - **dimension_index** holds one partition per type: `pk = meta#county`, `meta#borough`, `meta#postcode` for location values; `pk = meta#house_type`, `meta#tenure`, `meta#size_band`, `meta#year_built_band` for filter values.  
  - ETL (or a loader) writes the actual values as items with `sk` = value (e.g. `Greater London`, `flat`, `50_75`).  
  - **GET /api/dimensions/{dimension_name}** queries `pk = meta#{dimension_name}` and returns the list of values, with `all` prepended for dropdowns. The `dimension_name` path segment is one of the hardcoded type names.

So: types are fixed in the API; values are read from the tables and exposed as-is. When the ETL adds a new county or house type, it only needs to write to **dimension_index**; the existing dimensions endpoint will expose it.

---

## Table creation (local / test)

The app can create the tables if they do not exist (e.g. for DynamoDB Local). See `app.db.tables.ensure_tables`. It creates:

- **house_price_performance**: pk (S) only, pay-per-request. API scans with `begins_with(pk, location#)` and filters in memory.
- **dimension_index**: pk (S), sk (S), pay-per-request.
