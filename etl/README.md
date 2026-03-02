# ETL: Price Paid → Spark → (key, value) CSVs → DynamoDB

Process UK Land Registry Price Paid Data (PPD) with Spark into two-column CSVs, then load into DynamoDB.

## Setup

Use a virtual environment and install dependencies (from repo root or from `etl/`):

```bash
cd etl
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Requires **Java** (e.g. OpenJDK 11+) for Spark. PySpark uses `JAVA_HOME` if set.

**Using SDKMAN:** install a JDK and use it in the same shell before running the job:

```bash
sdk list java
sdk install java 21.0.2-tem    # or another 11+ build
sdk use java 21.0.2-tem
# then run the ETL (see below)
```

Run the Spark job with `spark-submit` (from your Spark install) or, with the venv activated, `python etl/job/etl.py` (uses PySpark from the venv; Spark driver runs in local mode).

## Input

Place raw PPD CSV(s) under **`etl/input/ppd/`**. The Spark job reads from this path (override in `etl/conf/etl_config.yaml` or env). **The CSV must not have a header row**; columns are read by position. Expected column order: `transaction_id`, `price`, `date_of_transfer`, `postcode`, `property_type`, `duration`, `paon`, `saon`, `street`, `locality`, `town`, `district`, `county`. For the **heatmap** (repeat-sales by buy/sell year), address fields (paon, saon, street, postcode) are used to group by property; the line graph uses all sales regardless.

## 1. Run the Spark job

From the repo root, with PySpark available:

```bash
# Optional: set input/output paths (defaults in conf/etl_config.yaml)
export ETL_INPUT_PATH=etl/input/ppd
export ETL_OUTPUT_PATH=etl/output

spark-submit etl/job/etl.py
```

Or run with local Spark (e.g. `python etl/job/etl.py` if PySpark is on PYTHONPATH and Spark is configured). The job writes **key,value** CSVs under `etl/output/house_price_performance/` and `etl/output/dimension_index/`.

## 2. Load CSVs into DynamoDB

With DynamoDB Local (or AWS) and tables created, from the repo root **with the etl venv activated**:

```bash
source etl/.venv/bin/activate   # Windows: etl\.venv\Scripts\activate
python etl/scripts/load_csv_to_dynamodb.py
```

No env or args required: it reads from `etl/output/` and uses the backend default DynamoDB endpoint (`http://localhost:8000`). Set `HOUSES_DYNAMODB_ENDPOINT_URL=""` for real AWS, or to another URL for a different local port.

## Line graph vs heatmap

- **Line graph:** Aggregates **all sales** by segment and **year_sold** only. One point per year: average price and count of sales in that year for the segment.
- **Heatmap:** Aggregates **repeat sales** only (same property sold more than once). Each cell is (year_bought, year_sold): average price and count of properties that were bought (previous sale) in that buy year and sold in that sell year. Requires address fields (PAON, SAON, Street, Postcode) to group sales by property; if absent, heatmap is empty.

## PPD-only behaviour

- **size_band** and **year_built_band** are set to `all` (no EPC data).

## Config

- **etl/conf/etl_config.yaml**: `input_path`, `output_path` for the Spark job.
