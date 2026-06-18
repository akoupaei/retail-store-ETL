# Retail Store ETL & Sales Forecasting Pipeline

A full end-to-end data engineering and machine learning pipeline for retail sales forecasting, built on **Databricks** using Delta Live Tables, XGBoost, MLflow, and Lakeview Dashboards.

This was developed as a final project for **Harvard CSCI E-103: Data Engineering for Analytics**.

---

## Project Overview

The pipeline ingests raw retail sales data, processes it through a **Bronze → Silver → Gold** medallion architecture, trains an XGBoost demand forecasting model, and serves predictions through interactive Databricks dashboards.

**Dataset:** Retail store sales records including daily sales by store and product family, store metadata, oil prices, holidays, and transaction counts — structured after the [Kaggle Store Sales Time Series Forecasting](https://www.kaggle.com/competitions/store-sales-time-series-forecasting) dataset.

---

## Architecture

```
Raw CSVs (Volume)
      │
      ▼
┌─────────────┐
│   BRONZE    │  Auto Loader streaming ingestion via Delta Live Tables
│  (01_bronze)│  sales_train_raw, sales_test_raw, stores_raw,
│             │  oil_prices_raw, holidays_raw, transactions_raw
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Cleansing, enrichment, and quality enforcement (DLT expectations)
│  (02_silver)│  sales, holidays, oil_prices, transactions
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Aggregated materialized views for BI and ML
│  (03_gold)  │  stores_daily_sales, product_family_daily_sales
└──────┬──────┘
       │
       ├──────────────────────────┐
       ▼                          ▼
┌─────────────┐          ┌──────────────────┐
│  ML Pipeline│          │  BI Dashboards   │
│  (XGBoost)  │          │  (Lakeview)      │
└─────────────┘          └──────────────────┘
```

---

## Repository Structure

```
retail-store-ETL/
├── retail_sales/                        # Databricks Asset Bundle (ETL pipeline)
│   ├── databricks.yml                   # Bundle config (dev/prod targets)
│   ├── pyproject.toml                   # Python project metadata & dependencies
│   ├── resources/
│   │   ├── retail_sales_etl.pipeline.yml  # DLT pipeline definition
│   │   └── sample_job.job.yml             # Orchestration job (daily schedule)
│   └── src/retail_sales_etl/
│       └── transformations/
│           ├── 01_bronze.py             # Raw ingestion layer
│           ├── 02_silver.py             # Cleansing & enrichment layer
│           └── 03_gold.py               # Aggregation & BI-ready layer
│
├── ml/                                  # Machine learning notebooks
│   ├── 01_feature_engineering.ipynb     # Lag features, rolling stats, encodings
│   ├── 02_model_developement.ipynb      # XGBoost training & MLflow tracking
│   └── 03_dashboard_prep.ipynb          # Future predictions for BI
│
└── BI/                                  # Databricks Lakeview dashboard definitions
    ├── Demand Forecasting Dashboard.lvdash.json
    └── Performance Dashboard.lvdash.json
```

---

## ETL Pipeline (`retail_sales/`)

The ETL pipeline is built as a **Databricks Asset Bundle** using **Delta Live Tables (DLT)** with serverless compute.

### Bronze Layer — `01_bronze.py`

Streams raw CSV files from a Unity Catalog Volume into Delta tables using Auto Loader (`cloudFiles`). Six tables are created:

| Table | Description |
|---|---|
| `sales_train_raw` | Daily sales per store and product family (training set) |
| `sales_test_raw` | Test set records (no sales, used for forecasting) |
| `stores_raw` | Store metadata: city, state, type, cluster |
| `oil_prices_raw` | Daily WTI crude oil prices |
| `holidays_raw` | National, regional, and local holiday events |
| `transactions_raw` | Daily transaction counts per store |

### Silver Layer — `02_silver.py`

Cleans and enriches the raw data. DLT data quality expectations (`@dp.expect_or_drop`) enforce schema integrity by dropping records with null keys.

| Table | Key Transformations |
|---|---|
| `sales` | Joined with store metadata; adds `day`, `month`, `year`, `day_of_week`, `week_of_year`, `is_weekend`, `has_promotion` |
| `holidays` | Adds boolean flags: `is_national_holiday`, `is_regional_holiday`, `is_local_holiday`, `is_holiday`, `is_event`, `is_special_day` |
| `oil_prices` | Selects `date` and `dcoilwtico`; drops rows missing either field |
| `transactions` | Validates non-negative transaction counts |

### Gold Layer — `03_gold.py`

Creates **materialized views** that join all silver tables into business-ready aggregates.

**`stores_daily_sales`** — Daily store-level metrics:
- `total_sales`, `avg_transaction_amount`, `avg_basket_size`
- `total_items_on_promotion`, `num_product_families`
- `avg_oil_price`, `is_national_holiday`, `is_weekend`, `daily_transactions`

**`product_family_daily_sales`** — Daily product-family metrics:
- `total_sales`, `avg_sales_per_store`, `num_stores_selling`
- `total_items_on_promotion`, `promotion_penetration`

### Orchestration

A Databricks Job (`sample_job.job.yml`) runs the full pipeline on a **daily schedule** with these sequential tasks:

1. `refresh_pipeline` — Triggers the DLT pipeline
2. `ml_feature_engineering` — Runs `01_feature_engineering.ipynb`
3. `model_development` — Runs `02_model_developement.ipynb`
4. `dashboard_prep` — Runs `03_dashboard_prep.ipynb`

---

## ML Pipeline (`ml/`)

### 1. Feature Engineering — `01_feature_engineering.ipynb`

Reads the `sales` silver table and engineers predictive features:

- **Lag features:** sales lagged by 1, 7, 14, 28, 30, 60, and 90 days (per store and product family)
- **Rolling statistics:** 7, 28, and 90-day rolling mean and standard deviation
- **Time features:** `day_of_week`, `day_of_month`, `is_weekend`
- **Frequency encodings:** for `store_type` and `product_family`

Output is saved to `cscie103_catalog.final_project.silver_ml_features`.

### 2. Model Development — `02_model_developement.ipynb`

Trains an **XGBoost** regressor with temporal train/validation/test splits:

| Split | Period |
|---|---|
| Train | Before 2017-01-01 |
| Validation | 2017-01-01 – 2017-07-31 |
| Test | 2017-08-01 onwards |

**XGBoost hyperparameters:**

```python
params = {
    "objective": "reg:squarederror",
    "tree_method": "hist",
    "max_depth": 10,
    "eta": 0.03,
    "subsample": 0.9,
    "colsample_bytree": 0.9,
}
```

**Model performance:**

| Metric | Value |
|---|---|
| Train RMSE | ~74.2 |
| Validation RMSE | ~243.4 |
| Validation MAE | ~54.5 |

The model is tracked and registered via **MLflow** as `store_sales_forecast_model`. Predictions are written to `cscie103_catalog.final_project.xgb_predictions`.

### 3. Dashboard Prep — `03_dashboard_prep.ipynb`

Generates future sales forecasts for the BI dashboards:

- Loads the test CSV and builds a full calendar spanning all store × product family combinations
- Forward-fills historical features (oil price, promotions, holidays, transactions)
- Engineers lag and rolling features across the extended timeline
- Applies the trained XGBoost model to produce `future_sales_predictions`

---

## BI Dashboards (`BI/`)

Two **Databricks Lakeview** dashboards are included as JSON definitions:

- **Demand Forecasting Dashboard** — Visualizes `future_sales_predictions` broken down by product family and store. Intended for top management with national-level views.
- **Performance Dashboard** — Visualizes historical store performance metrics from the gold layer.

To import: In Databricks, go to **Dashboards → Import** and upload the `.lvdash.json` files.

---

## Infrastructure

- **Platform:** Databricks (workspace: `https://dbc-f80ed02c-3092.cloud.databricks.com`)
- **Catalog:** Unity Catalog — `cscie103_catalog`
- **Schemas:** `final_project` (dev), `prod` (production)
- **Compute:** Serverless Delta Live Tables pipeline
- **IaC:** Databricks Asset Bundle (DAB)
- **Experiment tracking:** MLflow (experiment: `/Users/dak6610@g.harvard.edu/retail-sales-forecast`)

---

## Getting Started

### Prerequisites

- Databricks CLI installed and configured
- Access to the `cscie103_catalog` Unity Catalog
- Raw CSV files uploaded to `/Volumes/cscie103_catalog/final_project/data/`

### Deploy the ETL Pipeline

```bash
cd retail_sales

# Install dependencies
pip install -e ".[dev]"

# Deploy to dev
databricks bundle deploy --target dev

# Run the pipeline
databricks bundle run retail_sales_etl --target dev
```

### Run the ML Notebooks

Run the notebooks in order in the Databricks workspace, or trigger the full orchestration job:

```bash
databricks bundle run dev_dak6610_sample_job --target dev
```

### Import Dashboards

Upload the `.lvdash.json` files from the `BI/` folder into Databricks Dashboards.

---

## License

MIT License — see [LICENSE](LICENSE) for details.
