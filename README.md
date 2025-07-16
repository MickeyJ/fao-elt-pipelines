# FAO Data ELT Pipeline - Medallion Architecture Demo

A complete ELT (Extract, Load, Transform) pipeline demonstrating the medallion architecture pattern using FAO (Food and Agriculture Organization) data. This project uses only free, open-source tools.

## 🏗️ Architecture Overview

### Medallion Layers
1. **Bronze Layer**: Raw JSON data from FAO APIs stored as-is in PostgreSQL
2. **Silver Layer**: Cleaned, validated, and standardized data
3. **Gold Layer**: Business-ready aggregations and analytics

### Tech Stack (All Free)
- **Database**: PostgreSQL
- **Ingestion**: Python (requests, pandas)
- **Transformation**: dbt-core
- **Orchestration**: Prefect (free tier)
- **Containerization**: Docker (optional)

## 📊 Data Sources

- **Prices API**: Producer prices for agricultural products by country/year
- **Food Balance API**: Production data for food items by country/year

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL (already configured based on your credentials)
- Git

### Setup Instructions

1. **Clone and setup the project**
```bash
# Create project directory
mkdir fao-elt-pipeline
cd fao-elt-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

2. **Configure environment variables**
```bash
# Copy the .env file and update with your PostgreSQL password
cp .env.example .env
# Edit .env and set your DB_PASSWORD
```

3. **Initialize the database**
```bash
# If using existing PostgreSQL
psql -U mickey -d fao -f sql/create_schemas.sql

# Or using Docker
docker-compose up -d
```

4. **Run the pipeline**
```bash
# Run the complete ELT pipeline
python orchestration/elt_pipeline.py
```

## 📁 Project Structure

```
fao-elt-pipeline/
├── ingestion/              # Bronze layer data extraction
├── dbt_project/           # Silver & Gold transformations
│   ├── models/
│   │   ├── bronze/       # Source definitions
│   │   ├── silver/       # Data cleaning & standardization
│   │   └── gold/         # Business aggregations
├── orchestration/         # Prefect pipeline orchestration
└── sql/                   # Database setup scripts
```

## 🔄 Pipeline Flow

1. **Extract**: Fetch data from FAO APIs
   - Prices data (producer prices)
   - Food balance sheets (production data)

2. **Load**: Store raw JSON in PostgreSQL bronze layer
   - `bronze.raw_prices`
   - `bronze.raw_food_balance`

3. **Transform**: Process through medallion layers using dbt
   - **Silver**: Clean data, standardize country names, add quality flags
   - **Gold**: Create business metrics, regional summaries, price-production analysis

## 📈 Gold Layer Outputs

### 1. Country Metrics (`gold.gold_country_metrics`)
- Total production by country
- Average prices
- Production trends (Growing/Declining/Stable)
- Producer categories (Major/Medium/Small)

### 2. Price-Production Analysis (`gold.gold_price_production_analysis`)
- Market value calculations
- Price volatility metrics
- Commodity classifications

### 3. Regional Summary (`gold.gold_regional_summary`)
- Aggregated metrics by world regions
- Regional production trends
- Market value by region

## 🛠️ Development

### Running Individual Components

```bash
# Extract data only
python -c "from ingestion.api_client import FAOApiClient; client = FAOApiClient('https://kw2aqt7p3p.us-west-2.awsapprunner.com/v1'); print(len(client.fetch_prices_data(1)))"

# Run dbt models only
cd dbt_project
dbt run

# Run dbt tests
dbt test

# Generate dbt documentation
dbt docs generate
dbt docs serve
```

### Monitoring with Prefect

```bash
# Start Prefect server (optional, for UI)
prefect server start

# In another terminal, run the pipeline
python orchestration/elt_pipeline.py
```

## 📊 Sample Queries

```sql
-- Top 10 producing countries
SELECT * FROM gold.gold_country_metrics
ORDER BY total_production_metric_tons DESC
LIMIT 10;

-- Most valuable commodities
SELECT * FROM gold.gold_price_production_analysis
ORDER BY total_market_value DESC
LIMIT 10;

-- Regional comparison
SELECT * FROM gold.gold_regional_summary
ORDER BY total_production_metric_tons DESC;
```

## 🔧 Configuration Options

Edit `orchestration/elt_pipeline.py` to adjust:
- `max_pages_prices`: Number of pages to fetch from prices API
- `max_pages_food_balance`: Number of pages to fetch from food balance API
- `truncate_bronze`: Whether to clear bronze tables before loading
- `run_tests`: Whether to run dbt tests
- `generate_docs`: Whether to generate dbt documentation

## 📝 Notes

- This is a demo project with limited data fetching (2-3 pages per API)
- For production, consider:
  - Incremental loading strategies
  - Better error handling and monitoring
  - Data partitioning for large datasets
  - Proper secrets management
  - Cloud deployment (e.g., Prefect Cloud, dbt Cloud)

## 🤝 Contributing

Feel free to extend this demo with:
- Additional data sources
- More sophisticated transformations
- Data quality tests
- Visualization layer
- API endpoints for serving gold data

## 📜 License

This project is for demonstration purposes. The FAO data is publicly available.