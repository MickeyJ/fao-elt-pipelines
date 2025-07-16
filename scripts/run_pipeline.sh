#!/bin/bash
# Run the complete FAO ELT pipeline
# Make executable with: chmod +x run_pipeline.sh

echo "🚀 Starting FAO ELT Pipeline..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Check PostgreSQL connection
echo "🔍 Checking database connection..."
python -c "
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    conn.close()
    print('✅ Database connection successful')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    exit(1)
"

# Initialize database schemas
echo "🗄️ Initializing database schemas..."
PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sql/create_schemas.sql

# Run the pipeline
echo "🔄 Running ELT pipeline..."
python orchestration/elt_pipeline.py

echo "✅ Pipeline completed!"
echo ""
echo "📊 View your data:"
echo "psql -U $DB_USER -d $DB_NAME -c 'SELECT * FROM gold.gold_country_metrics ORDER BY total_production_metric_tons DESC LIMIT 10;'"