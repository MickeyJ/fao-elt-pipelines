#!/bin/bash
# Load environment variables and run dbt command
set -a  # automatically export all variables
source .env
set +a
cd dbt_project && dbt "$@"