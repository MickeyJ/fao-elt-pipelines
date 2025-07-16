"""Quick start script to set up and run the FAO ELT pipeline."""

import os
import sys
import subprocess
from pathlib import Path


def main():
    """Set up and run the complete pipeline."""
    print("🚀 FAO ELT Pipeline Quick Start\n")

    # Check Python version
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ is required")
        sys.exit(1)

    print("✅ Python version OK")

    # Check if .env exists
    if not Path(".env").exists():
        print("\n❌ .env file not found!")
        print("Please create a .env file with your database credentials.")
        print("You can copy from .env.example if available.")
        sys.exit(1)

    print("✅ Environment file found")

    # Create virtual environment if it doesn't exist
    if not Path("venv").exists():
        print("\n📦 Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)

    # Determine pip path based on OS
    if os.name == "nt":  # Windows
        pip_path = Path("venv/Scripts/pip")
        python_path = Path("venv/Scripts/python")
    else:  # Unix/Linux/Mac
        pip_path = Path("venv/bin/pip")
        python_path = Path("venv/bin/python")

    # Install requirements
    print("\n📦 Installing dependencies...")
    subprocess.run([str(pip_path), "install", "-r", "requirements.txt"], check=True)

    # Run the pipeline
    print("\n🔄 Running ELT pipeline...")
    print("This will:")
    print("  1. Extract data from FAO APIs")
    print("  2. Load raw data to PostgreSQL (Bronze layer)")
    print("  3. Transform data through Silver and Gold layers using dbt")
    print("  4. Run data quality tests")
    print("  5. Generate documentation\n")

    try:
        subprocess.run([str(python_path), "orchestration/elt_pipeline.py"], check=True)

        print("\n✅ Pipeline completed successfully!")
        print("\n📊 Verifying results...")

        # Run verification
        subprocess.run([str(python_path), "verify_pipeline.py"], check=True)

    except subprocess.CalledProcessError as e:
        print(f"\n❌ Pipeline failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check your PostgreSQL connection in .env")
        print("2. Ensure PostgreSQL is running")
        print("3. Check the logs above for specific errors")
        sys.exit(1)

    print("\n🎉 All done! Your medallion architecture is ready.")
    print("\nNext steps:")
    print("1. Explore the data in PostgreSQL")
    print("2. View dbt documentation: cd dbt_project && dbt docs serve")
    print("3. Modify the pipeline for your needs")


if __name__ == "__main__":
    main()
