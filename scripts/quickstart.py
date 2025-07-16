"""Quick start script to set up and run the FAO ELT pipeline."""

import os
import subprocess
import sys
from pathlib import Path


def detect_environment():
    """Detect if we're in conda or venv, or need to create one."""
    # Check if already in a conda environment
    if os.environ.get("CONDA_DEFAULT_ENV"):
        print(f"✅ Using conda environment: {os.environ.get('CONDA_DEFAULT_ENV')}")
        return "conda", sys.executable, "pip"

    # Check if already in a virtual environment
    if os.environ.get("VIRTUAL_ENV"):
        print(f"✅ Using virtual environment: {os.environ.get('VIRTUAL_ENV')}")
        return "venv", sys.executable, "pip"

    # Check if venv exists but not activated
    if Path("venv").exists():
        print("❌ Virtual environment exists but not activated!")
        print("Please activate it first:")
        print("  source venv/bin/activate  (macOS/Linux)")
        print("  venv\\Scripts\\activate     (Windows)")
        sys.exit(1)

    # No environment found
    return None, None, None


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

    # Detect environment
    env_type, python_path, pip_cmd = detect_environment()

    if not env_type:
        print("\n❌ No environment detected!")
        print("Please either:")
        print(
            "  1. Create and activate a conda environment: conda create -n fao python=3.10 && conda activate fao"
        )
        print("  2. Create and activate a venv: python -m venv venv && source venv/bin/activate")
        sys.exit(1)

    # Install requirements
    print("\n📦 Installing dependencies...")
    subprocess.run([pip_cmd, "install", "-r", "requirements.txt"], check=True)

    # Run the pipeline
    print("\n🔄 Running ELT pipeline...")
    print("This will:")
    print("  1. Extract data from FAO APIs")
    print("  2. Load raw data to PostgreSQL (Bronze layer)")
    print("  3. Transform data through Silver and Gold layers using dbt")
    print("  4. Run data quality tests")
    print("  5. Generate documentation\n")

    try:
        subprocess.run([python_path, "orchestration/elt_pipeline.py"], check=True)

        print("\n✅ Pipeline completed successfully!")

        # Note: verify_pipeline.py doesn't exist, so removing this check

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
