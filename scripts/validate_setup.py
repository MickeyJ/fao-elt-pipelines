"""Validate the project setup before running pipeline."""

import os
import sys
from pathlib import Path
import importlib.util


def check_python_version():
    """Check if Python version is 3.8+"""
    print("🐍 Checking Python version...", end=" ")
    if sys.version_info >= (3, 8):
        print(f"✅ {sys.version.split()[0]}")
        return True
    else:
        print(f"❌ {sys.version.split()[0]} (3.8+ required)")
        return False


def check_env_file():
    """Check if .env file exists and has required variables."""
    print("📄 Checking .env file...", end=" ")
    if not Path(".env").exists():
        print("❌ Not found")
        print("   → Copy .env.example to .env and update credentials")
        return False

    # Check for required variables
    from dotenv import dotenv_values

    config = dotenv_values(".env")
    required = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    missing = [var for var in required if not config.get(var)]

    if missing:
        print(f"❌ Missing variables: {', '.join(missing)}")
        return False

    if config.get("DB_PASSWORD") == "your_password_here":
        print("❌ DB_PASSWORD not updated")
        print("   → Update DB_PASSWORD in .env file")
        return False

    print("✅ Found and configured")
    return True


def check_directory_structure():
    """Check if all required directories exist."""
    print("📁 Checking directory structure...", end=" ")
    required_dirs = [
        "ingestion",
        "dbt_project/models/bronze",
        "dbt_project/models/silver",
        "dbt_project/models/gold",
        "dbt_project/macros",
        "orchestration",
        "sql",
    ]

    missing = [d for d in required_dirs if not Path(d).exists()]
    if missing:
        print(f"❌ Missing directories: {', '.join(missing)}")
        return False

    print("✅ All directories present")
    return True


def check_required_files():
    """Check if key files exist."""
    print("📄 Checking required files...", end=" ")
    required_files = [
        "requirements.txt",
        "sql/create_schemas.sql",
        "ingestion/api_client.py",
        "ingestion/load_to_postgres.py",
        "orchestration/elt_pipeline.py",
        "dbt_project/dbt_project.yml",
        "dbt_project/profiles.yml",
    ]

    missing = [f for f in required_files if not Path(f).exists()]
    if missing:
        print(f"❌ Missing files: {', '.join(missing[:3])}...")
        return False

    print("✅ All files present")
    return True


def check_database_connection():
    """Test PostgreSQL connection."""
    print("🗄️  Checking database connection...", end=" ")
    try:
        import psycopg2
        from dotenv import load_dotenv

        load_dotenv()

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.close()
        print("✅ Connected successfully")
        return True
    except ImportError:
        print("❌ psycopg2 not installed")
        print("   → Run: pip install -r requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Connection failed")
        print(f"   → Error: {str(e)}")
        return False


def check_api_connectivity():
    """Test API connectivity."""
    print("🌐 Checking API connectivity...", end=" ")
    try:
        import requests

        response = requests.get("https://kw2aqt7p3p.us-west-2.awsapprunner.com/v1/prices/prices/?limit=1", timeout=5)
        if response.status_code == 200:
            print("✅ API accessible")
            return True
        else:
            print(f"❌ API returned status {response.status_code}")
            return False
    except ImportError:
        print("❌ requests not installed")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def main():
    """Run all validation checks."""
    print("🔍 FAO ELT Pipeline - Setup Validation\n")

    checks = [
        check_python_version(),
        check_env_file(),
        check_directory_structure(),
        check_required_files(),
        check_database_connection(),
        check_api_connectivity(),
    ]

    print(f"\n{'='*50}")
    passed = sum(checks)
    total = len(checks)

    if passed == total:
        print(f"✅ All checks passed ({passed}/{total})")
        print("\nYour environment is ready! Run:")
        print("  python orchestration/elt_pipeline.py")
    else:
        print(f"❌ Some checks failed ({passed}/{total} passed)")
        print("\nPlease fix the issues above before running the pipeline.")
        sys.exit(1)


if __name__ == "__main__":
    main()
