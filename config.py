from pathlib import Path

# --- Project Directories ---
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "mutual_funds.db"

# --- External Data Sources ---
AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"

# --- Analysis Parameters ---
RISK_FREE_RATE = 0.07

# --- Setup Directories ---
ROOT_DIR.mkdir(parents=True, exist_ok=True) 
