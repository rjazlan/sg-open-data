from pathlib import Path

# Set root directory
ROOT_DIR = Path(__file__).parent.parent.absolute()
DATA_DIR = ROOT_DIR / "data"
CONFIG_DIR = ROOT_DIR / "config"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
CONFIG_DIR.mkdir(exist_ok=True)
(DATA_DIR / "raw").mkdir(exist_ok=True)
(DATA_DIR / "processed").mkdir(exist_ok=True)
