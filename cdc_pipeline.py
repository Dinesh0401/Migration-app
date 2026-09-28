import sys
from pathlib import Path

# Add data-engineering-service to path
service_dir = Path(__file__).resolve().parent / "data-engineering-service"
if str(service_dir) not in sys.path:
    sys.path.insert(0, str(service_dir))

from cdc_pipeline import main

if __name__ == "__main__":
    main()
