import requests
import json
from pathlib import Path
from datetime import datetime

# Configuración de la ciudad
CITY = "Aarhus"
API_URL = "https://admin.opendata.dk/api/action/datastore_search?resource_id=2a82a145-0195-4081-a13c-b0e587e9b89c"

# Carpeta para guardar los datos crudos
RAW_DIR = Path(f"data/raw/{CITY}")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def fetch_data():
    print(f"Fetching data for {CITY}...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        
        # Guardar JSON crudo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = RAW_DIR / f"parking_{timestamp}.json"
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"Saved raw data for {CITY} at {raw_file}")
        return raw_file

    except requests.HTTPError as e:
        print(f"HTTP error for {CITY}: {e}")
    except requests.RequestException as e:
        print(f"Request error for {CITY}: {e}")
    except Exception as e:
        print(f"Error fetching {CITY}: {e}")

if __name__ == "__main__":
    fetch_data()
