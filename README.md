# Urban Parking Availability Prediction – Basel

## Project Overview
End-to-end data platform for real-time urban parking availability analysis and prediction, focused on **Basel, Switzerland**.

This project ingests parking data from public APIs, processes and stores it in PostgreSQL, applies machine learning models to predict future parking occupancy, and exposes results via REST APIs and an interactive Streamlit dashboard. The system is containerized with Docker for reproducibility.

## Features
- Data ingestion from **Basel public parking API**
- Data cleaning and processing with Pandas & NumPy
- Storage in PostgreSQL with reproducible pipelines
- Exploratory data analysis and visualization
- Predictive modeling using Random Forest
- API design with FastAPI
- Interactive dashboard with Streamlit
- Dockerized deployment for development and testing

## Skills Demonstrated
- **Data Engineering:** ingestion, cleaning, SQL modeling, pipelines
- **Machine Learning:** feature engineering, model training, predictions
- **Backend:** FastAPI, PostgreSQL, REST APIs
- **Visualization:** Matplotlib, Seaborn, Streamlit dashboards
- **DevOps:** Docker, environment variables, modular architecture

## Installation & Usage
```bash
# Clone repository
git clone <your-repo-url>
cd smart-parking-data-platform

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Fetch Basel data
python src/ingestion/fetch_parking_data.py --city Basel

# Process data
python src/processing/process_parking_data.py --city Basel

# Run FastAPI server
uvicorn src.api.app:app --reload

# Run dashboard
streamlit run src/visualization/dashboard.py```

## Database Architecture

The platform uses SQLAlchemy ORM with support for both PostgreSQL (production) and SQLite (development).

### Tables

| Table | Description |
|-------|-------------|
| `parking_locations` | Master table of parking facilities (id, city, name, capacity, coords, address) |
| `raw_parking_data` | Raw JSON responses from APIs for auditing |
| `processed_parking_data` | Cleaned, ML-ready parking records |
| `predictions` | Model predictions with timestamps and model version |
| `model_metadata` | Model training history (version, MAE, RMSE, R2, training date) |

### Configuration

```bash
# Development (default): Uses SQLite at data/parking.db
# No configuration needed

# Production: Set environment variable
export DATABASE_URL=postgresql://user:password@host:5432/parking_db
```

### Migration

To migrate existing CSV data to the database:
```bash
python src/database/migrate_data.py
```

### Key Commands

```bash
# Update data (fetches fresh data and saves to CSV + DB)
python src/ingestion/update_data.py

# Train model (reads from DB, saves metadata to DB)
python src/ml/train_model.py

# Start API server (logs predictions to DB)
uvicorn src.api.app:app --reload

# Start dashboard (reads from DB with CSV fallback)
streamlit run src/visualization/dashboard.py
```

## Project Structure

```
smart-parking-data-platform/
├── data/
│   ├── historical/          # Time-series CSV backups
│   ├── processed/           # Current state CSVs
│   ├── raw/                 # Raw JSON files
│   └── parking.db           # SQLite database (dev)
├── models/
│   └── basel_parking_model.pkl
├── src/
│   ├── api/                 # FastAPI endpoints
│   ├── database/            # SQLAlchemy models & repository
│   ├── ingestion/           # Data fetching scripts
│   ├── ml/                  # Model training
│   ├── processing/          # Data transformation
│   └── visualization/       # Streamlit dashboard
├── docker/
├── notebooks/
├── tests/
├── requirements.txt
└── README.md
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API info + model version |
| `/predict` | POST | Predict occupancy for garage at datetime |
| `/health` | GET | Service health check |

### Example Request

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"garage": "Steinen", "datetime": "2026-01-20T14:00:00"}'
```

## Technologies

- **Python 3.12**
- **SQLAlchemy 2.0** + PostgreSQL/SQLite
- **FastAPI** + Uvicorn
- **Streamlit** + Pydeck
- **scikit-learn** (RandomForest)
- **Pandas** + NumPy
- **Docker** (optional)