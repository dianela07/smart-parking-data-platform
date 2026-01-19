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
streamlit run src/visualization/dashboard.py
