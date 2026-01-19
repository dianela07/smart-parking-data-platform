import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

st.set_page_config(page_title="Basel Parking Dashboard", layout="wide")
st.title("Basel Parking Availability Dashboard")

# Cargar datos procesados
df = pd.read_csv("data/processed/Basel_parking.csv")
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Sidebar: seleccionar garage
garage = st.sidebar.selectbox("Select garage", df['parking_name'].unique())

# Filtrar datos del garage
garage_df = df[df['parking_name'] == garage]

# Mostrar datos históricos
st.subheader(f"Historical Occupancy - {garage}")
st.line_chart(garage_df.set_index('timestamp')['occupied'])

# Predicción próxima hora
next_hour = datetime.now() + timedelta(hours=1)
payload = {"garage": garage, "datetime": next_hour.isoformat()}

try:
    response = requests.post("http://127.0.0.1:8000/predict", json=payload)
    pred_data = response.json()
    st.subheader("Prediction for next hour")
    st.write(pred_data)
except:
    st.error("API not running. Start FastAPI server first.")
