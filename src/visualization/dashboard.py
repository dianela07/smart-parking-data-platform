import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import ast
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Panel de estacionamientos - Basilea",
    page_icon="🅿️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────
# Constants & Mappings
# ─────────────────────────────────────────────────────────────
STATUS_MAP = {
    "offen": "Abierto",
    "zu": "Cerrado"
}

COLUMN_LABELS = {
    "parking_name": "Estacionamiento",
    "capacity": "Capacidad",
    "free_spaces": "Espacios libres",
    "occupied": "Ocupados",
    "occupancy_pct": "Ocupación %",
    "status": "Estado",
    "address": "Dirección",
    "lot_type": "Tipo",
    "timestamp": "Última actualización"
}

# ─────────────────────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .status-open { color: #00c853; font-weight: bold; }
    .status-closed { color: #ff1744; font-weight: bold; }
    .high-occupancy { background-color: #ffcdd2; }
    .low-occupancy { background-color: #c8e6c9; }
    
    /* Professional styling */
    .stMetric > div {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #1976d2;
    }
    
    h1, h2, h3 {
        color: #1a237e;
    }
    
    .sidebar .sidebar-content {
        background-color: #fafafa;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# Load Data
# ─────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    df = pd.read_csv("data/processed/Basel_parking.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Parse coordinates
    def parse_coords(coord_str):
        try:
            coords = ast.literal_eval(coord_str)
            return coords.get('lat'), coords.get('lon')
        except:
            return None, None
    
    df[['lat', 'lon']] = df['coords'].apply(lambda x: pd.Series(parse_coords(x)))
    
    # Map status to readable labels
    df['status_display'] = df['status'].map(STATUS_MAP).fillna(df['status'])
    
    return df


def update_data_from_api():
    """Ejecuta el script de actualización de datos."""
    import subprocess
    import sys
    
    python_exe = sys.executable
    script_path = "src/ingestion/update_data.py"
    
    result = subprocess.run(
        [python_exe, script_path],
        capture_output=True,
        text=True,
        cwd="."
    )
    
    return result.returncode == 0, result.stdout, result.stderr


def get_historical_stats():
    """Obtiene estadísticas del archivo histórico."""
    from pathlib import Path
    
    historical_file = Path("data/historical/Basel_parking_history.csv")
    if not historical_file.exists():
        return None
    
    hist_df = pd.read_csv(historical_file)
    hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])
    
    return {
        'total_records': len(hist_df),
        'unique_timestamps': hist_df['timestamp'].nunique(),
        'date_start': hist_df['timestamp'].min().strftime('%Y-%m-%d %H:%M'),
        'date_end': hist_df['timestamp'].max().strftime('%Y-%m-%d %H:%M'),
    }


df = load_data()

# ─────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("Estacionamientos Basilea")
    st.markdown("---")
    
    # Botón de actualización de datos
    st.subheader("Actualizar datos")
    if st.button("Obtener datos frescos", type="primary", use_container_width=True):
        with st.spinner("Descargando datos de la API..."):
            success, stdout, stderr = update_data_from_api()
            if success:
                st.success("Datos actualizados correctamente")
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Error al actualizar datos")
                if stderr:
                    st.code(stderr, language="text")
    
    # Mostrar estadísticas del histórico
    hist_stats = get_historical_stats()
    if hist_stats:
        st.caption(f"Histórico: {hist_stats['unique_timestamps']} capturas")
        st.caption(f"Desde: {hist_stats['date_start']}")
    
    st.markdown("---")
    
    # Create display options for status filter
    status_options = df['status'].unique()
    status_display_options = [STATUS_MAP.get(s, s) for s in status_options]
    
    # Filter by status
    selected_status_display = st.multiselect(
        "Filtrar por estado",
        options=status_display_options,
        default=status_display_options
    )
    
    # Convert display values back to original values
    reverse_status_map = {v: k for k, v in STATUS_MAP.items()}
    selected_status = [reverse_status_map.get(s, s) for s in selected_status_display]
    
    # Filter by occupancy
    occupancy_range = st.slider(
        "Rango de ocupación (%)",
        min_value=0,
        max_value=100,
        value=(0, 100)
    )
    
    st.markdown("---")
    st.markdown("**Fuente de datos**")
    st.markdown("[Basel Open Data](https://data.bs.ch)")

# Apply filters
df_filtered = df[
    (df['status'].isin(selected_status)) &
    (df['occupancy_pct'].fillna(0) >= occupancy_range[0]) &
    (df['occupancy_pct'].fillna(0) <= occupancy_range[1])
]

# ─────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────
st.title("Panel de disponibilidad de estacionamientos - Basilea")
st.markdown(f"**Última actualización:** {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')} | **Estacionamientos:** {len(df_filtered)}")

# ─────────────────────────────────────────────────────────────
# KPI Metrics
# ─────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

total_capacity = df_filtered['capacity'].sum()
total_free = df_filtered['free_spaces'].sum()
total_occupied = df_filtered['occupied'].sum()
avg_occupancy = df_filtered['occupancy_pct'].mean()

with col1:
    st.metric(
        label="Capacidad total",
        value=f"{int(total_capacity):,}",
        delta=None
    )

with col2:
    st.metric(
        label="Espacios libres",
        value=f"{int(total_free):,}",
        delta=f"{(total_free/total_capacity*100):.1f}% disponible" if total_capacity > 0 else "N/A"
    )

with col3:
    st.metric(
        label="Ocupados",
        value=f"{int(max(0, total_occupied)):,}",
        delta=None
    )

with col4:
    st.metric(
        label="Ocupación promedio",
        value=f"{avg_occupancy:.1f}%",
        delta="Alta" if avg_occupancy > 70 else "Normal",
        delta_color="inverse" if avg_occupancy > 70 else "off"
    )

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Main Content - Two Columns
# ─────────────────────────────────────────────────────────────
left_col, right_col = st.columns([2, 1])

with left_col:
    st.subheader("Ubicación de estacionamientos")
    
    # Prepare map data
    map_data = df_filtered[['lat', 'lon', 'parking_name', 'occupancy_pct']].dropna().copy()
    
    if not map_data.empty:
        # Color by occupancy (RGB)
        def get_color(occupancy):
            if occupancy > 70:
                return [220, 53, 69, 200]  # Red
            elif occupancy > 40:
                return [255, 193, 7, 200]  # Orange/Yellow
            else:
                return [40, 167, 69, 200]  # Green
        
        map_data['color'] = map_data['occupancy_pct'].apply(get_color)
        
        # Formatear el porcentaje para el tooltip
        map_data['occupancy_display'] = map_data['occupancy_pct'].apply(lambda x: f"{x:.1f}%")
        
        # Calculate center
        center_lat = map_data['lat'].mean()
        center_lon = map_data['lon'].mean()
        
        # PyDeck map with zoom controls
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_data,
            get_position=['lon', 'lat'],
            get_color='color',
            get_radius=80,
            pickable=True,
            auto_highlight=True,
        )
        
        view_state = pdk.ViewState(
            latitude=center_lat,
            longitude=center_lon,
            zoom=13,
            pitch=0,
        )
        
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "{parking_name}\nOcupación: {occupancy_display}"},
            map_style=None,  # Usar estilo por defecto sin Mapbox
        )
        
        st.pydeck_chart(deck, use_container_width=True)
        
        # Leyenda con colores visuales
        legend_html = '<div style="display: flex; justify-content: center; gap: 20px; margin-top: 10px;">'
        legend_html += '<span><span style="display: inline-block; width: 12px; height: 12px; background-color: #28a745; border-radius: 50%; margin-right: 5px;"></span>Baja (&lt;40%)</span>'
        legend_html += '<span><span style="display: inline-block; width: 12px; height: 12px; background-color: #ffc107; border-radius: 50%; margin-right: 5px;"></span>Media (40-70%)</span>'
        legend_html += '<span><span style="display: inline-block; width: 12px; height: 12px; background-color: #dc3545; border-radius: 50%; margin-right: 5px;"></span>Alta (&gt;70%)</span>'
        legend_html += '</div>'
        st.markdown(legend_html, unsafe_allow_html=True)
    else:
        st.warning("No hay datos de ubicación disponibles")

with right_col:
    st.subheader("Top 5 más disponibles")
    
    top_available = df_filtered.nlargest(5, 'free_spaces')[['parking_name', 'free_spaces', 'capacity']]
    
    for _, row in top_available.iterrows():
        pct = (row['free_spaces'] / row['capacity'] * 100) if row['capacity'] > 0 else 0
        st.progress(pct / 100, text=f"**{row['parking_name']}**: {int(row['free_spaces'])} libres / {int(row['capacity'])}")

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Occupancy Bar Chart
# ─────────────────────────────────────────────────────────────
st.subheader("Ocupación actual por estacionamiento")

chart_data = df_filtered[['parking_name', 'occupancy_pct', 'capacity']].dropna()
chart_data = chart_data.sort_values('occupancy_pct', ascending=True)

st.bar_chart(
    chart_data.set_index('parking_name')['occupancy_pct'],
    horizontal=True,
    height=400
)

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Detailed Table
# ─────────────────────────────────────────────────────────────
st.subheader("Información detallada del estacionamiento")

# Select garage for prediction
selected_garage = st.selectbox(
    "Seleccionar estacionamiento para predicción",
    options=df_filtered['parking_name'].unique()
)

# Display details
garage_data = df_filtered[df_filtered['parking_name'] == selected_garage].iloc[0]

detail_col1, detail_col2, detail_col3 = st.columns(3)

with detail_col1:
    st.markdown(f"""
    **{garage_data['parking_name']}**
    - Dirección: {garage_data['address']}
    - Tipo: {garage_data['lot_type']}
    """)

with detail_col2:
    status_display = STATUS_MAP.get(garage_data['status'], garage_data['status'])
    status_indicator = "●" if garage_data['status'] == 'offen' else "○"
    status_color = "green" if garage_data['status'] == 'offen' else "red"
    st.markdown(f"""
    **Estado Actual**
    - Estado: <span style="color:{status_color}">{status_indicator} {status_display}</span>
    - Capacidad: {int(garage_data['capacity']) if pd.notna(garage_data['capacity']) else 'N/A'}
    - Libres: {int(garage_data['free_spaces']) if pd.notna(garage_data['free_spaces']) else 'N/A'}
    """, unsafe_allow_html=True)

with detail_col3:
    occupancy = garage_data['occupancy_pct'] if pd.notna(garage_data['occupancy_pct']) else 0
    st.markdown(f"**Ocupación: {occupancy:.1f}%**")
    st.progress(min(max(occupancy / 100, 0), 1))

# ─────────────────────────────────────────────────────────────
# ML Prediction Section
# ─────────────────────────────────────────────────────────────
st.markdown("---")
st.subheader("Predicción de ocupación")

pred_col1, pred_col2 = st.columns([1, 2])

with pred_col1:
    pred_hours = st.slider("Predecir para las próximas N horas", 1, 12, 1)
    predict_button = st.button("Obtener Predicción", type="primary")

with pred_col2:
    if predict_button:
        predictions = []
        current_occupied = int(garage_data['occupied']) if pd.notna(garage_data['occupied']) else 0
        capacity = int(garage_data['capacity']) if pd.notna(garage_data['capacity']) else 0
        
        for h in range(1, pred_hours + 1):
            future_time = datetime.now() + timedelta(hours=h)
            payload = {"garage": selected_garage, "datetime": future_time.isoformat()}
            
            try:
                response = requests.post("http://127.0.0.1:8000/predict", json=payload, timeout=5)
                pred_data = response.json()
                
                if "error" not in pred_data:
                    pred_occupied = pred_data.get("predicted_occupied", "N/A")
                    pred_capacity = pred_data.get("capacity", capacity)
                    predictions.append({
                        "Hora": future_time.strftime("%H:%M"),
                        "Ocupados predichos": int(pred_occupied) if isinstance(pred_occupied, (int, float)) else pred_occupied,
                        "Capacidad": int(pred_capacity) if isinstance(pred_capacity, (int, float)) else pred_capacity
                    })
            except requests.exceptions.RequestException:
                st.error("API no disponible. Inicie el servidor FastAPI: `uvicorn src.api.app:app --reload`")
                break
        
        if predictions:
            pred_df = pd.DataFrame(predictions)
            
            # Mostrar tabla con formato
            st.dataframe(pred_df, use_container_width=True, hide_index=True)
            
            # Gráfico de predicción mejorado
            st.markdown("**Evolución de ocupación predicha**")
            
            # Verificar si hay variación en los datos
            pred_values = pred_df['Ocupados predichos'].tolist()
            has_variation = len(set(pred_values)) > 1
            
            if has_variation:
                # Crear datos para el gráfico con área
                chart_df = pred_df.set_index('Hora')[['Ocupados predichos']].copy()
                st.area_chart(chart_df, color=["#1976d2"])
            else:
                # Si no hay variación, mostrar como métrica
                st.info(f"Ocupación predicha constante: **{pred_values[0]}** espacios ocupados para las próximas {pred_hours} hora(s)")

# ─────────────────────────────────────────────────────────────
# Full Data Table
# ─────────────────────────────────────────────────────────────
with st.expander("Ver tabla completa de datos"):
    display_cols = ['parking_name', 'capacity', 'free_spaces', 'occupied', 'occupancy_pct', 'status_display', 'address']
    display_df = df_filtered[display_cols].copy()
    
    # Formatear columnas numéricas como enteros o mostrar "-" si es None
    for col in ['capacity', 'free_spaces', 'occupied']:
        display_df[col] = display_df[col].apply(lambda x: int(x) if pd.notna(x) else "-")
    
    # Formatear porcentaje con 1 decimal como string o "-" si es None
    display_df['occupancy_pct'] = display_df['occupancy_pct'].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "-")
    
    # Renombrar columnas a español
    display_df.columns = [COLUMN_LABELS.get(c, c) for c in display_cols]
    display_df = display_df.rename(columns={'status_display': 'Estado'})
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

# ─────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        Panel de estacionamientos Basilea | Datos: Basel Open Data Portal | 
        Desarrollado con Streamlit + FastAPI + ML
    </div>
    """,
    unsafe_allow_html=True
)
