import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="FPD Daily Dashboard", layout="wide")

st.title("📊 FPD Daily: Análisis de Cosechas")
st.markdown("Monitoreo de indicadores **FPD2** por mes de apertura.")

@st.cache_data
def load_data():
    df = pd.read_parquet("fpd_gemini.parquet")
    df['fecha_apertura'] = pd.to_datetime(df['fecha_apertura'])
    df['cosecha_mes'] = df['fecha_apertura'].dt.to_period('M').astype(str)
    return df

data = load_data()

# --- Cálculos ---
vintage_df = data.groupby('cosecha_mes').agg(
    total_creditos=('id_credito', 'count'),
    casos_fpd2=('fpd2', 'sum')
).reset_index()

vintage_df['fpd2_rate'] = (vintage_df['casos_fpd2'] / vintage_df['total_creditos']) * 100

# --- Visualización ---
fig = px.line(
    vintage_df, x='cosecha_mes', y='fpd2_rate',
    markers=True, text=vintage_df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
    title="Evolución de Tasa FPD2 por Cosecha"
)

st.plotly_chart(fig, use_container_width=True)

# --- Tabla de Datos ---
st.subheader("Detalle de Cosechas")
st.dataframe(vintage_df, use_container_width=True)