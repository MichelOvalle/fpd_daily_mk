import streamlit as st
import pandas as pd
import plotly.express as px

# Configuración de la página
st.set_page_config(page_title="FPD Daily - Local", layout="wide")

st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Analizando el comportamiento de la variable **FPD2**")

# Función para cargar datos con caché para que sea veloz
@st.cache_data
def load_data():
    df = pd.read_parquet("fpd_gemini.parquet")
    df['fecha_apertura'] = pd.to_datetime(df['fecha_apertura'])
    # Creamos la cosecha (Año-Mes)
    df['cosecha'] = df['fecha_apertura'].dt.to_period('M').astype(str)
    return df

try:
    df = load_data()

    # --- Lógica de Cosechas ---
    vintage_df = df.groupby('cosecha').agg(
        total_creditos=('id_credito', 'count'),
        casos_fpd2=('fpd2', 'sum')
    ).reset_index()

    vintage_df['fpd2_rate'] = (vintage_df['casos_fpd2'] / vintage_df['total_creditos']) * 100

    # --- Gráfica de Cosechas ---
    fig = px.line(
        vintage_df, 
        x='cosecha', 
        y='fpd2_rate',
        markers=True,
        text=vintage_df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
        title="<b>Evolución de Tasa FPD2 por Cosecha</b>",
        labels={'cosecha': 'Mes de Apertura', 'fpd2_rate': '% FPD2'}
    )
    
    fig.update_traces(textposition="top center")

    # Mostrar en Streamlit
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Datos de Cosecha")
        st.dataframe(vintage_df.sort_values('cosecha', ascending=False), hide_index=True)

except Exception as e:
    st.error(f"Asegúrate de que 'fpd_gemini.parquet' esté en la misma carpeta. Error: {e}")