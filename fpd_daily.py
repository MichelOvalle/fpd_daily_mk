import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Intentar importar duckdb
try:
    import duckdb
except ImportError:
    st.error("Falta la librería 'duckdb'. Instálala ejecutando: pip install duckdb")
    st.stop()

# 1. Configuración de la página
st.set_page_config(
    page_title="FPD Daily - Dashboard",
    layout="wide",
    page_icon="📊"
)

# 2. Función para procesar datos con DuckDB usando la variable 'cosecha' del archivo
@st.cache_data
def get_data_fpd2():
    # Usamos la columna 'cosecha' existente en el parquet
    # Filtramos por fecha_apertura para asegurar la madurez de 2 meses
    query = """
    SELECT 
        cosecha, 
        COUNT(id_credito) as total_casos, 
        SUM(TRY_CAST(fpd2 AS INTEGER)) as fpd2_si
    FROM 'fpd_gemini.parquet'
    WHERE TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) <= (CURRENT_DATE - INTERVAL 2 MONTH)
    GROUP BY cosecha
    ORDER BY cosecha ASC
    """
    df = duckdb.query(query).to_df()
    
    # Calcular la tasa fpd2 = 1 / total_casos
    if not df.empty:
        df['fpd2_rate'] = (df['fpd2_si'] * 100.0 / df['total_casos'])
    
    return df

# --- TÍTULO PRINCIPAL ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Seguimiento de calidad de cartera - **Variable: Cosecha**")

# 3. Creación de las 4 pestañas
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Resumen General", 
    "🍇 Análisis de Cosechas", 
    "🏢 Por Sucursal", 
    "📋 Detalle de Datos"
])

# --- CONTENIDO DE LA PESTAÑA 1 ---
with tab1:
    try:
        df = get_data_fpd2()

        if not df.empty:
            # Indicadores Clave (KPIs) de la última cosecha disponible
            ult_registro = df.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Última Cosecha", ult_registro['cosecha'])
            k2.metric("Total Casos", f"{int(ult_registro['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ult_registro['fpd2_rate']:.2f}%")

            st.markdown("### Tendencia de FPD2 por Cosecha")
            st.caption("Filtro aplicado: Fecha de apertura ≤ Mes actual - 2 meses.")

            # CREACIÓN DE LA GRÁFICA PROFESIONAL (Estilo Área)
            fig = go.Figure()

            # Añadimos la serie de datos
            fig.add_trace(go.Scatter(
                x=df['cosecha'], 
                y=df['fpd2_rate'],
                mode='lines+markers+text',
                text=df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                textposition="top center",
                line=dict(color='#1A5276', width=4),
                marker=dict(size=8, color='#1A5276'),
                fill='tozeroy', # Relleno hacia el eje X
                fillcolor='rgba(26, 82, 118, 0.12)', # Color azul tenue para el área
                name='Tasa FPD2'
            ))

            # Configuración estética de la gráfica
            fig.update_layout(
                hovermode="x unified",
                plot_bgcolor='white',
                height=500,
                margin=dict(l=20, r=20, t=20, b=20),
                xaxis=dict(
                    title="Cosecha",
                    showgrid=False,
                    linecolor='black'
                ),
                yaxis=dict(
                    title="Porcentaje (%)",
                    ticksuffix="%",
                    gridcolor='#f0f0f0',
                    zeroline=False
                )
            )

            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.warning("No se encontraron datos para los criterios seleccionados.")

    except Exception as e:
        st.error(f"Error al procesar la variable 'cosecha': {e}")

# --- PESTAÑAS RESTANTES (Vacias) ---
with tab2:
    pass

with tab3:
    pass

with tab4:
    pass