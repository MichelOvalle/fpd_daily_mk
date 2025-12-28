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

# 2. Función para procesar datos con DuckDB
@st.cache_data
def get_data_fpd2():
    # SQL: Filtramos meses con madurez (Mes actual - 2 meses)
    # Convertimos fpd2 a entero para poder sumarlo
    query = """
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            TRY_CAST(fpd2 AS INTEGER) as fpd2_val,
            id_credito
        FROM 'fpd_gemini.parquet'
    ),
    filtrado AS (
        SELECT * FROM base 
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
    ),
    agrupado AS (
        SELECT 
            strftime(fecha_dt, '%Y-%m') as mes_cosecha,
            COUNT(id_credito) as total_casos,
            SUM(fpd2_val) as fpd2_si
        FROM filtrado
        WHERE fecha_dt IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        mes_cosecha,
        total_casos,
        fpd2_si,
        (fpd2_si * 100.0 / total_casos) as fpd2_rate
    FROM agrupado
    ORDER BY mes_cosecha ASC
    """
    return duckdb.query(query).to_df()

# --- TÍTULO PRINCIPAL ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Seguimiento de calidad de cartera - **Métrica FPD2**")

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
            # KPIs en la parte superior
            ult_mes = df.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Última Cosecha", ult_mes['mes_cosecha'])
            k2.metric("Casos Evaluados", f"{int(ult_mes['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ult_mes['fpd2_rate']:.2f}%")

            st.markdown("### Tendencia Histórica de FPD2")
            st.caption("Nota: Se excluyen los últimos 2 meses para asegurar la madurez del indicador.")

            # CREACIÓN DE LA GRÁFICA ESTILO VINTAGE (Área sombreada)
            fig = go.Figure()

            # Añadir la línea con área rellena
            fig.add_trace(go.Scatter(
                x=df['mes_cosecha'], 
                y=df['fpd2_rate'],
                mode='lines+markers+text',
                name='Tasa FPD2',
                text=df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                textposition="top center",
                line=dict(color='#1A5276', width=4),
                marker=dict(size=10, color='#1A5276', symbol='circle'),
                fill='tozeroy',
                fillcolor='rgba(26, 82, 118, 0.1)' # Azul muy tenue
            ))

            # Ajustes de diseño de la gráfica
            fig.update_layout(
                hovermode="x unified",
                plot_bgcolor='white',
                margin=dict(l=20, r=20, t=20, b=20),
                height=500,
                xaxis=dict(
                    title="Mes de Originación (Cosecha)",
                    showgrid=False,
                    linecolor='black'
                ),
                yaxis=dict(
                    title="Porcentaje FPD2",
                    ticksuffix="%",
                    gridcolor='#f0f0f0',
                    zeroline=False
                )
            )

            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.warning("No hay suficientes datos históricos para mostrar la tendencia (mínimo 2 meses de antigüedad requeridos).")

    except Exception as e:
        st.error(f"Error en el procesamiento: {e}")

# --- PESTAÑAS RESTANTES (Vacías como solicitaste) ---
with tab2:
    pass

with tab3:
    pass

with tab4:
    pass