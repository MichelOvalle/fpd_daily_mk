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
    # Nueva lógica de SQL:
    # Si fpd2 es 'FPD' entonces 1, de lo contrario 0.
    # Usamos la columna 'cosecha' del archivo para el agrupamiento.
    query = """
    WITH base_datos AS (
        SELECT 
            cosecha,
            id_credito,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt
        FROM 'fpd_gemini.parquet'
    )
    SELECT 
        cosecha,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si
    FROM base_datos
    WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
    GROUP BY cosecha
    ORDER BY cosecha ASC
    """
    df = duckdb.query(query).to_df()
    
    # Calcular la tasa fpd2_rate
    if not df.empty:
        df['fpd2_rate'] = (df['fpd2_si'] * 100.0 / df['total_casos'])
    
    return df

# --- TÍTULO PRINCIPAL ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Cálculo de **FPD2** (Tratando 'FPD' como 1 y vacíos como 0)")

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
            # Indicadores Clave (KPIs) de la última cosecha evaluada
            ult_registro = df.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Última Cosecha", ult_registro['cosecha'])
            k2.metric("Créditos Colocados", f"{int(ult_registro['total_casos']):,}")
            k3.metric("Tasa FPD2 (%)", f"{ult_registro['fpd2_rate']:.2f}%")

            st.markdown("### Evolución de la Tasa FPD2 por Cosecha")
            st.caption("Filtro: Madurez mínima de 2 meses aplicada a las fechas de apertura.")

            # GRÁFICA PROFESIONAL
            fig = go.Figure()

            # Línea con área sombreada
            fig.add_trace(go.Scatter(
                x=df['cosecha'], 
                y=df['fpd2_rate'],
                mode='lines+markers+text',
                text=df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                textposition="top center",
                line=dict(color='#2E86C1', width=4),
                marker=dict(size=9, color='#1B4F72', symbol='circle'),
                fill='tozeroy',
                fillcolor='rgba(46, 134, 193, 0.1)',
                name='Tasa FPD2'
            ))

            # Diseño de la gráfica
            fig.update_layout(
                hovermode="x unified",
                plot_bgcolor='white',
                height=500,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(
                    title="Cosecha (Mes de Apertura)",
                    showgrid=False,
                    linecolor='black'
                ),
                yaxis=dict(
                    title="Incumplimiento (%)",
                    ticksuffix="%",
                    gridcolor='#F2F3F4',
                    zeroline=False
                )
            )

            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.warning("No hay datos disponibles para el rango de fechas solicitado (Antigüedad > 2 meses).")

    except Exception as e:
        st.error(f"Error en el cálculo: {e}")

# --- PESTAÑAS VACÍAS (Para futuros desarrollos) ---
with tab2:
    pass

with tab3:
    pass

with tab4:
    pass