import streamlit as st
import pandas as pd
import plotly.express as px

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

# 2. Función para procesar datos de la Pestaña 1 con DuckDB
@st.cache_data
def get_resumen_general():
    # SQL con filtro: fecha_apertura <= (Mes Actual - 2 Meses)
    # strptime convierte el texto DD/MM/YYYY a Fecha
    # fpd2_num asegura que sumemos números
    query = """
    WITH base AS (
        SELECT 
            id_credito,
            TRY_CAST(fpd2 AS INTEGER) as fpd2_num,
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt
        FROM 'fpd_gemini.parquet'
    ),
    filtrado AS (
        SELECT * FROM base 
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
    ),
    cosechas AS (
        SELECT 
            strftime(fecha_dt, '%Y-%m') as mes_cosecha,
            count(id_credito) as total_casos,
            sum(fpd2_num) as fpd2_si
        FROM filtrado
        WHERE fecha_dt IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        *,
        (fpd2_si * 100.0 / total_casos) as fpd2_rate
    FROM cosechas
    ORDER BY mes_cosecha ASC
    """
    return duckdb.query(query).to_df()

# --- TÍTULO ---
st.title("📊 FPD Daily: Monitor de Riesgo")
st.markdown("Dashboard automatizado para el seguimiento de indicadores de cartera.")

# 3. Creación de las 4 pestañas
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Resumen General", 
    "🍇 Análisis de Cosechas", 
    "🏢 Por Sucursal", 
    "📋 Detalle de Datos"
])

# --- CONTENIDO DE LA PESTAÑA 1 ---
with tab1:
    st.header("Resumen de Tendencia FPD2")
    st.write("Indicador: `fpd2=1 / total_casos` | Filtro: Cosechas con madurez (>2 meses)")

    try:
        # Obtener los datos filtrados
        df_resumen = get_resumen_general()

        if not df_resumen.empty:
            # Métricas rápidas (KPIs) del último mes disponible
            ultimo_mes = df_resumen.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Última Cosecha Evaluada", ultimo_mes['mes_cosecha'])
            k2.metric("Total Casos", f"{int(ultimo_mes['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ultimo_mes['fpd2_rate']:.2f}%")

            # Gráfica de tendencia
            fig = px.line(
                df_resumen, 
                x='mes_cosecha', 
                y='fpd2_rate',
                markers=True,
                text=df_resumen['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                title="<b>Evolución Histórica de FPD2 (Cosechas Maduras)</b>",
                labels={'mes_cosecha': 'Mes de Apertura', 'fpd2_rate': '% FPD2'},
                template='plotly_white'
            )
            
            fig.update_traces(textposition="top center", line=dict(width=4, color='#2E86C1'))
            fig.update_layout(yaxis_ticksuffix="%", hovermode="x unified")
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Nota sobre el filtro
            st.caption(f"Nota: La gráfica excluye los meses posteriores a {df_resumen['mes_cosecha'].max()} por falta de madurez en el dato.")
        else:
            st.warning("No se encontraron datos que cumplan con el criterio de antigüedad (> 2 meses).")

    except Exception as e:
        st.error(f"Error al cargar la Pestaña 1: {e}")

# --- PESTAÑAS VACÍAS (Listas para desarrollo) ---
with tab2:
    st.header("Análisis de Cosechas (Vintage)")
    st.info("Próximamente: Vista matricial de maduración de cartera.")

with tab3:
    st.header("Desempeño por Sucursal")
    st.info("Próximamente: Comparativa de riesgo regional.")

with tab4:
    st.header("Explorador de Datos")
    st.info("Próximamente: Tabla interactiva y filtros por ID.")