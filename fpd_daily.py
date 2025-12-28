import streamlit as st
import pandas as pd
import plotly.express as px

# Intentar importar duckdb y mostrar aviso si falta
try:
    import duckdb
except ImportError:
    st.error("Falta la librería 'duckdb'. Instálala ejecutando: pip install duckdb")
    st.stop()

# 1. Configuración de la página
st.set_page_config(page_title="FPD Daily - DuckDB", layout="wide", page_icon="🦆")

# 2. Función para procesar datos con DuckDB
@st.cache_data
def get_data_with_duckdb():
    # El SQL lee directamente del archivo parquet
    # strptime(fecha_apertura, '%d/%m/%Y') soluciona el error de formato de fecha
    query = """
    WITH base AS (
        SELECT 
            id_credito,
            fpd2,
            monto_otorgado,
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt
        FROM 'fpd_gemini.parquet'
    ),
    cosechas AS (
        SELECT 
            strftime(fecha_dt, '%Y-%m') as cosecha,
            count(id_credito) as total_creditos,
            sum(fpd2) as casos_fpd2,
            sum(monto_otorgado) as monto_total
        FROM base
        WHERE fecha_dt IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        *,
        (casos_fpd2 * 100.0 / total_creditos) as fpd2_rate
    FROM cosechas
    ORDER BY cosecha ASC
    """
    return duckdb.query(query).to_df()

# --- INTERFAZ DE USUARIO ---
st.title("📊 FPD Daily: Análisis de Cosechas")
st.markdown("Procesamiento de alta velocidad con **DuckDB** y visualización en **Streamlit**.")

try:
    # Obtener datos
    vintage_df = get_data_with_duckdb()

    # 3. Indicadores Clave (KPIs)
    total_creditos = int(vintage_df['total_creditos'].sum())
    total_fpd2 = int(vintage_df['casos_fpd2'].sum())
    tasa_global = (total_fpd2 / total_creditos) * 100

    k1, k2, k3 = st.columns(3)
    k1.metric("Créditos Totales", f"{total_creditos:,}")
    k2.metric("Casos FPD2", f"{total_fpd2:,}")
    k3.metric("Tasa FPD2 Promedio", f"{tasa_global:.2f}%")

    st.divider()

    # 4. Gráfica y Tabla
    col_left, col_right = st.columns([2, 1])

    with col_left:
        fig = px.line(
            vintage_df, 
            x='cosecha', 
            y='fpd2_rate',
            markers=True,
            text=vintage_df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
            title="📈 Evolución de Tasa FPD2 por Mes de Apertura",
            labels={'cosecha': 'Cosecha (Mes)', 'fpd2_rate': '% FPD2'},
            template='plotly_white'
        )
        fig.update_traces(textposition="top center", line=dict(width=3, color='#17becf'))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("📋 Resumen de Cosechas")
        st.dataframe(
            vintage_df.sort_values('cosecha', ascending=False),
            column_config={
                "cosecha": "Mes",
                "total_creditos": "Créditos",
                "casos_fpd2": "FPD2",
                "fpd2_rate": st.column_config.NumberColumn("% FPD2", format="%.2f%%")
            },
            hide_index=True,
            use_container_width=True
        )

except Exception as e:
    st.error(f"Se produjo un error al cargar los datos: {e}")
    st.info("Asegúrate de que 'fpd_gemini.parquet' esté en la misma carpeta que este script.")

st.caption("FPD Daily v2.0 | Motor SQL: DuckDB")