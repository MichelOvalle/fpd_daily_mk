import streamlit as st
import duckdb
import plotly.express as px

# 1. Configuración de la página
st.set_page_config(page_title="FPD Daily - DuckDB Edition", layout="wide", page_icon="🦆")

# 2. Función para procesar datos con DuckDB
@st.cache_data
def get_data_with_duckdb():
    # Usamos DuckDB para leer el parquet y procesar la fecha y las métricas en un solo paso SQL
    # La función strptime maneja el formato DD/MM/YYYY que causaba error antes
    query = """
    WITH base AS (
        SELECT 
            id_credito,
            fpd2,
            monto_otorgado,
            -- Intentamos convertir la fecha manejando el formato día/mes/año
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
    # Ejecutamos la consulta y convertimos el resultado a un DataFrame de Pandas para Streamlit
    df = duckdb.query(query).to_df()
    return df

# --- INICIO DE LA APP ---
st.title("🦆 FPD Daily: Dashboard (Powered by DuckDB)")
st.markdown("Análisis de **Cosechas FPD2** procesado con motor SQL de alta velocidad.")

try:
    # Obtener los datos ya procesados desde SQL
    vintage_df = get_data_with_duckdb()

    # 3. Métricas Principales (KPIs)
    total_creditos = int(vintage_df['total_creditos'].sum())
    total_fpd2 = int(vintage_df['casos_fpd2'].sum())
    tasa_global = (total_fpd2 / total_creditos) * 100

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total Créditos", f"{total_creditos:,}")
    kpi2.metric("Total Casos FPD2", f"{total_fpd2:,}")
    kpi3.metric("Tasa Global FPD2", f"{tasa_global:.2f}%")

    st.divider()

    # 4. Visualización
    col_grafica, col_tabla = st.columns([2, 1])

    with col_grafica:
        fig = px.line(
            vintage_df, 
            x='cosecha', 
            y='fpd2_rate',
            markers=True,
            text=vintage_df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
            title="📈 Tendencia de Tasa FPD2 por Cosecha",
            labels={'cosecha': 'Mes de Apertura', 'fpd2_rate': '% Tasa FPD2'},
            template='plotly_white'
        )
        fig.update_traces(textposition="top center", line=dict(width=3, color='#0083B0'))
        st.plotly_chart(fig, use_container_width=True)

    with col_tabla:
        st.subheader("📋 Detalle de Cosechas")
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
    st.error(f"❌ Error al procesar con DuckDB: {e}")

st.caption("FPD Daily | DuckDB + Streamlit | Datos: fpd_gemini.parquet")