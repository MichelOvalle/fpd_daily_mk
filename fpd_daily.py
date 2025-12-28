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
    # SQL: 
    # 1. Convertimos fpd2 a binario (1 o 0).
    # 2. Forzamos el formato de cosecha a %Y%m (ej. 202510).
    # 3. Filtramos madurez de 2 meses sobre fecha_apertura.
    query = """
    WITH base_datos AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito
        FROM 'fpd_gemini.parquet'
    ),
    agregado AS (
        SELECT 
            strftime(fecha_dt, '%Y%m') as cosecha_id, -- Formato 202510
            COUNT(id_credito) as total_casos,
            SUM(fpd2_num) as fpd2_si
        FROM base_datos
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        cosecha_id,
        total_casos,
        fpd2_si,
        (fpd2_si * 100.0 / total_casos) as fpd2_rate
    FROM agregado
    ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- TÍTULO PRINCIPAL ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Análisis de Cosechas en formato **YYYYMM**")

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
            # KPIs de la última cosecha disponible
            ult = df.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Última Cosecha (YYYYMM)", ult['cosecha_id'])
            k2.metric("Total Créditos", f"{int(ult['total_casos']):,}")
            k3.metric("Tasa FPD2 (%)", f"{ult['fpd2_rate']:.2f}%")

            st.markdown("### Tendencia Histórica de FPD2")
            
            # GRÁFICA PROFESIONAL
            fig = go.Figure()

            # Serie de datos con área sombreada
            fig.add_trace(go.Scatter(
                x=df['cosecha_id'], 
                y=df['fpd2_rate'],
                mode='lines+markers+text',
                text=df['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                textposition="top center",
                line=dict(color='#1B4F72', width=4),
                marker=dict(size=8, color='#1B4F72'),
                fill='tozeroy',
                fillcolor='rgba(27, 79, 114, 0.1)',
                name='Tasa FPD2'
            ))

            # Ajustes del eje X para que respete el formato YYYYMM
            fig.update_layout(
                hovermode="x unified",
                plot_bgcolor='white',
                height=500,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis=dict(
                    title="Cosecha (AñoMes)",
                    type='category', # Forzamos a que trate 202510 como etiqueta
                    showgrid=False,
                    linecolor='black'
                ),
                yaxis=dict(
                    title="Incumplimiento (%)",
                    ticksuffix="%",
                    gridcolor='#F0F0F0',
                    zeroline=False
                )
            )

            st.plotly_chart(fig, use_container_width=True)
            
            # Tabla de apoyo
            with st.expander("Ver tabla de datos"):
                st.dataframe(df.sort_values('cosecha_id', ascending=False), use_container_width=True)
            
        else:
            st.warning("No se encontraron cosechas maduras (antigüedad > 2 meses).")

    except Exception as e:
        st.error(f"Error al procesar el formato YYYYMM: {e}")

# --- PESTAÑAS VACÍAS ---
with tab2:
    pass

with tab3:
    pass

with tab4:
    pass