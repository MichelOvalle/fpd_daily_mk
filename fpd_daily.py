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
    # - Transformamos fpd2 a numérico (1 si es 'FPD', 0 si no).
    # - Formateamos la cosecha a YYYYMM.
    # - Filtramos por madurez de 2 meses.
    query = """
    WITH base_datos AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito,
            origen2
        FROM 'fpd_gemini.parquet'
    ),
    agregado_detalle AS (
        SELECT 
            strftime(fecha_dt, '%Y%m') as cosecha_id,
            origen2,
            COUNT(id_credito) as total_casos,
            SUM(fpd2_num) as fpd2_si
        FROM base_datos
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT 
        *,
        (fpd2_si * 100.0 / total_casos) as fpd2_rate
    FROM agregado_detalle
    ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- TÍTULO PRINCIPAL ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Comparativa de Calidad de Cartera: **Total vs. Por Origen** (Cosechas YYYYMM)")

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
        df_detalle = get_data_fpd2()

        if not df_detalle.empty:
            # Calculamos el total agrupado para la primera gráfica
            df_total = df_detalle.groupby('cosecha_id').agg({
                'total_casos': 'sum',
                'fpd2_si': 'sum'
            }).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])

            # KPIs de la última cosecha (Total)
            ult = df_total.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Última Cosecha", ult['cosecha_id'])
            k2.metric("Créditos Colocados", f"{int(ult['total_casos']):,}")
            k3.metric("Tasa FPD2 Total", f"{ult['fpd2_rate']:.2f}%")

            st.divider()

            # --- COLUMNAS PARA LAS GRÁFICAS ---
            col_izq, col_der = st.columns(2)

            with col_izq:
                st.subheader("Tendencia Global")
                fig_total = go.Figure()
                fig_total.add_trace(go.Scatter(
                    x=df_total['cosecha_id'], 
                    y=df_total['fpd2_rate'],
                    mode='lines+markers+text',
                    text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                    textposition="top center",
                    line=dict(color='#1B4F72', width=4),
                    marker=dict(size=8),
                    fill='tozeroy',
                    fillcolor='rgba(27, 79, 114, 0.1)',
                    name='Total'
                ))
                fig_total.update_layout(
                    xaxis=dict(type='category', title="Cosecha"),
                    yaxis=dict(title="Tasa FPD2 (%)", ticksuffix="%"),
                    plot_bgcolor='white', height=450, margin=dict(l=10, r=10, t=30, b=10)
                )
                st.plotly_chart(fig_total, use_container_width=True)

            with col_der:
                st.subheader("Desglose por Origen")
                # Gráfica comparativa usando Plotly Express para manejar las líneas por color
                fig_origen = px.line(
                    df_detalle, 
                    x='cosecha_id', 
                    y='fpd2_rate', 
                    color='origen2',
                    markers=True,
                    text=df_detalle['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                    labels={'cosecha_id': 'Cosecha', 'fpd2_rate': '% FPD2', 'origen2': 'Origen'},
                    color_discrete_map={'fisico': '#2E86C1', 'digital': '#CB4335'}
                )
                fig_origen.update_traces(textposition="top center", line=dict(width=3))
                fig_origen.update_layout(
                    xaxis=dict(type='category'),
                    yaxis=dict(ticksuffix="%"),
                    plot_bgcolor='white', height=450, margin=dict(l=10, r=10, t=30, b=10)
                )
                st.plotly_chart(fig_origen, use_container_width=True)
            
            # Tabla detallada opcional
            with st.expander("Ver tabla comparativa"):
                st.dataframe(df_detalle.sort_values(['cosecha_id', 'origen2'], ascending=[False, True]), use_container_width=True)
            
        else:
            st.warning("No se encontraron datos maduros.")

    except Exception as e:
        st.error(f"Error al generar la comparativa por origen: {e}")

# --- OTRAS PESTAÑAS ---
with tab2: pass
with tab3: pass
with tab4: pass