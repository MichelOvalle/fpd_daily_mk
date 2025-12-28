import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

# 2. Función de procesamiento principal con DuckDB
@st.cache_data
def get_fpd_data():
    query = """
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito,
            origen2,
            sucursal
        FROM 'fpd_gemini.parquet'
    ),
    filtrado AS (
        SELECT * FROM base 
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
    )
    SELECT 
        strftime(fecha_dt, '%Y%m') as cosecha_id,
        origen2,
        sucursal,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si
    FROM filtrado
    GROUP BY 1, 2, 3
    ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

# --- TÍTULO ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Análisis de Cosechas y Comportamiento Mensual (FPD2)")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Resumen General", "🍇 Análisis de Cosechas", "🏢 Por Sucursal", "📋 Detalle de Datos"])

with tab1:
    try:
        df_raw = get_fpd_data()
        
        if not df_raw.empty:
            # --- PREPARACIÓN DE DATOS ---
            # Total por cosecha
            df_total = df_raw.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
            
            # Por origen
            df_origen = df_raw.groupby(['cosecha_id', 'origen2']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_origen['fpd2_rate'] = (df_origen['fpd2_si'] * 100.0 / df_origen['total_casos'])

            # Cosechas para comparación
            lista_cosechas = sorted(df_total['cosecha_id'].unique())
            ultima_cosecha = lista_cosechas[-1]
            cosecha_ant = lista_cosechas[-2] if len(lista_cosechas) > 1 else ultima_cosecha

            # KPIs
            k1, k2, k3 = st.columns(3)
            ult_row = df_total.iloc[-1]
            k1.metric("Cosecha Actual", ultima_cosecha)
            k2.metric("Volumen Total", f"{int(ult_row['total_casos']):,}")
            k3.metric("Tasa FPD2 Total", f"{ult_row['fpd2_rate']:.2f}%")

            st.divider()

            # --- FILA 1: LAS DOS GRÁFICAS ORIGINALES ---
            col_1, col_2 = st.columns(2)

            with col_1:
                st.subheader("Tendencia Global (FPD2)")
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(
                    x=df_total['cosecha_id'], y=df_total['fpd2_rate'],
                    mode='lines+markers+text',
                    text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                    textposition="top center",
                    line=dict(color='#1B4F72', width=4),
                    fill='tozeroy', fillcolor='rgba(27, 79, 114, 0.1)',
                    name='Tasa Total'
                ))
                fig1.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), 
                                   plot_bgcolor='white', height=400, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig1, use_container_width=True)

            with col_2:
                st.subheader("FPD2 por Origen")
                fig2 = px.line(df_origen, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True,
                               text=df_origen['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                               color_discrete_map={'fisico': '#2E86C1', 'digital': '#CB4335'})
                fig2.update_traces(textposition="top center", line=dict(width=3))
                fig2.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), 
                                   plot_bgcolor='white', height=400, margin=dict(l=10, r=10, t=30, b=10))
                st.plotly_chart(fig2, use_container_width=True)

            # --- FILA 2: NUEVA GRÁFICA DE COMPORTAMIENTO (BARRAS) ---
            st.subheader("Comportamiento Mensual FPD2")
            # Gráfica de barras para resaltar el comportamiento mes a mes
            fig3 = px.bar(
                df_total, x='cosecha_id', y='fpd2_rate',
                text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                color_discrete_sequence=['#5D6D7E']
            )
            fig3.update_traces(textposition='outside', marker_line_color='rgb(8,48,107)', marker_line_width=1.5, opacity=0.8)
            fig3.update_layout(
                xaxis=dict(type='category', title="Cosecha (YYYYMM)"),
                yaxis=dict(title="Tasa FPD2 (%)", ticksuffix="%"),
                plot_bgcolor='white', height=400, margin=dict(l=10, r=10, t=30, b=10)
            )
            st.plotly_chart(fig3, use_container_width=True)

            st.divider()

            # --- FILA 3: RANKING SUCURSALES ---
            st.subheader(f"🏆 Desempeño por Sucursal - Cosecha {ultima_cosecha}")
            
            # Obtener datos para el ranking
            query_rank = f"""
            SELECT 
                sucursal,
                COUNT(*) as total_casos,
                (SUM(CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as fpd2_rate,
                (SELECT (SUM(CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) 
                 FROM 'fpd_gemini.parquet' b 
                 WHERE b.sucursal = a.sucursal AND strftime(TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE), '%Y%m') = '{cosecha_ant}') as fpd2_rate_ant
            FROM 'fpd_gemini.parquet' a
            WHERE strftime(TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE), '%Y%m') = '{ultima_cosecha}'
            GROUP BY sucursal
            HAVING total_casos > 5
            """
            df_suc = duckdb.query(query_rank).to_df()

            if not df_suc.empty:
                col_top, col_bottom = st.columns(2)
                conf = {
                    "sucursal": "Sucursal", "total_casos": "Créditos",
                    "fpd2_rate": st.column_config.NumberColumn(f"% FPD {ultima_cosecha}", format="%.2f%%"),
                    "fpd2_rate_ant": st.column_config.NumberColumn(f"% FPD {cosecha_ant}", format="%.2f%%")
                }
                with col_top:
                    st.markdown("🔴 **Peores 10 (Mayor FPD)**")
                    st.dataframe(df_suc.sort_values('fpd2_rate', ascending=False).head(10), column_config=conf, hide_index=True, use_container_width=True)
                with col_bottom:
                    st.markdown("🟢 **Mejores 10 (Menor FPD)**")
                    st.dataframe(df_suc.sort_values('fpd2_rate', ascending=True).head(10), column_config=conf, hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Error en la visualización: {e}")

# Pestañas vacías
with tab2: pass
with tab3: pass
with tab4: pass