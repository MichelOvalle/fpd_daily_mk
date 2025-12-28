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

# 2. Funciones de Procesamiento con DuckDB
@st.cache_data
def get_main_data():
    """Obtiene la tendencia global y por origen."""
    query = """
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            id_credito,
            origen2
        FROM 'fpd_gemini.parquet'
    ),
    agregado AS (
        SELECT 
            strftime(fecha_dt, '%Y%m') as cosecha_id,
            origen2,
            COUNT(id_credito) as total_casos,
            SUM(fpd2_num) as fpd2_si
        FROM base
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
        GROUP BY 1, 2
    )
    SELECT *, (fpd2_si * 100.0 / total_casos) as fpd2_rate
    FROM agregado
    ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

@st.cache_data
def get_branch_comparison(target_cosecha, prev_cosecha):
    """Obtiene el desempeño por sucursal comparando cosecha actual vs anterior."""
    query = f"""
    WITH stats AS (
        SELECT 
            strftime(TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE), '%Y%m') as cosecha_id,
            sucursal,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num
        FROM 'fpd_gemini.parquet'
    ),
    agrupado AS (
        SELECT 
            sucursal,
            cosecha_id,
            COUNT(*) as casos,
            (SUM(fpd2_num) * 100.0 / COUNT(*)) as rate
        FROM stats
        WHERE cosecha_id IN ('{target_cosecha}', '{prev_cosecha}')
        GROUP BY 1, 2
    )
    SELECT 
        t.sucursal,
        t.casos as total_casos,
        t.rate as fpd2_rate,
        p.rate as fpd2_rate_ant
    FROM (SELECT * FROM agrupado WHERE cosecha_id = '{target_cosecha}') t
    LEFT JOIN (SELECT * FROM agrupado WHERE cosecha_id = '{prev_cosecha}') p
      ON t.sucursal = p.sucursal
    WHERE t.casos > 5
    """
    return duckdb.query(query).to_df()

# --- INTERFAZ ---
st.title("📊 FPD Daily: Monitor de Riesgo")
st.markdown("Análisis de Cosechas con Comparativa Inter-mensual")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Resumen General", "🍇 Análisis de Cosechas", "🏢 Por Sucursal", "📋 Detalle de Datos"])

with tab1:
    try:
        df_detalle = get_main_data()
        
        if not df_detalle.empty:
            # Consolidar Total
            df_total = df_detalle.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
            
            # Identificar última y penúltima cosecha
            lista_cosechas = sorted(df_total['cosecha_id'].unique())
            ultima_cosecha = lista_cosechas[-1]
            cosecha_anterior = lista_cosechas[-2] if len(lista_cosechas) > 1 else ultima_cosecha

            # --- SECCIÓN 1: KPIs ---
            ult = df_total.iloc[-1]
            k1, k2, k3 = st.columns(3)
            k1.metric("Cosecha Actual", ultima_cosecha, delta=f"Vs {cosecha_anterior}", delta_color="off")
            k2.metric("Créditos Colocados", f"{int(ult['total_casos']):,}")
            k3.metric("Tasa FPD2 Total", f"{ult['fpd2_rate']:.2f}%")

            # --- SECCIÓN 2: GRÁFICAS ---
            col_izq, col_der = st.columns(2)
            with col_izq:
                st.subheader("Tendencia Global")
                fig_total = go.Figure()
                fig_total.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['fpd2_rate'], mode='lines+markers+text',
                    text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'), textposition="top center",
                    line=dict(color='#1B4F72', width=4), fill='tozeroy', fillcolor='rgba(27,79,114,0.1)', name='Total'))
                fig_total.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=380)
                st.plotly_chart(fig_total, use_container_width=True)

            with col_der:
                st.subheader("Desglose por Origen")
                fig_orig = px.line(df_detalle, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True,
                                   text=df_detalle['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                                   color_discrete_map={'fisico': '#2E86C1', 'digital': '#CB4335'})
                fig_orig.update_traces(textposition="top center")
                fig_orig.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), plot_bgcolor='white', height=380)
                st.plotly_chart(fig_orig, use_container_width=True)

            st.divider()

            # --- SECCIÓN 3: RANKING DE SUCURSALES ---
            st.subheader(f"🏆 Desempeño por Sucursal - Cosecha {ultima_cosecha}")
            st.caption(f"Comparativo vs. Cosecha anterior ({cosecha_anterior})")
            
            df_suc = get_branch_comparison(ultima_cosecha, cosecha_anterior)

            if not df_suc.empty:
                col_top, col_bottom = st.columns(2)
                
                # Configuración de columnas para las tablas
                conf_columnas = {
                    "sucursal": "Sucursal",
                    "total_casos": "Créditos",
                    "fpd2_rate": st.column_config.NumberColumn(f"% FPD {ultima_cosecha}", format="%.2f%%"),
                    "fpd2_rate_ant": st.column_config.NumberColumn(f"% FPD {cosecha_anterior}", format="%.2f%%")
                }

                with col_top:
                    st.markdown("🔴 **Top 10: Mayor FPD (Riesgo Alto)**")
                    top_10 = df_suc.sort_values('fpd2_rate', ascending=False).head(10)
                    st.dataframe(top_10, column_config=conf_columnas, hide_index=True, use_container_width=True)

                with col_bottom:
                    st.markdown("🟢 **Bottom 10: Menor FPD (Riesgo Bajo)**")
                    bottom_10 = df_suc.sort_values('fpd2_rate', ascending=True).head(10)
                    st.dataframe(bottom_10, column_config=conf_columnas, hide_index=True, use_container_width=True)
            else:
                st.info("No hay suficientes datos comparativos para las sucursales.")

    except Exception as e:
        st.error(f"Error general: {e}")

# --- PESTAÑAS VACÍAS ---
with tab2: pass
with tab3: pass
with tab4: pass