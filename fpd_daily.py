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

tab1, tab2, tab3, tab4 = st.tabs(["📈 Resumen General", "🍇 Análisis de Cosechas", "🏢 Por Sucursal", "📋 Detalle de Datos"])

with tab1:
    try:
        df_detalle = get_main_data()
        
        if not df_detalle.empty:
            # Consolidar datos para la gráfica de comportamiento total
            df_total = df_detalle.groupby('cosecha_id').agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
            
            # Identificar última y penúltima cosecha
            lista_cosechas = sorted(df_total['cosecha_id'].unique())
            ultima_cosecha = lista_cosechas[-1]
            cosecha_anterior = lista_cosechas[-2] if len(lista_cosechas) > 1 else ultima_cosecha

            # KPIs
            k1, k2, k3 = st.columns(3)
            ult = df_total.iloc[-1]
            k1.metric("Cosecha Actual", ultima_cosecha)
            k2.metric("Volumen Colocado", f"{int(ult['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ult['fpd2_rate']:.2f}%")

            st.markdown("### Comportamiento Mensual: Volumen vs. Riesgo")
            
            # --- GRÁFICA DE COMPORTAMIENTO (DOBLE EJE) ---
            fig_behavior = make_subplots(specs=[[{"secondary_y": True}]])

            # Barras para Volumen
            fig_behavior.add_trace(
                go.Bar(x=df_total['cosecha_id'], y=df_total['total_casos'], name="Créditos Colocados",
                       marker_color='rgba(200, 200, 200, 0.5)', hovertemplate='%{y:,.0f} créditos'),
                secondary_y=False
            )

            # Línea para Tasa FPD2
            fig_behavior.add_trace(
                go.Scatter(x=df_total['cosecha_id'], y=df_total['fpd2_rate'], name="Tasa FPD2 (%)",
                           mode='lines+markers+text', text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                           textposition="top center", line=dict(color='#1B4F72', width=4),
                           marker=dict(size=8), hovertemplate='%{y:.2f}%'),
                secondary_y=True
            )

            fig_behavior.update_layout(
                hovermode="x unified", plot_bgcolor='white', height=500,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(type='category', title="Cosecha (YYYYMM)", showgrid=False)
            )

            fig_behavior.update_yaxes(title_text="Cantidad de Créditos", secondary_y=False, showgrid=False)
            fig_behavior.update_yaxes(title_text="Tasa de Incumplimiento (%)", secondary_y=True, ticksuffix="%", 
                                      range=[0, df_total['fpd2_rate'].max() * 1.5], showgrid=True, gridcolor='#F0F0F0')

            st.plotly_chart(fig_behavior, use_container_width=True)

            st.divider()

            # --- RANKING DE SUCURSALES ---
            st.subheader(f"🏆 Desempeño por Sucursal - Cosecha {ultima_cosecha}")
            df_suc = get_branch_comparison(ultima_cosecha, cosecha_anterior)

            if not df_suc.empty:
                col_top, col_bottom = st.columns(2)
                conf_cols = {
                    "sucursal": "Sucursal", "total_casos": "Créditos",
                    "fpd2_rate": st.column_config.NumberColumn(f"% FPD {ultima_cosecha}", format="%.2f%%"),
                    "fpd2_rate_ant": st.column_config.NumberColumn(f"% FPD {cosecha_anterior}", format="%.2f%%")
                }

                with col_top:
                    st.markdown("🔴 **Peores 10 (Mayor FPD)**")
                    st.dataframe(df_suc.sort_values('fpd2_rate', ascending=False).head(10), 
                                 column_config=conf_cols, hide_index=True, use_container_width=True)

                with col_bottom:
                    st.markdown("🟢 **Mejores 10 (Menor FPD)**")
                    st.dataframe(df_suc.sort_values('fpd2_rate', ascending=True).head(10), 
                                 column_config=conf_cols, hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Error en la visualización: {e}")

# Pestañas vacías
with tab2: pass
with tab3: pass
with tab4: pass