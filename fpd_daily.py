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

# Estilo global para leyendas debajo del eje X (Tu instrucción preferida)
LEGEND_BOTTOM = dict(
    orientation="h",
    yanchor="top",
    y=-0.25,
    xanchor="center",
    x=0.5
)

# 2. Funciones de procesamiento de datos con DuckDB
@st.cache_data
def get_main_data():
    """Obtiene datos de tendencia incluyendo FPD2, NP y Tipo de Cliente."""
    query = """
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num,
            CASE WHEN NP = 'NP' THEN 1 ELSE 0 END as np_num,
            id_credito,
            origen2,
            tipo_cliente
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
        tipo_cliente,
        COUNT(id_credito) as total_casos,
        SUM(fpd2_num) as fpd2_si,
        SUM(np_num) as np_si
    FROM filtrado
    GROUP BY 1, 2, 3
    ORDER BY cosecha_id ASC
    """
    return duckdb.query(query).to_df()

@st.cache_data
def get_yoy_data():
    """Datos para comparativa interanual (3 líneas)."""
    query = """
    WITH base AS (
        SELECT 
            TRY_CAST(strptime(fecha_apertura, '%d/%m/%Y') AS DATE) as fecha_dt,
            CASE WHEN fpd2 = 'FPD' THEN 1 ELSE 0 END as fpd2_num
        FROM 'fpd_gemini.parquet'
    ),
    pre_agregado AS (
        SELECT 
            EXTRACT(YEAR FROM fecha_dt) as anio,
            strftime(fecha_dt, '%m') as mes,
            fpd2_num
        FROM base
        WHERE fecha_dt <= (CURRENT_DATE - INTERVAL 2 MONTH)
          AND fecha_dt IS NOT NULL
          AND EXTRACT(YEAR FROM fecha_dt) IN (2023, 2024, 2025)
    )
    SELECT 
        anio,
        mes,
        COUNT(*) as total_casos,
        SUM(fpd2_num) as fpd2_si,
        (SUM(fpd2_num) * 100.0 / COUNT(*)) as fpd2_rate
    FROM pre_agregado
    GROUP BY 1, 2
    ORDER BY mes ASC, anio ASC
    """
    return duckdb.query(query).to_df()

# --- INTERFAZ ---
st.title("📊 FPD Daily: Dashboard de Riesgo")
st.markdown("Análisis Multidimensional de Cartera | Cosechas **YYYYMM**")

tab1, tab2, tab3, tab4 = st.tabs(["📈 Resumen General", "🍇 Análisis de Cosechas", "🏢 Por Sucursal", "📋 Detalle de Datos"])

with tab1:
    try:
        df_raw = get_main_data()
        df_yoy = get_yoy_data()
        
        if not df_raw.empty:
            # --- AGREGACIONES ---
            df_total = df_raw.groupby('cosecha_id').agg({
                'total_casos':'sum', 
                'fpd2_si':'sum',
                'np_si':'sum'
            }).reset_index()
            df_total['fpd2_rate'] = (df_total['fpd2_si'] * 100.0 / df_total['total_casos'])
            df_total['np_rate'] = (df_total['np_si'] * 100.0 / df_total['total_casos'])
            
            df_origen = df_raw.groupby(['cosecha_id', 'origen2']).agg({'total_casos':'sum', 'fpd2_si':'sum'}).reset_index()
            df_origen['fpd2_rate'] = (df_origen['fpd2_si'] * 100.0 / df_origen['total_casos'])

            # Filtrado para la quinta gráfica (Tipo de Cliente ignorando Formers)
            df_tipo = df_raw[df_raw['tipo_cliente'] != 'Formers'].groupby(['cosecha_id', 'tipo_cliente']).agg({
                'total_casos':'sum', 'fpd2_si':'sum'
            }).reset_index()
            df_tipo['fpd2_rate'] = (df_tipo['fpd2_si'] * 100.0 / df_tipo['total_casos'])

            # Variables de tiempo
            lista_cosechas = sorted(df_total['cosecha_id'].unique())
            ultima_cosecha = lista_cosechas[-1]
            cosecha_ant = lista_cosechas[-2] if len(lista_cosechas) > 1 else ultima_cosecha

            # KPIs
            ult_row = df_total.iloc[-1]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Cosecha Actual", ultima_cosecha)
            k2.metric("Créditos", f"{int(ult_row['total_casos']):,}")
            k3.metric("Tasa FPD2", f"{ult_row['fpd2_rate']:.2f}%")
            k4.metric("Tasa NP", f"{ult_row['np_rate']:.2f}%")

            st.divider()

            # --- FILA 1: GRÁFICAS (50/50) ---
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("Tendencia Global (FPD2)")
                fig1 = go.Figure()
                fig1.add_trace(go.Scatter(
                    x=df_total['cosecha_id'], y=df_total['fpd2_rate'],
                    mode='lines+markers+text',
                    text=df_total['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                    textposition="top center",
                    line=dict(color='#1B4F72', width=4),
                    fill='tozeroy', fillcolor='rgba(27, 79, 114, 0.1)',
                    name='Global FPD2'
                ))
                fig1.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), 
                                   plot_bgcolor='white', height=380, showlegend=True, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig1, use_container_width=True)

            with c2:
                st.subheader("FPD2 por Origen")
                fig2 = px.line(df_origen, x='cosecha_id', y='fpd2_rate', color='origen2', markers=True,
                               text=df_origen['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                               color_discrete_map={'fisico': '#2E86C1', 'digital': '#CB4335'})
                fig2.update_traces(textposition="top center", line=dict(width=3))
                fig2.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%"), 
                                   plot_bgcolor='white', height=380, legend=LEGEND_BOTTOM)
                st.plotly_chart(fig2, use_container_width=True)

            # --- FILA 2: COMPARATIVAS (50/50) ---
            c3, c4 = st.columns(2)
            with c3:
                st.subheader("Comparativa Interanual (FPD2)")
                df_yoy['anio'] = df_yoy['anio'].astype(str)
                fig3 = px.line(
                    df_yoy, x='mes', y='fpd2_rate', color='anio', markers=True,
                    text=df_yoy['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                    color_discrete_map={'2023': '#BDC3C7', '2024': '#5499C7', '2025': '#1A5276'}
                )
                fig3.update_traces(textposition="top center", line=dict(width=3), marker=dict(size=8, line=dict(width=1, color='white')))
                fig3.update_layout(
                    xaxis=dict(tickmode='array', tickvals=['01','02','03','04','05','06','07','08','09','10','11','12'],
                               ticktext=['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'], showgrid=False),
                    yaxis=dict(ticksuffix="%", gridcolor='#F2F3F4'),
                    plot_bgcolor='white', height=420, legend=LEGEND_BOTTOM
                )
                st.plotly_chart(fig3, use_container_width=True)

            with c4:
                st.subheader("Correlación: % FPD2 vs % NP")
                fig4 = go.Figure()
                fig4.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['fpd2_rate'], mode='lines+markers', name='% FPD2',
                                          line=dict(color='#1B4F72', width=3), marker=dict(size=8)))
                fig4.add_trace(go.Scatter(x=df_total['cosecha_id'], y=df_total['np_rate'], mode='lines+markers', name='% NP',
                                          line=dict(color='#D35400', width=3, dash='dash'), marker=dict(size=8)))
                fig4.update_layout(xaxis=dict(type='category'), yaxis=dict(ticksuffix="%", gridcolor='#F2F3F4'),
                                   plot_bgcolor='white', height=420, legend=LEGEND_BOTTOM, hovermode="x unified")
                st.plotly_chart(fig4, use_container_width=True)

            # --- FILA 3: TIPO DE CLIENTE (ANCHO COMPLETO) ---
            st.subheader("Tendencia FPD2 por Tipo de Cliente (Excluyendo Formers)")
            fig5 = px.line(
                df_tipo, x='cosecha_id', y='fpd2_rate', color='tipo_cliente', markers=True,
                text=df_tipo['fpd2_rate'].apply(lambda x: f'{x:.1f}%'),
                labels={'cosecha_id': 'Cosecha', 'fpd2_rate': '% FPD2', 'tipo_cliente': 'Cliente'},
                color_discrete_map={'Nuevo': '#7D3C98', 'Renovacion': '#27AE60'} # Púrpura y Verde
            )
            fig5.update_traces(textposition="top center", line=dict(width=4))
            fig5.update_layout(
                xaxis=dict(type='category'), yaxis=dict(ticksuffix="%", gridcolor='#F2F3F4'),
                plot_bgcolor='white', height=450, legend=LEGEND_BOTTOM
            )
            st.plotly_chart(fig5, use_container_width=True)

            st.divider()

            # --- SECCIÓN 4: RANKING SUCURSALES ---
            st.subheader(f"🏆 Rankings de Sucursales - Cosecha {ultima_cosecha}")
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
                    st.markdown("🔴 **Top 10: Mayor FPD (Riesgo)**")
                    st.dataframe(df_suc.sort_values('fpd2_rate', ascending=False).head(10), column_config=conf, hide_index=True, use_container_width=True)
                with col_bottom:
                    st.markdown("🟢 **Bottom 10: Menor FPD (Sano)**")
                    st.dataframe(df_suc.sort_values('fpd2_rate', ascending=True).head(10), column_config=conf, hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Error en el dashboard: {e}")

# Pestañas restantes vacías
with tab2: pass
with tab3: pass
with tab4: pass